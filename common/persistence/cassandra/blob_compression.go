package cassandra

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/klauspost/compress/zstd"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/dynamicconfig"
	p "go.temporal.io/server/common/persistence"
)

const (
	blobCompressionMagic      = "\x89TCBLOB\n"
	blobCompressionVersion    = byte(1)
	blobCompressionCodecZstd  = byte(1)
	blobCompressionHeaderSize = len(blobCompressionMagic) + 1 + 1 + 4
)

var (
	blobCompressionMagicBytes = []byte(blobCompressionMagic)
	defaultBlobCompressor     = mustNewBlobCompressor(dynamicconfig.GetBoolPropertyFn(false))
)

type blobCompressor struct {
	enabled dynamicconfig.BoolPropertyFn
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func newBlobCompressor(enabled dynamicconfig.BoolPropertyFn) (*blobCompressor, error) {
	if enabled == nil {
		enabled = dynamicconfig.GetBoolPropertyFn(false)
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		_ = encoder.Close()
		return nil, err
	}

	return &blobCompressor{
		enabled: enabled,
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func mustNewBlobCompressor(enabled dynamicconfig.BoolPropertyFn) *blobCompressor {
	compressor, err := newBlobCompressor(enabled)
	if err != nil {
		panic(err) // nolint:forbidigo // static zstd options failed during process initialization
	}
	return compressor
}

func selectBlobCompressor(compressors []*blobCompressor) *blobCompressor {
	if len(compressors) > 0 && compressors[0] != nil {
		return compressors[0]
	}
	return defaultBlobCompressor
}

func (c *blobCompressor) close() {
	if c == nil {
		return
	}
	_ = c.encoder.Close()
	c.decoder.Close()
}

func (c *blobCompressor) compressBlob(blob *commonpb.DataBlob) ([]byte, string, error) {
	if blob == nil {
		return nil, "", nil
	}
	data, err := c.compressData(blob.Data)
	if err != nil {
		return nil, "", err
	}
	return data, blob.EncodingType.String(), nil
}

func (c *blobCompressor) compressData(data []byte) ([]byte, error) {
	if c == nil {
		c = defaultBlobCompressor
	}
	if !c.enabled() || len(data) == 0 || isCompressedBlobData(data) {
		return data, nil
	}
	if len(data) > math.MaxUint32 {
		return nil, fmt.Errorf("cassandra blob compression input too large: %d bytes", len(data))
	}

	compressed := c.encoder.EncodeAll(data, nil)
	result := make([]byte, blobCompressionHeaderSize, blobCompressionHeaderSize+len(compressed))
	copy(result, blobCompressionMagicBytes)
	result[len(blobCompressionMagic)] = blobCompressionVersion
	result[len(blobCompressionMagic)+1] = blobCompressionCodecZstd
	binary.BigEndian.PutUint32(result[len(blobCompressionMagic)+2:], uint32(len(data)))
	result = append(result, compressed...)
	return result, nil
}

func (c *blobCompressor) newDataBlob(data []byte, encoding string) (*commonpb.DataBlob, error) {
	data, err := c.decompressData(data)
	if err != nil {
		return nil, err
	}
	return p.NewDataBlob(data, encoding), nil
}

func (c *blobCompressor) decompressData(data []byte) ([]byte, error) {
	if c == nil {
		c = defaultBlobCompressor
	}
	if !isCompressedBlobData(data) {
		return data, nil
	}
	if len(data) < blobCompressionHeaderSize {
		return nil, fmt.Errorf("cassandra compressed blob header is truncated: %d bytes", len(data))
	}

	version := data[len(blobCompressionMagic)]
	if version != blobCompressionVersion {
		return nil, fmt.Errorf("unsupported cassandra compressed blob version: %d", version)
	}
	codec := data[len(blobCompressionMagic)+1]
	if codec != blobCompressionCodecZstd {
		return nil, fmt.Errorf("unsupported cassandra compressed blob codec: %d", codec)
	}

	uncompressedSize := int(binary.BigEndian.Uint32(data[len(blobCompressionMagic)+2:]))
	output, err := c.decoder.DecodeAll(data[blobCompressionHeaderSize:], make([]byte, 0, uncompressedSize))
	if err != nil {
		return nil, fmt.Errorf("decompress cassandra blob: %w", err)
	}
	if len(output) != uncompressedSize {
		return nil, fmt.Errorf("decompress cassandra blob: size mismatch, expected %d got %d", uncompressedSize, len(output))
	}
	return output, nil
}

func isCompressedBlobData(data []byte) bool {
	return bytes.HasPrefix(data, blobCompressionMagicBytes)
}
