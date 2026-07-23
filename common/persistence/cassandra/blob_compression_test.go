package cassandra

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	p "go.temporal.io/server/common/persistence"
)

func TestBlobCompressorDisabledWritesPlainData(t *testing.T) {
	compressor, err := newBlobCompressor(dynamicconfig.GetBoolPropertyFn(false))
	require.NoError(t, err)
	defer compressor.close()

	blob := p.NewDataBlob([]byte("plain-data"), enumspb.ENCODING_TYPE_PROTO3.String())
	data, encoding, err := compressor.compressBlob(blob)
	require.NoError(t, err)

	require.Equal(t, blob.Data, data)
	require.Equal(t, enumspb.ENCODING_TYPE_PROTO3.String(), encoding)
	require.False(t, isCompressedBlobData(data))
}

func TestBlobCompressorEnabledRoundTrip(t *testing.T) {
	compressor, err := newBlobCompressor(dynamicconfig.GetBoolPropertyFn(true))
	require.NoError(t, err)
	defer compressor.close()

	blob := p.NewDataBlob([]byte("compressible-data-compressible-data-compressible-data"), enumspb.ENCODING_TYPE_PROTO3.String())
	data, encoding, err := compressor.compressBlob(blob)
	require.NoError(t, err)

	require.True(t, isCompressedBlobData(data))
	require.Equal(t, enumspb.ENCODING_TYPE_PROTO3.String(), encoding)

	decoded, err := compressor.newDataBlob(data, encoding)
	require.NoError(t, err)
	require.Equal(t, blob, decoded)
}

func TestBlobCompressorReadsLegacyPlainData(t *testing.T) {
	compressor, err := newBlobCompressor(dynamicconfig.GetBoolPropertyFn(true))
	require.NoError(t, err)
	defer compressor.close()

	plain := []byte("legacy-plain-data")
	decoded, err := compressor.decompressData(plain)
	require.NoError(t, err)
	require.Equal(t, plain, decoded)
}

func TestBlobCompressorRejectsCorruptCompressedData(t *testing.T) {
	compressor, err := newBlobCompressor(dynamicconfig.GetBoolPropertyFn(false))
	require.NoError(t, err)
	defer compressor.close()

	data := append([]byte(blobCompressionMagic), blobCompressionVersion, blobCompressionCodecZstd, 0, 0, 0, 1)
	data = append(data, "not-zstd"...)

	_, err = compressor.decompressData(data)
	require.Error(t, err)
}

func BenchmarkBlobCompressorWrite(b *testing.B) {
	payload := bytes.Repeat([]byte("history-event-payload-"), 4096)
	blob := p.NewDataBlob(payload, enumspb.ENCODING_TYPE_PROTO3.String())

	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "disabled", enabled: false},
		{name: "enabled", enabled: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			compressor, err := newBlobCompressor(dynamicconfig.GetBoolPropertyFn(tc.enabled))
			require.NoError(b, err)
			defer compressor.close()

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, _, err := compressor.compressBlob(blob)
				if err != nil {
					b.Fatal(err)
				}
				if len(data) == 0 {
					b.Fatal("empty compressed payload")
				}
			}
		})
	}
}

func BenchmarkBlobCompressorRead(b *testing.B) {
	payload := bytes.Repeat([]byte("history-event-payload-"), 4096)
	compressor, err := newBlobCompressor(dynamicconfig.GetBoolPropertyFn(true))
	require.NoError(b, err)
	defer compressor.close()

	compressed, err := compressor.compressData(payload)
	require.NoError(b, err)

	b.ReportMetric(float64(len(compressed))/float64(len(payload)), "compressed/original")

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "plain", data: payload},
		{name: "compressed", data: compressed},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := compressor.decompressData(tc.data)
				if err != nil {
					b.Fatal(err)
				}
				if len(data) != len(payload) {
					b.Fatalf("decoded size mismatch: %d", len(data))
				}
			}
		})
	}
}
