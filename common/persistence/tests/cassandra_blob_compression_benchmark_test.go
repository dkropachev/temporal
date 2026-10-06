package tests

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
	"go.uber.org/zap/zaptest"
)

func BenchmarkCassandraBlobCompressionShardStore(b *testing.B) {
	testData, tearDown := setUpCassandraBenchmark(b, true)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	shardID := int32(33001)
	shardInfo := persistence.NewDataBlob(
		bytes.Repeat([]byte("compressible-shard-info-"), 16*1024),
		enumspb.ENCODING_TYPE_PROTO3.String(),
	)
	_, err = shardStore.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID:          shardID,
		LifecycleContext: ctx,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 1, shardInfo, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	rangeID := int64(1)
	b.Run("write", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(shardInfo.Data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			nextRangeID := rangeID + 1
			err := shardStore.UpdateShard(ctx, &persistence.InternalUpdateShardRequest{
				ShardID:         shardID,
				RangeID:         nextRangeID,
				ShardInfo:       shardInfo,
				PreviousRangeID: rangeID,
			})
			if err != nil {
				b.Fatal(err)
			}
			rangeID = nextRangeID
		}
	})

	b.Run("read", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(shardInfo.Data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := shardStore.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{
				ShardID:          shardID,
				LifecycleContext: ctx,
			})
			if err != nil {
				b.Fatal(err)
			}
			if len(resp.ShardInfo.Data) != len(shardInfo.Data) {
				b.Fatalf("shard data size mismatch: %d", len(resp.ShardInfo.Data))
			}
		}
	})
}

func setUpCassandraBenchmark(b *testing.B, blobCompressionEnabled bool) (CassandraTestData, func()) {
	b.Helper()

	var testData CassandraTestData
	testData.Cfg = NewCassandraConfig()
	setCassandraBlobCompressionEnabledForBenchmark(testData.Cfg, blobCompressionEnabled)
	testData.Logger = log.NewZapLogger(zaptest.NewLogger(b))
	SetUpCassandraDatabase(b, testData.Cfg, testData.Logger)
	SetUpCassandraSchema(b, testData.Cfg, testData.Logger)

	testData.Factory = cassandra.NewFactory(
		*testData.Cfg,
		resolver.NewNoopResolver(),
		testCassandraClusterName,
		testData.Logger,
		metrics.NoopMetricsHandler,
		serialization.NewSerializer(),
	)

	tearDown := func() {
		testData.Factory.Close()
		TearDownCassandraKeyspace(b, testData.Cfg)
	}

	return testData, tearDown
}

func setCassandraBlobCompressionEnabledForBenchmark(cfg any, enabled bool) {
	field := reflect.ValueOf(cfg).Elem().FieldByName("BlobCompressionEnabled")
	if !field.IsValid() || !field.CanSet() {
		return
	}
	field.Set(reflect.ValueOf(func() bool { return enabled }))
}
