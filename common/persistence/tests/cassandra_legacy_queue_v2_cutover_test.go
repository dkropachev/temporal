//go:build integration

package tests

import (
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func TestCassandraLegacyQueueV2ConcurrentCutoverAndTargetOnlySourceDropped(t *testing.T) {
	const bucketSize = int64(4)
	testData, tearDown := setUpCassandraTest(t)
	t.Cleanup(tearDown)
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	t.Cleanup(session.Close)
	blob := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("concurrent-cutover"),
	}
	queueTypes := []p.QueueType{
		p.NamespaceReplicationQueueType,
		-p.NamespaceReplicationQueueType,
	}
	options := cassandra.LegacyQueueV2MigrationOptions{
		PageSize:          2,
		MessageBucketSize: bucketSize,
		MaxMismatches:     20,
	}

	source, err := cassandra.NewQueueStore(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
	)
	require.NoError(t, err)
	require.NoError(t, source.Init(t.Context(), blob))
	for _, queueType := range queueTypes {
		require.NoError(t, cassandra.InitializeLegacyQueueV2SourceAuthority(
			t.Context(),
			session,
			queueType,
			bucketSize,
		))
	}
	sourceDual, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeSourceDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, sourceDual.Init(t.Context(), blob))
	for range 10 {
		require.NoError(t, sourceDual.EnqueueMessage(t.Context(), blob))
		_, err := sourceDual.EnqueueMessageToDLQ(t.Context(), blob)
		require.NoError(t, err)
	}
	for _, queueType := range queueTypes {
		_, err := cassandra.BackfillLegacyQueueV2(t.Context(), session, queueType, options)
		require.NoError(t, err)
	}

	blocking := &blockOnceLegacyQueueSourceScanSession{
		Session: session,
		blocked: make(chan struct{}),
		release: make(chan struct{}),
	}
	type cutoverResult struct {
		result cassandra.LegacyQueueV2CutoverResult
		err    error
	}
	firstDone := make(chan cutoverResult, 1)
	go func() {
		result, err := cassandra.CutoverLegacyQueueV2(
			t.Context(),
			blocking,
			p.NamespaceReplicationQueueType,
			options,
		)
		firstDone <- cutoverResult{result: result, err: err}
	}()
	select {
	case <-blocking.blocked:
	case <-time.After(20 * time.Second):
		t.Fatal("first legacy queue cutover did not reach its fenced source scan")
	}

	second, err := cassandra.CutoverLegacyQueueV2(
		t.Context(),
		session,
		p.NamespaceReplicationQueueType,
		options,
	)
	require.NoError(t, err)
	require.True(t, second.Validation.Matches(), second.Validation)
	targetDual, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, targetDual.Init(t.Context(), blob))
	require.NoError(t, targetDual.EnqueueMessage(t.Context(), blob))
	require.NoError(t, targetDual.DeleteMessagesBefore(t.Context(), 3))

	close(blocking.release)
	first := <-firstDone
	require.Error(t, first.err)
	require.ErrorContains(t, first.err, "requires authority sealing")
	messages, err := targetDual.ReadMessages(t.Context(), p.EmptyQueueMessageID, 20)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4, 5, 6, 7, 8, 9, 10}, legacyQueueMessageIDs(messages))
	require.Equal(
		t,
		[]int64{3, 4, 5, 6, 7, 8, 9, 10},
		legacyQueueSourceMessageIDs(t, session, p.NamespaceReplicationQueueType),
	)

	retry, err := cassandra.CutoverLegacyQueueV2(
		t.Context(),
		session,
		p.NamespaceReplicationQueueType,
		options,
	)
	require.NoError(t, err)
	require.True(t, retry.AlreadyTarget)
	_, err = cassandra.CutoverLegacyQueueV2(
		t.Context(),
		session,
		-p.NamespaceReplicationQueueType,
		options,
	)
	require.NoError(t, err)

	require.NoError(t, session.Query("DROP TABLE queue").Exec())
	targetOnly, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetOnly,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, targetOnly.Init(t.Context(), blob))
	require.NoError(t, targetOnly.EnqueueMessage(t.Context(), blob))
	messages, err = targetOnly.ReadMessages(t.Context(), p.EmptyQueueMessageID, 20)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4, 5, 6, 7, 8, 9, 10, 11}, legacyQueueMessageIDs(messages))
}

type blockOnceLegacyQueueSourceScanSession struct {
	cgocql.Session
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockOnceLegacyQueueSourceScanSession) Query(statement string, args ...any) cgocql.Query {
	if strings.Contains(statement, "SELECT message_id, message_payload, message_encoding") &&
		strings.Contains(statement, "FROM queue WHERE queue_type") {
		s.once.Do(func() {
			close(s.blocked)
			<-s.release
		})
	}
	return s.Session.Query(statement, args...)
}

func TestCassandraLegacyQueueV2FencedCutover(t *testing.T) {
	const bucketSize = int64(4)
	testData, tearDown := setUpCassandraTest(t)
	t.Cleanup(tearDown)
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	t.Cleanup(session.Close)
	blob := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("message"),
	}

	source, err := cassandra.NewQueueStore(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
	)
	require.NoError(t, err)
	require.NoError(t, source.Init(t.Context(), blob))
	for range 3 {
		require.NoError(t, source.EnqueueMessage(t.Context(), blob))
	}
	for _, queueType := range []p.QueueType{
		p.NamespaceReplicationQueueType,
		-p.NamespaceReplicationQueueType,
	} {
		require.NoError(t, cassandra.InitializeLegacyQueueV2SourceAuthority(
			t.Context(),
			session,
			queueType,
			bucketSize,
		))
	}

	sourceDual, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeSourceDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, sourceDual.Init(t.Context(), blob))
	require.NoError(t, sourceDual.EnqueueMessage(t.Context(), blob))
	for _, queueType := range []p.QueueType{
		p.NamespaceReplicationQueueType,
		-p.NamespaceReplicationQueueType,
	} {
		_, err := cassandra.BackfillLegacyQueueV2(
			t.Context(),
			session,
			queueType,
			cassandra.LegacyQueueV2MigrationOptions{
				PageSize:          16,
				MessageBucketSize: bucketSize,
				MaxMismatches:     20,
			},
		)
		require.NoError(t, err)
	}

	targetDual, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, targetDual.Init(t.Context(), blob))
	require.NoError(t, targetDual.EnqueueMessage(t.Context(), blob))
	require.NoError(t, session.Query(
		`INSERT INTO legacy_queue_v2_messages
			(queue_type, bucket_id, row_type, message_id, message_payload, message_encoding)
			VALUES (?, ?, ?, ?, ?, ?)`,
		p.NamespaceReplicationQueueType,
		int64(1),
		int16(1),
		int64(5),
		[]byte("target-extra"),
		enumspb.ENCODING_TYPE_PROTO3.String(),
	).Exec())

	applied, err := session.Query(
		`UPDATE queue SET migration_authority = ? WHERE queue_type = ? AND message_id = ?
			IF migration_authority = ? AND message_bucket_size = ?`,
		2,
		p.NamespaceReplicationQueueType,
		int64(math.MinInt64),
		1,
		bucketSize,
	).MapScanCAS(make(map[string]any))
	require.NoError(t, err)
	require.True(t, applied)
	require.Error(t, sourceDual.EnqueueMessage(t.Context(), blob))
	require.Error(t, targetDual.EnqueueMessage(t.Context(), blob))

	options := cassandra.LegacyQueueV2MigrationOptions{
		PageSize:                  16,
		MessageBucketSize:         bucketSize,
		MaxMismatches:             20,
		ConfirmQueueTrafficFenced: true,
	}
	result, err := cassandra.CutoverLegacyQueueV2(
		t.Context(),
		session,
		p.NamespaceReplicationQueueType,
		options,
	)
	require.NoError(t, err)
	require.False(t, result.AlreadyTarget)
	require.True(t, result.Validation.Matches(), result.Validation)
	applied, err = session.Query(
		`UPDATE queue SET migration_authority = ? WHERE queue_type = ? AND message_id = ?
			IF migration_authority = ? AND message_bucket_size = ?`,
		2,
		-p.NamespaceReplicationQueueType,
		int64(math.MinInt64),
		1,
		bucketSize,
	).MapScanCAS(make(map[string]any))
	require.NoError(t, err)
	require.True(t, applied)
	applied, err = session.Query(
		`UPDATE legacy_queue_v2_state SET migration_authority = ? WHERE queue_type = ?
			IF migration_authority = ? AND message_bucket_size = ?`,
		3,
		-p.NamespaceReplicationQueueType,
		1,
		bucketSize,
	).MapScanCAS(make(map[string]any))
	require.NoError(t, err)
	require.True(t, applied)
	_, err = cassandra.CutoverLegacyQueueV2(
		t.Context(),
		session,
		-p.NamespaceReplicationQueueType,
		options,
	)
	require.NoError(t, err)

	require.Error(t, sourceDual.EnqueueMessage(t.Context(), blob))
	require.NoError(t, targetDual.EnqueueMessage(t.Context(), blob))
	var sourceLastMessageID int64
	require.NoError(t, session.Query(
		`SELECT message_id FROM queue WHERE queue_type = ? AND message_id >= ? ORDER BY message_id DESC LIMIT 1`,
		p.NamespaceReplicationQueueType,
		int64(0),
	).Scan(&sourceLastMessageID))
	require.Equal(t, int64(5), sourceLastMessageID)

	require.NoError(t, session.Query("DROP TABLE queue").Exec())
	targetOnly, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetOnly,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, targetOnly.Init(t.Context(), blob))
	require.NoError(t, targetOnly.EnqueueMessage(t.Context(), blob))
	messages, err := targetOnly.ReadMessages(t.Context(), p.EmptyQueueMessageID, 20)
	require.NoError(t, err)
	require.Len(t, messages, 7)
}

func TestCassandraLegacyQueueV2TargetDualMaintainsSourceCopyAndQuiescentRepair(t *testing.T) {
	const bucketSize = int64(4)
	testData, tearDown := setUpCassandraTest(t)
	t.Cleanup(tearDown)
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	t.Cleanup(session.Close)
	blob := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("target-dual-message"),
	}
	queueTypes := []p.QueueType{
		p.NamespaceReplicationQueueType,
		-p.NamespaceReplicationQueueType,
	}
	options := cassandra.LegacyQueueV2MigrationOptions{
		PageSize:                  16,
		MessageBucketSize:         bucketSize,
		MaxMismatches:             20,
		ConfirmQueueTrafficFenced: true,
	}

	source, err := cassandra.NewQueueStore(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
	)
	require.NoError(t, err)
	require.NoError(t, source.Init(t.Context(), blob))
	for _, queueType := range queueTypes {
		require.NoError(t, cassandra.InitializeLegacyQueueV2SourceAuthority(
			t.Context(),
			session,
			queueType,
			bucketSize,
		))
	}
	sourceDual, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeSourceDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, sourceDual.Init(t.Context(), blob))
	for expectedID := int64(0); expectedID < 6; expectedID++ {
		require.NoError(t, sourceDual.EnqueueMessage(t.Context(), blob))
		messageID, err := sourceDual.EnqueueMessageToDLQ(t.Context(), blob)
		require.NoError(t, err)
		require.Equal(t, expectedID, messageID)
	}
	for _, queueType := range queueTypes {
		_, err := cassandra.BackfillLegacyQueueV2(t.Context(), session, queueType, options)
		require.NoError(t, err)
		_, err = cassandra.CutoverLegacyQueueV2(t.Context(), session, queueType, options)
		require.NoError(t, err)
	}

	targetDual, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, targetDual.Init(t.Context(), blob))
	require.NoError(t, targetDual.EnqueueMessage(t.Context(), blob))
	dlqMessageID, err := targetDual.EnqueueMessageToDLQ(t.Context(), blob)
	require.NoError(t, err)
	require.Equal(t, int64(6), dlqMessageID)

	require.NoError(t, targetDual.DeleteMessagesBefore(t.Context(), 3))
	require.NoError(t, targetDual.DeleteMessageFromDLQ(t.Context(), 2))
	require.NoError(t, targetDual.RangeDeleteMessagesFromDLQ(t.Context(), 2, 4))
	require.NoError(t, targetDual.DeleteMessagesBefore(t.Context(), 3))
	require.NoError(t, targetDual.DeleteMessageFromDLQ(t.Context(), 2))
	require.NoError(t, targetDual.RangeDeleteMessagesFromDLQ(t.Context(), 2, 4))

	messages, err := targetDual.ReadMessages(t.Context(), p.EmptyQueueMessageID, 20)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4, 5, 6}, legacyQueueSourceMessageIDs(t, session, p.NamespaceReplicationQueueType))
	require.Equal(t, []int64{3, 4, 5, 6}, legacyQueueMessageIDs(messages))
	dlqMessages, _, err := targetDual.ReadMessagesFromDLQ(t.Context(), p.EmptyQueueMessageID, 20, 20, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 5, 6}, legacyQueueSourceMessageIDs(t, session, -p.NamespaceReplicationQueueType))
	require.Equal(t, []int64{0, 1, 5, 6}, legacyQueueMessageIDs(dlqMessages))
	for _, queueType := range queueTypes {
		validation, err := cassandra.ValidateLegacyQueueV2(t.Context(), session, queueType, options)
		require.NoError(t, err)
		require.True(t, validation.Matches(), validation)
	}

	require.NoError(t, session.Query(
		`DELETE FROM queue WHERE queue_type = ? AND message_id = ?`,
		p.NamespaceReplicationQueueType,
		int64(6),
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO queue (queue_type, message_id, message_payload, message_encoding) VALUES (?, ?, ?, ?)`,
		-p.NamespaceReplicationQueueType,
		int64(2),
		blob.Data,
		blob.EncodingType.String(),
	).Exec())
	for _, queueType := range queueTypes {
		validation, err := cassandra.ReconcileSourceLegacyQueueFromV2(
			t.Context(),
			session,
			queueType,
			options,
		)
		require.NoError(t, err)
		require.True(t, validation.Matches(), validation)
	}
	require.Equal(t, []int64{3, 4, 5, 6}, legacyQueueSourceMessageIDs(t, session, p.NamespaceReplicationQueueType))
	require.Equal(t, []int64{0, 1, 5, 6}, legacyQueueSourceMessageIDs(t, session, -p.NamespaceReplicationQueueType))
}

func TestCassandraLegacyQueueV2FirstTargetAuthoritativeRolloverPersistsIdentity(t *testing.T) {
	const bucketSize = int64(4)
	session, targetDual, blob, _ := setUpCassandraLegacyQueueV2TargetDual(t, bucketSize, 4)

	var absentAuthority int
	err := session.Query(
		`SELECT migration_authority FROM legacy_queue_v2_messages
			WHERE queue_type = ? AND bucket_id = ? LIMIT 1`,
		p.NamespaceReplicationQueueType,
		int64(1),
	).Scan(&absentAuthority)
	require.True(t, cgocql.IsNotFoundError(err), err)

	require.NoError(t, targetDual.EnqueueMessage(t.Context(), blob))

	var (
		sourceAuthority  int
		sourceGeneration gocql.UUID
		sourceBucketSize int64
	)
	require.NoError(t, session.Query(
		`SELECT migration_authority, migration_generation, message_bucket_size FROM queue
			WHERE queue_type = ? AND message_id = ?`,
		p.NamespaceReplicationQueueType,
		int64(math.MinInt64),
	).Scan(&sourceAuthority, &sourceGeneration, &sourceBucketSize))

	var (
		targetAuthority  int
		targetGeneration gocql.UUID
		targetBucketSize int64
		activeBucket     int64
	)
	require.NoError(t, session.Query(
		`SELECT migration_authority, migration_generation, message_bucket_size, active_bucket
			FROM legacy_queue_v2_state WHERE queue_type = ?`,
		p.NamespaceReplicationQueueType,
	).Scan(&targetAuthority, &targetGeneration, &targetBucketSize, &activeBucket))

	var (
		bucketAuthority  int
		bucketGeneration gocql.UUID
		bucketSizeValue  int64
		lastMessageID    int64
	)
	require.NoError(t, session.Query(
		`SELECT migration_authority, migration_generation, message_bucket_size, last_message_id
			FROM legacy_queue_v2_messages WHERE queue_type = ? AND bucket_id = ?
			AND row_type = ? AND message_id = ?`,
		p.NamespaceReplicationQueueType,
		int64(1),
		int16(0),
		int64(-1),
	).Scan(&bucketAuthority, &bucketGeneration, &bucketSizeValue, &lastMessageID))

	require.Equal(t, 3, sourceAuthority)
	require.Equal(t, sourceAuthority, targetAuthority)
	require.Equal(t, sourceAuthority, bucketAuthority)
	require.Equal(t, sourceGeneration, targetGeneration)
	require.Equal(t, sourceGeneration, bucketGeneration)
	require.Equal(t, bucketSize, sourceBucketSize)
	require.Equal(t, bucketSize, targetBucketSize)
	require.Equal(t, bucketSize, bucketSizeValue)
	require.Equal(t, int64(1), activeBucket)
	require.Equal(t, int64(4), lastMessageID)

	messages, err := targetDual.ReadMessages(t.Context(), p.EmptyQueueMessageID, 10)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4}, legacyQueueMessageIDs(messages))
	require.Equal(
		t,
		[]int64{0, 1, 2, 3, 4},
		legacyQueueSourceMessageIDs(t, session, p.NamespaceReplicationQueueType),
	)
}

func TestCassandraLegacyQueueV2TargetDualMirrorFailuresResumeExactly(t *testing.T) {
	const bucketSize = int64(4)
	session, targetDual, blob, options := setUpCassandraLegacyQueueV2TargetDual(t, bucketSize, 6)

	failedEnqueueStore, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		&failNthLegacyQueueBatchSession{Session: session, nth: 2},
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, failedEnqueueStore.Init(t.Context(), blob))
	require.ErrorContains(t, failedEnqueueStore.EnqueueMessage(t.Context(), blob), "injected legacy queue mirror crash")

	messages, err := targetDual.ReadMessages(t.Context(), p.EmptyQueueMessageID, 20)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6}, legacyQueueMessageIDs(messages))
	require.Equal(
		t,
		[]int64{0, 1, 2, 3, 4, 5},
		legacyQueueSourceMessageIDs(t, session, p.NamespaceReplicationQueueType),
	)

	failedPrefixStore, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		&failNthLegacyQueueBatchSession{Session: session, nth: 1},
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, failedPrefixStore.Init(t.Context(), blob))
	require.ErrorContains(t, failedPrefixStore.DeleteMessagesBefore(t.Context(), 3), "injected legacy queue mirror crash")
	require.Equal(
		t,
		[]int64{0, 1, 2, 3, 4, 5},
		legacyQueueSourceMessageIDs(t, session, p.NamespaceReplicationQueueType),
	)
	require.NoError(t, targetDual.DeleteMessagesBefore(t.Context(), 3))

	failedSingleDeleteStore, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		&failNthLegacyQueueBatchSession{Session: session, nth: 1},
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, failedSingleDeleteStore.Init(t.Context(), blob))
	require.ErrorContains(t, failedSingleDeleteStore.DeleteMessageFromDLQ(t.Context(), 2), "injected legacy queue mirror crash")
	require.Equal(
		t,
		[]int64{0, 1, 2, 3, 4, 5},
		legacyQueueSourceMessageIDs(t, session, -p.NamespaceReplicationQueueType),
	)
	require.NoError(t, targetDual.DeleteMessageFromDLQ(t.Context(), 2))

	failedRangeDeleteStore, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		&failNthLegacyQueueBatchSession{Session: session, nth: 1},
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, failedRangeDeleteStore.Init(t.Context(), blob))
	require.ErrorContains(
		t,
		failedRangeDeleteStore.RangeDeleteMessagesFromDLQ(t.Context(), 2, 4),
		"injected legacy queue mirror crash",
	)
	require.Equal(
		t,
		[]int64{0, 1, 3, 4, 5},
		legacyQueueSourceMessageIDs(t, session, -p.NamespaceReplicationQueueType),
	)
	require.NoError(t, targetDual.RangeDeleteMessagesFromDLQ(t.Context(), 2, 4))

	validation, err := cassandra.ValidateLegacyQueueV2(
		t.Context(), session, p.NamespaceReplicationQueueType, options,
	)
	require.NoError(t, err)
	require.False(t, validation.Matches())
	for _, queueType := range []p.QueueType{
		p.NamespaceReplicationQueueType,
		-p.NamespaceReplicationQueueType,
	} {
		validation, err = cassandra.ReconcileSourceLegacyQueueFromV2(t.Context(), session, queueType, options)
		require.NoError(t, err)
		require.True(t, validation.Matches(), validation)
	}

	messages, err = targetDual.ReadMessages(t.Context(), p.EmptyQueueMessageID, 20)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4, 5, 6}, legacyQueueMessageIDs(messages))
	require.Equal(
		t,
		[]int64{3, 4, 5, 6},
		legacyQueueSourceMessageIDs(t, session, p.NamespaceReplicationQueueType),
	)
	dlqMessages, _, err := targetDual.ReadMessagesFromDLQ(t.Context(), p.EmptyQueueMessageID, 20, 20, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 5}, legacyQueueMessageIDs(dlqMessages))
	require.Equal(
		t,
		[]int64{0, 1, 5},
		legacyQueueSourceMessageIDs(t, session, -p.NamespaceReplicationQueueType),
	)
}

func setUpCassandraLegacyQueueV2TargetDual(
	t *testing.T,
	bucketSize int64,
	messageCount int64,
) (cgocql.Session, p.Queue, *commonpb.DataBlob, cassandra.LegacyQueueV2MigrationOptions) {
	t.Helper()
	testData, tearDown := setUpCassandraTest(t)
	t.Cleanup(tearDown)
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	t.Cleanup(session.Close)
	blob := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("target-dual-fault-test"),
	}
	queueTypes := []p.QueueType{
		p.NamespaceReplicationQueueType,
		-p.NamespaceReplicationQueueType,
	}
	options := cassandra.LegacyQueueV2MigrationOptions{
		PageSize:                  2,
		MessageBucketSize:         bucketSize,
		MaxMismatches:             20,
		ConfirmQueueTrafficFenced: true,
	}

	source, err := cassandra.NewQueueStore(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
	)
	require.NoError(t, err)
	require.NoError(t, source.Init(t.Context(), blob))
	for _, queueType := range queueTypes {
		require.NoError(t, cassandra.InitializeLegacyQueueV2SourceAuthority(
			t.Context(), session, queueType, bucketSize,
		))
	}
	sourceDual, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeSourceDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, sourceDual.Init(t.Context(), blob))
	for expectedID := int64(0); expectedID < messageCount; expectedID++ {
		require.NoError(t, sourceDual.EnqueueMessage(t.Context(), blob))
		messageID, err := sourceDual.EnqueueMessageToDLQ(t.Context(), blob)
		require.NoError(t, err)
		require.Equal(t, expectedID, messageID)
	}
	for _, queueType := range queueTypes {
		_, err := cassandra.BackfillLegacyQueueV2(t.Context(), session, queueType, options)
		require.NoError(t, err)
		result, err := cassandra.CutoverLegacyQueueV2(t.Context(), session, queueType, options)
		require.NoError(t, err)
		require.True(t, result.Validation.Matches(), result.Validation)
	}

	targetDual, err := cassandra.NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetDual,
		bucketSize,
	)
	require.NoError(t, err)
	require.NoError(t, targetDual.Init(t.Context(), blob))
	return session, targetDual, blob, options
}

type failNthLegacyQueueBatchSession struct {
	cgocql.Session
	nth int

	mu    sync.Mutex
	count int
}

func (s *failNthLegacyQueueBatchSession) MapExecuteBatchCAS(
	batch *cgocql.Batch,
	values map[string]any,
) (bool, cgocql.Iter, error) {
	s.mu.Lock()
	s.count++
	fail := s.count == s.nth
	s.mu.Unlock()
	if fail {
		return false, nil, errors.New("injected legacy queue mirror crash")
	}
	return s.Session.MapExecuteBatchCAS(batch, values)
}

func legacyQueueSourceMessageIDs(
	t *testing.T,
	session cgocql.Session,
	queueType p.QueueType,
) []int64 {
	t.Helper()
	iter := session.Query(
		`SELECT message_id FROM queue WHERE queue_type = ? AND message_id >= ? ORDER BY message_id ASC`,
		queueType,
		int64(p.FirstQueueMessageID),
	).Iter()
	var messageIDs []int64
	var messageID int64
	for iter.Scan(&messageID) {
		messageIDs = append(messageIDs, messageID)
	}
	require.NoError(t, iter.Close())
	return messageIDs
}
