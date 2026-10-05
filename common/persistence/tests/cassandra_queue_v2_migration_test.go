//go:build integration

package tests

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/persistencetest"
)

func TestCassandraQueueV2OnlineMessageMigration(t *testing.T) {
	const (
		messageBucketSpan = int64(4)
		queueName         = "queue-v2-online-message-migration"
	)
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.QueueV2MigrationMode = config.CassandraQueueV2MigrationModeSourceOnly
		cfg.QueueV2MessageBucketSpan = messageBucketSpan
	})
	defer tearDown()

	sourceQueue, err := testData.Factory.NewQueueV2()
	require.NoError(t, err)
	_, err = sourceQueue.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
	})
	require.NoError(t, err)
	for expectedID := int64(0); expectedID < 10; expectedID++ {
		message, err := persistencetest.EnqueueMessage(t.Context(), sourceQueue, p.QueueTypeHistoryDLQ, queueName)
		require.NoError(t, err)
		require.Equal(t, expectedID, message.Metadata.ID)
	}

	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	metadataPage, err := cassandra.BackfillQueueV2MetadataPage(
		t.Context(),
		session,
		p.QueueTypeHistoryDLQ,
		10,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, metadataPage.RowsProcessed)

	var token []byte
	for {
		page, err := cassandra.BackfillQueueV2MessagesPage(
			t.Context(),
			session,
			p.QueueTypeHistoryDLQ,
			queueName,
			3,
			token,
			messageBucketSpan,
		)
		require.NoError(t, err)
		token = page.NextPageToken
		if len(token) == 0 {
			break
		}
	}
	validation, err := cassandra.ValidateQueueV2Messages(
		t.Context(),
		session,
		p.QueueTypeHistoryDLQ,
		queueName,
		3,
		messageBucketSpan,
	)
	require.NoError(t, err)
	require.True(t, validation.Matches(), validation.Mismatches)
	require.Equal(t, int64(10), validation.SourceRows)
	require.Equal(t, int64(10), validation.TargetRows)
	require.NoError(t, session.Query(
		`INSERT INTO queue_messages_v3 (queue_type, queue_name, message_bucket, row_type, message_id, message_payload, message_encoding) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		p.QueueTypeHistoryDLQ,
		queueName,
		int64(99),
		int16(1),
		int64(1),
		[]byte("wrong-bucket-duplicate"),
		"Proto3",
	).Exec())
	validation, err = cassandra.ValidateQueueV2Messages(
		t.Context(),
		session,
		p.QueueTypeHistoryDLQ,
		queueName,
		3,
		messageBucketSpan,
	)
	require.NoError(t, err)
	require.False(t, validation.Matches())
	require.NoError(t, session.Query(
		`DELETE FROM queue_messages_v3 WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ? AND message_id = ?`,
		p.QueueTypeHistoryDLQ,
		queueName,
		int64(99),
		int16(1),
		int64(1),
	).Exec())

	require.NoError(t, session.Query(
		`DELETE FROM queue_messages_v3 WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ? AND message_id = ?`,
		p.QueueTypeHistoryDLQ,
		queueName,
		int64(1),
		int16(1),
		int64(5),
	).Exec())
	validation, err = cassandra.ValidateQueueV2Messages(
		t.Context(),
		session,
		p.QueueTypeHistoryDLQ,
		queueName,
		3,
		messageBucketSpan,
	)
	require.NoError(t, err)
	require.False(t, validation.Matches())
	validation, err = cassandra.ReconcileQueueV2Messages(
		t.Context(),
		session,
		p.QueueTypeHistoryDLQ,
		queueName,
		3,
		messageBucketSpan,
	)
	require.NoError(t, err)
	require.True(t, validation.Matches(), validation.Mismatches)
	sourceDual, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeSourceDual,
		messageBucketSpan,
	)
	require.NoError(t, err)
	message, err := persistencetest.EnqueueMessage(t.Context(), sourceDual, p.QueueTypeHistoryDLQ, queueName)
	require.NoError(t, err)
	require.Equal(t, int64(10), message.Metadata.ID)
	validation, err = cassandra.ValidateQueueV2Messages(
		t.Context(),
		session,
		p.QueueTypeHistoryDLQ,
		queueName,
		3,
		messageBucketSpan,
	)
	require.NoError(t, err)
	require.True(t, validation.Matches(), validation.Mismatches)
	targetShadow, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeTargetShadow,
		messageBucketSpan,
	)
	require.NoError(t, err)
	shadowPage, err := targetShadow.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
		PageSize:  20,
	})
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, queueV2MessageIDs(shadowPage.Messages))
	cutover, err := cassandra.CutoverQueueV2ToTarget(
		t.Context(),
		session,
		cassandra.QueueV2CutoverOptions{
			QueueType:         p.QueueTypeHistoryDLQ,
			QueueName:         queueName,
			PageSize:          3,
			MessageBucketSpan: messageBucketSpan,
		},
	)
	require.NoError(t, err)
	require.True(t, cutover.Validation.Matches(), cutover.Validation.Mismatches)
	_, err = persistencetest.EnqueueMessage(t.Context(), sourceDual, p.QueueTypeHistoryDLQ, queueName)
	require.Error(t, err)

	targetDual, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeTargetDual,
		messageBucketSpan,
	)
	require.NoError(t, err)
	message, err = persistencetest.EnqueueMessage(t.Context(), targetDual, p.QueueTypeHistoryDLQ, queueName)
	require.NoError(t, err)
	require.Equal(t, int64(11), message.Metadata.ID)
	deleted, err := targetDual.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType:                   p.QueueTypeHistoryDLQ,
		QueueName:                   queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{ID: 7},
	})
	require.NoError(t, err)
	require.Equal(t, int64(8), deleted.MessagesDeleted)
	validation, err = cassandra.ValidateQueueV2Messages(
		t.Context(),
		session,
		p.QueueTypeHistoryDLQ,
		queueName,
		3,
		messageBucketSpan,
	)
	require.NoError(t, err)
	require.True(t, validation.Matches(), validation.Mismatches)

	dropCassandraTables(t, testData, "queues", "queue_messages")
	targetOnly, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeTargetOnly,
		messageBucketSpan,
	)
	require.NoError(t, err)
	response, err := targetOnly.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
		PageSize:  10,
	})
	require.NoError(t, err)
	require.Equal(t, []int64{8, 9, 10, 11}, queueV2MessageIDs(response.Messages))
	message, err = persistencetest.EnqueueMessage(t.Context(), targetOnly, p.QueueTypeHistoryDLQ, queueName)
	require.NoError(t, err)
	require.Equal(t, int64(12), message.Metadata.ID)
	deleted, err = targetOnly.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType:                   p.QueueTypeHistoryDLQ,
		QueueName:                   queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{ID: 11},
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), deleted.MessagesDeleted)
	response, err = targetOnly.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
		PageSize:  10,
	})
	require.NoError(t, err)
	require.Equal(t, []int64{12}, queueV2MessageIDs(response.Messages))
}

func TestCassandraQueueV2CutoverResumesAfterPublishCrash(t *testing.T) {
	const (
		messageBucketSpan = int64(4)
		queueName         = "queue-v2-cutover-crash-resume"
	)
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.QueueV2MigrationMode = config.CassandraQueueV2MigrationModeSourceOnly
		cfg.QueueV2MessageBucketSpan = messageBucketSpan
	})
	defer tearDown()

	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	source, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeSourceDual,
		messageBucketSpan,
	)
	require.NoError(t, err)
	_, err = source.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
	})
	require.NoError(t, err)
	for expectedID := int64(0); expectedID < 7; expectedID++ {
		message, err := persistencetest.EnqueueMessage(t.Context(), source, p.QueueTypeHistoryDLQ, queueName)
		require.NoError(t, err)
		require.Equal(t, expectedID, message.Metadata.ID)
	}

	options := cassandra.QueueV2CutoverOptions{
		QueueType:         p.QueueTypeHistoryDLQ,
		QueueName:         queueName,
		PageSize:          2,
		MessageBucketSpan: messageBucketSpan,
	}
	faultSession := &failNthQueueV2CASSession{
		Session: session,
		match:   "UPDATE queue_messages SET migration_authority = ?",
		nth:     2,
	}
	_, err = cassandra.CutoverQueueV2ToTarget(t.Context(), faultSession, options)
	require.ErrorContains(t, err, "injected QueueV2 cutover crash")

	result, err := cassandra.CutoverQueueV2ToTarget(t.Context(), session, options)
	require.NoError(t, err)
	require.True(t, result.Validation.Matches(), result.Validation.Mismatches)
	retry, err := cassandra.CutoverQueueV2ToTarget(t.Context(), session, options)
	require.NoError(t, err)
	require.Equal(t, result.Generation, retry.Generation)
	require.Equal(t, result.Epoch, retry.Epoch)

	_, err = persistencetest.EnqueueMessage(t.Context(), source, p.QueueTypeHistoryDLQ, queueName)
	require.Error(t, err)
	target, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeTargetDual,
		messageBucketSpan,
	)
	require.NoError(t, err)
	message, err := persistencetest.EnqueueMessage(t.Context(), target, p.QueueTypeHistoryDLQ, queueName)
	require.NoError(t, err)
	require.Equal(t, int64(7), message.Metadata.ID)

	var sourceMaximum int64
	require.NoError(t, session.Query(
		`SELECT message_id FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? ORDER BY message_id DESC LIMIT 1`,
		p.QueueTypeHistoryDLQ,
		queueName,
		0,
	).Scan(&sourceMaximum))
	require.Equal(t, int64(7), sourceMaximum)
}

func TestCassandraQueueV2ConcurrentCutoverCannotClobberActivatedTarget(t *testing.T) {
	const (
		messageBucketSpan = int64(4)
		queueName         = "queue-v2-concurrent-cutover-fence"
	)
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.QueueV2MigrationMode = config.CassandraQueueV2MigrationModeSourceOnly
		cfg.QueueV2MessageBucketSpan = messageBucketSpan
	})
	defer tearDown()

	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	source, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeSourceDual,
		messageBucketSpan,
	)
	require.NoError(t, err)
	_, err = source.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
	})
	require.NoError(t, err)
	for expectedID := int64(0); expectedID < 8; expectedID++ {
		message, err := persistencetest.EnqueueMessage(t.Context(), source, p.QueueTypeHistoryDLQ, queueName)
		require.NoError(t, err)
		require.Equal(t, expectedID, message.Metadata.ID)
	}

	options := cassandra.QueueV2CutoverOptions{
		QueueType:         p.QueueTypeHistoryDLQ,
		QueueName:         queueName,
		PageSize:          2,
		MessageBucketSpan: messageBucketSpan,
	}
	blocked := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	delayedSession := &blockOnceQueueV2RepairBatchSession{
		Session: session,
		blocked: blocked,
		release: release,
	}
	type cutoverResult struct {
		result cassandra.QueueV2CutoverResult
		err    error
	}
	delayedResultCh := make(chan cutoverResult, 1)
	go func() {
		result, err := cassandra.CutoverQueueV2ToTarget(t.Context(), delayedSession, options)
		delayedResultCh <- cutoverResult{result: result, err: err}
	}()
	select {
	case <-blocked:
	case <-t.Context().Done():
		t.Fatal("timed out waiting for delayed QueueV2 cutover repair")
	}

	var sourceAuthority int
	require.NoError(t, session.Query(
		`SELECT migration_authority FROM queues WHERE queue_type = ? AND queue_name = ?`,
		p.QueueTypeHistoryDLQ,
		queueName,
	).Scan(&sourceAuthority))
	require.Equal(t, 2, sourceAuthority)
	winner, err := cassandra.CutoverQueueV2ToTarget(t.Context(), session, options)
	require.NoError(t, err)
	require.True(t, winner.Validation.Matches(), winner.Validation.Mismatches)

	target, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeTargetDual,
		messageBucketSpan,
	)
	require.NoError(t, err)
	message, err := persistencetest.EnqueueMessage(t.Context(), target, p.QueueTypeHistoryDLQ, queueName)
	require.NoError(t, err)
	require.Equal(t, int64(8), message.Metadata.ID)
	deleted, err := target.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType:                   p.QueueTypeHistoryDLQ,
		QueueName:                   queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{ID: 3},
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), deleted.MessagesDeleted)
	requireQueueV2MessageIDs(t, target, queueName, []int64{4, 5, 6, 7, 8})

	close(release)
	var delayed cutoverResult
	select {
	case delayed = <-delayedResultCh:
	case <-t.Context().Done():
		t.Fatal("timed out waiting for delayed QueueV2 cutover to finish")
	}
	require.NoError(t, delayed.err)
	require.True(t, delayed.result.Validation.Matches(), delayed.result.Validation.Mismatches)
	require.Equal(t, winner.Generation, delayed.result.Generation)
	require.Equal(t, winner.Epoch, delayed.result.Epoch)
	requireQueueV2MessageIDs(t, target, queueName, []int64{4, 5, 6, 7, 8})
	validation, err := cassandra.ValidateQueueV2Messages(
		t.Context(),
		session,
		p.QueueTypeHistoryDLQ,
		queueName,
		2,
		messageBucketSpan,
	)
	require.NoError(t, err)
	require.True(t, validation.Matches(), validation.Mismatches)
}

func requireQueueV2MessageIDs(
	t *testing.T,
	queue p.QueueV2,
	queueName string,
	expected []int64,
) {
	t.Helper()
	response, err := queue.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
		PageSize:  20,
	})
	require.NoError(t, err)
	require.Equal(t, expected, queueV2MessageIDs(response.Messages))
}

func TestCassandraQueueV2TargetDualMaintainsRollbackCopyDuringFinish(t *testing.T) {
	const (
		messageBucketSpan = int64(4)
		queueName         = "queue-v2-target-dual-rollback-copy"
	)
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.QueueV2MigrationMode = config.CassandraQueueV2MigrationModeSourceOnly
		cfg.QueueV2MessageBucketSpan = messageBucketSpan
	})
	defer tearDown()

	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	source, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeSourceDual,
		messageBucketSpan,
	)
	require.NoError(t, err)
	_, err = source.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
	})
	require.NoError(t, err)
	for expectedID := int64(0); expectedID < 7; expectedID++ {
		message, err := persistencetest.EnqueueMessage(t.Context(), source, p.QueueTypeHistoryDLQ, queueName)
		require.NoError(t, err)
		require.Equal(t, expectedID, message.Metadata.ID)
	}

	options := cassandra.QueueV2CutoverOptions{
		QueueType:         p.QueueTypeHistoryDLQ,
		QueueName:         queueName,
		PageSize:          2,
		MessageBucketSpan: messageBucketSpan,
	}
	cutover, err := cassandra.CutoverQueueV2ToTarget(t.Context(), session, options)
	require.NoError(t, err)
	require.True(t, cutover.Validation.Matches(), cutover.Validation.Mismatches)

	failedMirrorSession := &failNthQueueV2BatchSession{Session: session, nth: 2}
	failedMirrorStore, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		failedMirrorSession,
		testData.Logger,
		config.CassandraQueueV2MigrationModeTargetDual,
		messageBucketSpan,
	)
	require.NoError(t, err)
	_, err = persistencetest.EnqueueMessage(
		t.Context(),
		failedMirrorStore,
		p.QueueTypeHistoryDLQ,
		queueName,
	)
	require.ErrorContains(t, err, "injected QueueV2 mirror crash")

	cutover, err = cassandra.CutoverQueueV2ToTarget(t.Context(), session, options)
	require.NoError(t, err)
	require.True(t, cutover.Validation.Matches(), cutover.Validation.Mismatches)

	target, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeTargetDual,
		messageBucketSpan,
	)
	require.NoError(t, err)
	message, err := persistencetest.EnqueueMessage(t.Context(), target, p.QueueTypeHistoryDLQ, queueName)
	require.NoError(t, err)
	require.Equal(t, int64(8), message.Metadata.ID)

	blocked := make(chan struct{})
	release := make(chan struct{})
	blockingSession := &blockOnceQueueV2TargetReadSession{
		Session: session,
		blocked: blocked,
		release: release,
	}
	type cutoverResult struct {
		result cassandra.QueueV2CutoverResult
		err    error
	}
	resultCh := make(chan cutoverResult, 1)
	go func() {
		result, err := cassandra.CutoverQueueV2ToTarget(t.Context(), blockingSession, options)
		resultCh <- cutoverResult{result: result, err: err}
	}()
	select {
	case <-blocked:
	case <-t.Context().Done():
		t.Fatal("timed out waiting for QueueV2 finish validation")
	}

	message, err = persistencetest.EnqueueMessage(t.Context(), target, p.QueueTypeHistoryDLQ, queueName)
	require.NoError(t, err)
	require.Equal(t, int64(9), message.Metadata.ID)
	deleted, err := target.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType:                   p.QueueTypeHistoryDLQ,
		QueueName:                   queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{ID: 3},
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), deleted.MessagesDeleted)
	close(release)

	var concurrentCutover cutoverResult
	select {
	case concurrentCutover = <-resultCh:
	case <-t.Context().Done():
		t.Fatal("timed out waiting for QueueV2 finish validation to retry")
	}
	require.NoError(t, concurrentCutover.err)
	require.True(t, concurrentCutover.result.Validation.Matches(), concurrentCutover.result.Validation.Mismatches)

	validation, err := cassandra.ValidateQueueV2Messages(
		t.Context(),
		session,
		p.QueueTypeHistoryDLQ,
		queueName,
		2,
		messageBucketSpan,
	)
	require.NoError(t, err)
	require.True(t, validation.Matches(), validation.Mismatches)

	sourceOnly, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeSourceOnly,
		messageBucketSpan,
	)
	require.NoError(t, err)
	response, err := sourceOnly.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
		PageSize:  20,
	})
	require.NoError(t, err)
	require.Equal(t, []int64{4, 5, 6, 7, 8, 9}, queueV2MessageIDs(response.Messages))
}

func TestCassandraQueueV2TargetDualCreateResumes(t *testing.T) {
	const (
		messageBucketSpan = int64(4)
		queueName         = "queue-v2-target-dual-create-resume"
	)
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.QueueV2MigrationMode = config.CassandraQueueV2MigrationModeSourceOnly
		cfg.QueueV2MessageBucketSpan = messageBucketSpan
	})
	defer tearDown()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	queue, err := cassandra.NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		testData.Logger,
		config.CassandraQueueV2MigrationModeTargetDual,
		messageBucketSpan,
	)
	require.NoError(t, err)
	request := &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
	}
	_, err = queue.CreateQueue(t.Context(), request)
	require.NoError(t, err)

	var metadataBucket int
	require.NoError(t, session.Query(
		`SELECT metadata_bucket FROM queues_v2 WHERE queue_type = ? AND queue_name = ? ALLOW FILTERING`,
		request.QueueType,
		queueName,
	).Scan(&metadataBucket))
	require.NoError(t, session.Query(
		`UPDATE queues SET migration_authority = ?, migration_epoch = ? WHERE queue_type = ? AND queue_name = ?`,
		-1,
		int64(1),
		request.QueueType,
		queueName,
	).Exec())
	require.NoError(t, session.Query(
		`UPDATE queue_messages SET migration_authority = ?, migration_epoch = ? WHERE queue_type = ? AND queue_name = ? AND queue_partition = ?`,
		-1,
		int64(1),
		request.QueueType,
		queueName,
		0,
	).Exec())
	require.NoError(t, session.Query(
		`DELETE FROM queues_v2 WHERE queue_type = ? AND metadata_bucket = ? AND queue_name = ?`,
		request.QueueType,
		metadataBucket,
		queueName,
	).Exec())
	for _, bucket := range []int64{-1, 0} {
		require.NoError(t, session.Query(
			`DELETE FROM queue_messages_v3 WHERE queue_type = ? AND queue_name = ? AND message_bucket = ?`,
			request.QueueType,
			queueName,
			bucket,
		).Exec())
	}

	_, err = queue.CreateQueue(t.Context(), request)
	require.ErrorIs(t, err, p.ErrQueueAlreadyExists)
	message, err := persistencetest.EnqueueMessage(t.Context(), queue, request.QueueType, queueName)
	require.NoError(t, err)
	require.Equal(t, int64(0), message.Metadata.ID)
}

func TestCassandraQueueV2TargetOnlyInventoryAudit(t *testing.T) {
	const (
		messageBucketSpan = int64(4)
		queueName         = "queue-v2-target-only-inventory"
	)
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.QueueV2MigrationMode = config.CassandraQueueV2MigrationModeSourceOnly
		cfg.QueueV2MessageBucketSpan = messageBucketSpan
	})
	defer tearDown()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	metadataSpec, err := cassandra.SchemaLayoutSpecForTable(
		t.Context(),
		session,
		testData.Cfg.Keyspace,
		cassandra.SchemaLayoutQueueV2Metadata,
		"queues_v2",
		64,
	)
	require.NoError(t, err)
	messagesSpec, err := cassandra.SchemaLayoutSpecForTable(
		t.Context(),
		session,
		testData.Cfg.Keyspace,
		cassandra.SchemaLayoutQueueV2Messages,
		"queue_messages_v3",
		messageBucketSpan,
	)
	require.NoError(t, err)
	metadataStore := cassandra.NewSchemaLayoutMetadataStore(session)
	for _, spec := range []cassandra.SchemaLayoutSpec{metadataSpec, messagesSpec} {
		_, err := metadataStore.InitializePreparing(t.Context(), spec)
		require.NoError(t, err)
		_, err = metadataStore.MarkTargetReady(t.Context(), spec)
		require.NoError(t, err)
	}

	sourceQueue, err := testData.Factory.NewQueueV2()
	require.NoError(t, err)
	_, err = sourceQueue.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
	})
	require.NoError(t, err)
	for expectedID := int64(0); expectedID < 6; expectedID++ {
		message, err := persistencetest.EnqueueMessage(t.Context(), sourceQueue, p.QueueTypeHistoryDLQ, queueName)
		require.NoError(t, err)
		require.Equal(t, expectedID, message.Metadata.ID)
	}
	cutover, err := cassandra.CutoverQueueV2ToTarget(
		t.Context(),
		session,
		cassandra.QueueV2CutoverOptions{
			QueueType:         p.QueueTypeHistoryDLQ,
			QueueName:         queueName,
			PageSize:          2,
			MessageBucketSpan: messageBucketSpan,
			Generation:        messagesSpec.Generation,
		},
	)
	require.NoError(t, err)
	require.True(t, cutover.Validation.Matches(), cutover.Validation.Mismatches)
	options := cassandra.SchemaLayoutInventoryAuditOptions{
		PageSize:            2,
		QueueV2MetadataSpec: metadataSpec,
		QueueV2MessagesSpec: messagesSpec,
	}
	result, err := cassandra.AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		metadataSpec,
		options,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.SourceEntities)
	require.Equal(t, int64(1), result.TargetEntities)

	require.NoError(t, session.Query(
		`UPDATE queue_messages_v3 SET message_payload = ? WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ? AND message_id = ?`,
		[]byte("corrupt"),
		p.QueueTypeHistoryDLQ,
		queueName,
		int64(0),
		int16(1),
		int64(2),
	).Exec())
	_, err = cassandra.AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		messagesSpec,
		options,
	)
	require.ErrorContains(t, err, "QueueV2 messages differ")
}

type failNthQueueV2CASSession struct {
	cgocql.Session
	match string
	nth   int

	mu    sync.Mutex
	count int
}

type failNthQueueV2BatchSession struct {
	cgocql.Session
	nth int

	mu    sync.Mutex
	count int
}

type blockOnceQueueV2RepairBatchSession struct {
	cgocql.Session
	blocked chan struct{}
	release chan struct{}

	mu       sync.Mutex
	didBlock bool
}

func (s *blockOnceQueueV2RepairBatchSession) MapExecuteBatchCAS(
	batch *cgocql.Batch,
	values map[string]any,
) (bool, cgocql.Iter, error) {
	s.mu.Lock()
	if !s.didBlock {
		s.didBlock = true
		close(s.blocked)
		s.mu.Unlock()
		<-s.release
	} else {
		s.mu.Unlock()
	}
	return s.Session.MapExecuteBatchCAS(batch, values)
}

func (s *failNthQueueV2BatchSession) MapExecuteBatchCAS(
	batch *cgocql.Batch,
	values map[string]any,
) (bool, cgocql.Iter, error) {
	s.mu.Lock()
	s.count++
	fail := s.count == s.nth
	s.mu.Unlock()
	if fail {
		return false, nil, errors.New("injected QueueV2 mirror crash")
	}
	return s.Session.MapExecuteBatchCAS(batch, values)
}

type blockOnceQueueV2TargetReadSession struct {
	cgocql.Session
	blocked chan struct{}
	release chan struct{}

	mu       sync.Mutex
	didBlock bool
}

func (s *blockOnceQueueV2TargetReadSession) Query(statement string, args ...any) cgocql.Query {
	if strings.Contains(statement, "SELECT message_id, message_payload, message_encoding FROM queue_messages_v3") {
		s.mu.Lock()
		if !s.didBlock {
			s.didBlock = true
			close(s.blocked)
			s.mu.Unlock()
			<-s.release
			return s.Session.Query(statement, args...)
		}
		s.mu.Unlock()
	}
	return s.Session.Query(statement, args...)
}

func (s *failNthQueueV2CASSession) Query(statement string, args ...any) cgocql.Query {
	query := s.Session.Query(statement, args...)
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.Contains(statement, s.match) {
		s.count++
		if s.count == s.nth {
			return &failQueueV2CASQuery{Query: query, fail: true}
		}
	}
	return query
}

type failQueueV2CASQuery struct {
	cgocql.Query
	fail bool
}

func (q *failQueueV2CASQuery) WithContext(ctx context.Context) cgocql.Query {
	q.Query = q.Query.WithContext(ctx)
	return q
}

func (q *failQueueV2CASQuery) MapScanCAS(values map[string]any) (bool, error) {
	if q.fail {
		return false, errors.New("injected QueueV2 cutover crash")
	}
	return q.Query.MapScanCAS(values)
}

func queueV2MessageIDs(messages []p.QueueV2Message) []int64 {
	ids := make([]int64, len(messages))
	for index, message := range messages {
		ids[index] = message.MetaData.ID
	}
	return ids
}
