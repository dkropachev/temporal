//go:build integration

package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/persistencetest"
)

func TestCassandraQueueV2MetadataTargetOnly(t *testing.T) {
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.QueueV2MigrationMode = config.CassandraQueueV2MigrationModeTargetOnly
		cfg.QueueV2MessageBucketSpan = 4
	})
	t.Cleanup(tearDown)
	dropCassandraTables(t, testData, "queues", "queue_messages")
	queue, err := testData.Factory.NewQueueV2()
	require.NoError(t, err)
	RunQueueV2TestSuite(t, queue)
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()

	createCrashQueueName := "queue-v3-crash-create-" + t.Name()
	_, err = queue.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: createCrashQueueName,
	})
	require.NoError(t, err)
	var createCrashMetadataBucket int
	require.NoError(t, session.Query(
		`SELECT metadata_bucket FROM queues_v2 WHERE queue_type = ? AND queue_name = ? ALLOW FILTERING`,
		p.QueueTypeHistoryDLQ,
		createCrashQueueName,
	).Scan(&createCrashMetadataBucket))
	require.NoError(t, session.Query(
		`UPDATE queues_v2 SET migration_authority = ?, migration_epoch = ? WHERE queue_type = ? AND metadata_bucket = ? AND queue_name = ?`,
		-1,
		int64(1),
		p.QueueTypeHistoryDLQ,
		createCrashMetadataBucket,
		createCrashQueueName,
	).Exec())
	for _, bucket := range []int64{-1, 0} {
		require.NoError(t, session.Query(
			`DELETE FROM queue_messages_v3 WHERE queue_type = ? AND queue_name = ? AND message_bucket = ?`,
			p.QueueTypeHistoryDLQ,
			createCrashQueueName,
			bucket,
		).Exec())
	}
	_, err = queue.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: createCrashQueueName,
	})
	require.Error(t, err)
	message, err := persistencetest.EnqueueMessage(t.Context(), queue, p.QueueTypeHistoryDLQ, createCrashQueueName)
	require.NoError(t, err)
	require.Equal(t, int64(0), message.Metadata.ID)

	queueName := "queue-v3-rollover-" + t.Name()
	_, err = queue.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
	})
	require.NoError(t, err)
	for expectedID := int64(0); expectedID < 10; expectedID++ {
		message, err := persistencetest.EnqueueMessage(t.Context(), queue, p.QueueTypeHistoryDLQ, queueName)
		require.NoError(t, err)
		require.Equal(t, expectedID, message.Metadata.ID)
	}
	var (
		messageIDs []int64
		pageToken  []byte
	)
	for {
		page, err := queue.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
			QueueType:     p.QueueTypeHistoryDLQ,
			QueueName:     queueName,
			PageSize:      3,
			NextPageToken: pageToken,
		})
		require.NoError(t, err)
		for _, message := range page.Messages {
			messageIDs = append(messageIDs, message.MetaData.ID)
		}
		pageToken = page.NextPageToken
		if len(pageToken) == 0 {
			break
		}
	}
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, messageIDs)
	deleteResult, err := queue.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType:                   p.QueueTypeHistoryDLQ,
		QueueName:                   queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{ID: 7},
	})
	require.NoError(t, err)
	require.Equal(t, int64(8), deleteResult.MessagesDeleted)
	listResult, err := queue.ListQueues(t.Context(), &p.InternalListQueuesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		PageSize:  100,
	})
	require.NoError(t, err)
	var found bool
	for _, info := range listResult.Queues {
		if info.QueueName == queueName {
			found = true
			require.Equal(t, int64(2), info.MessageCount)
			require.Equal(t, int64(9), info.LastMessageID)
		}
	}
	require.True(t, found)

	rolloverQueueName := "queue-v3-crash-rollover-" + t.Name()
	_, err = queue.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: rolloverQueueName,
	})
	require.NoError(t, err)
	for expectedID := int64(0); expectedID < 4; expectedID++ {
		message, err := persistencetest.EnqueueMessage(t.Context(), queue, p.QueueTypeHistoryDLQ, rolloverQueueName)
		require.NoError(t, err)
		require.Equal(t, expectedID, message.Metadata.ID)
	}
	applied, err := session.Query(
		`INSERT INTO queue_messages_v3
			(queue_type, queue_name, message_bucket, row_type, message_id, last_message_id, version)
			VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
		p.QueueTypeHistoryDLQ,
		rolloverQueueName,
		int64(1),
		int16(0),
		int64(-1),
		int64(3),
		int64(0),
	).MapScanCAS(make(map[string]any))
	require.NoError(t, err)
	require.True(t, applied)
	message, err = persistencetest.EnqueueMessage(t.Context(), queue, p.QueueTypeHistoryDLQ, rolloverQueueName)
	require.NoError(t, err)
	require.Equal(t, int64(4), message.Metadata.ID)
	for expectedID := int64(5); expectedID < 8; expectedID++ {
		message, err = persistencetest.EnqueueMessage(t.Context(), queue, p.QueueTypeHistoryDLQ, rolloverQueueName)
		require.NoError(t, err)
		require.Equal(t, expectedID, message.Metadata.ID)
	}
	applied, err = session.Query(
		`INSERT INTO queue_messages_v3
			(queue_type, queue_name, message_bucket, row_type, message_id, last_message_id, version)
			VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
		p.QueueTypeHistoryDLQ,
		rolloverQueueName,
		int64(2),
		int16(0),
		int64(-1),
		int64(7),
		int64(0),
	).MapScanCAS(make(map[string]any))
	require.NoError(t, err)
	require.True(t, applied)
	applied, err = session.Query(
		`UPDATE queue_messages_v3 SET active_message_bucket = ?, version = ?
			WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ? AND message_id = ?
			IF active_message_bucket = ? AND version = ?`,
		int64(2),
		int64(2),
		p.QueueTypeHistoryDLQ,
		rolloverQueueName,
		int64(-1),
		int16(0),
		int64(-1),
		int64(1),
		int64(1),
	).MapScanCAS(make(map[string]any))
	require.NoError(t, err)
	require.True(t, applied)
	message, err = persistencetest.EnqueueMessage(t.Context(), queue, p.QueueTypeHistoryDLQ, rolloverQueueName)
	require.NoError(t, err)
	require.Equal(t, int64(8), message.Metadata.ID)

	deleteCrashQueueName := "queue-v3-crash-delete-" + t.Name()
	_, err = queue.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: deleteCrashQueueName,
	})
	require.NoError(t, err)
	for range 5 {
		_, err := persistencetest.EnqueueMessage(t.Context(), queue, p.QueueTypeHistoryDLQ, deleteCrashQueueName)
		require.NoError(t, err)
	}
	_, err = queue.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType:                   p.QueueTypeHistoryDLQ,
		QueueName:                   deleteCrashQueueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{ID: 2},
	})
	require.NoError(t, err)
	for messageID := int64(0); messageID <= 2; messageID++ {
		require.NoError(t, session.Query(
			`INSERT INTO queue_messages_v3
				(queue_type, queue_name, message_bucket, row_type, message_id, message_payload, message_encoding)
				VALUES (?, ?, ?, ?, ?, ?, ?)`,
			p.QueueTypeHistoryDLQ,
			deleteCrashQueueName,
			int64(0),
			int16(1),
			messageID,
			[]byte("stale"),
			enumspb.ENCODING_TYPE_PROTO3.String(),
		).Exec())
	}
	retryDelete, err := queue.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType:                   p.QueueTypeHistoryDLQ,
		QueueName:                   deleteCrashQueueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{ID: 2},
	})
	require.NoError(t, err)
	require.Zero(t, retryDelete.MessagesDeleted)
	iter := session.Query(
		`SELECT message_id FROM queue_messages_v3
			WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ?
			AND message_id >= ? AND message_id <= ?`,
		p.QueueTypeHistoryDLQ,
		deleteCrashQueueName,
		int64(0),
		int16(1),
		int64(0),
		int64(2),
	).Iter()
	require.False(t, iter.Scan(new(int64)))
	require.NoError(t, iter.Close())
}

func TestCassandraQueueV2TargetOnlyStaleBucketCacheRecovers(t *testing.T) {
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.QueueV2MigrationMode = config.CassandraQueueV2MigrationModeTargetOnly
		cfg.QueueV2MessageBucketSpan = 4
	})
	t.Cleanup(tearDown)
	dropCassandraTables(t, testData, "queues", "queue_messages")

	staleStore, err := testData.Factory.NewQueueV2()
	require.NoError(t, err)
	rolloverStore, err := testData.Factory.NewQueueV2()
	require.NoError(t, err)
	queueName := "queue-v3-stale-bucket-" + t.Name()
	_, err = staleStore.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: queueName,
	})
	require.NoError(t, err)

	for expectedID := int64(0); expectedID <= 4; expectedID++ {
		message, err := persistencetest.EnqueueMessage(
			t.Context(),
			rolloverStore,
			p.QueueTypeHistoryDLQ,
			queueName,
		)
		require.NoError(t, err)
		require.Equal(t, expectedID, message.Metadata.ID)
	}

	message, err := persistencetest.EnqueueMessage(
		t.Context(),
		staleStore,
		p.QueueTypeHistoryDLQ,
		queueName,
	)
	require.NoError(t, err)
	require.Equal(t, int64(5), message.Metadata.ID)
	requireQueueV2MessageIDs(t, staleStore, queueName, []int64{0, 1, 2, 3, 4, 5})
}

func TestCassandraTaskQueueUserDataTargetOnly(t *testing.T) {
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.TaskQueueUserDataMigrationMode = config.CassandraTaskQueueUserDataMigrationModeTargetOnly
		cfg.TaskQueueUserDataBucketCount = cassandra.DefaultTaskQueueUserDataBucketCount
	})
	t.Cleanup(tearDown)
	dropCassandraTables(t, testData, "task_queue_user_data")
	store, err := testData.Factory.NewTaskStore()
	require.NoError(t, err)
	suite.Run(t, NewTaskQueueUserDataSuite(t, store, testData.Logger))
}

func TestCassandraLegacyQueueV2TargetOnly(t *testing.T) {
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.LegacyQueueMigrationMode = config.CassandraLegacyQueueMigrationModeTargetOnly
		cfg.LegacyQueueMessageBucketSize = 4
	})
	defer tearDown()
	dropCassandraTables(t, testData, "queue")
	queue, err := testData.Factory.NewQueue(p.NamespaceReplicationQueueType)
	require.NoError(t, err)
	blob := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("message")}
	require.NoError(t, queue.Init(t.Context(), blob))
	for range 10 {
		require.NoError(t, queue.EnqueueMessage(t.Context(), blob))
	}
	messages, err := queue.ReadMessages(t.Context(), p.EmptyQueueMessageID, 20)
	require.NoError(t, err)
	require.Len(t, messages, 10)
	for index, message := range messages {
		require.Equal(t, int64(index), message.ID)
	}
	require.NoError(t, queue.DeleteMessagesBefore(t.Context(), 5))
	messages, err = queue.ReadMessages(t.Context(), p.EmptyQueueMessageID, 20)
	require.NoError(t, err)
	require.Equal(t, []int64{5, 6, 7, 8, 9}, legacyQueueMessageIDs(messages))

	for expectedID := int64(0); expectedID < 6; expectedID++ {
		messageID, err := queue.EnqueueMessageToDLQ(t.Context(), blob)
		require.NoError(t, err)
		require.Equal(t, expectedID, messageID)
	}
	dlq, token, err := queue.ReadMessagesFromDLQ(t.Context(), p.EmptyQueueMessageID, 10, 10, nil)
	require.NoError(t, err)
	require.Empty(t, token)
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5}, legacyQueueMessageIDs(dlq))
	require.NoError(t, queue.DeleteMessageFromDLQ(t.Context(), 2))
	require.NoError(t, queue.RangeDeleteMessagesFromDLQ(t.Context(), 2, 4))
	dlq, _, err = queue.ReadMessagesFromDLQ(t.Context(), p.EmptyQueueMessageID, 10, 10, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 5}, legacyQueueMessageIDs(dlq))
}

func dropCassandraTables(t testing.TB, testData CassandraTestData, tables ...string) {
	t.Helper()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	for _, table := range tables {
		require.NoError(t, session.Query("DROP TABLE "+table).Exec())
	}
}

func legacyQueueMessageIDs(messages []*p.QueueMessage) []int64 {
	ids := make([]int64, len(messages))
	for index, message := range messages {
		ids[index] = message.ID
	}
	return ids
}
