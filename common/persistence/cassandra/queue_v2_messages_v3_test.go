package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func TestQueueV2MessageBucketBoundaries(t *testing.T) {
	const span = int64(4)
	tests := []struct {
		messageID int64
		bucket    int64
	}{
		{messageID: 0, bucket: 0},
		{messageID: 3, bucket: 0},
		{messageID: 4, bucket: 1},
		{messageID: 7, bucket: 1},
		{messageID: 8, bucket: 2},
	}
	for _, test := range tests {
		bucket, err := queueV2MessageBucketForID(test.messageID, span)
		require.NoError(t, err)
		require.Equal(t, test.bucket, bucket)
	}
	_, err := queueV2MessageBucketForID(-1, span)
	require.Error(t, err)
	_, err = queueV2MessageBucketForID(0, 0)
	require.Error(t, err)
}

func TestQueueV2MessageMigrationModeLayouts(t *testing.T) {
	tests := []struct {
		mode      config.CassandraQueueV2MigrationMode
		read      queueV2MessageLayout
		primary   queueV2MessageLayout
		mirror    queueV2MessageLayout
		hasMirror bool
	}{
		{mode: config.CassandraQueueV2MigrationModeSourceOnly, read: queueV2MessageLayoutSource, primary: queueV2MessageLayoutSource},
		{mode: config.CassandraQueueV2MigrationModeSourceDual, read: queueV2MessageLayoutSource, primary: queueV2MessageLayoutSource, mirror: queueV2MessageLayoutTarget, hasMirror: true},
		{mode: config.CassandraQueueV2MigrationModeTargetShadow, read: queueV2MessageLayoutSource, primary: queueV2MessageLayoutSource, mirror: queueV2MessageLayoutTarget, hasMirror: true},
		{mode: config.CassandraQueueV2MigrationModeTargetDual, read: queueV2MessageLayoutTarget, primary: queueV2MessageLayoutTarget, mirror: queueV2MessageLayoutSource, hasMirror: true},
		{mode: config.CassandraQueueV2MigrationModeTargetOnly, read: queueV2MessageLayoutTarget, primary: queueV2MessageLayoutTarget},
	}
	for _, test := range tests {
		store := &queueV2Store{migrationMode: test.mode}
		require.Equal(t, test.read, store.messageReadLayout())
		primary, mirror, hasMirror := store.messageWriteLayouts()
		require.Equal(t, test.primary, primary)
		require.Equal(t, test.mirror, mirror)
		require.Equal(t, test.hasMirror, hasMirror)
	}
}

func TestQueueV2MessagePageTokenBindsRequestAndLayout(t *testing.T) {
	token := encodeQueueV2MessagePageToken(p.QueueTypeHistoryDLQ, "queue-a", 64, 129)
	messageID, err := decodeQueueV2MessagePageToken(token, p.QueueTypeHistoryDLQ, "queue-a", 64)
	require.NoError(t, err)
	require.Equal(t, int64(129), messageID)

	_, err = decodeQueueV2MessagePageToken(token, p.QueueTypeHistoryNormal, "queue-a", 64)
	require.ErrorIs(t, err, p.ErrInvalidReadQueueMessagesNextPageToken)
	_, err = decodeQueueV2MessagePageToken(token, p.QueueTypeHistoryDLQ, "queue-b", 64)
	require.ErrorIs(t, err, p.ErrInvalidReadQueueMessagesNextPageToken)
	_, err = decodeQueueV2MessagePageToken(token, p.QueueTypeHistoryDLQ, "queue-a", 32)
	require.ErrorIs(t, err, p.ErrInvalidReadQueueMessagesNextPageToken)
	_, err = decodeQueueV2MessagePageToken([]byte("source-token"), p.QueueTypeHistoryDLQ, "queue-a", 64)
	require.ErrorIs(t, err, p.ErrInvalidReadQueueMessagesNextPageToken)

	badGeneration := append([]byte(nil), token...)
	badGeneration[len(queueV2MessagePageTokenPrefix)+1]++
	_, err = decodeQueueV2MessagePageToken(badGeneration, p.QueueTypeHistoryDLQ, "queue-a", 64)
	require.ErrorIs(t, err, p.ErrInvalidReadQueueMessagesNextPageToken)
}

func TestQueueV2TargetOnlyDeleteSplitsBucketsWithoutSourceQueries(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{}
		},
	}
	store := &queueV2Store{
		session:       session,
		logger:        log.NewNoopLogger(),
		migrationMode: config.CassandraQueueV2MigrationModeTargetOnly,
		messageSpan:   4,
	}

	err := store.deleteMessageRange(
		t.Context(),
		p.QueueTypeHistoryDLQ,
		"queue-a",
		2,
		9,
		queueV2OperationRoute{layout: queueV2MetadataLayoutTarget},
	)

	require.NoError(t, err)
	require.Equal(t, []string{
		templateDeleteQueueV2MessagesV3,
		templateDeleteQueueV2MessagesV3,
		templateDeleteQueueV2MessagesV3,
	}, recordedStatements(session.queries))
	require.Equal(t, []any{p.QueueTypeHistoryDLQ, "queue-a", int64(0), queueV2MessageDataRowType, int64(2), int64(3)}, session.queries[0].args)
	require.Equal(t, []any{p.QueueTypeHistoryDLQ, "queue-a", int64(1), queueV2MessageDataRowType, int64(4), int64(7)}, session.queries[1].args)
	require.Equal(t, []any{p.QueueTypeHistoryDLQ, "queue-a", int64(2), queueV2MessageDataRowType, int64(8), int64(9)}, session.queries[2].args)
}

func TestQueueV2TargetOnlyReadCrossesMessageBuckets(t *testing.T) {
	const queueName = "queue-a"
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case templateGetQueueV2MessageDirectory:
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*int64) = 1
				*dest[1].(*int64) = 4
				*dest[2].(*int64) = 3
				return nil
			}}
		case templateGetQueueV2MessageBucketState:
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*int64) = 5
				*dest[1].(*int64) = 2
				return nil
			}}
		case templateGetQueueV2MessagesV3:
			bucket := args[2].(int64)
			if bucket == 0 {
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{int64(2), []byte("2"), enumspb.ENCODING_TYPE_PROTO3.String()},
					{int64(3), []byte("3"), enumspb.ENCODING_TYPE_PROTO3.String()},
				}}}
			}
			return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
				{int64(4), []byte("4"), enumspb.ENCODING_TYPE_PROTO3.String()},
				{int64(5), []byte("5"), enumspb.ENCODING_TYPE_PROTO3.String()},
			}}}
		default:
			return &recordingQuery{}
		}
	}
	store := &queueV2Store{session: session, messageSpan: 4}

	messages, err := store.readQueueV2TargetMessagesFrom(
		t.Context(),
		p.QueueTypeHistoryNormal,
		queueName,
		2,
		4,
	)

	require.NoError(t, err)
	require.Equal(t, []int64{2, 3, 4, 5}, queueV2TestMessageIDs(messages))
	require.NotContains(t, recordedStatements(session.queries), TemplateGetMessagesQuery)
	require.NotContains(t, recordedStatements(session.queries), TemplateGetMaxMessageIDQuery)
}

func TestQueueV2MessageBucketSpanValidation(t *testing.T) {
	_, err := NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		nil,
		log.NewNoopLogger(),
		config.CassandraQueueV2MigrationModeTargetOnly,
		0,
	)
	require.Error(t, err)
}

func TestScanTargetQueueV2MessagesForValidationRejectsStructuralExtras(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			require.Equal(t, templateScanTargetQueueV2MessagesForValidation, stmt)
			return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
				{queueV2MessageDirectoryBucket, int(queueV2MessageStateRowType), int64(0)},
				{int64(0), int(queueV2MessageDataRowType), int64(0)},
				{int64(9), int(queueV2MessageDataRowType), int64(1)},
				{int64(0), 99, int64(2)},
			}}}
		},
	}
	result := QueueV2MessageValidationResult{}

	rows, err := scanTargetQueueV2MessagesForValidation(
		t.Context(),
		session,
		p.QueueTypeHistoryDLQ,
		"queue-a",
		7,
		0,
		0,
		true,
		4,
		&result,
	)

	require.NoError(t, err)
	require.Equal(t, int64(2), rows)
	require.Len(t, result.Mismatches, 3)
	require.Contains(t, result.Mismatches[0], "expected")
	require.Contains(t, result.Mismatches[1], "outside source range")
	require.Contains(t, result.Mismatches[2], "unsupported type")
	require.Equal(t, 7, session.queries[0].query.pageSize)
}

func queueV2TestMessageIDs(messages []p.QueueV2Message) []int64 {
	ids := make([]int64, len(messages))
	for index, message := range messages {
		ids[index] = message.MetaData.ID
	}
	return ids
}
