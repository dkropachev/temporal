package cassandra

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.uber.org/mock/gomock"
)

func TestMatchingTaskStorageBucketRotatesByRange(t *testing.T) {
	for rangeID, expected := range map[int64]int16{
		1:  0,
		2:  1,
		16: 15,
		17: 0,
		18: 1,
	} {
		bucket, err := matchingTaskStorageBucket(rangeID, 16)
		require.NoError(t, err)
		require.Equal(t, expected, bucket)
	}

	_, err := matchingTaskStorageBucket(0, 16)
	require.Error(t, err)
	_, err = matchingTaskStorageBucket(1, 0)
	require.Error(t, err)
}

func TestMatchingTaskMigrationModeValidation(t *testing.T) {
	for _, mode := range []MatchingTaskMigrationMode{
		"",
		MatchingTaskMigrationModeSourceOnly,
		MatchingTaskMigrationModeSourceDual,
		MatchingTaskMigrationModeTargetDual,
		MatchingTaskMigrationModeTargetOnly,
	} {
		require.NoError(t, ValidateMatchingTaskMigrationMode(mode))
	}
	require.Error(t, ValidateMatchingTaskMigrationMode("unknown"))
}

func TestMatchingTaskMigrationQueriesFenceAuthority(t *testing.T) {
	target := newMatchingTaskStoreV3(nil, false, 16)
	require.Contains(t, target.metadataUpdateQuery(false), "AND migration_authority = ?")
	require.Contains(t, target.metadataFenceQuery(false), "AND migration_authority = ?")
	require.Contains(t, target.metadataDeleteQuery(), "AND migration_authority = ?")
	require.Contains(t, target.metadataClearTransitionQuery(), "AND migration_authority = ?")
	require.Contains(t, target.metadataPromoteAuthorityQuery(), "IF range_id = ? AND metadata_state = ? AND bucket_count = ? AND migration_authority = ?")

	sourceV2 := switchTasksTable(templateUpdateTaskQueueWithMigrationQuery, matchingTaskVersion2)
	sourceV1 := switchTasksTable(templateUpdateTaskQueueWithMigrationQuery, matchingTaskVersion1)
	for _, query := range []string{sourceV1, sourceV2} {
		require.Contains(t, query, "migration_authority = ?")
		require.Contains(t, query, "migration_bucket_count = ?")
	}
}

func TestMatchingTaskBackfillReadsSourceWriteTimes(t *testing.T) {
	for _, fair := range []bool{false, true} {
		query := matchingTaskBackfillScanQuery(fair)
		require.Contains(t, query, "WRITETIME(task)")
		require.Contains(t, query, "WRITETIME(task_encoding)")
		require.Contains(t, query, "WRITETIME(range_id)")
		require.Contains(t, query, "WRITETIME(task_queue)")
		require.Contains(t, query, "WRITETIME(task_queue_encoding)")
	}
}

func TestMatchingTaskV3ClassicReadBuckets(t *testing.T) {
	store := newMatchingTaskStoreV3(nil, false, 16)
	require.Equal(t, []int16{0, 1}, store.readBuckets(&p.GetTasksRequest{
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: 100,
		TaskIDRangeSize:    100000,
		TaskIDMaxBatchSize: 1000,
	}))
	require.Equal(t, []int16{0, 1, 2}, store.readBuckets(&p.GetTasksRequest{
		InclusiveMinTaskID: 99999,
		ExclusiveMaxTaskID: 100002,
		TaskIDRangeSize:    100000,
		TaskIDMaxBatchSize: 1000,
	}))
	require.Len(t, store.readBuckets(&p.GetTasksRequest{
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: math.MaxInt64,
		TaskIDRangeSize:    100000,
		TaskIDMaxBatchSize: 1000,
	}), 16)
	require.Len(t, newMatchingTaskStoreV3(nil, true, 16).readBuckets(&p.GetTasksRequest{
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: 100,
		TaskIDRangeSize:    100000,
		TaskIDMaxBatchSize: 1000,
	}), 16)
	require.Len(t, store.readBuckets(&p.GetTasksRequest{
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: 100,
		TaskIDRangeSize:    10,
		TaskIDMaxBatchSize: 11,
	}), 16)
}

func TestMatchingTaskV3PageTokenRoundTrip(t *testing.T) {
	for _, expected := range []matchingTaskV3PageToken{
		{fair: false, id: 42},
		{fair: true, pass: 101, id: 202},
	} {
		encoded := encodeMatchingTaskV3PageToken(expected)
		actual, err := decodeMatchingTaskV3PageToken(encoded, expected.fair)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}

	_, err := decodeMatchingTaskV3PageToken([]byte("legacy-cassandra-state"), false)
	require.Error(t, err)
	_, err = decodeMatchingTaskV3PageToken(
		encodeMatchingTaskV3PageToken(matchingTaskV3PageToken{fair: true, pass: 1, id: 1}),
		false,
	)
	require.Error(t, err)
}

func TestMatchingTaskV3DeduplicatesBackfillRace(t *testing.T) {
	blob := p.NewDataBlob([]byte("task"), enumspb.ENCODING_TYPE_PROTO3.String())
	rows, err := deduplicateMatchingTaskV3Rows([]matchingTaskV3Row{
		{id: 1, blob: blob},
		{id: 1, blob: p.NewDataBlob([]byte("task"), enumspb.ENCODING_TYPE_PROTO3.String())},
		{id: 2, blob: blob},
	}, false)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	_, err = deduplicateMatchingTaskV3Rows([]matchingTaskV3Row{
		{id: 1, blob: blob},
		{id: 1, blob: p.NewDataBlob([]byte("different"), enumspb.ENCODING_TYPE_PROTO3.String())},
	}, false)
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
}

func TestMatchingTaskTargetOnlyClassicReadNeverQueriesSource(t *testing.T) {
	controller := gomock.NewController(t)
	source := mock.NewMockTaskStore(controller)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Contains(t, stmt, "FROM tasks_v3 ")
			require.NotContains(t, stmt, "FROM tasks ")
			bucket := args[3].(int16)
			rows := [][]any{}
			switch bucket {
			case 0:
				rows = [][]any{{int64(1), []byte("one"), enumspb.ENCODING_TYPE_PROTO3.String()}}
			case 1:
				rows = [][]any{{int64(2), []byte("two"), enumspb.ENCODING_TYPE_PROTO3.String()}}
			default:
				require.FailNow(t, "unexpected bucket", "bucket %d", bucket)
			}
			return &recordingQuery{iter: &recordingIter{scanRows: rows}}
		},
	}
	store := &matchingTaskMigrationStore{
		TaskStore: source,
		target:    newMatchingTaskStoreV3(session, false, 2),
		mode:      MatchingTaskMigrationModeTargetOnly,
	}

	response, err := store.GetTasks(t.Context(), &p.GetTasksRequest{
		NamespaceID:        "11111111-1111-1111-1111-111111111111",
		TaskQueue:          "queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: math.MaxInt64,
		PageSize:           10,
	})
	require.NoError(t, err)
	require.Equal(t, []*commonpb.DataBlob{
		p.NewDataBlob([]byte("one"), enumspb.ENCODING_TYPE_PROTO3.String()),
		p.NewDataBlob([]byte("two"), enumspb.ENCODING_TYPE_PROTO3.String()),
	}, response.Tasks)
	require.Empty(t, response.NextPageToken)
	require.Len(t, session.queries, 2)
}

func TestMatchingTaskTargetOnlyFairReadMergesAndPaginates(t *testing.T) {
	controller := gomock.NewController(t)
	source := mock.NewMockTaskStore(controller)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Contains(t, stmt, "FROM tasks_v3_fair ")
			bucket := args[3].(int16)
			rows := [][]any{}
			switch bucket {
			case 0:
				rows = [][]any{{int64(1), int64(2), []byte("two"), enumspb.ENCODING_TYPE_PROTO3.String()}}
			case 1:
				rows = [][]any{
					{int64(1), int64(1), []byte("one"), enumspb.ENCODING_TYPE_PROTO3.String()},
					{int64(2), int64(3), []byte("three"), enumspb.ENCODING_TYPE_PROTO3.String()},
				}
			default:
				require.FailNow(t, "unexpected bucket", "bucket %d", bucket)
			}
			return &recordingQuery{iter: &recordingIter{scanRows: rows}}
		},
	}
	store := &matchingTaskMigrationStore{
		TaskStore: source,
		target:    newMatchingTaskStoreV3(session, true, 2),
		mode:      MatchingTaskMigrationModeTargetOnly,
	}

	response, err := store.GetTasks(t.Context(), &p.GetTasksRequest{
		NamespaceID:        "11111111-1111-1111-1111-111111111111",
		TaskQueue:          "queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinPass:   1,
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: math.MaxInt64,
		PageSize:           2,
		UseLimit:           true,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("one"), response.Tasks[0].Data)
	require.Equal(t, []byte("two"), response.Tasks[1].Data)
	require.NotEmpty(t, response.NextPageToken)

	token, err := decodeMatchingTaskV3PageToken(response.NextPageToken, true)
	require.NoError(t, err)
	require.Equal(t, int64(1), token.pass)
	require.Equal(t, int64(3), token.id)
	for _, query := range session.queries {
		require.Contains(t, query.stmt, "tasks_v3_fair")
	}
}
