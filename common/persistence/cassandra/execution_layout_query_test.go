package cassandra

import (
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

func TestExecutionTargetOnlyCurrentReadUsesV2Partition(t *testing.T) {
	layout, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, 16)
	require.NoError(t, err)
	expectedPartition, err := layout.workflowPartition(7, "namespace-id", "workflow-id")
	require.NoError(t, err)

	serializer := serialization.NewSerializer()
	executionState := &persistencespb.WorkflowExecutionState{
		RunId: "11111111-1111-1111-1111-111111111111",
		State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
	}
	executionStateBlob, err := serializer.WorkflowExecutionStateToBlob(executionState)
	require.NoError(t, err)
	currentRunID, err := gocql.ParseUUID(executionState.RunId)
	require.NoError(t, err)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, layout.query(templateGetCurrentExecutionQuery), stmt)
			require.NotContains(t, stmt, " FROM executions ")
			require.Equal(t, expectedPartition, args[0])
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*gocql.UUID) = currentRunID
				*dest[1].(*[]byte) = executionStateBlob.Data
				*dest[2].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
				return nil
			}}
		},
	}
	store := newMutableStateStore(session, serializer, log.NewNoopLogger(), layout)

	response, err := store.GetCurrentExecution(t.Context(), &p.GetCurrentExecutionRequest{
		ShardID:     7,
		NamespaceID: "namespace-id",
		WorkflowID:  "workflow-id",
	})

	require.NoError(t, err)
	require.Equal(t, executionState.RunId, response.RunID)
}

func TestExecutionTargetOnlyHistoryTaskReadMergesBuckets(t *testing.T) {
	layout, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, 4)
	require.NoError(t, err)
	partitions, err := layout.partitions(3)
	require.NoError(t, err)
	tasksByPartition := map[int32][]int64{
		partitions[0]: {4},
		partitions[1]: {1},
		partitions[3]: {3},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, layout.query(templateGetTransferTasksQuery), stmt)
			require.NotContains(t, stmt, " FROM executions ")
			partition := args[0].(int32)
			minTaskID := args[6].(int64)
			rows := make([][]any, 0, len(tasksByPartition[partition]))
			for _, taskID := range tasksByPartition[partition] {
				if taskID >= minTaskID {
					rows = append(rows, []any{taskID, []byte(fmt.Sprintf("task-%d", taskID)), enumspb.ENCODING_TYPE_PROTO3.String()})
				}
			}
			return &recordingQuery{iter: &recordingIter{scanRows: rows}}
		},
	}
	store := newMutableStateTaskStore(session, serialization.NewSerializer(), layout, log.NewNoopLogger())
	request := &p.GetHistoryTasksRequest{
		ShardID:             3,
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(10),
		BatchSize:           2,
	}

	first, err := store.GetHistoryTasks(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3}, historyTaskIDs(first.Tasks))
	require.NotEmpty(t, first.NextPageToken)

	request.NextPageToken = first.NextPageToken
	second, err := store.GetHistoryTasks(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, []int64{4}, historyTaskIDs(second.Tasks))
	require.Empty(t, second.NextPageToken)
}

func historyTaskIDs(historyTasks []p.InternalHistoryTask) []int64 {
	ids := make([]int64, len(historyTasks))
	for i, task := range historyTasks {
		ids[i] = task.Key.TaskID
	}
	return ids
}
