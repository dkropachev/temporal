package cassandra

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

func TestExecutionBackfillTargetRowsReplicatesFence(t *testing.T) {
	layout, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, 4)
	require.NoError(t, err)

	rows, err := executionBackfillTargetRows([]byte(`{"shard_id":3,"type":0,"task_id":-11,"migration_wt_range_id":101}`), layout)

	require.NoError(t, err)
	require.Len(t, rows, 4)
	require.Equal(t, []int64{9, 10, 11, 12}, backfillShardIDs(t, rows))
	require.Equal(t, int64(101), rows[0].timestamp)
}

func TestExecutionBackfillTargetRowsRoutesWorkflow(t *testing.T) {
	layout, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, 16)
	require.NoError(t, err)
	expected, err := layout.workflowPartition(3, "11111111-1111-1111-1111-111111111111", "workflow")
	require.NoError(t, err)

	rows, err := executionBackfillTargetRows([]byte(
		`{"shard_id":3,"type":1,"namespace_id":"11111111-1111-1111-1111-111111111111","workflow_id":"workflow","task_id":-10,"migration_wt_execution":102}`,
	), layout)

	require.NoError(t, err)
	require.Equal(t, []int64{int64(expected)}, backfillShardIDs(t, rows))
}

func TestExecutionBackfillTargetRowsRoutesTaskByWorkflow(t *testing.T) {
	layout, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, 16)
	require.NoError(t, err)
	expected, err := layout.workflowPartition(3, "namespace", "workflow")
	require.NoError(t, err)
	blob, err := serialization.NewSerializer().SerializeTask(&tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey("namespace", "workflow", "run"),
		TaskID:      23,
	})
	require.NoError(t, err)

	rows, err := executionBackfillTargetRows([]byte(fmt.Sprintf(
		`{"shard_id":3,"type":2,"namespace_id":"10000000-3000-f000-f000-000000000000","task_id":23,"transfer":"0x%s","transfer_encoding":"%s","migration_wt_transfer":103}`,
		hex.EncodeToString(blob.Data),
		blob.EncodingType.String(),
	)), layout)

	require.NoError(t, err)
	require.Equal(t, []int64{int64(expected)}, backfillShardIDs(t, rows))

	source := []byte(fmt.Sprintf(
		`{"shard_id":3,"type":2,"namespace_id":"10000000-3000-f000-f000-000000000000","workflow_id":"20000000-3000-f000-f000-000000000000","run_id":"30000000-3000-f000-f000-000000000000","visibility_ts":"2000-01-01 00:00:00.000Z","task_id":23,"transfer":"0x%s","transfer_encoding":"%s"}`,
		hex.EncodeToString(blob.Data),
		blob.EncodingType.String(),
	))
	wrongRow, err := decodeExecutionJSON(source)
	require.NoError(t, err)
	wrongRow["shard_id"] = expected + 1
	wrongTarget, err := json.Marshal(wrongRow)
	require.NoError(t, err)
	valid, err := executionValidationTargetPartition(wrongTarget, source, layout)
	require.NoError(t, err)
	require.False(t, valid)
}

func TestExecutionReconcileTargetRowKeepsShardSealed(t *testing.T) {
	layout, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, 16)
	require.NoError(t, err)

	reconciled, err := executionReconcileTargetRow(
		[]byte(`{"shard_id":17,"type":0,"task_id":-11,"range_id":9}`),
		layout,
	)
	require.NoError(t, err)
	row, err := decodeExecutionJSON(reconciled)
	require.NoError(t, err)
	authority, err := executionBackfillInt(row, "migration_authority")
	require.NoError(t, err)
	require.Equal(t, int(executionShardAuthoritySealing), authority)
	bucketCount, err := executionBackfillInt(row, "storage_bucket_count")
	require.NoError(t, err)
	require.Equal(t, 16, bucketCount)
}

func TestExecutionJSONEqualIgnoresAuthorityMetadata(t *testing.T) {
	require.True(t, executionJSONEqual(
		[]byte(`{"shard_id":1,"type":0,"migration_authority":1,"storage_bucket_count":16}`),
		[]byte(`{"shard_id":1,"type":0,"migration_authority":3,"storage_bucket_count":16}`),
	))
}

func backfillShardIDs(t *testing.T, rows []executionBackfillRow) []int64 {
	t.Helper()
	ids := make([]int64, len(rows))
	for i, row := range rows {
		decoder := json.NewDecoder(bytes.NewReader(row.data))
		decoder.UseNumber()
		row := make(map[string]any)
		require.NoError(t, decoder.Decode(&row))
		id, err := row["shard_id"].(json.Number).Int64()
		require.NoError(t, err)
		ids[i] = id
	}
	return ids
}
