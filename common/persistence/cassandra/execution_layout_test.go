package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
)

func TestExecutionLayoutLegacy(t *testing.T) {
	layout, err := newExecutionLayout("", 0)
	require.NoError(t, err)
	require.Equal(t, int32(7), mustWorkflowPartition(t, layout, 7, "namespace", "workflow"))
	require.Equal(t, []int32{7}, mustExecutionPartitions(t, layout, 7))
	require.Equal(t, "SELECT * FROM executions WHERE shard_id = ?", layout.query("SELECT * FROM executions WHERE shard_id = ?"))
}

func TestExecutionLayoutTargetOnly(t *testing.T) {
	layout, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, 16)
	require.NoError(t, err)

	first := mustWorkflowPartition(t, layout, 7, "namespace", "workflow")
	require.Equal(t, first, mustWorkflowPartition(t, layout, 7, "namespace", "workflow"))
	require.GreaterOrEqual(t, first, int32(97))
	require.LessOrEqual(t, first, int32(112))
	require.Equal(t, "SELECT * FROM executions_v2 WHERE shard_id = ?", layout.query("SELECT * FROM executions WHERE shard_id = ?"))
	require.Equal(
		t,
		"UPDATE executions_v2 SET child_executions_map = ? WHERE shard_id = ?",
		layout.query("UPDATE executions SET child_executions_map = ? WHERE shard_id = ?"),
	)
	require.Equal(t, []int32{97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112}, mustExecutionPartitions(t, layout, 7))
}

func TestExecutionLayoutRejectsInvalidConfiguration(t *testing.T) {
	for _, buckets := range []int{3, -1, 2048} {
		_, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, buckets)
		require.Error(t, err)
	}
	_, err := newExecutionLayout("unknown", 16)
	require.Error(t, err)
}

func TestExecutionLayoutDualAuthority(t *testing.T) {
	rebuild, err := newExecutionLayout(config.CassandraExecutionMigrationModeSourceRebuild, 16)
	require.NoError(t, err)
	require.False(t, rebuild.mirrorRequired())
	_, ok := rebuild.mirrorLayout()
	require.True(t, ok)

	source, err := newExecutionLayout(config.CassandraExecutionMigrationModeSourceDual, 16)
	require.NoError(t, err)
	require.False(t, source.isTarget())
	sourcePrimary := source.authoritativeLayout()
	require.Equal(t, config.CassandraExecutionMigrationModeLegacy, sourcePrimary.mode)
	require.Equal(t, executionShardAuthoritySource, sourcePrimary.authority)
	require.Equal(t, int32(16), sourcePrimary.buckets)
	targetMirror, ok := source.mirrorLayout()
	require.True(t, ok)
	require.True(t, targetMirror.isTarget())
	require.Equal(t, executionShardAuthoritySource, targetMirror.authority)

	target, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetDual, 16)
	require.NoError(t, err)
	require.True(t, target.isTarget())
	legacyMirror, ok := target.mirrorLayout()
	require.True(t, ok)
	require.False(t, legacyMirror.isTarget())
	require.Equal(t, executionShardAuthorityTarget, target.authoritativeLayout().authority)
	require.Equal(t, executionShardAuthorityTarget, legacyMirror.authority)
}

func mustWorkflowPartition(t *testing.T, layout executionLayout, shardID int32, namespaceID string, workflowID string) int32 {
	t.Helper()
	partition, err := layout.workflowPartition(shardID, namespaceID, workflowID)
	require.NoError(t, err)
	return partition
}

func mustExecutionPartitions(t *testing.T, layout executionLayout, shardID int32) []int32 {
	t.Helper()
	partitions, err := layout.partitions(shardID)
	require.NoError(t, err)
	return partitions
}
