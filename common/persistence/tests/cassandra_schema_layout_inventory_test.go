//go:build integration

package tests

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence/cassandra"
)

func TestCassandraHistoryTargetOnlyInventoryAudits(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()

	const (
		treeID         = "11111111-1111-1111-1111-111111111111"
		branchID       = "22222222-2222-2222-2222-222222222222"
		authorityID    = "00000000-0000-0000-0000-000000000000"
		writeTimestamp = int64(1234567)
	)
	for _, table := range []string{"history_node", "history_node_v2"} {
		require.NoError(t, session.Query(
			`INSERT INTO `+table+` (tree_id, branch_id, node_id, txn_id, prev_txn_id, data, data_encoding) VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
			treeID,
			branchID,
			int64(3),
			int64(4),
			int64(2),
			[]byte("events"),
			"Proto3",
			writeTimestamp,
		).Exec())
	}
	nodeSpec, err := cassandra.SchemaLayoutSpecForTable(
		t.Context(),
		session,
		testData.Cfg.Keyspace,
		cassandra.SchemaLayoutHistoryNode,
		"history_node_v2",
		0,
	)
	require.NoError(t, err)
	result, err := cassandra.AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		nodeSpec,
		cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.SourceEntities)
	require.Equal(t, int64(1), result.TargetEntities)

	bucket := historyTreeInventoryBucket(t, branchID)
	require.NoError(t, session.Query(
		`INSERT INTO history_tree (tree_id, branch_id, branch, branch_encoding) VALUES (?, ?, ?, ?) USING TIMESTAMP ?`,
		treeID,
		branchID,
		[]byte("branch"),
		"Proto3",
		writeTimestamp,
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO history_tree (tree_id, branch_id, migration_authority, migration_timestamp) VALUES (?, ?, ?, ?)`,
		treeID,
		authorityID,
		3,
		int64(10),
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO history_tree_v2 (tree_id, branch_bucket, branch_id, branch, branch_encoding) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		treeID,
		bucket,
		branchID,
		[]byte("branch"),
		"Proto3",
		writeTimestamp,
	).Exec())
	treeSpec, err := cassandra.SchemaLayoutSpecForTable(
		t.Context(),
		session,
		testData.Cfg.Keyspace,
		cassandra.SchemaLayoutHistoryTree,
		"history_tree_v2",
		16,
	)
	require.NoError(t, err)
	result, err = cassandra.AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		treeSpec,
		cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.SourceEntities)
	require.Equal(t, int64(1), result.TargetEntities)

	require.NoError(t, session.Query(
		`DELETE FROM history_node_v2 WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ?`,
		treeID,
		branchID,
		int64(3),
		int64(4),
	).Exec())
	_, err = cassandra.AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		nodeSpec,
		cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2},
	)
	require.ErrorContains(t, err, "missing-target=1")

	require.NoError(t, session.Query(
		`UPDATE history_tree SET migration_authority = ? WHERE tree_id = ? AND branch_id = ?`,
		1,
		treeID,
		authorityID,
	).Exec())
	_, err = cassandra.AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		treeSpec,
		cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2},
	)
	require.ErrorContains(t, err, "not target-authoritative")
}

func TestCassandraReverseTargetOnlyInventoryAudits(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()

	t.Run("executions", func(t *testing.T) {
		const insertShard = `INSERT INTO %s
			(shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id, range_id,
			migration_authority, storage_bucket_count) VALUES (?, 0, ?, ?, ?, ?, -11, 7, 3, 1)`
		visibility := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
		for _, table := range []string{"executions", "executions_v2"} {
			require.NoError(t, session.Query(
				fmt.Sprintf(insertShard, table),
				1,
				"10000000-1000-f000-f000-000000000000",
				"20000000-1000-f000-f000-000000000000",
				"30000000-1000-f000-f000-000000000000",
				visibility,
			).Exec())
		}
		spec, err := cassandra.SchemaLayoutSpecForTable(
			t.Context(), session, testData.Cfg.Keyspace, cassandra.SchemaLayoutExecutions, "executions_v2", 1,
		)
		require.NoError(t, err)
		result, err := cassandra.AuditSchemaLayoutTargetOnlyInventory(
			t.Context(), session, spec, cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2, ExpectedExecutionShards: 1},
		)
		require.NoError(t, err)
		require.Equal(t, cassandra.SchemaLayoutInventoryAuditResult{SourceEntities: 1, TargetEntities: 1}, result)

		require.NoError(t, session.Query(
			fmt.Sprintf(insertShard, "executions_v2"),
			2,
			"10000000-1000-f000-f000-000000000000",
			"20000000-1000-f000-f000-000000000000",
			"30000000-1000-f000-f000-000000000000",
			visibility,
		).Exec())
		_, err = cassandra.AuditSchemaLayoutTargetOnlyInventory(
			t.Context(), session, spec, cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2, ExpectedExecutionShards: 1},
		)
		require.ErrorContains(t, err, "orphan partition 2")
	})

	for _, fair := range []bool{false, true} {
		name := "matching-classic"
		targetTable := "tasks_v3"
		layoutName := cassandra.SchemaLayoutMatchingTasks
		sourceInsert := `INSERT INTO tasks
			(namespace_id, task_queue_name, task_queue_type, type, task_id, range_id, migration_authority,
			migration_bucket_count, migration_timestamp, task_queue, task_queue_encoding)
			VALUES (?, ?, 1, 1, -12345, 1, 3, 1, 1, ?, 'Proto3')`
		targetInsert := `INSERT INTO tasks_v3
			(namespace_id, task_queue_name, task_queue_type, storage_bucket, type, task_id, range_id,
			bucket_count, migration_authority, metadata_state, task_queue, task_queue_encoding)
			VALUES (?, ?, 1, 0, 1, -12345, 1, 1, 3, 1, ?, 'Proto3')`
		if fair {
			name = "matching-fair"
			targetTable = "tasks_v3_fair"
			layoutName = cassandra.SchemaLayoutMatchingTasksFair
			sourceInsert = `INSERT INTO tasks_v2
				(namespace_id, task_queue_name, task_queue_type, type, pass, task_id, range_id, migration_authority,
				migration_bucket_count, migration_timestamp, task_queue, task_queue_encoding)
				VALUES (?, ?, 1, 1, 0, -12345, 1, 3, 1, 1, ?, 'Proto3')`
			targetInsert = `INSERT INTO tasks_v3_fair
				(namespace_id, task_queue_name, task_queue_type, storage_bucket, type, pass, task_id, range_id,
				bucket_count, migration_authority, metadata_state, task_queue, task_queue_encoding)
				VALUES (?, ?, 1, 0, 1, 0, -12345, 1, 1, 3, 1, ?, 'Proto3')`
		}
		t.Run(name, func(t *testing.T) {
			const namespaceID = "41111111-1111-1111-1111-111111111111"
			require.NoError(t, session.Query(sourceInsert, namespaceID, "queue", []byte("metadata")).Exec())
			require.NoError(t, session.Query(targetInsert, namespaceID, "queue", []byte("metadata")).Exec())
			spec, err := cassandra.SchemaLayoutSpecForTable(
				t.Context(), session, testData.Cfg.Keyspace, layoutName, targetTable, 1,
			)
			require.NoError(t, err)
			result, err := cassandra.AuditSchemaLayoutTargetOnlyInventory(
				t.Context(), session, spec, cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2},
			)
			require.NoError(t, err)
			require.Equal(t, cassandra.SchemaLayoutInventoryAuditResult{SourceEntities: 1, TargetEntities: 1}, result)

			require.NoError(t, session.Query(targetInsert, namespaceID, "orphan", []byte("metadata")).Exec())
			_, err = cassandra.AuditSchemaLayoutTargetOnlyInventory(
				t.Context(), session, spec, cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2},
			)
			require.ErrorContains(t, err, "has no valid source")
			require.NoError(t, session.Query(
				"DELETE FROM "+targetTable+" WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = 1 AND storage_bucket = 0",
				namespaceID,
				"orphan",
			).Exec())
		})
	}

	t.Run("task-queue-user-data", func(t *testing.T) {
		const namespaceID = "51111111-1111-1111-1111-111111111111"
		const orphanNamespaceID = "52222222-2222-2222-2222-222222222222"
		spec, err := cassandra.SchemaLayoutSpecForTable(
			t.Context(), session, testData.Cfg.Keyspace, cassandra.SchemaLayoutTaskQueueUserData, "task_queue_user_data_v2", 1,
		)
		require.NoError(t, err)
		require.NoError(t, session.Query(
			`UPDATE task_queue_user_data SET migration_authority = 3, migration_bucket_count = 1,
			migration_generation = ? WHERE namespace_id = ?`,
			spec.Generation,
			namespaceID,
		).Exec())
		require.NoError(t, session.Query(
			`UPDATE task_queue_user_data_v2_txn SET migration_authority = 3, migration_bucket_count = 1,
			migration_generation = ? WHERE namespace_id = ? AND txn_id = ''`,
			spec.Generation,
			namespaceID,
		).Exec())
		require.NoError(t, session.Query(
			`INSERT INTO task_queue_user_data_v2 (namespace_id, bucket_id, build_id, task_queue_name, present)
			VALUES (?, 0, '', 'queue', true)`,
			namespaceID,
		).Exec())
		result, err := cassandra.AuditSchemaLayoutTargetOnlyInventory(
			t.Context(), session, spec, cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2},
		)
		require.NoError(t, err)
		require.Equal(t, cassandra.SchemaLayoutInventoryAuditResult{SourceEntities: 1, TargetEntities: 1}, result)

		require.NoError(t, session.Query(
			`INSERT INTO task_queue_user_data_v2 (namespace_id, bucket_id, build_id, task_queue_name, present)
			VALUES (?, 0, '', 'orphan', true)`,
			orphanNamespaceID,
		).Exec())
		_, err = cassandra.AuditSchemaLayoutTargetOnlyInventory(
			t.Context(), session, spec, cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2},
		)
		require.ErrorContains(t, err, "has no valid source")
	})

	t.Run("legacy-queue", func(t *testing.T) {
		generation, err := gocql.RandomUUID()
		require.NoError(t, err)
		require.NoError(t, session.Query(
			`INSERT INTO queue (queue_type, message_id, migration_authority, migration_generation, message_bucket_size)
			VALUES (7, ?, 3, ?, 4096)`,
			int64(math.MinInt64),
			generation,
		).Exec())
		require.NoError(t, session.Query(
			`INSERT INTO legacy_queue_v2_state (queue_type, active_bucket, minimum_message_id, cleanup_message_id,
			version, migration_authority, migration_generation, message_bucket_size) VALUES (7, 0, 0, 0, 0, 3, ?, 4096)`,
			generation,
		).Exec())
		require.NoError(t, session.Query(
			`INSERT INTO legacy_queue_v2_messages (queue_type, bucket_id, row_type, message_id, last_message_id,
			version, migration_authority, migration_generation, message_bucket_size) VALUES (7, 0, 0, -1, -1, 0, 3, ?, 4096)`,
			generation,
		).Exec())
		require.NoError(t, session.Query(
			`UPDATE legacy_queue_v2_delete_ranges SET migration_authority = 3, migration_generation = ?,
			message_bucket_size = 4096 WHERE queue_type = 7`,
			generation,
		).Exec())
		spec, err := cassandra.SchemaLayoutSpecForTable(
			t.Context(), session, testData.Cfg.Keyspace, cassandra.SchemaLayoutLegacyQueue, "legacy_queue_v2_messages", 4096,
		)
		require.NoError(t, err)
		result, err := cassandra.AuditSchemaLayoutTargetOnlyInventory(
			t.Context(), session, spec, cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2},
		)
		require.NoError(t, err)
		require.Equal(t, cassandra.SchemaLayoutInventoryAuditResult{SourceEntities: 1, TargetEntities: 1}, result)

		require.NoError(t, session.Query(
			`INSERT INTO legacy_queue_v2_state (queue_type, active_bucket, minimum_message_id, cleanup_message_id,
			version, migration_authority, migration_generation, message_bucket_size) VALUES (8, 0, 0, 0, 0, 3, ?, 4096)`,
			generation,
		).Exec())
		_, err = cassandra.AuditSchemaLayoutTargetOnlyInventory(
			t.Context(), session, spec, cassandra.SchemaLayoutInventoryAuditOptions{PageSize: 2},
		)
		require.ErrorContains(t, err, "has no source authority")
	})
}

func historyTreeInventoryBucket(t *testing.T, branchID string) int {
	t.Helper()
	uuid, err := gocql.ParseUUID(branchID)
	require.NoError(t, err)
	hash := uint32(2166136261)
	for _, value := range uuid {
		hash ^= uint32(value)
		hash *= 16777619
	}
	return int(hash % 16)
}
