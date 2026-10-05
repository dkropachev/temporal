//go:build integration

package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/definition"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

const (
	testExecutionShardNamespaceID = "10000000-1000-f000-f000-000000000000"
	testExecutionShardWorkflowID  = "20000000-1000-f000-f000-000000000000"
	testExecutionShardRunID       = "30000000-1000-f000-f000-000000000000"
	testExecutionDLQNamespaceID   = "10000000-6000-f000-f000-000000000000"
	testExecutionDLQRunID         = "30000000-6000-f000-f000-000000000000"
)

func TestCassandraExecutionsV2BackfillAndValidation(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	visibility := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, session.Query(
		`INSERT INTO executions (shard_id,type,namespace_id,workflow_id,run_id,visibility_ts,task_id,range_id,shard,shard_encoding) VALUES (?,?,?,?,?,?,?,?,?,?)`,
		1, 0, "10000000-1000-f000-f000-000000000000", "20000000-1000-f000-f000-000000000000",
		"30000000-1000-f000-f000-000000000000", visibility, int64(-11), int64(1), []byte("shard"), "Proto3",
	).Exec())
	taskBlob, err := serialization.NewSerializer().SerializeTask(&tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			"11111111-1111-1111-1111-111111111111",
			"workflow",
			"22222222-2222-2222-2222-222222222222",
		),
		TaskID: 7,
	})
	require.NoError(t, err)
	require.NoError(t, session.Query(
		`INSERT INTO executions (shard_id,type,namespace_id,workflow_id,run_id,visibility_ts,task_id,execution,execution_encoding,db_record_version) VALUES (?,?,?,?,?,?,?,?,?,?)`,
		1, 1, "11111111-1111-1111-1111-111111111111", "workflow", "22222222-2222-2222-2222-222222222222",
		visibility, int64(-10), []byte("execution"), "Proto3", int64(1),
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO executions (shard_id,type,namespace_id,workflow_id,run_id,visibility_ts,task_id,transfer,transfer_encoding) VALUES (?,?,?,?,?,?,?,?,?)`,
		1, 2, "10000000-3000-f000-f000-000000000000", "20000000-3000-f000-f000-000000000000",
		"30000000-3000-f000-f000-000000000000", visibility, int64(7), taskBlob.Data, taskBlob.EncodingType.String(),
	).Exec())

	require.NoError(t, cassandra.PrepareExecutionsV2Backfill(t.Context(), session, testData.Cfg.Keyspace, 4, true))
	copied, err := cassandra.BackfillExecutionsV2(t.Context(), session, cassandra.ExecutionBackfillOptions{
		PageSize: 10, Concurrency: 2, TokenRangeCount: 1, StorageBuckets: 4,
	})
	require.NoError(t, err)
	require.Equal(t, int64(6), copied)
	result, err := cassandra.ValidateExecutionsV2(t.Context(), session, cassandra.ExecutionValidationOptions{
		PageSize: 10, TokenRangeCount: 1, StorageBuckets: 4,
	})
	require.NoError(t, err)
	require.True(t, result.Matches(), result.Mismatches)
	require.Equal(t, int64(3), result.SourceRows)
	require.Equal(t, int64(6), result.TargetRows)
}

func TestCassandraExecutionsV2TargetOnlyMutableState(t *testing.T) {
	testData, tearDown := setUpCassandraExecutionsV2TargetOnly(t)
	defer tearDown()
	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatal(err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatal(err)
	}
	suite.Run(t, NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	))
}

func TestCassandraExecutionsV2TargetOnlyHistoryTasks(t *testing.T) {
	testData, tearDown := setUpCassandraExecutionsV2TargetOnly(t)
	defer tearDown()
	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatal(err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatal(err)
	}
	suite.Run(t, NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	))
}

func TestCassandraExecutionsV2TargetOnlyHistory(t *testing.T) {
	testData, tearDown := setUpCassandraExecutionsV2TargetOnly(t)
	defer tearDown()
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatal(err)
	}
	suite.Run(t, NewHistoryEventsSuite(t, executionStore, testData.Logger))
}

func TestCassandraExecutionCutoverResumesAfterSourceSeal(t *testing.T) {
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.ExecutionMigrationMode = config.CassandraExecutionMigrationModeSourceDual
		cfg.ExecutionStorageBuckets = 4
	})
	defer tearDown()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	markExecutionLayoutTargetReady(t, session, testData)

	const shardID int32 = 1
	sourceShardStore, err := testData.Factory.NewShardStore()
	require.NoError(t, err)
	oldShard := executionTestBlob("old-shard")
	_, err = sourceShardStore.GetOrCreateShard(t.Context(), &p.InternalGetOrCreateShardRequest{
		ShardID: shardID,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 10, oldShard, nil
		},
	})
	require.NoError(t, err)

	const sourceCluster = "source-cluster"
	insertExecutionDLQRow(t, session, "executions", shardID, sourceCluster, 42)
	targetFactory := newExecutionTargetDualFactory(t, testData)
	defer targetFactory.Close()
	targetExecutionStore, err := targetFactory.NewExecutionStore()
	require.NoError(t, err)
	preCutover, err := targetExecutionStore.GetReplicationTasksFromDLQ(
		t.Context(),
		&p.GetReplicationTasksFromDLQRequest{
			GetHistoryTasksRequest: p.GetHistoryTasksRequest{
				ShardID:             shardID,
				TaskCategory:        tasks.CategoryReplication,
				InclusiveMinTaskKey: tasks.NewImmediateKey(0),
				ExclusiveMaxTaskKey: tasks.NewImmediateKey(100),
				BatchSize:           10,
			},
			SourceClusterName: sourceCluster,
		},
	)
	require.NoError(t, err)
	require.Len(t, preCutover.Tasks, 1)
	sealExecutionSourceForTest(t, session, shardID, 10, 11, 4)
	sealExecutionTargetPartitionForTest(t, session, 1, 4)

	sourceExecutionStore, err := testData.Factory.NewExecutionStore()
	require.NoError(t, err)
	err = sourceExecutionStore.DeleteReplicationTaskFromDLQ(t.Context(), &p.DeleteReplicationTaskFromDLQRequest{
		CompleteHistoryTaskRequest: p.CompleteHistoryTaskRequest{
			ShardID:      shardID,
			TaskCategory: tasks.CategoryReplication,
			TaskKey:      tasks.NewImmediateKey(42),
		},
		SourceClusterName: sourceCluster,
	})
	require.Error(t, err)
	require.True(t, executionDLQRowExists(t, session, "executions", shardID, sourceCluster, 42))

	targetShardStore, err := targetFactory.NewShardStore()
	require.NoError(t, err)
	_, err = targetShardStore.GetOrCreateShard(t.Context(), &p.InternalGetOrCreateShardRequest{ShardID: shardID})
	require.NoError(t, err)
	newShard := executionTestBlob("new-shard")
	require.NoError(t, targetShardStore.UpdateShard(t.Context(), &p.InternalUpdateShardRequest{
		ShardID: shardID, RangeID: 11, PreviousRangeID: 10, ShardInfo: newShard,
	}))
	requireExecutionAuthority(t, session, "executions", shardID, 11, 3, 4)
	for partition := int32(1); partition <= 4; partition++ {
		requireExecutionAuthority(t, session, "executions_v2", partition, 11, 3, 4)
	}

	require.NoError(t, targetExecutionStore.DeleteReplicationTaskFromDLQ(
		t.Context(),
		&p.DeleteReplicationTaskFromDLQRequest{
			CompleteHistoryTaskRequest: p.CompleteHistoryTaskRequest{
				ShardID: shardID, TaskCategory: tasks.CategoryReplication, TaskKey: tasks.NewImmediateKey(42),
			},
			SourceClusterName: sourceCluster,
		},
	))
	require.False(t, executionDLQRowExists(t, session, "executions", shardID, sourceCluster, 42))
	require.False(t, executionDLQRowExistsInTarget(t, session, shardID, sourceCluster, 42, 4))

	insertExecutionDLQRow(t, session, "executions", shardID, "must-not-reconcile", 99)
	require.NoError(t, targetShardStore.UpdateShard(t.Context(), &p.InternalUpdateShardRequest{
		ShardID: shardID, RangeID: 12, PreviousRangeID: 11, ShardInfo: executionTestBlob("renewed-shard"),
	}))
	require.False(t, executionDLQRowExistsInTarget(t, session, shardID, "must-not-reconcile", 99, 4))
}

func TestCassandraExecutionSourceDeleteRaceWithSealConverges(t *testing.T) {
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.ExecutionMigrationMode = config.CassandraExecutionMigrationModeSourceDual
		cfg.ExecutionStorageBuckets = 4
	})
	defer tearDown()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	markExecutionLayoutTargetReady(t, session, testData)

	const shardID int32 = 2
	sourceShardStore, err := testData.Factory.NewShardStore()
	require.NoError(t, err)
	_, err = sourceShardStore.GetOrCreateShard(t.Context(), &p.InternalGetOrCreateShardRequest{
		ShardID: shardID,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 20, executionTestBlob("source"), nil
		},
	})
	require.NoError(t, err)
	sourceExecutionStore, err := testData.Factory.NewExecutionStore()
	require.NoError(t, err)
	const sourceCluster = "racing-cluster"
	require.NoError(t, sourceExecutionStore.PutReplicationTaskToDLQ(
		t.Context(),
		&p.PutReplicationTaskToDLQRequest{
			ShardID: shardID, SourceClusterName: sourceCluster,
			TaskInfo: &persistencespb.ReplicationTaskInfo{TaskId: 7},
		},
	))

	targetFactory := newExecutionTargetDualFactory(t, testData)
	defer targetFactory.Close()
	targetShardStore, err := targetFactory.NewShardStore()
	require.NoError(t, err)
	_, err = targetShardStore.GetOrCreateShard(t.Context(), &p.InternalGetOrCreateShardRequest{ShardID: shardID})
	require.NoError(t, err)

	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	var deleteErr, cutoverErr error
	go func() {
		defer waitGroup.Done()
		<-start
		deleteErr = sourceExecutionStore.DeleteReplicationTaskFromDLQ(
			t.Context(),
			&p.DeleteReplicationTaskFromDLQRequest{
				CompleteHistoryTaskRequest: p.CompleteHistoryTaskRequest{
					ShardID: shardID, TaskCategory: tasks.CategoryReplication, TaskKey: tasks.NewImmediateKey(7),
				},
				SourceClusterName: sourceCluster,
			},
		)
	}()
	go func() {
		defer waitGroup.Done()
		<-start
		cutoverErr = targetShardStore.UpdateShard(t.Context(), &p.InternalUpdateShardRequest{
			ShardID: shardID, RangeID: 21, PreviousRangeID: 20, ShardInfo: executionTestBlob("target"),
		})
	}()
	close(start)
	waitGroup.Wait()
	require.NoError(t, cutoverErr)
	if deleteErr != nil {
		require.ErrorContains(t, deleteErr, "authority")
	}
	sourceExists := executionDLQRowExists(t, session, "executions", shardID, sourceCluster, 7)
	targetExists := executionDLQRowExistsInTarget(t, session, shardID, sourceCluster, 7, 4)
	require.Equal(t, sourceExists, targetExists)
}

func executionTestBlob(data string) *commonpb.DataBlob {
	return &commonpb.DataBlob{Data: []byte(data), EncodingType: enumspb.ENCODING_TYPE_PROTO3}
}

func markExecutionLayoutTargetReady(
	t *testing.T,
	session commongocql.Session,
	testData CassandraTestData,
) {
	t.Helper()
	spec, err := cassandra.SchemaLayoutSpecForTable(
		t.Context(),
		session,
		testData.Cfg.Keyspace,
		cassandra.SchemaLayoutExecutions,
		"executions_v2",
		4,
	)
	require.NoError(t, err)
	store := cassandra.NewSchemaLayoutMetadataStore(session)
	_, err = store.InitializePreparing(t.Context(), spec)
	require.NoError(t, err)
	_, err = store.MarkTargetReady(t.Context(), spec)
	require.NoError(t, err)
}

func newExecutionTargetDualFactory(
	t *testing.T,
	testData CassandraTestData,
) *cassandra.Factory {
	t.Helper()
	targetConfig := *testData.Cfg
	targetConfig.ExecutionMigrationMode = config.CassandraExecutionMigrationModeTargetDual
	session := newCassandraTestSession(t, &targetConfig, testData.Logger)
	return cassandra.NewFactoryFromSession(
		targetConfig,
		testCassandraClusterName,
		testData.Logger,
		session,
		serialization.NewSerializer(),
	)
}

func sealExecutionSourceForTest(
	t *testing.T,
	session commongocql.Session,
	shardID int32,
	previousRangeID int64,
	rangeID int64,
	bucketCount int,
) {
	t.Helper()
	applied, err := session.Query(
		`UPDATE executions SET range_id = ?, migration_authority = ? `+
			`WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? `+
			`AND run_id = ? AND visibility_ts = ? AND task_id = ? `+
			`IF range_id = ? AND migration_authority = ? AND storage_bucket_count = ?`,
		rangeID,
		2,
		shardID,
		0,
		testExecutionShardNamespaceID,
		testExecutionShardWorkflowID,
		testExecutionShardRunID,
		time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
		int64(-11),
		previousRangeID,
		1,
		bucketCount,
	).WithContext(t.Context()).MapScanCAS(make(map[string]any))
	require.NoError(t, err)
	require.True(t, applied)
}

func requireExecutionAuthority(
	t *testing.T,
	session commongocql.Session,
	table string,
	shardID int32,
	rangeID int64,
	authority int,
	bucketCount int,
) {
	t.Helper()
	var actualRangeID int64
	var actualAuthority, actualBucketCount int
	err := session.Query(
		fmt.Sprintf(
			`SELECT range_id, migration_authority, storage_bucket_count FROM %s `+
				`WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? `+
				`AND run_id = ? AND visibility_ts = ? AND task_id = ?`,
			table,
		),
		shardID,
		0,
		testExecutionShardNamespaceID,
		testExecutionShardWorkflowID,
		testExecutionShardRunID,
		time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
		int64(-11),
	).WithContext(t.Context()).Scan(&actualRangeID, &actualAuthority, &actualBucketCount)
	require.NoError(t, err)
	require.Equal(t, rangeID, actualRangeID)
	require.Equal(t, authority, actualAuthority)
	require.Equal(t, bucketCount, actualBucketCount)
}

func sealExecutionTargetPartitionForTest(
	t *testing.T,
	session commongocql.Session,
	partition int32,
	bucketCount int,
) {
	t.Helper()
	applied, err := session.Query(
		`UPDATE executions_v2 SET migration_authority = ? `+
			`WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? `+
			`AND run_id = ? AND visibility_ts = ? AND task_id = ? `+
			`IF migration_authority = ? AND storage_bucket_count = ?`,
		2,
		partition,
		0,
		testExecutionShardNamespaceID,
		testExecutionShardWorkflowID,
		testExecutionShardRunID,
		time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
		int64(-11),
		1,
		bucketCount,
	).WithContext(t.Context()).MapScanCAS(make(map[string]any))
	require.NoError(t, err)
	require.True(t, applied)
}

func insertExecutionDLQRow(
	t *testing.T,
	session commongocql.Session,
	table string,
	shardID int32,
	sourceCluster string,
	taskID int64,
) {
	t.Helper()
	require.NoError(t, session.Query(
		fmt.Sprintf(
			`INSERT INTO %s (shard_id, type, namespace_id, workflow_id, run_id, replication, `+
				`replication_encoding, visibility_ts, task_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			table,
		),
		shardID,
		5,
		testExecutionDLQNamespaceID,
		sourceCluster,
		testExecutionDLQRunID,
		[]byte("task"),
		enumspb.ENCODING_TYPE_PROTO3.String(),
		time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
		taskID,
	).WithContext(t.Context()).Exec())
}

func executionDLQRowExists(
	t *testing.T,
	session commongocql.Session,
	table string,
	shardID int32,
	sourceCluster string,
	taskID int64,
) bool {
	t.Helper()
	var data []byte
	err := session.Query(
		fmt.Sprintf(
			`SELECT replication FROM %s WHERE shard_id = ? AND type = ? AND namespace_id = ? `+
				`AND workflow_id = ? AND run_id = ? AND visibility_ts = ? AND task_id = ?`,
			table,
		),
		shardID,
		5,
		testExecutionDLQNamespaceID,
		sourceCluster,
		testExecutionDLQRunID,
		time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
		taskID,
	).WithContext(t.Context()).Scan(&data)
	if commongocql.IsNotFoundError(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

func executionDLQRowExistsInTarget(
	t *testing.T,
	session commongocql.Session,
	logicalShardID int32,
	sourceCluster string,
	taskID int64,
	bucketCount int32,
) bool {
	t.Helper()
	firstPartition := (logicalShardID-1)*bucketCount + 1
	for partition := firstPartition; partition < firstPartition+bucketCount; partition++ {
		if executionDLQRowExists(t, session, "executions_v2", partition, sourceCluster, taskID) {
			return true
		}
	}
	return false
}

func setUpCassandraExecutionsV2TargetOnly(t testing.TB) (CassandraTestData, func()) {
	t.Helper()
	testData, tearDown := setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.ExecutionMigrationMode = config.CassandraExecutionMigrationModeTargetOnly
		cfg.ExecutionStorageBuckets = 16
		cfg.HistoryNodeMigrationMode = config.CassandraHistoryNodeMigrationModeV2Only
		cfg.HistoryTreeMigrationMode = config.CassandraHistoryTreeMigrationModeTargetOnly
	})
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	for _, table := range []string{"executions", "history_node", "history_tree"} {
		if err := session.Query("DROP TABLE " + table).Exec(); err != nil {
			session.Close()
			tearDown()
			t.Fatal(err)
		}
	}
	session.Close()
	return testData, tearDown
}
