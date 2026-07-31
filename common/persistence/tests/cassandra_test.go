package tests

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/common/persistence/serialization"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/util"
)

type (
	// failingSession is a [gocql.Session] which fails any query whose template matches a string in failingQueries.
	failingSession struct {
		gocql.Session
		failingQueries []string
	}
	// failingQuery is a [gocql.Query] which fails when executed.
	failingQuery struct {
		gocql.Query
	}
	// recordingSession is a [gocql.Session] which records all queries executed on it.
	recordingSession struct {
		gocql.Session
		statements []statement
	}
	statement struct {
		query string
		args  []any
	}
	// failingIter is a [gocql.Iter] which fails when iterated.
	failingIter struct{}
	// blockingSession is a [gocql.Session] designed for testing concurrent updates.
	blockingSession struct {
		gocql.Session
		queryToBlockOn   string
		queryShouldBlock func(string) bool
		queryStarted     chan struct{}
		queryCanContinue chan struct{}
	}
	countingBatchSession struct {
		gocql.Session
		executeBatchCalls atomic.Int64
	}
	testQueueParams struct {
		logger log.Logger
	}
	testLogger struct {
		log.Logger
		warningMsgs []string
	}
	rangeDeleteTestQueue struct {
		persistence.QueueV2
		session       *blockingSession
		deleteErrs    chan error
		maxIDToDelete int
	}
)

func (s *countingBatchSession) ExecuteBatch(batch *gocql.Batch) error {
	s.executeBatchCalls.Add(1)
	return s.Session.ExecuteBatch(batch)
}

func (f failingIter) Scan(...any) bool {
	return false
}

func (f failingIter) MapScan(map[string]any) bool {
	return false
}

func (f failingIter) NumRows() int {
	return 0
}

func (f failingIter) PageState() []byte {
	return nil
}

func (f failingIter) Close() error {
	return assert.AnError
}

func (q failingQuery) Iter() gocql.Iter {
	return failingIter{}
}

func (q failingQuery) Scan(...any) error {
	return assert.AnError
}

func (q failingQuery) Exec() error {
	return assert.AnError
}

func (l *testLogger) Warn(msg string, _ ...tag.Tag) {
	l.warningMsgs = append(l.warningMsgs, msg)
}

func (s *recordingSession) Query(query string, args ...any) gocql.Query {
	s.statements = append(s.statements, statement{
		query: query,
		args:  args,
	})

	return s.Session.Query(query, args...)
}

func TestCassandraShardStoreSuite(t *testing.T) {
	t.Parallel()
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewShardSuite(
		t,
		shardStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestCassandraExecutionMutableStateStoreSuite(t *testing.T) {
	t.Parallel()
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestCassandraExecutionMutableStateTaskStoreSuite(t *testing.T) {
	t.Parallel()
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

// TODO: Merge persistence-tests into the tests directory.

func TestCassandraHistoryStoreSuite(t *testing.T) {
	t.Parallel()
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewHistoryEventsSuite(t, store, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraHistoryStoreV2Suite(t *testing.T) {
	t.Parallel()
	testData, tearDown := setUpCassandraTestWithHistoryNodeV2Reads(t)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewHistoryEventsSuite(t, store, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraHistoryNodeV2MigrationPreservesLegacyReads(t *testing.T) {
	t.Parallel()

	cfg := NewCassandraConfig()
	logger := log.NewNoopLogger()
	SetUpCassandraDatabase(t, cfg, logger)
	t.Cleanup(func() {
		TearDownCassandraKeyspace(t, cfg)
	})
	ApplySchemaUpdate(t, cfg, "../../../schema/cassandra/temporal/versioned/v1.0/schema.cql", logger)
	for _, schemaFile := range GetSchemaFiles(t, "../../../schema/cassandra/temporal", logger) {
		if strings.Contains(schemaFile, "/v1.0/") {
			continue
		}
		if strings.Contains(schemaFile, "/v1.15/") {
			break
		}
		ApplySchemaUpdate(t, cfg, schemaFile, logger)
	}

	treeID := uuid.NewString()
	branchID := uuid.NewString()
	session := newCassandraTestSession(t, cfg, logger)
	for nodeID := int64(1); nodeID <= 2; nodeID++ {
		err := session.Query(
			`INSERT INTO history_node (tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) `+
				`VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
			treeID,
			branchID,
			nodeID,
			nodeID-1,
			nodeID,
			[]byte("events"),
			enumspb.ENCODING_TYPE_PROTO3.String(),
			int64(1000),
		).Exec()
		require.NoError(t, err)
	}
	session.Close()

	ApplySchemaUpdate(t, cfg, "../../../schema/cassandra/temporal/versioned/v1.15/history_node_v2.cql", logger)
	session = newCassandraTestSession(t, cfg, logger)
	defer session.Close()
	err := session.Query(
		`DELETE FROM history_node_v2 USING TIMESTAMP ? `+
			`WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ?`,
		int64(2000),
		treeID,
		branchID,
		int64(2),
		int64(2),
	).Exec()
	require.NoError(t, err)

	var (
		nodeID       int64
		prevTxnID    int64
		txnID        int64
		data         []byte
		dataEncoding string
	)
	err = session.Query(
		`SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node `+
			`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? `+
			`ORDER BY branch_id DESC, node_id DESC`,
		treeID,
		branchID,
		int64(1),
		int64(2),
	).Scan(&nodeID, &prevTxnID, &txnID, &data, &dataEncoding)
	require.NoError(t, err)
	require.Equal(t, int64(1), nodeID)
	require.Equal(t, []byte("events"), data)

	copied, err := cassandra.BackfillHistoryNodeV2(
		t.Context(),
		session,
		cassandra.HistoryNodeV2BackfillOptions{
			PageSize:        10,
			Concurrency:     2,
			TokenRangeCount: 8,
		},
	)
	require.NoError(t, err)
	require.Equal(t, int64(2), copied)

	err = session.Query(
		`SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node_v2 `+
			`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? ORDER BY node_id DESC`,
		treeID,
		branchID,
		int64(1),
		int64(2),
	).Scan(&nodeID, &prevTxnID, &txnID, &data, &dataEncoding)
	require.NoError(t, err)
	require.Equal(t, int64(1), nodeID)
	require.Equal(t, []byte("events"), data)

	err = session.Query(
		`SELECT node_id FROM history_node_v2 `+
			`WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ?`,
		treeID,
		branchID,
		int64(2),
		int64(2),
	).Scan(&nodeID)
	require.True(t, gocql.IsNotFoundError(err), "backfill must not resurrect a row with a newer tombstone")
}

func TestCassandraHistoryNodeOnlineMigrationFromOldV2(t *testing.T) {
	t.Parallel()

	cfg := NewCassandraConfig()
	logger := log.NewNoopLogger()
	SetUpCassandraDatabase(t, cfg, logger)
	t.Cleanup(func() {
		TearDownCassandraKeyspace(t, cfg)
	})
	SetUpCassandraSchema(t, cfg, logger)

	session := newCassandraTestSession(t, cfg, logger)
	require.NoError(t, session.Query(`DROP TABLE history_node_v2`).Exec())
	require.NoError(t, session.Query(`DROP TABLE history_node`).Exec())
	require.NoError(t, session.AwaitSchemaAgreement(t.Context()))
	require.NoError(t, session.Query(
		`CREATE TABLE history_node (`+
			`tree_id uuid, branch_id uuid, node_id bigint, txn_id bigint, prev_txn_id bigint, `+
			`data blob, data_encoding text, PRIMARY KEY ((tree_id, branch_id), node_id, txn_id)) `+
			`WITH CLUSTERING ORDER BY (node_id ASC, txn_id DESC)`,
	).Exec())
	require.NoError(t, session.AwaitSchemaAgreement(t.Context()))

	treeID := uuid.NewString()
	branchID := uuid.NewString()
	deletedBranchID := uuid.NewString()
	for nodeID := int64(1); nodeID <= 2; nodeID++ {
		insertHistoryNodeForMigrationTest(t, session, treeID, branchID, nodeID)
	}
	session.Close()

	ApplySchemaUpdate(t, cfg, "../../../schema/cassandra/temporal/versioned/v1.15/history_node_v2.cql", logger)
	session = newCassandraTestSession(t, cfg, logger)
	defer session.Close()

	require.NoError(t, cassandra.ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		session,
		cfg.Keyspace,
		config.CassandraHistoryNodeMigrationModeOldV2RebuildV2,
	))
	rebuildV2Store := cassandra.NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeOldV2RebuildV2,
	)
	branchInfo := &persistencespb.HistoryBranch{TreeId: treeID, BranchId: branchID}
	deletedBranchInfo := &persistencespb.HistoryBranch{TreeId: treeID, BranchId: deletedBranchID}
	appendHistoryNodeForMigrationTest(t, rebuildV2Store, branchInfo, 3)
	require.NoError(t, session.Query(
		`DELETE FROM history_node `+
			`WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ?`,
		treeID,
		branchID,
		int64(3),
		int64(3),
	).Exec())
	appendHistoryNodeForMigrationTest(t, rebuildV2Store, deletedBranchInfo, 10)

	recreateV2Session := &blockingSession{
		Session: session,
		queryShouldBlock: func(query string) bool {
			return strings.HasPrefix(query, "CREATE TABLE")
		},
		queryStarted:     make(chan struct{}, 1),
		queryCanContinue: make(chan struct{}),
	}
	recreateV2Result := make(chan error, 1)
	go func() {
		recreateV2Result <- cassandra.RecreateHistoryNodeV2(
			t.Context(),
			recreateV2Session,
			cfg.Keyspace,
			true,
		)
	}()
	select {
	case <-recreateV2Session.queryStarted:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	appendHistoryNodeForMigrationTest(t, rebuildV2Store, branchInfo, 4)
	appendHistoryNodeForMigrationTest(t, rebuildV2Store, branchInfo, 40)
	deleteWhileV2Missing := &persistence.InternalDeleteHistoryNodesRequest{
		BranchInfo:    branchInfo,
		NodeID:        40,
		TransactionID: 40,
	}
	require.Error(t, rebuildV2Store.DeleteHistoryNodes(t.Context(), deleteWhileV2Missing))
	require.NoError(t, rebuildV2Store.DeleteHistoryBranch(
		t.Context(),
		&persistence.InternalDeleteHistoryBranchRequest{
			BranchInfo:   &persistencespb.HistoryBranch{TreeId: treeID, BranchId: uuid.NewString()},
			BranchRanges: nil,
		},
	))
	close(recreateV2Session.queryCanContinue)
	require.NoError(t, <-recreateV2Result)
	appendHistoryNodeForMigrationTest(t, rebuildV2Store, branchInfo, 5)

	copied, err := cassandra.BackfillHistoryNodeV2(
		t.Context(),
		session,
		cassandra.HistoryNodeBackfillOptions{
			PageSize:        2,
			Concurrency:     2,
			TokenRangeCount: 8,
			SourceLayout:    cassandra.HistoryNodeTableLayoutBranchV2,
		},
	)
	require.NoError(t, err)
	require.Equal(t, int64(6), copied)
	require.NoError(t, rebuildV2Store.DeleteHistoryNodes(t.Context(), deleteWhileV2Missing))
	var staleNodeID int64
	err = session.Query(
		`SELECT node_id FROM history_node_v2 `+
			`WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ?`,
		treeID,
		branchID,
		int64(3),
		int64(3),
	).Scan(&staleNodeID)
	require.True(t, gocql.IsNotFoundError(err), "rebuild must remove rows deleted by old source-only writers")

	oldV2Store := cassandra.NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeOldV2Dual,
	)
	require.NoError(t, oldV2Store.DeleteHistoryNodes(t.Context(), &persistence.InternalDeleteHistoryNodesRequest{
		BranchInfo:    branchInfo,
		NodeID:        2,
		TransactionID: 2,
	}))
	appendHistoryNodeForMigrationTest(t, oldV2Store, branchInfo, 6)
	insertHistoryNodeForMigrationTest(t, session, treeID, branchID, 7)

	require.NoError(t, cassandra.ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		session,
		cfg.Keyspace,
		config.CassandraHistoryNodeMigrationModeOldV2PrepareCutoverDual,
	))
	prepareCutoverStore := cassandra.NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeOldV2PrepareCutoverDual,
	)
	appendHistoryNodeForMigrationTest(t, prepareCutoverStore, branchInfo, 8)
	copied, err = cassandra.BackfillHistoryNodeV2(
		t.Context(),
		session,
		cassandra.HistoryNodeBackfillOptions{
			PageSize:        2,
			Concurrency:     2,
			TokenRangeCount: 8,
			SourceLayout:    cassandra.HistoryNodeTableLayoutBranchV2,
		},
	)
	require.NoError(t, err)
	require.Equal(t, int64(7), copied)

	require.NoError(t, cassandra.ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		session,
		cfg.Keyspace,
		config.CassandraHistoryNodeMigrationModeOldV2CutoverDual,
	))
	cutoverStore := cassandra.NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeOldV2CutoverDual,
	)
	require.Equal(t, []int64{8, 7, 6, 5, 4, 1}, readHistoryNodeIDsForMigrationTest(
		t,
		cutoverStore,
		treeID,
		branchID,
	))

	require.NoError(t, cassandra.ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		session,
		cfg.Keyspace,
		config.CassandraHistoryNodeMigrationModeV1RebuildDual,
	))
	rebuildV1Store := cassandra.NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeV1RebuildDual,
	)
	recreateV1Session := &blockingSession{
		Session: session,
		queryShouldBlock: func(query string) bool {
			return strings.HasPrefix(query, "CREATE TABLE")
		},
		queryStarted:     make(chan struct{}, 1),
		queryCanContinue: make(chan struct{}),
	}
	recreateV1Result := make(chan error, 1)
	go func() {
		recreateV1Result <- cassandra.RecreateHistoryNodeV1(
			t.Context(),
			recreateV1Session,
			cfg.Keyspace,
			true,
		)
	}()
	select {
	case <-recreateV1Session.queryStarted:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	for nodeID := int64(20); nodeID < 30; nodeID++ {
		appendHistoryNodeForMigrationTest(t, rebuildV1Store, branchInfo, nodeID)
	}
	deleteWhileV1Missing := &persistence.InternalDeleteHistoryNodesRequest{
		BranchInfo:    branchInfo,
		NodeID:        8,
		TransactionID: 8,
	}
	require.Error(t, rebuildV1Store.DeleteHistoryNodes(t.Context(), deleteWhileV1Missing))
	deleteBranchWhileV1Missing := &persistence.InternalDeleteHistoryBranchRequest{
		BranchInfo: deletedBranchInfo,
		BranchRanges: []persistence.InternalDeleteHistoryBranchRange{{
			BranchId:    deletedBranchID,
			BeginNodeId: 10,
		}},
	}
	require.Error(t, rebuildV1Store.DeleteHistoryBranch(t.Context(), deleteBranchWhileV1Missing))
	close(recreateV1Session.queryCanContinue)
	require.NoError(t, <-recreateV1Result)
	appendHistoryNodeForMigrationTest(t, rebuildV1Store, branchInfo, 9)
	require.NoError(t, session.Query(
		`INSERT INTO history_node_v2 (`+
			`tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) `+
			`VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		treeID,
		branchID,
		int64(99),
		int64(98),
		int64(99),
		[]byte("events"),
		enumspb.ENCODING_TYPE_PROTO3.String(),
		int64(1000),
	).Exec())
	require.NoError(t, session.Query(
		`DELETE FROM history_node USING TIMESTAMP ? `+
			`WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ?`,
		int64(2000),
		treeID,
		branchID,
		int64(99),
		int64(99),
	).Exec())

	copied, err = cassandra.BackfillHistoryNodeV1(
		t.Context(),
		session,
		cassandra.HistoryNodeBackfillOptions{
			PageSize:        2,
			Concurrency:     2,
			TokenRangeCount: 8,
		},
	)
	require.NoError(t, err)
	require.Equal(t, int64(19), copied)
	require.NoError(t, rebuildV1Store.DeleteHistoryNodes(t.Context(), deleteWhileV1Missing))
	require.NoError(t, rebuildV1Store.DeleteHistoryBranch(t.Context(), deleteBranchWhileV1Missing))
	var tombstonedNodeID int64
	err = session.Query(
		`SELECT node_id FROM history_node `+
			`WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ?`,
		treeID,
		branchID,
		int64(99),
		int64(99),
	).Scan(&tombstonedNodeID)
	require.True(t, gocql.IsNotFoundError(err), "reverse backfill must not resurrect a row with a newer V1 tombstone")
	err = session.Query(
		`SELECT node_id FROM history_node `+
			`WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ?`,
		treeID,
		branchID,
		int64(8),
		int64(8),
	).Scan(&staleNodeID)
	require.True(t, gocql.IsNotFoundError(err), "V1 rebuild must not restore a row deleted while V1 was absent")

	require.NoError(t, cassandra.ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		session,
		cfg.Keyspace,
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
	))
	canonicalStore := cassandra.NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
	)
	appendHistoryNodeForMigrationTest(t, canonicalStore, branchInfo, 10)
	branchToken, err := canonicalStore.NewHistoryBranch(
		"",
		"",
		"",
		treeID,
		util.Ptr(branchID),
		nil,
		0,
		0,
		0,
	)
	require.NoError(t, err)
	page, err := canonicalStore.ReadHistoryBranch(
		t.Context(),
		&persistence.InternalReadHistoryBranchRequest{
			BranchToken: branchToken,
			BranchID:    branchID,
			MinNodeID:   1,
			MaxNodeID:   100,
			PageSize:    1,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, page.NextPageToken)
	continuation, err := canonicalStore.ReadHistoryBranch(
		t.Context(),
		&persistence.InternalReadHistoryBranchRequest{
			BranchToken:           branchToken,
			BranchID:              branchID,
			MinNodeID:             1,
			MaxNodeID:             100,
			PageSize:              1,
			NextPageToken:         page.NextPageToken,
			NextPageTokenMetadata: page.NextPageTokenMetadata,
		},
	)
	require.NoError(t, err)
	require.Len(t, page.Nodes, 1)
	require.Len(t, continuation.Nodes, 1)
	require.NotEqual(t, page.Nodes[0].NodeID, continuation.Nodes[0].NodeID)
	legacyIter := session.Query(
		`SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node `+
			`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ?`,
		treeID,
		branchID,
		int64(1),
		int64(100),
	).PageSize(1).PageState(page.NextPageToken).Iter()
	require.Error(t, legacyIter.Close(), "a canonical page token must fail safely on an older V1 query")

	require.NoError(t, canonicalStore.DeleteHistoryBranch(
		t.Context(),
		&persistence.InternalDeleteHistoryBranchRequest{
			BranchInfo: branchInfo,
			BranchRanges: []persistence.InternalDeleteHistoryBranchRange{{
				BranchId:    branchID,
				BeginNodeId: 5,
			}},
		},
	))
	require.Equal(t, []int64{4, 1}, readHistoryNodeIDsForMigrationTest(
		t,
		canonicalStore,
		treeID,
		branchID,
	))

	insertHistoryNodeV2ForMigrationTest(t, session, treeID, branchID, 3)
	require.NoError(t, cassandra.ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		session,
		cfg.Keyspace,
		config.CassandraHistoryNodeMigrationModeV1CutoverDual,
	))
	v1CutoverStore := cassandra.NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeV1CutoverDual,
	)
	require.Equal(t, []int64{4, 3, 1}, readHistoryNodeIDsForMigrationTest(
		t,
		v1CutoverStore,
		treeID,
		branchID,
	))
	copied, err = cassandra.BackfillHistoryNodeV1(
		t.Context(),
		session,
		cassandra.HistoryNodeBackfillOptions{
			PageSize:        2,
			Concurrency:     2,
			TokenRangeCount: 8,
		},
	)
	require.NoError(t, err)
	require.Equal(t, int64(3), copied)

	rollbackStore := cassandra.NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeLegacyV1RollbackDual,
	)
	require.Equal(t, []int64{4, 3, 1}, readHistoryNodeIDsForMigrationTest(
		t,
		rollbackStore,
		treeID,
		branchID,
	))
	require.Empty(t, readHistoryNodeIDsForMigrationTest(
		t,
		rollbackStore,
		treeID,
		deletedBranchID,
	))
}

func TestCassandraHistoryBranchDeleteUsesBoundedBatches(t *testing.T) {
	t.Parallel()

	cfg := NewCassandraConfig()
	logger := log.NewNoopLogger()
	SetUpCassandraDatabase(t, cfg, logger)
	t.Cleanup(func() {
		TearDownCassandraKeyspace(t, cfg)
	})
	SetUpCassandraSchema(t, cfg, logger)

	session := newCassandraTestSession(t, cfg, logger)
	defer session.Close()
	countingSession := &countingBatchSession{Session: session}
	store := cassandra.NewHistoryStore(
		countingSession,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
	)
	const branchCount = 128
	ranges := make([]persistence.InternalDeleteHistoryBranchRange, branchCount)
	for i := range ranges {
		ranges[i] = persistence.InternalDeleteHistoryBranchRange{
			BranchId:    uuid.NewString(),
			BeginNodeId: 1,
		}
	}

	err := store.DeleteHistoryBranch(
		t.Context(),
		&persistence.InternalDeleteHistoryBranchRequest{
			BranchInfo: &persistencespb.HistoryBranch{
				TreeId:   uuid.NewString(),
				BranchId: uuid.NewString(),
			},
			BranchRanges: ranges,
		},
	)

	require.NoError(t, err)
	require.Equal(t, int64(branchCount), countingSession.executeBatchCalls.Load())
}

func insertHistoryNodeForMigrationTest(
	t *testing.T,
	session gocql.Session,
	treeID string,
	branchID string,
	nodeID int64,
) {
	t.Helper()
	err := session.Query(
		`INSERT INTO history_node (`+
			`tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) `+
			`VALUES (?, ?, ?, ?, ?, ?, ?)`,
		treeID,
		branchID,
		nodeID,
		nodeID-1,
		nodeID,
		[]byte("events"),
		enumspb.ENCODING_TYPE_PROTO3.String(),
	).Exec()
	require.NoError(t, err)
}

func insertHistoryNodeV2ForMigrationTest(
	t *testing.T,
	session gocql.Session,
	treeID string,
	branchID string,
	nodeID int64,
) {
	t.Helper()
	err := session.Query(
		`INSERT INTO history_node_v2 (`+
			`tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) `+
			`VALUES (?, ?, ?, ?, ?, ?, ?)`,
		treeID,
		branchID,
		nodeID,
		nodeID-1,
		nodeID,
		[]byte("events"),
		enumspb.ENCODING_TYPE_PROTO3.String(),
	).Exec()
	require.NoError(t, err)
}

func appendHistoryNodeForMigrationTest(
	t *testing.T,
	store *cassandra.HistoryStore,
	branchInfo *persistencespb.HistoryBranch,
	nodeID int64,
) {
	t.Helper()
	require.NoError(t, store.AppendHistoryNodes(t.Context(), &persistence.InternalAppendHistoryNodesRequest{
		BranchInfo: branchInfo,
		Node: persistence.InternalHistoryNode{
			NodeID:            nodeID,
			PrevTransactionID: nodeID - 1,
			TransactionID:     nodeID,
			Events: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         []byte("events"),
			},
		},
	}))
}

func readHistoryNodeIDsForMigrationTest(
	t *testing.T,
	store *cassandra.HistoryStore,
	treeID string,
	branchID string,
) []int64 {
	t.Helper()
	branchToken, err := store.NewHistoryBranch(
		"",
		"",
		"",
		treeID,
		util.Ptr(branchID),
		nil,
		0,
		0,
		0,
	)
	require.NoError(t, err)
	response, err := store.ReadHistoryBranch(t.Context(), &persistence.InternalReadHistoryBranchRequest{
		BranchToken:  branchToken,
		BranchID:     branchID,
		MinNodeID:    1,
		MaxNodeID:    100,
		PageSize:     100,
		ReverseOrder: true,
	})
	require.NoError(t, err)
	nodeIDs := make([]int64, len(response.Nodes))
	for i, node := range response.Nodes {
		nodeIDs[i] = node.NodeID
	}
	return nodeIDs
}

func TestCassandraTaskQueueSuite(t *testing.T) {
	t.Parallel()
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewTaskQueueSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraFairTaskQueueSuite(t *testing.T) {
	t.Parallel()
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewFairTaskStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewTaskQueueSuite(t, taskQueueStore, testData.Logger) // same suite, different store
	suite.Run(t, s)
}

func TestCassandraTaskQueueTaskSuite(t *testing.T) {
	t.Parallel()
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewTaskQueueTaskSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraTaskQueueFairTaskSuite(t *testing.T) {
	t.Parallel()
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewFairTaskStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewTaskQueueFairTaskSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraTaskQueueUserDataSuite(t *testing.T) {
	t.Parallel()
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewTaskQueueUserDataSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraHistoryV2Persistence(t *testing.T) {
	t.Parallel()
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestCassandraMetadataPersistenceV2(t *testing.T) {
	t.Parallel()
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestCassandraClusterMetadataPersistence(t *testing.T) {
	t.Parallel()
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestCassandraQueuePersistence(t *testing.T) {
	t.Parallel()
	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestCassandraQueueConcurrentEnqueueKeepsReadableOrder(t *testing.T) {
	t.Parallel()

	cluster := persistencetests.NewTestClusterForCassandra(&persistencetests.TestBaseOptions{}, log.NewNoopLogger())
	cluster.SetupTestDatabase()
	t.Cleanup(cluster.TearDownTestDatabase)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sessions := make([]*blockingSession, 2)
	queues := make([]persistence.Queue, 2)
	queueType := persistence.QueueType(1000)
	for i := range sessions {
		sessions[i] = &blockingSession{
			Session: cluster.GetSession(),
			queryShouldBlock: func(query string) bool {
				return strings.HasPrefix(query, "INSERT INTO queue ")
			},
			queryStarted:     make(chan struct{}, 1),
			queryCanContinue: make(chan struct{}),
		}
		var err error
		queues[i], err = cassandra.NewQueueStore(queueType, sessions[i], log.NewNoopLogger())
		require.NoError(t, err)
	}

	results := []chan error{make(chan error, 1), make(chan error, 1)}
	for i := range queues {
		go func() {
			results[i] <- queues[i].EnqueueMessage(ctx, &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         []byte{byte(i)},
			})
		}()
	}
	for i := range sessions {
		select {
		case <-sessions[i].queryStarted:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	close(sessions[0].queryCanContinue)
	require.NoError(t, <-results[0])
	close(sessions[1].queryCanContinue)
	require.ErrorIs(t, <-results[1], cassandra.ErrEnqueueMessageConflict)

	require.NoError(t, queues[1].EnqueueMessage(ctx, &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("retry"),
	}))

	messages, err := queues[0].ReadMessages(ctx, persistence.EmptyQueueMessageID, 10)
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Equal(t, int64(0), messages[0].ID)
	require.Equal(t, int64(1), messages[1].ID)
}

func TestCassandraQueueV2Persistence(t *testing.T) {
	// This test function is split up into two parts:
	// 1. Test the generic queue functionality, which is independent of the database choice (Cassandra here).
	//   This is done by calling the generic RunQueueV2TestSuite function.
	// 2. Test the Cassandra-specific implementation of the queue. For example, things like queue message ID conflicts
	//   can only happen in Cassandra due to its lack of transactions, so we need to test those here.

	t.Parallel()

	cluster := persistencetests.NewTestClusterForCassandra(&persistencetests.TestBaseOptions{}, log.NewNoopLogger())
	cluster.SetupTestDatabase()
	t.Cleanup(cluster.TearDownTestDatabase)

	t.Run("Generic", func(t *testing.T) {
		t.Parallel()
		RunQueueV2TestSuite(t, newQueueV2Store(cluster.GetSession()))
	})
	t.Run("CassandraSpecific", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2(t, cluster)
	})
}

func TestCassandraNexusEndpointPersistence(t *testing.T) {
	t.Parallel()
	cluster := persistencetests.NewTestClusterForCassandra(&persistencetests.TestBaseOptions{}, log.NewNoopLogger())
	cluster.SetupTestDatabase()
	t.Cleanup(cluster.TearDownTestDatabase)

	tableVersion := atomic.Int64{}

	// NB: These tests cannot be run in parallel because of concurrent updates to the table version by different tests
	t.Run("Generic", func(t *testing.T) {
		RunNexusEndpointTestSuite(t, newNexusEndpointStore(cluster.GetSession()), &tableVersion)
	})
	t.Run("CassandraSpecific", func(t *testing.T) {
		testCassandraNexusEndpointStore(t, cluster, &tableVersion)
	})
}

func testCassandraQueueV2DataCorruption(t *testing.T, cluster *cassandra.TestCluster) {
	t.Run("ErrInvalidQueueMessageEncodingType", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrInvalidQueueMessageEncodingType(t, cluster)
	})
	t.Run("ErrInvalidPayloadEncodingType", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrInvalidPayloadEncodingType(t, cluster)
	})
	t.Run("ErrInvalidPayload", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrInvalidPayload(t, cluster)
	})
}

func testCassandraQueueV2(t *testing.T, cluster *cassandra.TestCluster) {
	t.Run("DataCorruption", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2DataCorruption(t, cluster)
	})
	t.Run("QueryErrors", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2QueryErrors(t, cluster)
	})
	t.Run("RangeDeleteUpperBoundHigherThanMaxMessageID", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2RangeDeleteUpperBoundHigherThanMaxMessageID(t, cluster)
	})
	t.Run("RepeatedRangeDelete", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2RepeatedRangeDelete(t, cluster)
	})
	t.Run("MinMessageIDOptimization", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2MinMessageIDOptimization(t, cluster)
	})
	t.Run("ConcurrentConflicts", func(t *testing.T) {
		testCassandraQueueV2ConcurrentConflicts(t, cluster)
	})
	t.Run("ConcurrentEnqueueKeepsContiguousIDs", func(t *testing.T) {
		testCassandraQueueV2ConcurrentEnqueueKeepsContiguousIDs(t, cluster)
	})
	t.Run("MultiplePartitions", func(t *testing.T) {
		testCassandraQueueV2MultiplePartitions(t, cluster)
	})
}

func testCassandraQueueV2ConcurrentEnqueueKeepsContiguousIDs(t *testing.T, cluster *cassandra.TestCluster) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sessions := make([]*blockingSession, 2)
	queues := make([]persistence.QueueV2, 2)
	for i := range sessions {
		sessions[i] = &blockingSession{
			Session:          cluster.GetSession(),
			queryToBlockOn:   cassandra.TemplateEnqueueMessageQuery,
			queryStarted:     make(chan struct{}, 1),
			queryCanContinue: make(chan struct{}),
		}
		queues[i] = newQueueV2Store(sessions[i])
	}

	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := queues[0].CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)

	type enqueueResult struct {
		response *persistence.InternalEnqueueMessageResponse
		err      error
	}
	results := []chan enqueueResult{
		make(chan enqueueResult, 1),
		make(chan enqueueResult, 1),
	}
	for i := range queues {
		go func() {
			response, err := persistencetest.EnqueueMessage(ctx, queues[i], queueType, queueName)
			results[i] <- enqueueResult{response: response, err: err}
		}()
	}
	for i := range sessions {
		select {
		case <-sessions[i].queryStarted:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	close(sessions[0].queryCanContinue)
	first := <-results[0]
	require.NoError(t, first.err)
	require.Equal(t, int64(persistence.FirstQueueMessageID), first.response.Metadata.ID)

	close(sessions[1].queryCanContinue)
	second := <-results[1]
	require.ErrorIs(t, second.err, cassandra.ErrEnqueueMessageConflict)

	retry, err := persistencetest.EnqueueMessage(ctx, queues[1], queueType, queueName)
	require.NoError(t, err)
	require.Equal(t, int64(persistence.FirstQueueMessageID+1), retry.Metadata.ID)

	readResponse, err := queues[0].ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  10,
	})
	require.NoError(t, err)
	require.Len(t, readResponse.Messages, 2)
	require.Equal(t, int64(persistence.FirstQueueMessageID), readResponse.Messages[0].MetaData.ID)
	require.Equal(t, int64(persistence.FirstQueueMessageID+1), readResponse.Messages[1].MetaData.ID)

	listResponse, err := queues[0].ListQueues(ctx, &persistence.InternalListQueuesRequest{
		QueueType: queueType,
		PageSize:  1000,
	})
	require.NoError(t, err)
	queueInfo := findQueueInfo(t, listResponse.Queues, queueName)
	require.Equal(t, int64(2), queueInfo.MessageCount)
	require.Equal(t, int64(persistence.FirstQueueMessageID+1), queueInfo.LastMessageID)

	deleteResponse, err := queues[0].RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{
			ID: persistence.FirstQueueMessageID,
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), deleteResponse.MessagesDeleted)
	require.Equal(t, 1, getNumMessages(t, cluster, queueType, queueName, 10))
}

func findQueueInfo(t *testing.T, queues []persistence.QueueInfo, queueName string) persistence.QueueInfo {
	t.Helper()
	for _, queue := range queues {
		if queue.QueueName == queueName {
			return queue
		}
	}
	t.Fatalf("queue %q not found", queueName)
	return persistence.QueueInfo{}
}

func testCassandraQueueV2ConcurrentConflicts(t *testing.T, cluster *cassandra.TestCluster) {
	t.Run("RangeDeleteMessages", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ConcurrentRangeDeleteMessages(t, cluster)
	})
}

func testCassandraQueueV2QueryErrors(t *testing.T, cluster *cassandra.TestCluster) {
	t.Run("GetQueueQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrGetQueueQuery(t, cluster)
	})
	t.Run("CreateQueueQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrCreateQueueQuery(t, cluster)
	})
	t.Run("RangeDeleteMessagesQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrRangeDeleteMessagesQuery(t, cluster)
	})
	t.Run("ErrReadMessagesQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrGetMessagesQuery(t, cluster)
	})
	t.Run("ErrEnqueueMessageQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrEnqueueMessageQuery(t, cluster)
	})
	t.Run("EnqueueMessageGetMaxMessageIDQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrEnqueueMessageGetMaxMessageIDQuery(t, cluster)
	})
	t.Run("ListQueuesGetMaxMessageIDQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrListQueuesGetMaxMessageIDQuery(t, cluster)
	})
	t.Run("RangeDeleteMessagesGetMaxMessageIDQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrRangeDeleteMessagesGetMaxMessageIDQuery(t, cluster)
	})
	t.Run("RangeDeleteMessagesUpdateQueueQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrRangeDeleteMessagesUpdateQueueQuery(t, cluster)
	})
}

func testCassandraQueueV2ErrRangeDeleteMessagesUpdateQueueQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateUpdateQueueMetadataQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	persistencetest.EnqueueMessagesForDelete(t, q, queueName, queueType)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID)
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2UpdateQueueMetadata")
}

func testCassandraQueueV2ErrRangeDeleteMessagesGetMaxMessageIDQuery(t *testing.T, cluster *cassandra.TestCluster) {
	session := &failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{},
	}
	q := newQueueV2Store(session)
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	persistencetest.EnqueueMessagesForDelete(t, q, queueName, queueType)
	session.failingQueries = []string{cassandra.TemplateGetMaxMessageIDQuery}
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID)
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2GetMaxMessageID")
}

func testCassandraQueueV2ErrEnqueueMessageGetMaxMessageIDQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateGetMaxMessageIDQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = persistencetest.EnqueueMessage(ctx, q, queueType, queueName)
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	require.ErrorContains(t, err, "QueueV2GetMaxMessageID")
}

func testCassandraQueueV2ErrListQueuesGetMaxMessageIDQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateGetMaxMessageIDQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryDLQ
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = q.ListQueues(ctx, &persistence.InternalListQueuesRequest{
		QueueType: queueType,
		PageSize:  100,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2GetMaxMessageID")
}

func testCassandraQueueV2MultiplePartitions(t *testing.T, cluster *cassandra.TestCluster) {
	t.Run("RangeDeleteMessages", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2MultiplePartitionsRangeDelete(t, cluster)
	})
	t.Run("ReadMessages", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2MultiplePartitionsReadMessages(t, cluster)
	})
	t.Run("ListQueues", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2MultiplePartitionsListQueues(t, cluster)
	})
}

// Query checks if the query matches queryToBlockOn, and, if so, it notifies the test and then blocks until the test
// unblocks it.
func (f *blockingSession) Query(query string, args ...any) gocql.Query {
	if query == f.queryToBlockOn || (f.queryShouldBlock != nil && f.queryShouldBlock(query)) {
		f.queryStarted <- struct{}{}
		<-f.queryCanContinue
	}

	return f.Session.Query(query, args...)
}

func testCassandraQueueV2ErrInvalidQueueMessageEncodingType(t *testing.T, cluster *cassandra.TestCluster) {
	session := cluster.GetSession()
	q := newQueueV2Store(session)
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(context.Background(), &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	err = session.Query(
		cassandra.TemplateEnqueueMessageQuery,
		queueType,
		queueName,
		0, // partition
		1, // messageID
		[]byte("test"),
		"bad-encoding-type",
	).Exec()
	require.NoError(t, err)
	_, err = q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.UnknownEncodingTypeError))
}

func (q failingQuery) MapScanCAS(map[string]any) (bool, error) {
	return false, assert.AnError
}

func (q failingQuery) WithContext(context.Context) gocql.Query {
	return q
}

func (f failingSession) Query(query string, args ...any) gocql.Query {
	for _, q := range f.failingQueries {
		if q == query {
			return failingQuery{}
		}
	}
	return f.Session.Query(query, args...)
}

func testCassandraQueueV2ErrGetMessagesQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateGetMessagesQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2ReadMessages")
}

func testCassandraQueueV2ErrEnqueueMessageQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateEnqueueMessageQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName)
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2EnqueueMessage")
}

func testCassandraQueueV2ErrInvalidPayloadEncodingType(t *testing.T, cluster *cassandra.TestCluster) {
	// Manually insert a row into the queues table that has an invalid metadata payload encoding type and then verify
	// that we gracefully handle the error when we try to read from this queue.

	session := cluster.GetSession()
	q := newQueueV2Store(session)
	// Using a different QueueType so that ListQueue tests are not failing because of corrupt queue metadata.
	queueType := persistence.QueueV2Type(3)
	queueName := "test-queue-" + t.Name()
	err := session.Query(
		cassandra.TemplateCreateQueueQuery,
		queueType,
		queueName,
		[]byte("test"),      // payload
		"bad-encoding-type", // payload encoding type
		0,                   // version
	).Exec()
	require.NoError(t, err)
	_, err = q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.UnknownEncodingTypeError))
	assert.ErrorContains(t, err, "bad-encoding-type")
	assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
	assert.ErrorContains(t, err, queueName)

	_, err = q.ListQueues(context.Background(), &persistence.InternalListQueuesRequest{
		QueueType: queueType,
		PageSize:  100,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.UnknownEncodingTypeError))
	assert.ErrorContains(t, err, "bad-encoding-type")
	assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
	assert.ErrorContains(t, err, queueName)
}

func testCassandraQueueV2ErrInvalidPayload(t *testing.T, cluster *cassandra.TestCluster) {
	// Manually insert a row into the queues table that has some invalid bytes for the queue metadata proto payload and
	// then verify that we gracefully handle the error when we try to read from this queue.

	session := cluster.GetSession()
	q := newQueueV2Store(session)
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	err := session.Query(
		cassandra.TemplateCreateQueueQuery,
		queueType,
		queueName,
		[]byte("invalid-payload"),             // payload
		enumspb.ENCODING_TYPE_PROTO3.String(), // payload encoding type
		0,                                     // version
	).Exec()
	require.NoError(t, err)
	_, err = q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.DeserializationError))
	assert.ErrorContains(t, err, "unmarshal")
	assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
	assert.ErrorContains(t, err, queueName)
}

func testCassandraQueueV2ErrGetQueueQuery(t *testing.T, cluster *cassandra.TestCluster) {
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	setupQueue := newQueueV2Store(cluster.GetSession())
	_, err := setupQueue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)

	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateGetQueueQuery},
	})
	_, err = q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2GetQueue")
}

func testCassandraQueueV2ErrCreateQueueQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateCreateQueueQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2CreateQueue")
}

func testCassandraQueueV2ErrRangeDeleteMessagesQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateRangeDeleteMessagesQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	persistencetest.EnqueueMessagesForDelete(t, q, queueName, queueType)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID)
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2RangeDeleteMessages")
}

func testCassandraQueueV2MinMessageIDOptimization(t *testing.T, cluster *cassandra.TestCluster) {
	session := &recordingSession{
		Session: cluster.GetSession(),
	}
	q := newQueueV2Store(session)
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	for range 2 {
		_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName)
		require.NoError(t, err)
	}
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID)
	require.NoError(t, err)
	pageSize := 10
	response, err := q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  pageSize,
	})
	require.NoError(t, err)
	require.Len(t, response.Messages, 1)
	assert.Equal(t, int64(persistence.FirstQueueMessageID+1), response.Messages[0].MetaData.ID)
	var lastReadStmt *statement
	for _, stmt := range session.statements {
		if stmt.query == cassandra.TemplateGetMessagesQuery {
			stmt := stmt
			lastReadStmt = &stmt
		}
	}
	require.NotNil(t, lastReadStmt, "expected to find a query to get messages")
	args := lastReadStmt.args
	require.Len(t, args, 5)
	assert.Equal(t, queueType, args[0])
	assert.Equal(t, queueName, args[1])
	assert.Equal(t, 0, args[2])
	require.Equal(t, int64(persistence.FirstQueueMessageID+1), args[3], "We should skip the first "+
		"message ID because we deleted it")
	assert.Equal(t, pageSize, args[4])
}

func testCassandraQueueV2ConcurrentRangeDeleteMessages(t *testing.T, cluster *cassandra.TestCluster) {
	// This test simulates a race condition between two RangeDeleteMessages calls. First, we enqueue 3 messages, then we
	// start a request to delete the first message, and then we start another request to delete the first two messages.
	// We have two cases, one where the first request to delete the first message is the leader and one where the second
	// request is the leader. Both requests rendezvous at the query to update queue metadata, but the leader query goes
	// first, and the follower query goes only after the leader query has completely finished. In the first case, the
	// first request should succeed and the second request should fail with a conflict error. In the second case, the
	// second request should succeed and the first request should fail with a conflict error. In both cases, only the
	// third message should be in the queue. More importantly, after we retry the failing request, it should succeed,
	// and the queue metadata should now record the min_message_id as 2.

	for _, tc := range []struct {
		name                  string
		smallerDeleteIsLeader bool
	}{
		{
			name:                  "smaller delete goes first",
			smallerDeleteIsLeader: true,
		},
		{
			name:                  "larger delete goes first",
			smallerDeleteIsLeader: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Make two queue stores to simulate the leader and follower queries. We use two separate queue stores
			// because it makes it easier to control which query goes first when they have separate blockingSession
			// instances.
			qs := make([]rangeDeleteTestQueue, 2)
			for i, q := range qs {
				// We need to use a blocking session here because we need to block the query to update the queue
				// metadata.
				q.session = &blockingSession{
					Session:          cluster.GetSession(),
					queryToBlockOn:   cassandra.TemplateUpdateQueueMetadataQuery,
					queryStarted:     make(chan struct{}, 1),
					queryCanContinue: make(chan struct{}, 1),
				}
				q.QueueV2 = newQueueV2Store(q.session)
				q.deleteErrs = make(chan error, 1)
				q.maxIDToDelete = persistence.FirstQueueMessageID + i
				qs[i] = q
			}

			// Create the queue
			ctx := context.Background()
			queueType := persistence.QueueTypeHistoryNormal
			queueName := "test-queue-" + t.Name()
			_, err := qs[0].CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
				QueueType: queueType,
				QueueName: queueName,
			})
			require.NoError(t, err)

			// Enqueue 3 messages
			for range 3 {
				_, err := persistencetest.EnqueueMessage(ctx, qs[0].QueueV2, queueType, queueName)
				require.NoError(t, err)
			}

			// Start both RangeDeleteMessages call
			for _, q := range qs {
				q := q
				go func() {
					err := deleteMessages(ctx, q.QueueV2, queueType, queueName, q.maxIDToDelete)
					q.deleteErrs <- err
				}()
			}

			// Wait for both queries to start
			for i := range 2 {
				<-qs[i].session.queryStarted
			}

			// Choose which query will be the leader and which will be the follower
			var leader, follower *rangeDeleteTestQueue
			if tc.smallerDeleteIsLeader {
				leader = &qs[0]
				follower = &qs[1]
			} else {
				leader = &qs[1]
				follower = &qs[0]
			}

			// Let the leader query finish
			close(leader.session.queryCanContinue)
			err = <-leader.deleteErrs
			require.NoError(t, err)

			// Let the follower query finish
			close(follower.session.queryCanContinue)
			err = <-follower.deleteErrs

			// Verify that the follower query failed
			require.Error(t, err)
			assert.ErrorIs(t, err, cassandra.ErrUpdateQueueConflict)
			assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
			assert.ErrorContains(t, err, queueName)

			// Verify that the queue metadata was updated by the leader query
			q, err := cassandra.GetQueue(ctx, qs[0].session, queueName, queueType)
			require.NoError(t, err)
			require.Len(t, q.Metadata.Partitions, 1)
			if tc.smallerDeleteIsLeader {
				// The smaller delete should have updated the min message ID to 1 because it succeeded, and the follower
				// query received a conflict. However, the follower query will get retried, so the min message ID will
				// eventually be updated correctly.
				assert.Equal(t, int64(persistence.FirstQueueMessageID+1), q.Metadata.Partitions[0].MinMessageId)
			} else {
				// The bigger delete should have updated the min message ID to 2 because it succeeded, so the min
				// message ID is already correct. However, the follower query will still get retried, so we need to
				// verify later that this retry does not change the min message ID to 1.
				assert.Equal(t, int64(persistence.FirstQueueMessageID+2), q.Metadata.Partitions[0].MinMessageId)
			}

			// Retry the follower query. Note that this would fail if it actually tried to update the queue metadata
			// since we haven't unblocked it, so this implicitly tests that both operations are idempotent.
			err = deleteMessages(ctx, follower.QueueV2, queueType, queueName, follower.maxIDToDelete)
			require.NoError(t, err)

			// Verify that the queue metadata was updated to reflect the new min message ID
			q, err = cassandra.GetQueue(ctx, qs[0].session, queueName, queueType)
			require.NoError(t, err)
			require.Len(t, q.Metadata.Partitions, 1)
			assert.Equal(t, int64(persistence.FirstQueueMessageID+2), q.Metadata.Partitions[0].MinMessageId)

			// Verify that the first two messages were deleted no matter which query was the leader
			response, err := qs[0].ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
				QueueType: queueType,
				QueueName: queueName,
				PageSize:  10,
			})
			require.NoError(t, err)
			require.Len(t, response.Messages, 1)
			assert.Equal(t, int64(persistence.FirstQueueMessageID+2), response.Messages[0].MetaData.ID)
		})
	}
}

func testCassandraQueueV2MultiplePartitionsRangeDelete(t *testing.T, cluster *cassandra.TestCluster) {
	// Manually insert a row into the queues table that has multiple partitions and then verify that we gracefully
	// handle the error when we try to range delete messages from this queue.

	session := cluster.GetSession()
	logger := &testLogger{}
	q := newQueueV2Store(session, func(params *testQueueParams) {
		params.logger = logger
	})
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	insertQueueMetadataWithMultiplePartitions(t, session, queueType, queueName)
	err := deleteMessages(context.Background(), q, queueType, queueName, 1)
	require.Error(t, err)
	assert.ErrorContains(t, err, "partitions")
}

func testCassandraQueueV2MultiplePartitionsReadMessages(t *testing.T, cluster *cassandra.TestCluster) {
	// Manually insert a row into the queues table that has multiple partitions and then verify that we gracefully
	// handle the error when we try to read messages from this queue.

	session := cluster.GetSession()
	logger := &testLogger{}
	q := newQueueV2Store(session, func(params *testQueueParams) {
		params.logger = logger
	})
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	insertQueueMetadataWithMultiplePartitions(t, session, queueType, queueName)
	_, err := q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "partitions")
}

func testCassandraQueueV2MultiplePartitionsListQueues(t *testing.T, cluster *cassandra.TestCluster) {
	// Manually insert a row into the queues table that has multiple partitions and then verify that we gracefully
	// handle the error when we try to list queues.

	session := cluster.GetSession()
	logger := &testLogger{}
	q := newQueueV2Store(session, func(params *testQueueParams) {
		params.logger = logger
	})
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	insertQueueMetadataWithMultiplePartitions(t, session, queueType, queueName)
	_, err := q.ListQueues(context.Background(), &persistence.InternalListQueuesRequest{
		QueueType: queueType,
		PageSize:  100,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "partitions")
}

func testCassandraQueueV2RangeDeleteUpperBoundHigherThanMaxMessageID(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(cluster.GetSession())
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = persistencetest.EnqueueMessage(ctx, q, queueType, queueName)
	require.NoError(t, err)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID+2)
	require.NoError(t, err)
	res, err := persistencetest.EnqueueMessage(ctx, q, queueType, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(persistence.FirstQueueMessageID+1), res.Metadata.ID)
}

func testCassandraQueueV2RepeatedRangeDelete(t *testing.T, cluster *cassandra.TestCluster) {
	// We never delete the last message from the queue. However, on a subsequent delete, the previous message with the
	// min_message_id of the queue should be deleted. This test verifies that.

	q := newQueueV2Store(cluster.GetSession())
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	numMessages := 3
	for range numMessages {
		_, err := persistencetest.EnqueueMessage(ctx, q, queueType, queueName)
		require.NoError(t, err)
	}
	numRemainingMessages := getNumMessages(t, cluster, queueType, queueName, numMessages)
	assert.Equal(t, 3, numRemainingMessages)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID)
	require.NoError(t, err)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID+1)
	require.NoError(t, err)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID+2)
	require.NoError(t, err)
	numRemainingMessages = getNumMessages(t, cluster, queueType, queueName, numMessages)
	assert.Equal(t, 1, numRemainingMessages, "expected only one message to remain in the queue"+
		" because we never delete the last message, but the first two messages should have been deleted")
}

func getNumMessages(
	t *testing.T,
	cluster *cassandra.TestCluster,
	queueType persistence.QueueV2Type,
	queueName string,
	numMessages int,
) int {
	// We query the database directly here to get the actual count of messages deleted. If we just rely on the store
	// method to read messages, that would indicate that we're updating min_message_id correctly, but it wouldn't verify
	// that any messages less than min_message_id are actually deleted from the database.

	iter := cluster.GetSession().Query(
		cassandra.TemplateGetMessagesQuery,
		queueType,
		queueName,
		0,                               // partition
		persistence.FirstQueueMessageID, // minMessageID
		numMessages,                     // limit
	).Iter()
	numRemainingMessages := 0
	for iter.MapScan(map[string]any{}) {
		numRemainingMessages++
	}
	require.NoError(t, iter.Close())
	return numRemainingMessages
}

func newQueueV2Store(session gocql.Session, opts ...func(params *testQueueParams)) persistence.QueueV2 {
	p := testQueueParams{
		logger: log.NewTestLogger(),
	}
	for _, opt := range opts {
		opt(&p)
	}
	return cassandra.NewQueueV2Store(session, p.logger)
}

func insertQueueMetadataWithMultiplePartitions(
	t *testing.T,
	session gocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
) {
	t.Helper()

	queuePB := persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {},
			1: {},
		},
	}
	bytes, _ := queuePB.Marshal()
	err := session.Query(
		cassandra.TemplateCreateQueueQuery,
		queueType,
		queueName,
		bytes,                                 // payload
		enumspb.ENCODING_TYPE_PROTO3.String(), // payload encoding type
		0,                                     // version
	).Exec()
	require.NoError(t, err)
}

func deleteMessages(
	ctx context.Context,
	q persistence.QueueV2,
	queueType persistence.QueueV2Type,
	queueName string,
	maxID int,
) error {
	_, err := q.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{
			ID: int64(maxID),
		},
	})

	return err
}

func testCassandraNexusEndpointStore(t *testing.T, cluster *cassandra.TestCluster, tableVersion *atomic.Int64) {
	store := newNexusEndpointStore(cluster.GetSession())
	t.Run("ConcurrentCreate", func(t *testing.T) {
		testCassandraNexusEndpointStoreConcurrentCreate(t, store, tableVersion)
	})
	t.Run("ConcurrentUpdate", func(t *testing.T) {
		testCassandraNexusEndpointStoreConcurrentUpdate(t, store, tableVersion)
	})
	t.Run("ConcurrentCreateAndUpdate", func(t *testing.T) {
		testCassandraNexusEndpointStoreConcurrentCreateAndUpdate(t, store, tableVersion)
	})
	t.Run("ConcurrentUpdateAndDelete", func(t *testing.T) {
		testCassandraNexusEndpointStoreConcurrentUpdateAndDelete(t, store, tableVersion)
	})
	t.Run("DeleteWhilePaging", func(t *testing.T) {
		testCassandraNexusEndpointStoreDeleteWhilePaging(t, store, tableVersion)
	})
}

func testCassandraNexusEndpointStoreConcurrentCreate(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	numConcurrentRequests := 4

	wg := sync.WaitGroup{}
	wg.Add(numConcurrentRequests)
	starter := make(chan struct{})

	endpointID := uuid.NewString()
	createErrors := make(chan error, numConcurrentRequests)
	defer close(createErrors)

	requestTableVersion := tableVersion.Load()

	for range numConcurrentRequests {
		go func() {
			<-starter
			err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
				LastKnownTableVersion: requestTableVersion,
				Endpoint: persistence.InternalNexusEndpoint{
					ID:      endpointID,
					Version: 0,
					Data: &commonpb.DataBlob{
						Data:         []byte("some dummy endpoint data"),
						EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					}},
			})
			if err != nil {
				createErrors <- err
			} else {
				tableVersion.Add(1)
			}
			wg.Done()
		}()
	}

	close(starter)
	wg.Wait()

	require.Len(t, createErrors, numConcurrentRequests-1, "exactly 1 create request should succeed")
	assertNexusEndpointsTableVersion(t, tableVersion.Load(), store)
}

func testCassandraNexusEndpointStoreConcurrentUpdate(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	endpoint := persistence.InternalNexusEndpoint{
		ID:      uuid.NewString(),
		Version: 0,
		Data: &commonpb.DataBlob{
			Data:         []byte("some dummy endpoint data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		}}

	// Create an endpoint
	createErr := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: tableVersion.Load(),
		Endpoint:              endpoint,
	})
	require.NoError(t, createErr)
	tableVersion.Add(1)
	endpoint.Version++

	numConcurrentRequests := 4
	wg := sync.WaitGroup{}
	wg.Add(numConcurrentRequests)
	starter := make(chan struct{})

	updateErrors := make(chan error, numConcurrentRequests)
	defer close(updateErrors)

	for range numConcurrentRequests {
		go func() {
			<-starter
			err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
				LastKnownTableVersion: tableVersion.Load(),
				Endpoint:              endpoint,
			})
			if err != nil {
				updateErrors <- err
			} else {
				tableVersion.Add(1)
			}
			wg.Done()
		}()
	}

	close(starter)
	wg.Wait()

	require.Len(t, updateErrors, numConcurrentRequests-1)
	assertNexusEndpointsTableVersion(t, tableVersion.Load(), store)
}

func testCassandraNexusEndpointStoreConcurrentCreateAndUpdate(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	firstEndpoint := persistence.InternalNexusEndpoint{
		ID:      uuid.NewString(),
		Version: 0,
		Data: &commonpb.DataBlob{
			Data:         []byte("some dummy endpoint data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		}}

	// Create an endpoint
	err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: tableVersion.Load(),
		Endpoint:              firstEndpoint,
	})
	require.NoError(t, err)
	tableVersion.Add(1)
	firstEndpoint.Version++

	wg := sync.WaitGroup{}
	wg.Add(2)
	starter := make(chan struct{})
	var createErr, updateErr error

	requestTableVersion := tableVersion.Load()

	// Concurrently create an endpoint
	go func() {
		<-starter
		createErr = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: requestTableVersion,
			Endpoint: persistence.InternalNexusEndpoint{
				ID:      uuid.NewString(),
				Version: 0,
				Data: &commonpb.DataBlob{
					Data:         []byte("some dummy endpoint data"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				}},
		})
		if createErr != nil {
			tableVersion.Add(1)
		}
		wg.Done()
	}()
	// Concurrently update the first endpoint
	go func() {
		<-starter
		updateErr = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: requestTableVersion,
			Endpoint:              firstEndpoint,
		})
		if updateErr != nil {
			tableVersion.Add(1)
		}
		wg.Done()
	}()

	close(starter)
	wg.Wait()

	if createErr == nil {
		require.ErrorContains(t, updateErr, "nexus endpoints table version mismatch")
	} else {
		require.ErrorContains(t, createErr, "nexus endpoints table version mismatch")
	}
	assertNexusEndpointsTableVersion(t, tableVersion.Load(), store)
}

func testCassandraNexusEndpointStoreConcurrentUpdateAndDelete(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	endpoint := persistence.InternalNexusEndpoint{
		ID:      uuid.NewString(),
		Version: 0,
		Data: &commonpb.DataBlob{
			Data:         []byte("some dummy endpoint data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		}}

	// Create an endpoint
	err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: tableVersion.Load(),
		Endpoint:              endpoint,
	})
	require.NoError(t, err)
	tableVersion.Add(1)
	endpoint.Version++

	wg := sync.WaitGroup{}
	wg.Add(2)
	starter := make(chan struct{})
	var updateErr, deleteErr error

	requestTableVersion := tableVersion.Load()

	// Concurrently update the endpoint
	go func() {
		<-starter
		updateErr = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: requestTableVersion,
			Endpoint:              endpoint,
		})
		if updateErr != nil {
			tableVersion.Add(1)
		}
		wg.Done()
	}()
	// Concurrently delete the endpoint
	go func() {
		<-starter
		deleteErr = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
			LastKnownTableVersion: requestTableVersion,
			ID:                    endpoint.ID,
		})
		if deleteErr != nil {
			tableVersion.Add(1)
		}
		wg.Done()
	}()

	close(starter)
	wg.Wait()

	if updateErr == nil {
		require.ErrorContains(t, deleteErr, "nexus endpoints table version mismatch")
	} else {
		require.ErrorContains(t, updateErr, "nexus endpoints table version mismatch")
	}
	assertNexusEndpointsTableVersion(t, tableVersion.Load(), store)
}

func testCassandraNexusEndpointStoreDeleteWhilePaging(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create some endpoints
	numEndpoints := 3
	for range numEndpoints {
		err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint: persistence.InternalNexusEndpoint{
				ID:      uuid.NewString(),
				Version: 0,
				Data: &commonpb.DataBlob{
					Data:         []byte("some dummy endpoint data"),
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				}},
		})
		require.NoError(t, err)
		tableVersion.Add(1)
	}

	// List first page
	resp1, err := store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		PageSize:              2,
	})
	require.NoError(t, err)
	require.NotNil(t, resp1)
	require.NotNil(t, resp1.NextPageToken)
	require.Equal(t, tableVersion.Load(), resp1.TableVersion)

	// Delete last endpoint in first page
	err = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
		LastKnownTableVersion: tableVersion.Load(),
		ID:                    resp1.Endpoints[1].ID,
	})
	require.NoError(t, err)
	tableVersion.Add(1)

	// List second page
	resp2, err := store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		NextPageToken:         resp1.NextPageToken,
		PageSize:              2,
	})
	require.NoError(t, err)
	require.NotNil(t, resp2)
	require.Equal(t, tableVersion.Load(), resp2.TableVersion)
}

func newNexusEndpointStore(session gocql.Session, opts ...func(params *testQueueParams)) persistence.NexusEndpointStore {
	p := testQueueParams{
		logger: log.NewTestLogger(),
	}
	for _, opt := range opts {
		opt(&p)
	}
	return cassandra.NewNexusEndpointStore(session, p.logger)
}

func assertNexusEndpointsTableVersion(t *testing.T, expected int64, store persistence.NexusEndpointStore) {
	t.Helper()

	resp, err := store.ListNexusEndpoints(context.Background(), &persistence.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		PageSize:              1,
	})

	require.NoError(t, err)
	require.Equal(t, expected, resp.TableVersion)
}
