//go:build integration

package tests

import (
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
)

const historyTreeAuthorityBranchIDForTest = "00000000-0000-0000-0000-000000000000"

func TestCassandraHistoryTreeV2FencedReconciliation(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()

	treeID := uuid.NewString()
	branchID := uuid.NewString()
	extraID := uuid.NewString()
	branchBucket := historyTreeBucketForTest(t, branchID)
	wrongBucket := (branchBucket + 1) % 16
	extraBucket := historyTreeBucketForTest(t, extraID)
	require.NoError(t, session.Query(
		`INSERT INTO history_tree (tree_id, branch_id, branch, branch_encoding) VALUES (?, ?, ?, ?) USING TIMESTAMP ?`,
		treeID, branchID, []byte("source"), "Proto3", int64(100),
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO history_tree_v2 (tree_id, branch_bucket, branch_id, branch, branch_encoding) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		treeID, wrongBucket, branchID, []byte("wrong-bucket"), "Proto3", int64(200),
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO history_tree_v2 (tree_id, branch_bucket, branch_id, branch, branch_encoding) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		treeID, extraBucket, extraID, []byte("extra"), "Proto3", int64(300),
	).Exec())
	// This is the state left by a process that crashed immediately after sealing.
	require.NoError(t, session.Query(
		`INSERT INTO history_tree (tree_id, branch_id, migration_authority, migration_timestamp) VALUES (?, ?, ?, ?)`,
		treeID, historyTreeAuthorityBranchIDForTest, 2, int64(1000),
	).Exec())

	result, err := cassandra.ReconcileHistoryTreeV2Tree(t.Context(), session, treeID, 10)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Trees)
	validation, err := cassandra.ValidateHistoryTreeV2(t.Context(), session, 10)
	require.NoError(t, err)
	require.True(t, validation.Matches(), "%+v", validation)
	requireHistoryTreeBranchMissing(t, session, treeID, wrongBucket, branchID)
	requireHistoryTreeBranchMissing(t, session, treeID, extraBucket, extraID)

	var authority int
	var repairTimestamp int64
	require.NoError(t, session.Query(
		`SELECT migration_authority, migration_timestamp FROM history_tree WHERE tree_id = ? AND branch_id = ?`,
		treeID, historyTreeAuthorityBranchIDForTest,
	).Scan(&authority, &repairTimestamp))
	require.Equal(t, 3, authority)
	require.Equal(t, int64(1000), repairTimestamp)
	lateID := uuid.NewString()
	lateBucket := historyTreeBucketForTest(t, lateID)
	require.NoError(t, session.Query(
		`INSERT INTO history_tree_v2 (tree_id, branch_bucket, branch_id, branch, branch_encoding) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		treeID, lateBucket, lateID, []byte("late-pre-seal-write"), "Proto3", repairTimestamp-2,
	).Exec())
	requireHistoryTreeBranchMissing(t, session, treeID, lateBucket, lateID)

	targetPrepare := cassandra.NewHistoryStoreWithMigrationModes(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeV2Only,
		config.CassandraHistoryTreeMigrationModeTargetPrepare,
	)
	preparedBranchID := uuid.NewString()
	require.NoError(t, targetPrepare.ForkHistoryBranch(t.Context(), &p.InternalForkHistoryBranchRequest{
		ForkBranchInfo: &persistencespb.HistoryBranch{TreeId: treeID},
		NewBranchID:    preparedBranchID,
		TreeInfo:       p.NewDataBlob([]byte("target-authoritative"), "Proto3"),
	}))

	reverseOnlyID := uuid.NewString()
	reverseOnlyBucket := historyTreeBucketForTest(t, reverseOnlyID)
	require.NoError(t, session.Query(
		`INSERT INTO history_tree_v2 (tree_id, branch_bucket, branch_id, branch, branch_encoding) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		treeID, reverseOnlyBucket, reverseOnlyID, []byte("target-only"), "Proto3", repairTimestamp+100,
	).Exec())
	reverseWrongID := uuid.NewString()
	reverseWrongCanonicalBucket := historyTreeBucketForTest(t, reverseWrongID)
	reverseWrongBucket := (reverseWrongCanonicalBucket + 1) % 16
	require.NoError(t, session.Query(
		`INSERT INTO history_tree_v2 (tree_id, branch_bucket, branch_id, branch, branch_encoding) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		treeID, reverseWrongBucket, reverseWrongID, []byte("move-me"), "Proto3", repairTimestamp+101,
	).Exec())
	legacyExtraID := uuid.NewString()
	require.NoError(t, session.Query(
		`INSERT INTO history_tree (tree_id, branch_id, branch, branch_encoding) VALUES (?, ?, ?, ?) USING TIMESTAMP ?`,
		treeID, legacyExtraID, []byte("legacy-extra"), "Proto3", repairTimestamp+102,
	).Exec())

	result, err = cassandra.ReconcileHistoryTreeV1Tree(t.Context(), session, treeID, 10)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Trees)
	validation, err = cassandra.ValidateHistoryTreeV2(t.Context(), session, 10)
	require.NoError(t, err)
	require.True(t, validation.Matches(), "%+v", validation)
	requireHistoryTreeBranchMissing(t, session, treeID, reverseWrongBucket, reverseWrongID)
	requireHistoryTreeBranchPresent(t, session, treeID, reverseWrongCanonicalBucket, reverseWrongID)
	requireLegacyHistoryTreeBranchMissing(t, session, treeID, legacyExtraID)

	require.NoError(t, session.Query(
		`SELECT migration_authority, migration_timestamp FROM history_tree WHERE tree_id = ? AND branch_id = ?`,
		treeID, historyTreeAuthorityBranchIDForTest,
	).Scan(&authority, &repairTimestamp))
	require.Equal(t, 1, authority)

	ranges, err := cassandra.HistoryNodeBackfillTokenRanges(1)
	require.NoError(t, err)
	mutations, err := cassandra.ReconcileHistoryTreeV1Range(
		t.Context(),
		session,
		cassandra.HistoryTreeBackfillOptions{
			PageSize:    10,
			Concurrency: 2,
			Partitioner: cassandra.HistoryNodeBackfillMurmur3Partitioner,
		},
		ranges[0],
	)
	require.NoError(t, err)
	require.Zero(t, mutations)
	require.Equal(t, int64(math.MinInt64), ranges[0].StartToken)
}

func historyTreeBucketForTest(t *testing.T, branchID string) int {
	t.Helper()
	parsed, err := uuid.Parse(branchID)
	require.NoError(t, err)
	hash := uint32(2166136261)
	for _, value := range parsed {
		hash ^= uint32(value)
		hash *= 16777619
	}
	return int(hash % 16)
}

func requireHistoryTreeBranchMissing(
	t *testing.T,
	session commongocql.Session,
	treeID string,
	bucket int,
	branchID string,
) {
	t.Helper()
	var branch []byte
	err := session.Query(
		`SELECT branch FROM history_tree_v2 WHERE tree_id = ? AND branch_bucket = ? AND branch_id = ?`,
		treeID, bucket, branchID,
	).Scan(&branch)
	require.True(t, commongocql.IsNotFoundError(err), "expected missing branch, got %v", err)
}

func requireHistoryTreeBranchPresent(
	t *testing.T,
	session commongocql.Session,
	treeID string,
	bucket int,
	branchID string,
) {
	t.Helper()
	var branch []byte
	require.NoError(t, session.Query(
		`SELECT branch FROM history_tree_v2 WHERE tree_id = ? AND branch_bucket = ? AND branch_id = ?`,
		treeID, bucket, branchID,
	).Scan(&branch))
}

func requireLegacyHistoryTreeBranchMissing(
	t *testing.T,
	session commongocql.Session,
	treeID string,
	branchID string,
) {
	t.Helper()
	var branch []byte
	err := session.Query(
		`SELECT branch FROM history_tree WHERE tree_id = ? AND branch_id = ?`,
		treeID, branchID,
	).Scan(&branch)
	require.True(t, commongocql.IsNotFoundError(err), "expected missing branch, got %v", err)
}
