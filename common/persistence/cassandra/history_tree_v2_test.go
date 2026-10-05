package cassandra

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/util"
)

const (
	testHistoryTreeID   = "11111111-1111-1111-1111-111111111111"
	testHistoryBranchID = "22222222-2222-2222-2222-222222222222"
)

func TestHistoryTreeV2SchemaPartitionsByTreeAndBucket(t *testing.T) {
	data, err := os.ReadFile("../../../schema/cassandra/temporal/versioned/v1.16/history_tree_v2.cql")
	require.NoError(t, err)
	require.Contains(t, string(data), "PRIMARY KEY ((tree_id, branch_bucket), branch_id)")
	authoritySchema, err := os.ReadFile("../../../schema/cassandra/temporal/versioned/v1.16/history_tree_source_authority.cql")
	require.NoError(t, err)
	require.Contains(t, string(authoritySchema), "ALTER TABLE history_tree ADD migration_authority int")
	require.Contains(t, string(authoritySchema), "ALTER TABLE history_tree ADD migration_timestamp bigint")
}

func TestHistoryTreeBranchBucketIsStableForUUIDFormatting(t *testing.T) {
	lower, err := historyTreeBranchBucket(testHistoryBranchID)
	require.NoError(t, err)
	upper, err := historyTreeBranchBucket(strings.ToUpper(testHistoryBranchID))
	require.NoError(t, err)
	require.Equal(t, lower, upper)
	require.GreaterOrEqual(t, lower, 0)
	require.Less(t, lower, historyTreeV2BucketCount)

	_, err = historyTreeBranchBucket("not-a-uuid")
	require.Error(t, err)
}

func TestHistoryTreeMigrationModeRoutesQueries(t *testing.T) {
	testCases := []struct {
		mode           config.CassandraHistoryTreeMigrationMode
		readLayout     historyTreeReadLayout
		primary        string
		mirror         string
		optionalMirror string
	}{
		{
			mode:       config.CassandraHistoryTreeMigrationModeSourceOnly,
			readLayout: historyTreeReadLayoutLegacyV1,
			primary:    v2templateInsertTree,
		},
		{
			mode:           config.CassandraHistoryTreeMigrationModeSourceRebuild,
			readLayout:     historyTreeReadLayoutLegacyV1,
			primary:        v2templateInsertTree,
			mirror:         v2templateInsertTreeV2,
			optionalMirror: historyTreeV2TableName,
		},
		{
			mode:       config.CassandraHistoryTreeMigrationModeSourceDual,
			readLayout: historyTreeReadLayoutLegacyV1,
			primary:    v2templateInsertTree,
			mirror:     v2templateInsertTreeV2,
		},
		{
			mode:       config.CassandraHistoryTreeMigrationModeTargetPrepare,
			readLayout: historyTreeReadLayoutLegacyV1,
			primary:    v2templateInsertTreeV2,
			mirror:     v2templateInsertTree,
		},
		{
			mode:       config.CassandraHistoryTreeMigrationModeTargetDual,
			readLayout: historyTreeReadLayoutBucketV2,
			primary:    v2templateInsertTreeV2,
			mirror:     v2templateInsertTree,
		},
		{
			mode:       config.CassandraHistoryTreeMigrationModeTargetOnly,
			readLayout: historyTreeReadLayoutBucketV2,
			primary:    v2templateInsertTreeV2,
		},
	}
	for _, tc := range testCases {
		t.Run(string(tc.mode), func(t *testing.T) {
			store := NewHistoryStoreWithMigrationModes(
				nil,
				serialization.NewSerializer(),
				config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
				tc.mode,
			)
			layout, err := store.historyTreeReadLayout()
			require.NoError(t, err)
			require.Equal(t, tc.readLayout, layout)
			plan, err := store.historyTreeMutationPlan(v2templateInsertTree, v2templateInsertTreeV2)
			require.NoError(t, err)
			require.Equal(t, tc.primary, plan.primaryQuery)
			require.Equal(t, tc.mirror, plan.mirrorQuery)
			require.Equal(t, tc.optionalMirror, plan.optionalMirrorTable)
		})
	}
}

func TestHistoryTreeMigrationModeDefaultsToSourceOnly(t *testing.T) {
	require.Equal(
		t,
		config.CassandraHistoryTreeMigrationModeSourceOnly,
		normalizeHistoryTreeMigrationMode(""),
	)
	store := NewHistoryStore(
		nil,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeV2Only,
	)
	plan, err := store.historyTreeMutationPlan(v2templateInsertTree, v2templateInsertTreeV2)
	require.NoError(t, err)
	require.Equal(t, v2templateInsertTree, plan.primaryQuery)
	require.Empty(t, plan.mirrorQuery)
}

func TestHistoryTreeMigrationModeRejectsUnknownMode(t *testing.T) {
	mode := config.CassandraHistoryTreeMigrationMode("invalid")
	require.Error(t, ValidateHistoryTreeMigrationMode(mode))
	store := NewHistoryStoreWithMigrationModes(
		nil,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeV2Only,
		mode,
	)
	_, err := store.historyTreeReadLayout()
	require.Error(t, err)
	_, err = store.historyTreeMutationPlan(v2templateInsertTree, v2templateInsertTreeV2)
	require.Error(t, err)
}

func TestGetHistoryTreeTableLayout(t *testing.T) {
	testCases := []struct {
		name     string
		rows     [][]any
		expected HistoryTreeTableLayout
	}{
		{name: "missing", expected: HistoryTreeTableLayoutMissing},
		{
			name: "legacy",
			rows: [][]any{
				{"tree_id", "partition_key", 0, "none"},
				{"branch_id", "clustering", 0, "asc"},
			},
			expected: HistoryTreeTableLayoutLegacyV1,
		},
		{
			name: "bucket-v2",
			rows: [][]any{
				{"tree_id", "partition_key", 0, "none"},
				{"branch_bucket", "partition_key", 1, "none"},
				{"branch_id", "clustering", 0, "asc"},
			},
			expected: HistoryTreeTableLayoutBucketV2,
		},
		{
			name: "unknown",
			rows: [][]any{
				{"tree_id", "partition_key", 0, "none"},
				{"branch_bucket", "clustering", 0, "asc"},
			},
			expected: HistoryTreeTableLayoutUnknown,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, args ...any) cgocql.Query {
					require.Equal(t, templateGetHistoryNodeSchemaColumns, stmt)
					require.Equal(t, []any{"temporal", historyTreeV2TableName}, args)
					return &recordingQuery{iter: &recordingIter{scanRows: tc.rows}}
				},
			}
			layout, err := GetHistoryTreeTableLayout(t.Context(), session, "temporal", historyTreeV2TableName)
			require.NoError(t, err)
			require.Equal(t, tc.expected, layout)
		})
	}
}

func TestValidateHistoryTreeMigrationModeSchema(t *testing.T) {
	newSession := func(legacy, target HistoryTreeTableLayout) *recordingSession {
		return &recordingSession{
			t: t,
			queryFn: func(_ string, args ...any) cgocql.Query {
				layout := legacy
				if args[1] == historyTreeV2TableName {
					layout = target
				}
				var rows [][]any
				switch layout {
				case HistoryTreeTableLayoutLegacyV1:
					rows = [][]any{
						{"tree_id", "partition_key", 0, "none"},
						{"branch_id", "clustering", 0, "asc"},
						{"migration_authority", "regular", -1, "none"},
						{"migration_timestamp", "regular", -1, "none"},
					}
				case HistoryTreeTableLayoutBucketV2:
					rows = [][]any{
						{"tree_id", "partition_key", 0, "none"},
						{"branch_bucket", "partition_key", 1, "none"},
						{"branch_id", "clustering", 0, "asc"},
					}
				default:
					rows = nil
				}
				return &recordingQuery{iter: &recordingIter{scanRows: rows}}
			},
		}
	}
	require.NoError(t, ValidateHistoryTreeMigrationModeSchema(
		t.Context(),
		newSession(HistoryTreeTableLayoutLegacyV1, HistoryTreeTableLayoutMissing),
		"temporal",
		config.CassandraHistoryTreeMigrationModeSourceRebuild,
	))
	require.NoError(t, ValidateHistoryTreeMigrationModeSchema(
		t.Context(),
		newSession(HistoryTreeTableLayoutLegacyV1, HistoryTreeTableLayoutBucketV2),
		"temporal",
		config.CassandraHistoryTreeMigrationModeTargetDual,
	))
	require.NoError(t, ValidateHistoryTreeMigrationModeSchema(
		t.Context(),
		newSession(HistoryTreeTableLayoutMissing, HistoryTreeTableLayoutBucketV2),
		"temporal",
		config.CassandraHistoryTreeMigrationModeTargetOnly,
	))
	require.Error(t, ValidateHistoryTreeMigrationModeSchema(
		t.Context(),
		newSession(HistoryTreeTableLayoutLegacyV1, HistoryTreeTableLayoutMissing),
		"temporal",
		config.CassandraHistoryTreeMigrationModeSourceDual,
	))
}

func TestHistoryV2OnlyUsesNoLegacyHistoryQueries(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			return &recordingQuery{iter: &recordingIter{}}
		},
	}
	store := NewHistoryStoreWithMigrationModes(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeV2Only,
		config.CassandraHistoryTreeMigrationModeTargetOnly,
	)
	request := testHistoryNodeAppendRequest([]byte("events"))
	require.NoError(t, store.AppendHistoryNodes(t.Context(), request))
	require.NoError(t, store.ForkHistoryBranch(t.Context(), &p.InternalForkHistoryBranchRequest{
		ForkBranchInfo: request.BranchInfo,
		NewBranchID:    "33333333-3333-3333-3333-333333333333",
		TreeInfo:       request.Node.Events,
	}))
	require.NoError(t, store.executeHistoryTreeDelete(
		t.Context(),
		v2templateDeleteBranchV2,
		testHistoryTreeID,
		testHistoryBranchID,
	))
	_, err := store.GetAllHistoryTreeBranches(t.Context(), &p.GetAllHistoryTreeBranchesRequest{PageSize: 10})
	require.NoError(t, err)
	branchToken, err := store.NewHistoryBranch(
		"",
		"",
		"",
		testHistoryTreeID,
		util.Ptr(testHistoryBranchID),
		nil,
		0,
		0,
		0,
	)
	require.NoError(t, err)
	_, err = store.ReadHistoryBranch(t.Context(), &p.InternalReadHistoryBranchRequest{
		BranchToken: branchToken,
		BranchID:    testHistoryBranchID,
		MinNodeID:   1,
		MaxNodeID:   20,
		PageSize:    10,
	})
	require.NoError(t, err)
	_, err = store.GetHistoryTreeContainingBranch(
		t.Context(),
		&p.InternalGetHistoryTreeContainingBranchRequest{BranchToken: branchToken},
	)
	require.NoError(t, err)

	require.NotEmpty(t, session.queries)
	for _, query := range session.queries {
		require.NotEqual(t, v2templateUpsertHistoryNode, query.stmt)
		require.NotEqual(t, v2templateReadHistoryNode, query.stmt)
		require.NotEqual(t, v2templateReadHistoryNodeReverse, query.stmt)
		require.NotEqual(t, v2templateReadHistoryNodeMetadata, query.stmt)
		require.NotEqual(t, v2templateInsertTree, query.stmt)
		require.NotEqual(t, v2templateDeleteBranch, query.stmt)
		require.NotEqual(t, v2templateReadAllBranches, query.stmt)
		require.NotEqual(t, v2templateScanAllTreeBranches, query.stmt)
	}
}

func TestForkHistoryBranchSourceRebuildWritesBothLayouts(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(_ string, _ ...any) cgocql.Query {
			return &recordingQuery{}
		},
	}
	store := NewHistoryStoreWithMigrationModes(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeV2Only,
		config.CassandraHistoryTreeMigrationModeSourceRebuild,
	)
	err := store.ForkHistoryBranch(t.Context(), &p.InternalForkHistoryBranchRequest{
		ForkBranchInfo: &persistencespb.HistoryBranch{TreeId: testHistoryTreeID},
		NewBranchID:    testHistoryBranchID,
		TreeInfo:       p.NewDataBlob([]byte("branch"), "Proto3"),
	})
	require.NoError(t, err)
	require.Equal(t, []string{v2templateInsertTree, v2templateInsertTreeV2}, recordedStatements(session.queries))
	require.NotZero(t, session.queries[0].query.timestamp)
	require.Equal(t, session.queries[0].query.timestamp, session.queries[1].query.timestamp)
	bucket, err := historyTreeBranchBucket(testHistoryBranchID)
	require.NoError(t, err)
	require.Equal(t, bucket, session.queries[1].args[1])
}

func TestRecreateHistoryTreeV2RequiresSourceRebuildConfirmation(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			t.Fatal("recreation without confirmation must not query Cassandra")
			return nil
		},
	}
	err := RecreateHistoryTreeV2(t.Context(), session, "temporal", false)
	require.ErrorContains(t, err, "source-rebuild mode")
}

func TestHistoryTreeV2PageTokenPreservesLayoutAndGeneration(t *testing.T) {
	generation := [16]byte{1, 2, 3}
	store := NewHistoryStoreWithMigrationModes(
		nil,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeV2Only,
		config.CassandraHistoryTreeMigrationModeTargetOnly,
	)
	store.historyNodeGenerations.historyTreeV2 = generation
	pageState := []byte("page-state")
	token := store.encodeHistoryTreePageState(pageState, historyTreeReadLayoutBucketV2)
	require.NotEqual(t, pageState, token)

	layout, decoded, err := store.decodeHistoryTreePageState(token)
	require.NoError(t, err)
	require.Equal(t, historyTreeReadLayoutBucketV2, layout)
	require.Equal(t, pageState, decoded)

	store.historyNodeGenerations.historyTreeV2[0]++
	_, _, err = store.decodeHistoryTreePageState(token)
	require.ErrorContains(t, err, "different table generation")
	_, _, err = store.decodeHistoryTreePageState(pageState)
	require.ErrorContains(t, err, "no source-layout metadata")
}

func TestBackfillHistoryTreeV2RangePreservesTimestampAndBucket(t *testing.T) {
	const writeTime = int64(123456)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateScanHistoryTreeForV2Backfill:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{testHistoryTreeID, historyTreeAuthorityBranchID, nil, "", nil},
					{testHistoryTreeID, testHistoryBranchID, []byte("branch"), "Proto3", writeTime},
				}}}
			case templateBackfillHistoryTreeV2:
				bucket, err := historyTreeBranchBucket(testHistoryBranchID)
				require.NoError(t, err)
				require.Equal(t, []any{
					testHistoryTreeID,
					bucket,
					testHistoryBranchID,
					[]byte("branch"),
					"Proto3",
					writeTime,
				}, args)
				return &recordingQuery{}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	copied, err := BackfillHistoryTreeV2Range(
		t.Context(),
		session,
		HistoryTreeBackfillOptions{
			PageSize:    10,
			Concurrency: 1,
			Partitioner: HistoryNodeBackfillMurmur3Partitioner,
		},
		HistoryNodeBackfillTokenRange{Index: 0, StartToken: -10, EndToken: 10},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), copied)
	require.True(t, session.queries[1].query.idempotent)
}

func TestValidateHistoryTreeV2IgnoresAuthorityMarker(t *testing.T) {
	const writeTime = int64(123456)
	bucket, err := historyTreeBranchBucket(testHistoryBranchID)
	require.NoError(t, err)
	legacyScan := `SELECT tree_id, branch_id, branch, branch_encoding, writetime(branch) FROM history_tree`
	v2Scan := `SELECT tree_id, branch_bucket, branch_id, branch, branch_encoding, writetime(branch) FROM history_tree_v2`
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case legacyScan:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{testHistoryTreeID, historyTreeAuthorityBranchID, nil, "", nil},
					{testHistoryTreeID, testHistoryBranchID, []byte("branch"), "Proto3", writeTime},
				}}}
			case v2Scan:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{testHistoryTreeID, bucket, testHistoryBranchID, []byte("branch"), "Proto3", writeTime},
				}}}
			case templateValidateHistoryTreeV1Row:
				require.Equal(t, []any{testHistoryTreeID, testHistoryBranchID}, args)
				fallthrough
			case templateValidateHistoryTreeV2Row:
				return &recordingQuery{scanFn: func(dest ...any) error {
					*dest[0].(*[]byte) = []byte("branch")
					*dest[1].(*string) = "Proto3"
					*dest[2].(*int64) = writeTime
					return nil
				}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	result, err := ValidateHistoryTreeV2(t.Context(), session, 10)
	require.NoError(t, err)
	require.True(t, result.Matches())
	require.Equal(t, int64(1), result.LegacyRows)
	require.Equal(t, int64(1), result.V2Rows)
}

func TestHistoryTreeValidationResultMatches(t *testing.T) {
	require.True(t, (HistoryTreeValidationResult{LegacyRows: 2, V2Rows: 2}).Matches())
	require.False(t, (HistoryTreeValidationResult{LegacyRows: 2, V2Rows: 1}).Matches())
	require.False(t, (HistoryTreeValidationResult{LegacyRows: 1, V2Rows: 1, MismatchedInV2: 1}).Matches())
}

func TestValidateHistoryNodeV2MatchesRowsInBothDirections(t *testing.T) {
	const (
		nodeID    = int64(10)
		prevTxnID = int64(9)
		txnID     = int64(11)
		writeTime = int64(123456)
	)
	row := []any{
		testHistoryTreeID,
		testHistoryBranchID,
		nodeID,
		prevTxnID,
		txnID,
		[]byte("events"),
		"Proto3",
		writeTime,
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateValidateScanHistoryNodeV1, templateValidateScanHistoryNodeV2:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{row}}}
			case templateValidateHistoryNodeV1Row, templateValidateHistoryNodeV2Row:
				require.Equal(t, []any{testHistoryTreeID, testHistoryBranchID, nodeID, txnID}, args)
				return &recordingQuery{scanFn: func(dest ...any) error {
					*dest[0].(*int64) = prevTxnID
					*dest[1].(*[]byte) = []byte("events")
					*dest[2].(*string) = "Proto3"
					*dest[3].(*int64) = writeTime
					return nil
				}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	result, err := ValidateHistoryNodeV2(t.Context(), session, 10, 1)
	require.NoError(t, err)
	require.True(t, result.Matches())
	require.Equal(t, int64(1), result.LegacyRows)
	require.Equal(t, int64(1), result.V2Rows)
}

func TestValidateHistoryNodeV2ValidatesOptions(t *testing.T) {
	_, err := ValidateHistoryNodeV2(t.Context(), nil, 0, 1)
	require.ErrorContains(t, err, "page size")
	_, err = ValidateHistoryNodeV2(t.Context(), nil, 1, 0)
	require.ErrorContains(t, err, "concurrency")
}
