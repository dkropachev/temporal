package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/util"
)

func TestReadHistoryBranchContinuationKeepsSourceLayout(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	pageState := []byte("cassandra-page-state")
	testCases := []struct {
		name             string
		sourceMode       config.CassandraHistoryNodeMigrationMode
		continuationMode config.CassandraHistoryNodeMigrationMode
		metadataOnly     bool
		reverseOrder     bool
		layout           historyNodeReadLayout
		query            string
	}{
		{
			name:             "legacy v1 token after canonical cutover",
			sourceMode:       config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
			continuationMode: config.CassandraHistoryNodeMigrationModeCanonicalDual,
			layout:           historyNodeReadLayoutLegacyV1,
			query:            v2templateReadHistoryNode,
		},
		{
			name:             "legacy v1 metadata token after canonical cutover",
			sourceMode:       config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
			continuationMode: config.CassandraHistoryNodeMigrationModeCanonicalDual,
			metadataOnly:     true,
			layout:           historyNodeReadLayoutLegacyV1,
			query:            v2templateReadHistoryNodeMetadata,
		},
		{
			name:             "canonical v2 token after legacy rollback",
			sourceMode:       config.CassandraHistoryNodeMigrationModeCanonicalDual,
			continuationMode: config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
			layout:           historyNodeReadLayoutCanonicalV2,
			query:            v2templateReadHistoryNodeV2,
		},
		{
			name:             "old v2 reverse token after cutover",
			sourceMode:       config.CassandraHistoryNodeMigrationModeOldV2Dual,
			continuationMode: config.CassandraHistoryNodeMigrationModeOldV2CutoverDual,
			reverseOrder:     true,
			layout:           historyNodeReadLayoutOldV2,
			query:            v2templateReadHistoryNodeReverseOldV2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			queryCount := 0
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, args ...any) cgocql.Query {
					require.Equal(t, tc.query, stmt)
					require.Equal(t, []any{treeID, branchID, int64(1), int64(10)}, args)
					queryCount++
					iter := &recordingIter{}
					if queryCount == 1 {
						iter.pageState = pageState
					}
					return &recordingQuery{iter: iter}
				},
			}
			sourceStore := NewHistoryStore(session, serialization.NewSerializer(), tc.sourceMode)
			branchToken, err := sourceStore.NewHistoryBranch(
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
			request := &p.InternalReadHistoryBranchRequest{
				BranchToken:  branchToken,
				BranchID:     branchID,
				MinNodeID:    1,
				MaxNodeID:    10,
				PageSize:     1,
				MetadataOnly: tc.metadataOnly,
				ReverseOrder: tc.reverseOrder,
			}

			firstPage, err := sourceStore.ReadHistoryBranch(t.Context(), request)
			require.NoError(t, err)
			require.Equal(t, pageState, firstPage.NextPageToken)
			require.Equal(
				t,
				encodeHistoryNodePageTokenMetadata(tc.layout),
				firstPage.NextPageTokenMetadata,
			)

			request.NextPageToken = firstPage.NextPageToken
			request.NextPageTokenMetadata = firstPage.NextPageTokenMetadata
			continuationStore := NewHistoryStore(
				session,
				serialization.NewSerializer(),
				tc.continuationMode,
			)
			secondPage, err := continuationStore.ReadHistoryBranch(t.Context(), request)
			require.NoError(t, err)
			require.Nil(t, secondPage.NextPageToken)
			require.Nil(t, secondPage.NextPageTokenMetadata)
			require.Len(t, session.queries, 2)
			require.Empty(t, session.queries[0].query.pageState)
			require.Equal(t, pageState, session.queries[1].query.pageState)
		})
	}
}

func TestReadHistoryBranchAcceptsLegacyRawPageToken(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	pageState := []byte("legacy-raw-page-state")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateReadHistoryNodeV2, stmt)
			require.Equal(t, []any{treeID, branchID, int64(1), int64(10)}, args)
			return &recordingQuery{iter: &recordingIter{}}
		},
	}
	store := NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
	)
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

	response, err := store.ReadHistoryBranch(t.Context(), &p.InternalReadHistoryBranchRequest{
		BranchToken:   branchToken,
		BranchID:      branchID,
		MinNodeID:     1,
		MaxNodeID:     10,
		PageSize:      1,
		NextPageToken: pageState,
	})

	require.NoError(t, err)
	require.Nil(t, response.NextPageToken)
	require.Nil(t, response.NextPageTokenMetadata)
	require.Len(t, session.queries, 1)
	require.Equal(t, pageState, session.queries[0].query.pageState)
}

func TestReadHistoryBranchRejectsMalformedPageTokenMetadata(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	testCases := []struct {
		name         string
		pageState    []byte
		metadata     []byte
		errorMessage string
	}{
		{
			name:         "truncated metadata",
			pageState:    []byte("state"),
			metadata:     []byte{historyNodePageTokenMetadataVersion},
			errorMessage: "metadata length",
		},
		{
			name:      "unsupported version",
			pageState: []byte("state"),
			metadata: []byte{
				historyNodePageTokenMetadataVersion + 1,
				byte(historyNodeReadLayoutLegacyV1),
			},
			errorMessage: "metadata version",
		},
		{
			name:      "unknown layout",
			pageState: []byte("state"),
			metadata: []byte{
				historyNodePageTokenMetadataVersion,
				255,
			},
			errorMessage: "metadata read layout",
		},
		{
			name: "metadata without Cassandra state",
			metadata: []byte{
				historyNodePageTokenMetadataVersion,
				byte(historyNodeReadLayoutLegacyV1),
			},
			errorMessage: "without Cassandra page state",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(string, ...any) cgocql.Query {
					t.Fatal("malformed page token must not query Cassandra")
					return nil
				},
			}
			store := NewHistoryStore(session, serialization.NewSerializer())
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

			response, err := store.ReadHistoryBranch(t.Context(), &p.InternalReadHistoryBranchRequest{
				BranchToken:           branchToken,
				BranchID:              branchID,
				MinNodeID:             1,
				MaxNodeID:             10,
				PageSize:              1,
				NextPageToken:         tc.pageState,
				NextPageTokenMetadata: tc.metadata,
			})

			require.Nil(t, response)
			var invalidRequest *p.InvalidPersistenceRequestError
			require.ErrorAs(t, err, &invalidRequest)
			require.ErrorContains(t, err, tc.errorMessage)
			require.Empty(t, session.queries)
		})
	}
}
