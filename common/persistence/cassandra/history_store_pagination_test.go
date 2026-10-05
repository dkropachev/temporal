package cassandra

import (
	"encoding/binary"
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
			if tc.layout == historyNodeReadLayoutCanonicalV2 {
				require.NotEqual(t, pageState, firstPage.NextPageToken)
				require.Equal(
					t,
					[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
					firstPage.NextPageToken[:9],
				)
			} else {
				require.Equal(t, pageState, firstPage.NextPageToken)
			}
			require.Equal(
				t,
				sourceStore.encodeHistoryNodePageTokenMetadata(tc.layout),
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

func TestHistoryNodeCanonicalPageStateEnvelope(t *testing.T) {
	pageState := []byte("cassandra-page-state")

	envelope := encodeHistoryNodePageState(
		pageState,
		historyNodeReadLayoutCanonicalV2,
	)
	require.NotEqual(t, pageState, envelope)
	require.Equal(
		t,
		[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		envelope[:9],
	)
	lengthOffset := len(historyNodePageTokenEnvelopePrefix) + 1
	require.Equal(t, uint32(len(pageState)), binary.BigEndian.Uint32(envelope[lengthOffset:]))

	decoded, err := decodeHistoryNodePageState(
		envelope,
		historyNodeReadLayoutCanonicalV2,
	)
	require.NoError(t, err)
	require.Equal(t, pageState, decoded)

	legacyState, err := decodeHistoryNodePageState(
		envelope,
		historyNodeReadLayoutLegacyV1,
	)
	require.NoError(t, err)
	require.Equal(t, envelope, legacyState)
	require.NotEqual(t, pageState, legacyState)

	rawLegacyState := encodeHistoryNodePageState(
		pageState,
		historyNodeReadLayoutLegacyV1,
	)
	require.Equal(t, pageState, rawLegacyState)
}

func TestHistoryNodeCanonicalPageStateEnvelopeRejectsMalformed(t *testing.T) {
	pageState := []byte("cassandra-page-state")
	validEnvelope := encodeHistoryNodePageState(
		pageState,
		historyNodeReadLayoutCanonicalV2,
	)

	badVersion := append([]byte(nil), validEnvelope...)
	badVersion[len(historyNodePageTokenEnvelopePrefix)]++
	badLength := append([]byte(nil), validEnvelope...)
	badLength[len(historyNodePageTokenEnvelopePrefix)+1+3]++

	testCases := []struct {
		name         string
		pageToken    []byte
		errorMessage string
	}{
		{
			name:         "unguarded",
			pageToken:    pageState,
			errorMessage: "not guarded",
		},
		{
			name:         "truncated",
			pageToken:    []byte(historyNodePageTokenEnvelopePrefix),
			errorMessage: "truncated",
		},
		{
			name:         "bad version",
			pageToken:    badVersion,
			errorMessage: "version",
		},
		{
			name:         "bad length",
			pageToken:    badLength,
			errorMessage: "length",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := decodeHistoryNodePageState(
				tc.pageToken,
				historyNodeReadLayoutCanonicalV2,
			)

			require.Nil(t, decoded)
			var invalidRequest *p.InvalidPersistenceRequestError
			require.ErrorAs(t, err, &invalidRequest)
			require.ErrorContains(t, err, tc.errorMessage)
		})
	}
}

func TestReadHistoryBranchRejectsDifferentTableGeneration(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	pageState := []byte("cassandra-page-state")
	queryCount := 0
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			queryCount++
			require.Equal(t, v2templateReadHistoryNodeV2, stmt)
			require.Equal(t, []any{treeID, branchID, int64(1), int64(10)}, args)
			return &recordingQuery{iter: &recordingIter{pageState: pageState}}
		},
	}
	firstGeneration := [16]byte{1}
	sourceStore := newHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
		historyNodeTableGenerations{historyNodeV2: firstGeneration},
	)
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
		BranchToken: branchToken,
		BranchID:    branchID,
		MinNodeID:   1,
		MaxNodeID:   10,
		PageSize:    1,
	}

	firstPage, err := sourceStore.ReadHistoryBranch(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, historyNodePageTokenMetadataVersion, firstPage.NextPageTokenMetadata[0])
	require.Len(t, firstPage.NextPageTokenMetadata, historyNodePageTokenMetadataLength)

	request.NextPageToken = firstPage.NextPageToken
	request.NextPageTokenMetadata = firstPage.NextPageTokenMetadata
	continuationStore := newHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
		historyNodeTableGenerations{historyNodeV2: [16]byte{2}},
	)
	response, err := continuationStore.ReadHistoryBranch(t.Context(), request)

	require.Nil(t, response)
	var invalidRequest *p.InvalidPersistenceRequestError
	require.ErrorAs(t, err, &invalidRequest)
	require.ErrorContains(t, err, "different table generation")
	require.Equal(t, 1, queryCount)
}

func TestReadHistoryBranchRejectsLegacyRawPageToken(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	for _, mode := range []config.CassandraHistoryNodeMigrationMode{
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
		config.CassandraHistoryNodeMigrationModeLegacyV1RollbackDual,
	} {
		t.Run(string(mode), func(t *testing.T) {
			pageState := []byte("legacy-raw-page-state")
			session := &recordingSession{
				t: t,
				queryFn: func(string, ...any) cgocql.Query {
					t.Fatal("raw page token must not query Cassandra")
					return nil
				},
			}
			store := NewHistoryStore(session, serialization.NewSerializer(), mode)
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

			require.Nil(t, response)
			var invalidRequest *p.InvalidPersistenceRequestError
			require.ErrorAs(t, err, &invalidRequest)
			require.ErrorContains(t, err, "restart pagination")
			require.Empty(t, session.queries)
		})
	}
}

func TestReadHistoryBranchAcceptsRawPageTokenBeforeReadCutover(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	testCases := []struct {
		name  string
		mode  config.CassandraHistoryNodeMigrationMode
		query string
	}{
		{
			name:  "legacy V1 rebuild",
			mode:  config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2,
			query: v2templateReadHistoryNode,
		},
		{
			name:  "old V2 rebuild",
			mode:  config.CassandraHistoryNodeMigrationModeOldV2RebuildV2,
			query: v2templateReadHistoryNode,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pageState := []byte("legacy-raw-page-state")
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, args ...any) cgocql.Query {
					require.Equal(t, tc.query, stmt)
					require.Equal(t, []any{treeID, branchID, int64(1), int64(10)}, args)
					return &recordingQuery{iter: &recordingIter{}}
				},
			}
			store := NewHistoryStore(session, serialization.NewSerializer(), tc.mode)
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
			require.NotNil(t, response)
			require.Len(t, session.queries, 1)
			require.Equal(t, pageState, session.queries[0].query.pageState)
		})
	}
}

func TestReadHistoryBranchRejectsContinuationFromRecreatedLayout(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	testCases := []struct {
		name   string
		mode   config.CassandraHistoryNodeMigrationMode
		layout historyNodeReadLayout
	}{
		{
			name:   "old V2 token after entering V1 rebuild",
			mode:   config.CassandraHistoryNodeMigrationModeV1RebuildDual,
			layout: historyNodeReadLayoutOldV2,
		},
		{
			name:   "old V2 token after canonical transition",
			mode:   config.CassandraHistoryNodeMigrationModeCanonicalDual,
			layout: historyNodeReadLayoutOldV2,
		},
		{
			name:   "canonical token while V2 is rebuilt",
			mode:   config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2,
			layout: historyNodeReadLayoutCanonicalV2,
		},
		{
			name:   "legacy token in V2 only mode",
			mode:   config.CassandraHistoryNodeMigrationModeV2Only,
			layout: historyNodeReadLayoutLegacyV1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(string, ...any) cgocql.Query {
					t.Fatal("unsafe continuation must not query Cassandra")
					return nil
				},
			}
			store := NewHistoryStore(session, serialization.NewSerializer(), tc.mode)
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
				NextPageToken:         []byte("state"),
				NextPageTokenMetadata: store.encodeHistoryNodePageTokenMetadata(tc.layout),
			})

			require.Nil(t, response)
			var invalidRequest *p.InvalidPersistenceRequestError
			require.ErrorAs(t, err, &invalidRequest)
			require.ErrorContains(t, err, "restart pagination")
			require.Empty(t, session.queries)
		})
	}
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
			metadata: append(
				[]byte{historyNodePageTokenMetadataVersion, 255},
				make([]byte, 16)...,
			),
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
