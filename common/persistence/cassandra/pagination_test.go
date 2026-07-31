package cassandra

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/util"
)

var (
	benchmarkExecutionStateSink            []*p.InternalWorkflowMutableState
	benchmarkConcreteExecutionResponseSink *p.InternalListConcreteExecutionsResponse
	benchmarkCurrentExecutionResponseSink  *p.InternalGetCurrentExecutionResponse
	benchmarkWorkflowExecutionResponseSink *p.InternalGetWorkflowExecutionResponse
	benchmarkHistoryBranchResponseSink     *p.InternalReadHistoryBranchResponse
	benchmarkTaskResponseSink              *p.InternalGetTasksResponse
	benchmarkBoolSink                      bool
)

func TestListNexusEndpointsUsesSameQueryForPageToken(t *testing.T) {
	endpointID := "11111111-1111-1111-1111-111111111111"
	token := []byte("page-token")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateListEndpointsFirstPageQuery:
				require.Empty(t, args)
				id, err := gocql.ParseUUID(endpointID)
				require.NoError(t, err)
				return &recordingQuery{
					iter: &recordingIter{
						mapRows: []map[string]any{
							{
								"id":            id,
								"version":       int64(1),
								"data":          []byte("endpoint"),
								"data_encoding": enumspb.ENCODING_TYPE_PROTO3.String(),
							},
						},
					},
				}
			case templateGetTableVersion:
				return &recordingQuery{scanFn: func(dest ...any) error {
					*dest[0].(*int64) = 1
					return nil
				}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store := &NexusEndpointStore{session: session}

	resp, err := store.ListNexusEndpoints(t.Context(), &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: 1,
		NextPageToken:         token,
		PageSize:              2,
	})

	require.NoError(t, err)
	require.Len(t, resp.Endpoints, 1)
	require.Len(t, session.queries, 2)
	require.Equal(t, templateListEndpointsFirstPageQuery, session.queries[0].stmt)
	require.Equal(t, token, session.queries[0].query.pageState)
}

func TestListNexusEndpointsTreatsEmptyPageTokenAsFirstPage(t *testing.T) {
	endpointID := "11111111-1111-1111-1111-111111111111"
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListEndpointsFirstPageQuery, stmt)
			require.Empty(t, args)
			id, err := gocql.ParseUUID(endpointID)
			require.NoError(t, err)
			return &recordingQuery{
				iter: &recordingIter{
					mapRows: []map[string]any{
						{
							"version": int64(7),
						},
						{
							"id":            id,
							"version":       int64(1),
							"data":          []byte("endpoint"),
							"data_encoding": enumspb.ENCODING_TYPE_PROTO3.String(),
						},
					},
				},
			}
		},
	}
	store := &NexusEndpointStore{session: session}

	resp, err := store.ListNexusEndpoints(t.Context(), &p.ListNexusEndpointsRequest{
		NextPageToken: []byte{},
		PageSize:      10,
	})

	require.NoError(t, err)
	require.Equal(t, int64(7), resp.TableVersion)
	require.Len(t, resp.Endpoints, 1)
	require.Equal(t, endpointID, resp.Endpoints[0].ID)
	require.Len(t, session.queries, 1)
	require.Empty(t, session.queries[0].query.pageState)
}

func TestNullableInt64UnmarshalCQL(t *testing.T) {
	info := gocql.NewNativeType(4, gocql.TypeBigInt)
	var value nullableInt64

	require.NoError(t, value.UnmarshalCQL(info, nil))
	require.False(t, value.valid)
	require.Zero(t, value.value)

	require.NoError(t, value.UnmarshalCQL(info, []byte{0, 0, 0, 0, 0, 0, 0, 42}))
	require.True(t, value.valid)
	require.Equal(t, int64(42), value.value)

	require.Error(t, value.UnmarshalCQL(info, []byte{1}))
}

func TestListNexusEndpointsFirstPageChecksTableVersion(t *testing.T) {
	iter := &recordingIter{
		mapRows: []map[string]any{
			{
				"version": int64(2),
			},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListEndpointsFirstPageQuery, stmt)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &NexusEndpointStore{session: session}

	_, err := store.ListNexusEndpoints(t.Context(), &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: 1,
		PageSize:              10,
	})

	require.ErrorIs(t, err, p.ErrNexusTableVersionConflict)
	require.Equal(t, 1, iter.closeCalls)
}

func TestListNexusEndpointsClosesIteratorOnEndpointRowError(t *testing.T) {
	iter := &recordingIter{
		mapRows: []map[string]any{
			{
				"id":      "endpoint-id",
				"version": int64(1),
			},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListEndpointsFirstPageQuery, stmt)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &NexusEndpointStore{session: session}

	resp, err := store.ListNexusEndpoints(t.Context(), &p.ListNexusEndpointsRequest{
		NextPageToken: []byte("next-page"),
		PageSize:      10,
	})

	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, 1, iter.closeCalls)
}

func TestListNexusEndpointsFirstPageClosesIteratorOnVersionRowError(t *testing.T) {
	iter := &recordingIter{
		mapRows: []map[string]any{
			{
				"version": "not-int64",
			},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListEndpointsFirstPageQuery, stmt)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &NexusEndpointStore{session: session}

	resp, err := store.ListNexusEndpoints(t.Context(), &p.ListNexusEndpointsRequest{
		PageSize: 10,
	})

	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, 1, iter.closeCalls)
}

func TestListQueuesUsesSameQueryForPageToken(t *testing.T) {
	pageState := []byte("page-token")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetQueueNamesQuery, stmt)
			require.Equal(t, []any{p.QueueTypeHistoryDLQ}, args)
			return &recordingQuery{iter: &recordingIter{}}
		},
	}
	store := &queueV2Store{session: session}

	resp, err := store.ListQueues(t.Context(), &p.InternalListQueuesRequest{
		QueueType:     p.QueueTypeHistoryDLQ,
		PageSize:      10,
		NextPageToken: pageState,
	})

	require.NoError(t, err)
	require.Empty(t, resp.Queues)
	require.Nil(t, resp.NextPageToken)
	require.Len(t, session.queries, 1)
	require.Equal(t, pageState, session.queries[0].query.pageState)
}

func TestListQueuesReturnsIteratorPageState(t *testing.T) {
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: p.FirstQueueMessageID,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateGetQueueNamesQuery:
				require.Equal(t, []any{p.QueueTypeHistoryDLQ}, args)
				return &recordingQuery{
					iter: &recordingIter{
						pageState: []byte("next-page"),
						scanRows: [][]any{
							{"queue-0", queueBytes, enumspb.ENCODING_TYPE_PROTO3.String(), int64(0)},
						},
					},
				}
			case TemplateGetMaxMessageIDQuery:
				return &recordingQuery{
					scanFn: func(dest ...any) error {
						return gocql.ErrNotFound
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store := &queueV2Store{session: session}

	resp, err := store.ListQueues(t.Context(), &p.InternalListQueuesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		PageSize:  1,
	})

	require.NoError(t, err)
	require.Len(t, resp.Queues, 1)
	require.Equal(t, []byte("next-page"), resp.NextPageToken)
}

func TestListQueuesQueryKeepsUpgradeCompatibleQueueSchema(t *testing.T) {
	require.Contains(t, templateGetQueueNamesQuery, "ALLOW FILTERING")
}

func TestListQueuesClosesIteratorOnMetadataError(t *testing.T) {
	iter := &recordingIter{
		scanRows: [][]any{
			{"bad-queue", []byte("metadata"), "invalid-encoding", int64(1)},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetQueueNamesQuery, stmt)
			require.Equal(t, []any{p.QueueTypeHistoryDLQ}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := NewQueueV2Store(session, log.NewNoopLogger())

	resp, err := store.ListQueues(t.Context(), &p.InternalListQueuesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		PageSize:  10,
	})

	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, 1, iter.closeCalls)
}

func TestScheduledTaskQueriesHaveSeparatedPredicates(t *testing.T) {
	require.NotContains(t, templateGetHistoryScheduledTasksQuery, "?and")
	require.NotContains(t, templateGetTimerTasksQuery, "?and")
}

func TestQueueMessageInsertsUseConditionalWrites(t *testing.T) {
	require.Contains(t, templateEnqueueMessageQuery, "IF NOT EXISTS")
	require.Contains(t, TemplateEnqueueMessageQuery, "IF NOT EXISTS")
}

func TestHistoryNodeSchemaKeepsRollbackCompatibleTable(t *testing.T) {
	requireHistoryNodeSchemaPrimaryKey(
		t,
		"../../../schema/cassandra/temporal/schema.cql",
		"history_node",
		"PRIMARY KEY ((tree_id), branch_id, node_id, txn_id",
	)
	requireHistoryNodeSchemaPrimaryKey(
		t,
		"../../../schema/cassandra/temporal/versioned/v1.0/schema.cql",
		"history_node",
		"PRIMARY KEY ((tree_id), branch_id, node_id, txn_id",
	)
}

func TestHistoryNodeV2SchemaPartitionsByTreeAndBranch(t *testing.T) {
	requireHistoryNodeSchemaPrimaryKey(
		t,
		"../../../schema/cassandra/temporal/schema.cql",
		"history_node_v2",
		"PRIMARY KEY ((tree_id, branch_id), node_id, txn_id",
	)
	requireHistoryNodeSchemaPrimaryKey(
		t,
		"../../../schema/cassandra/temporal/versioned/v1.15/history_node_v2.cql",
		"history_node_v2",
		"PRIMARY KEY ((tree_id, branch_id), node_id, txn_id",
	)
}

func TestHistoryNodeQueriesMatchSelectedSchema(t *testing.T) {
	require.Contains(t, v2templateReadHistoryNodeReverse, "ORDER BY branch_id DESC, node_id DESC")
	require.Contains(t, v2templateReadHistoryNodeReverseOldV2, "ORDER BY node_id DESC")
	require.NotContains(t, v2templateReadHistoryNodeReverseOldV2, "ORDER BY branch_id")
	require.Contains(t, v2templateReadHistoryNodeReverseV2, "ORDER BY node_id DESC")
	require.NotContains(t, v2templateReadHistoryNodeReverseV2, "ORDER BY branch_id")
	require.Contains(t, v2templateRangeDeleteHistoryNode, "WHERE tree_id = ? AND branch_id = ?")
	require.Contains(t, v2templateRangeDeleteHistoryNodeV2, "WHERE tree_id = ? AND branch_id = ?")
}

func TestHistoryNodeMigrationModeRoutesQueries(t *testing.T) {
	testCases := []struct {
		mode            config.CassandraHistoryNodeMigrationMode
		forwardRead     string
		reverseRead     string
		metadataRead    string
		mutationQueries []string
		optionalMirror  string
	}{
		{
			mode:            config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2,
			forwardRead:     v2templateReadHistoryNode,
			reverseRead:     v2templateReadHistoryNodeReverse,
			metadataRead:    v2templateReadHistoryNodeMetadata,
			mutationQueries: []string{v2templateUpsertHistoryNode, v2templateUpsertHistoryNodeV2},
			optionalMirror:  historyNodeV2TableName,
		},
		{
			mode:            config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
			forwardRead:     v2templateReadHistoryNode,
			reverseRead:     v2templateReadHistoryNodeReverse,
			metadataRead:    v2templateReadHistoryNodeMetadata,
			mutationQueries: []string{v2templateUpsertHistoryNode, v2templateUpsertHistoryNodeV2},
		},
		{
			mode:            config.CassandraHistoryNodeMigrationModeLegacyV1RollbackDual,
			forwardRead:     v2templateReadHistoryNode,
			reverseRead:     v2templateReadHistoryNodeReverse,
			metadataRead:    v2templateReadHistoryNodeMetadata,
			mutationQueries: []string{v2templateUpsertHistoryNode, v2templateUpsertHistoryNodeV2},
		},
		{
			mode:            config.CassandraHistoryNodeMigrationModeLegacyV1CutoverDual,
			forwardRead:     v2templateReadHistoryNode,
			reverseRead:     v2templateReadHistoryNodeReverse,
			metadataRead:    v2templateReadHistoryNodeMetadata,
			mutationQueries: []string{v2templateUpsertHistoryNodeV2, v2templateUpsertHistoryNode},
		},
		{
			mode:            config.CassandraHistoryNodeMigrationModeOldV2RebuildV2,
			forwardRead:     v2templateReadHistoryNode,
			reverseRead:     v2templateReadHistoryNodeReverseOldV2,
			metadataRead:    v2templateReadHistoryNodeMetadata,
			mutationQueries: []string{v2templateUpsertHistoryNode, v2templateUpsertHistoryNodeV2},
			optionalMirror:  historyNodeV2TableName,
		},
		{
			mode:            config.CassandraHistoryNodeMigrationModeOldV2Dual,
			forwardRead:     v2templateReadHistoryNode,
			reverseRead:     v2templateReadHistoryNodeReverseOldV2,
			metadataRead:    v2templateReadHistoryNodeMetadata,
			mutationQueries: []string{v2templateUpsertHistoryNode, v2templateUpsertHistoryNodeV2},
		},
		{
			mode:            config.CassandraHistoryNodeMigrationModeOldV2PrepareCutoverDual,
			forwardRead:     v2templateReadHistoryNode,
			reverseRead:     v2templateReadHistoryNodeReverseOldV2,
			metadataRead:    v2templateReadHistoryNodeMetadata,
			mutationQueries: []string{v2templateUpsertHistoryNodeV2, v2templateUpsertHistoryNode},
		},
		{
			mode:            config.CassandraHistoryNodeMigrationModeV1RebuildDual,
			forwardRead:     v2templateReadHistoryNodeV2,
			reverseRead:     v2templateReadHistoryNodeReverseV2,
			metadataRead:    v2templateReadHistoryNodeMetadataV2,
			mutationQueries: []string{v2templateUpsertHistoryNodeV2, v2templateUpsertHistoryNode},
			optionalMirror:  historyNodeTableName,
		},
		{
			mode:            config.CassandraHistoryNodeMigrationModeV1CutoverDual,
			forwardRead:     v2templateReadHistoryNodeV2,
			reverseRead:     v2templateReadHistoryNodeReverseV2,
			metadataRead:    v2templateReadHistoryNodeMetadataV2,
			mutationQueries: []string{v2templateUpsertHistoryNode, v2templateUpsertHistoryNodeV2},
		},
		{
			mode:            config.CassandraHistoryNodeMigrationModeOldV2CutoverDual,
			forwardRead:     v2templateReadHistoryNodeV2,
			reverseRead:     v2templateReadHistoryNodeReverseV2,
			metadataRead:    v2templateReadHistoryNodeMetadataV2,
			mutationQueries: []string{v2templateUpsertHistoryNodeV2, v2templateUpsertHistoryNode},
		},
		{
			mode:            config.CassandraHistoryNodeMigrationModeV2Only,
			forwardRead:     v2templateReadHistoryNodeV2,
			reverseRead:     v2templateReadHistoryNodeReverseV2,
			metadataRead:    v2templateReadHistoryNodeMetadataV2,
			mutationQueries: []string{v2templateUpsertHistoryNodeV2},
		},
		{
			mode:            config.CassandraHistoryNodeMigrationModeCanonicalDual,
			forwardRead:     v2templateReadHistoryNodeV2,
			reverseRead:     v2templateReadHistoryNodeReverseV2,
			metadataRead:    v2templateReadHistoryNodeMetadataV2,
			mutationQueries: []string{v2templateUpsertHistoryNodeV2, v2templateUpsertHistoryNode},
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.mode), func(t *testing.T) {
			store := NewHistoryStore(nil, serialization.NewSerializer(), tc.mode)

			query, err := store.historyNodeReadQuery(false, false)
			require.NoError(t, err)
			require.Equal(t, tc.forwardRead, query)
			query, err = store.historyNodeReadQuery(false, true)
			require.NoError(t, err)
			require.Equal(t, tc.reverseRead, query)
			query, err = store.historyNodeReadQuery(true, false)
			require.NoError(t, err)
			require.Equal(t, tc.metadataRead, query)

			queries, err := store.historyNodeMutationQueries(
				v2templateUpsertHistoryNode,
				v2templateUpsertHistoryNodeV2,
			)
			require.NoError(t, err)
			require.Equal(t, tc.mutationQueries, queries)
			plan, err := store.historyNodeMutationPlan(
				v2templateUpsertHistoryNode,
				v2templateUpsertHistoryNodeV2,
			)
			require.NoError(t, err)
			require.Equal(t, tc.optionalMirror, plan.optionalMirrorTable)
		})
	}
}

func TestAppendHistoryNodesWritesMirrorOutsideBatch(t *testing.T) {
	events := []byte("events")
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{}
		},
	}
	store := NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
	)

	err := store.AppendHistoryNodes(t.Context(), testHistoryNodeAppendRequest(events))

	require.NoError(t, err)
	require.Equal(t, []string{
		v2templateUpsertHistoryNodeV2,
		v2templateUpsertHistoryNode,
	}, recordedStatements(session.queries))
	require.Equal(t, events, session.queries[0].args[5])
	require.Equal(t, events, session.queries[1].args[5])
	require.NotZero(t, session.queries[0].query.timestamp)
	require.Equal(t, session.queries[0].query.timestamp, session.queries[1].query.timestamp)
}

func TestAppendHistoryNodesDualWriteFailureHandling(t *testing.T) {
	testCases := []struct {
		name           string
		mode           config.CassandraHistoryNodeMigrationMode
		failureQuery   string
		failure        error
		wantError      bool
		wantStatements []string
	}{
		{
			name:         "primary failure stops before mirror",
			mode:         config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
			failureQuery: v2templateUpsertHistoryNode,
			failure:      errors.New("primary failed"),
			wantError:    true,
			wantStatements: []string{
				v2templateUpsertHistoryNode,
			},
		},
		{
			name:         "required mirror failure is returned",
			mode:         config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
			failureQuery: v2templateUpsertHistoryNodeV2,
			failure:      errors.New("mirror failed"),
			wantError:    true,
			wantStatements: []string{
				v2templateUpsertHistoryNode,
				v2templateUpsertHistoryNodeV2,
			},
		},
		{
			name:         "missing optional mirror is ignored",
			mode:         config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2,
			failureQuery: v2templateUpsertHistoryNodeV2,
			failure: historyNodeRequestError{
				code:    gocql.ErrCodeInvalid,
				message: "unconfigured table history_node_v2",
			},
			wantStatements: []string{
				v2templateUpsertHistoryNode,
				v2templateUpsertHistoryNodeV2,
			},
		},
		{
			name:         "other optional mirror failure is returned",
			mode:         config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2,
			failureQuery: v2templateUpsertHistoryNodeV2,
			failure:      errors.New("mirror failed"),
			wantError:    true,
			wantStatements: []string{
				v2templateUpsertHistoryNode,
				v2templateUpsertHistoryNodeV2,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, _ ...any) cgocql.Query {
					return &recordingQuery{
						execFn: func() error {
							if stmt == tc.failureQuery {
								return tc.failure
							}
							return nil
						},
					}
				},
			}
			store := NewHistoryStore(session, serialization.NewSerializer(), tc.mode)

			err := store.AppendHistoryNodes(t.Context(), testHistoryNodeAppendRequest([]byte("events")))

			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantStatements, recordedStatements(session.queries))
		})
	}
}

func TestAppendHistoryNodesRetryRepairsRequiredMirror(t *testing.T) {
	persisted := make(map[string]bool)
	mirrorAttempts := 0
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			return &recordingQuery{
				execFn: func() error {
					if stmt == v2templateUpsertHistoryNode {
						mirrorAttempts++
						if mirrorAttempts == 1 {
							return errors.New("mirror failed")
						}
					}
					persisted[stmt] = true
					return nil
				},
			}
		},
	}
	store := NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
	)
	request := testHistoryNodeAppendRequest([]byte("events"))

	require.Error(t, store.AppendHistoryNodes(t.Context(), request))
	require.True(t, persisted[v2templateUpsertHistoryNodeV2])
	require.False(t, persisted[v2templateUpsertHistoryNode])

	require.NoError(t, store.AppendHistoryNodes(t.Context(), request))
	require.True(t, persisted[v2templateUpsertHistoryNodeV2])
	require.True(t, persisted[v2templateUpsertHistoryNode])
	require.Equal(t, []string{
		v2templateUpsertHistoryNodeV2,
		v2templateUpsertHistoryNode,
		v2templateUpsertHistoryNodeV2,
		v2templateUpsertHistoryNode,
	}, recordedStatements(session.queries))
}

func TestHistoryNodeMigrationModeRejectsUnknownMode(t *testing.T) {
	mode := config.CassandraHistoryNodeMigrationMode("invalid")
	require.Error(t, ValidateHistoryNodeMigrationMode(mode))

	store := NewHistoryStore(nil, serialization.NewSerializer(), mode)
	_, err := store.historyNodeReadQuery(false, false)
	require.Error(t, err)
	_, err = store.historyNodeMutationQueries(
		v2templateUpsertHistoryNode,
		v2templateUpsertHistoryNodeV2,
	)
	require.Error(t, err)
}

func TestFactoryRejectsUnknownHistoryNodeMigrationMode(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			t.Fatal("invalid migration mode must not query Cassandra")
			return nil
		},
	}
	factory := NewFactoryFromSession(
		config.Cassandra{
			Keyspace:                 "temporal",
			HistoryNodeMigrationMode: config.CassandraHistoryNodeMigrationMode("invalid"),
		},
		"test-cluster",
		log.NewNoopLogger(),
		session,
		serialization.NewSerializer(),
	)

	_, err := factory.NewExecutionStore()

	require.ErrorContains(t, err, "unsupported Cassandra history node migration mode")
}

func TestGetHistoryNodeTableLayout(t *testing.T) {
	testCases := []struct {
		name     string
		rows     [][]any
		expected HistoryNodeTableLayout
	}{
		{
			name:     "missing",
			expected: HistoryNodeTableLayoutMissing,
		},
		{
			name:     "legacy-v1",
			rows:     historyNodeSchemaRows(HistoryNodeTableLayoutLegacyV1),
			expected: HistoryNodeTableLayoutLegacyV1,
		},
		{
			name:     "branch-v2",
			rows:     historyNodeSchemaRows(HistoryNodeTableLayoutBranchV2),
			expected: HistoryNodeTableLayoutBranchV2,
		},
		{
			name: "unknown",
			rows: [][]any{
				{"tree_id", "partition_key", 0, "none"},
				{"node_id", "clustering", 0, "asc"},
			},
			expected: HistoryNodeTableLayoutUnknown,
		},
		{
			name: "wrong-clustering-order",
			rows: [][]any{
				{"tree_id", "partition_key", 0, "none"},
				{"branch_id", "clustering", 0, "asc"},
				{"node_id", "clustering", 1, "asc"},
				{"txn_id", "clustering", 2, "asc"},
			},
			expected: HistoryNodeTableLayoutUnknown,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, args ...any) cgocql.Query {
					require.Equal(t, templateGetHistoryNodeSchemaColumns, stmt)
					require.Equal(t, []any{"temporal", historyNodeTableName}, args)
					return &recordingQuery{
						iter: &recordingIter{scanRows: tc.rows},
					}
				},
			}

			layout, err := GetHistoryNodeTableLayout(
				t.Context(),
				session,
				"temporal",
				historyNodeTableName,
			)

			require.NoError(t, err)
			require.Equal(t, tc.expected, layout)
		})
	}
}

func TestGetHistoryNodeTableGenerations(t *testing.T) {
	historyNodeID := [16]byte{1}
	historyNodeV2ID := [16]byte{2}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetHistoryNodeTableID, stmt)
			require.Len(t, args, 2)
			table := args[1].(string)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					id := historyNodeID
					if table == historyNodeV2TableName {
						id = historyNodeV2ID
					}
					*dest[0].(*[16]byte) = id
					return nil
				},
			}
		},
	}

	generations, err := getHistoryNodeTableGenerations(t.Context(), session, "temporal")

	require.NoError(t, err)
	require.Equal(t, historyNodeID, generations.historyNode)
	require.Equal(t, historyNodeV2ID, generations.historyNodeV2)
	require.Equal(t, []string{
		templateGetHistoryNodeTableID,
		templateGetHistoryNodeTableID,
	}, recordedStatements(session.queries))
}

func TestValidateHistoryNodeMigrationModeSchema(t *testing.T) {
	newSession := func(
		historyNodeLayout HistoryNodeTableLayout,
		v2Layout HistoryNodeTableLayout,
	) *recordingSession {
		return &recordingSession{
			t: t,
			queryFn: func(stmt string, args ...any) cgocql.Query {
				require.Equal(t, templateGetHistoryNodeSchemaColumns, stmt)
				require.Len(t, args, 2)
				table := args[1].(string)
				layout := historyNodeLayout
				if table == historyNodeV2TableName {
					layout = v2Layout
				}
				return &recordingQuery{
					iter: &recordingIter{scanRows: historyNodeSchemaRows(layout)},
				}
			},
		}
	}

	require.NoError(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutLegacyV1, HistoryNodeTableLayoutBranchV2),
		"temporal",
		config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
	))
	require.NoError(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutLegacyV1, HistoryNodeTableLayoutBranchV2),
		"temporal",
		config.CassandraHistoryNodeMigrationModeLegacyV1RollbackDual,
	))
	require.NoError(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutLegacyV1, HistoryNodeTableLayoutBranchV2),
		"temporal",
		config.CassandraHistoryNodeMigrationModeLegacyV1CutoverDual,
	))
	require.NoError(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutLegacyV1, HistoryNodeTableLayoutMissing),
		"temporal",
		config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2,
	))
	require.NoError(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutBranchV2, HistoryNodeTableLayoutBranchV2),
		"temporal",
		config.CassandraHistoryNodeMigrationModeOldV2Dual,
	))
	require.NoError(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutBranchV2, HistoryNodeTableLayoutBranchV2),
		"temporal",
		config.CassandraHistoryNodeMigrationModeOldV2PrepareCutoverDual,
	))
	require.NoError(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutBranchV2, HistoryNodeTableLayoutMissing),
		"temporal",
		config.CassandraHistoryNodeMigrationModeOldV2RebuildV2,
	))
	require.NoError(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutBranchV2, HistoryNodeTableLayoutBranchV2),
		"temporal",
		config.CassandraHistoryNodeMigrationModeOldV2CutoverDual,
	))
	require.NoError(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutMissing, HistoryNodeTableLayoutBranchV2),
		"temporal",
		config.CassandraHistoryNodeMigrationModeV2Only,
	))
	require.NoError(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutMissing, HistoryNodeTableLayoutBranchV2),
		"temporal",
		config.CassandraHistoryNodeMigrationModeV1RebuildDual,
	))
	require.NoError(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutLegacyV1, HistoryNodeTableLayoutBranchV2),
		"temporal",
		config.CassandraHistoryNodeMigrationModeV1CutoverDual,
	))
	require.Error(t, ValidateHistoryNodeMigrationModeSchema(
		t.Context(),
		newSession(HistoryNodeTableLayoutBranchV2, HistoryNodeTableLayoutBranchV2),
		"temporal",
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
	))
}

func TestRecreateHistoryNodeV1RequiresRebuildConfirmation(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			t.Fatal("recreation without confirmation must not query Cassandra")
			return nil
		},
	}

	err := RecreateHistoryNodeV1(t.Context(), session, "temporal", false)

	require.ErrorContains(t, err, "every Temporal writer is in v1-rebuild mode")
}

func TestRecreateHistoryNodeV1ClearsExistingLegacyTable(t *testing.T) {
	awaitCalls := 0
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateGetHistoryNodeSchemaColumns:
				require.Equal(t, "temporal", args[0])
				table := args[1].(string)
				layout := HistoryNodeTableLayoutLegacyV1
				if table == historyNodeV2TableName {
					layout = HistoryNodeTableLayoutBranchV2
				}
				return &recordingQuery{
					iter: &recordingIter{
						scanRows: historyNodeSchemaRows(layout),
					},
				}
			case fmt.Sprintf(templateDropHistoryNodeTable, quoteCQLIdentifier("temporal")),
				fmt.Sprintf(templateCreateHistoryNodeV1, quoteCQLIdentifier("temporal")):
				require.Empty(t, args)
				return &recordingQuery{}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
		awaitSchemaAgreementFn: func(context.Context) error {
			awaitCalls++
			return nil
		},
	}

	err := RecreateHistoryNodeV1(t.Context(), session, "temporal", true)

	require.NoError(t, err)
	require.Equal(t, 2, awaitCalls)
	require.Equal(t, []string{
		templateGetHistoryNodeSchemaColumns,
		templateGetHistoryNodeSchemaColumns,
		fmt.Sprintf(templateDropHistoryNodeTable, quoteCQLIdentifier("temporal")),
		fmt.Sprintf(templateCreateHistoryNodeV1, quoteCQLIdentifier("temporal")),
		templateGetHistoryNodeSchemaColumns,
	}, recordedStatements(session.queries))
}

func TestRecreateHistoryNodeV2RequiresRebuildConfirmation(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			t.Fatal("recreation without confirmation must not query Cassandra")
			return nil
		},
	}

	err := RecreateHistoryNodeV2(t.Context(), session, "temporal", false)

	require.ErrorContains(t, err, "every Temporal writer is in a source-rebuild mode")
}

func historyNodeSchemaRows(layout HistoryNodeTableLayout) [][]any {
	switch layout {
	case HistoryNodeTableLayoutMissing:
		return nil
	case HistoryNodeTableLayoutLegacyV1:
		return [][]any{
			{"txn_id", "clustering", 2, "desc"},
			{"tree_id", "partition_key", 0, "none"},
			{"node_id", "clustering", 1, "asc"},
			{"branch_id", "clustering", 0, "asc"},
			{"data", "regular", -1, "none"},
		}
	case HistoryNodeTableLayoutBranchV2:
		return [][]any{
			{"txn_id", "clustering", 1, "desc"},
			{"branch_id", "partition_key", 1, "none"},
			{"node_id", "clustering", 0, "asc"},
			{"tree_id", "partition_key", 0, "none"},
		}
	default:
		return [][]any{
			{"tree_id", "partition_key", 0, "none"},
		}
	}
}

func TestQueueMetadataSchemaKeepsUpgradeCompatiblePrimaryKey(t *testing.T) {
	requireQueueMetadataSchemaKeepsUpgradeCompatiblePrimaryKey(t, "../../../schema/cassandra/temporal/schema.cql")
	requireQueueMetadataSchemaKeepsUpgradeCompatiblePrimaryKey(t, "../../../schema/cassandra/temporal/versioned/v1.9/queues.cql")
}

func TestFreshSchemaRetainsDeprecatedV114Tables(t *testing.T) {
	statements, err := p.LoadAndSplitQuery([]string{"../../../schema/cassandra/temporal/schema.cql"})
	require.NoError(t, err)

	for _, table := range []string{"queue_message_id_range", "queue_message_id_ranges"} {
		require.Conditionf(t, func() bool {
			for _, stmt := range statements {
				if strings.HasPrefix(stmt, "CREATE TABLE "+table) {
					return true
				}
			}
			return false
		}, "fresh schema is missing deprecated upgrade table %s", table)
	}
}

func requireHistoryNodeSchemaPrimaryKey(t *testing.T, schema string, table string, primaryKey string) {
	statements, err := p.LoadAndSplitQuery([]string{schema})
	require.NoError(t, err)

	for _, stmt := range statements {
		normalized := strings.ReplaceAll(stmt, "IF NOT EXISTS ", "")
		fields := strings.Fields(normalized)
		if len(fields) >= 3 && fields[0] == "CREATE" && fields[1] == "TABLE" && fields[2] == table {
			require.Contains(t, stmt, primaryKey)
			return
		}
	}
	require.Failf(t, "missing history node schema", "table %s not found in %s", table, schema)
}

func requireQueueMetadataSchemaKeepsUpgradeCompatiblePrimaryKey(t *testing.T, schema string) {
	statements, err := p.LoadAndSplitQuery([]string{schema})
	require.NoError(t, err)

	for _, stmt := range statements {
		if strings.Contains(stmt, "CREATE TABLE queues") {
			require.NotContains(t, stmt, "queue_bucket")
			require.Contains(t, stmt, "PRIMARY KEY ((queue_type, queue_name))")
			return
		}
	}
	require.Fail(t, "missing queues schema")
}

func TestGetClusterMembersOmitsAllowFilteringForPartitionScan(t *testing.T) {
	stmt := recordGetClusterMembersQuery(t, &p.GetClusterMembersRequest{})

	require.NotContains(t, stmt, templateAllowFiltering)
	require.Equal(t, templateGetClusterMembership, stmt)
}

func TestGetClusterMembersOmitsAllowFilteringForFullPrimaryKeyLookup(t *testing.T) {
	stmt := recordGetClusterMembersQuery(t, &p.GetClusterMembersRequest{
		HostIDEquals: []byte("host-id"),
		RoleEquals:   p.Matching,
	})

	require.NotContains(t, stmt, templateAllowFiltering)
	require.Contains(t, stmt, templateWithHostIDSuffix)
	require.Contains(t, stmt, templateWithRoleSuffix)
}

func TestGetClusterMembersKeepsAllowFilteringForSecondaryFilters(t *testing.T) {
	for _, request := range []*p.GetClusterMembersRequest{
		{HostIDEquals: []byte("host-id")},
		{RPCAddressEquals: net.ParseIP("127.0.0.1")},
		{SessionStartedAfter: time.Now().UTC()},
		{LastHeartbeatWithin: time.Minute},
	} {
		stmt := recordGetClusterMembersQuery(t, request)

		require.Contains(t, stmt, templateAllowFiltering)
	}
}

func TestCountTaskQueuesByBuildIDUsesLimit(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateLimitedCountTaskQueueByBuildIDQuery, stmt)
			require.Equal(t, []any{"namespace-id", "build-id", 2}, args)
			return &recordingQuery{
				iter: &recordingIter{
					scanRows: [][]any{
						{"task-queue-1"},
						{"task-queue-2"},
					},
				},
			}
		},
	}
	store := userDataStore{Session: session}

	count, err := store.CountTaskQueuesByBuildId(t.Context(), &p.CountTaskQueuesByBuildIdRequest{
		NamespaceID: "namespace-id",
		BuildID:     "build-id",
		Limit:       2,
	})

	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, []string{templateLimitedCountTaskQueueByBuildIDQuery}, recordedStatements(session.queries))
}

func TestCountTaskQueuesByBuildIDConvertsExactCountError(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateCountTaskQueueByBuildIDQuery, stmt)
			require.Equal(t, []any{"namespace-id", "build-id"}, args)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					return errors.New("cassandra unavailable")
				},
			}
		},
	}
	store := userDataStore{Session: session}

	count, err := store.CountTaskQueuesByBuildId(t.Context(), &p.CountTaskQueuesByBuildIdRequest{
		NamespaceID: "namespace-id",
		BuildID:     "build-id",
	})

	require.Error(t, err)
	require.Zero(t, count)
	require.Contains(t, err.Error(), "CountTaskQueuesByBuildId")
}

func TestGetTaskQueuesByBuildIDUsesNextPageToken(t *testing.T) {
	pageToken := []byte("next-page")
	queryCalls := 0
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			queryCalls++
			require.Equal(t, templateListTaskQueueNamesByBuildIdQuery, stmt)
			require.Equal(t, []any{"namespace-id", "build-id"}, args)
			switch queryCalls {
			case 1:
				return &recordingQuery{
					iter: &recordingIter{
						mapRows: []map[string]any{
							{"task_queue_name": "task-queue-1"},
						},
						pageState: pageToken,
					},
				}
			case 2:
				return &recordingQuery{
					iter: &recordingIter{
						mapRows: []map[string]any{
							{"task_queue_name": "task-queue-2"},
						},
					},
				}
			default:
				t.Fatalf("unexpected query call: %d", queryCalls)
				return nil
			}
		},
	}
	store := userDataStore{Session: session}

	taskQueues, err := store.GetTaskQueuesByBuildId(t.Context(), &p.GetTaskQueuesByBuildIdRequest{
		NamespaceID: "namespace-id",
		BuildID:     "build-id",
	})

	require.NoError(t, err)
	require.Equal(t, []string{"task-queue-1", "task-queue-2"}, taskQueues)
	require.Len(t, session.queries, 2)
	require.Empty(t, session.queries[0].query.pageState)
	require.Equal(t, pageToken, session.queries[1].query.pageState)
	require.Equal(t, listTaskQueueNamesByBuildIdPageSize, session.queries[0].query.pageSize)
	require.Equal(t, listTaskQueueNamesByBuildIdPageSize, session.queries[1].query.pageSize)
}

func TestListTaskQueueUserDataEntriesClosesIteratorOnRowError(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  map[string]any
	}{
		{
			name: "missing task queue",
			row: map[string]any{
				"data":          []byte("data"),
				"data_encoding": enumspb.ENCODING_TYPE_PROTO3.String(),
				"version":       int64(1),
			},
		},
		{
			name: "missing data",
			row: map[string]any{
				"task_queue_name": "task-queue",
				"data_encoding":   enumspb.ENCODING_TYPE_PROTO3.String(),
				"version":         int64(1),
			},
		},
		{
			name: "missing encoding",
			row: map[string]any{
				"task_queue_name": "task-queue",
				"data":            []byte("data"),
				"version":         int64(1),
			},
		},
		{
			name: "missing version",
			row: map[string]any{
				"task_queue_name": "task-queue",
				"data":            []byte("data"),
				"data_encoding":   enumspb.ENCODING_TYPE_PROTO3.String(),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			iter := &recordingIter{
				mapRows: []map[string]any{tc.row},
			}
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, args ...any) cgocql.Query {
					require.Equal(t, templateListTaskQueueUserDataQuery, stmt)
					require.Equal(t, []any{"namespace-id"}, args)
					return &recordingQuery{
						iter: iter,
					}
				},
			}
			store := userDataStore{Session: session}

			response, err := store.ListTaskQueueUserDataEntries(t.Context(), &p.ListTaskQueueUserDataEntriesRequest{
				NamespaceID: "namespace-id",
				PageSize:    100,
			})

			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, 1, iter.closeCalls)
		})
	}
}

func TestListConcreteExecutionsClosesIterator(t *testing.T) {
	iter := &recordingIter{
		closeErr: errors.New("close failed"),
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListWorkflowExecutionQuery, stmt)
			require.Equal(t, []any{int32(7), rowTypeExecution}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &MutableStateStore{Session: session}

	response, err := store.ListConcreteExecutions(t.Context(), &p.ListConcreteExecutionsRequest{
		ShardID:  7,
		PageSize: 100,
	})

	require.Error(t, err)
	require.Nil(t, response)
	require.Contains(t, err.Error(), "ListConcreteExecutions")
	require.Len(t, session.queries, 1)
	require.Equal(t, 1, iter.closeCalls)
}

func TestListConcreteExecutionsPreallocatesPage(t *testing.T) {
	iter := &recordingIter{}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListWorkflowExecutionQuery, stmt)
			require.Equal(t, []any{int32(7), rowTypeExecution}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &MutableStateStore{Session: session}

	response, err := store.ListConcreteExecutions(t.Context(), &p.ListConcreteExecutionsRequest{
		ShardID:  7,
		PageSize: 100,
	})

	require.NoError(t, err)
	require.Empty(t, response.States)
	require.Equal(t, 100, cap(response.States))
	require.Equal(t, 1, iter.closeCalls)
}

func BenchmarkListConcreteExecutionsResultAllocation(b *testing.B) {
	states := make([]*p.InternalWorkflowMutableState, 100)
	for i := range states {
		states[i] = &p.InternalWorkflowMutableState{}
	}

	b.Run("grow", func(b *testing.B) {
		for b.Loop() {
			result := make([]*p.InternalWorkflowMutableState, 0)
			//nolint:staticcheck // Model the row-at-a-time iterator used by ListConcreteExecutions.
			for _, state := range states {
				result = append(result, state)
			}
			benchmarkExecutionStateSink = result
		}
	})
	b.Run("preallocate", func(b *testing.B) {
		for b.Loop() {
			result := make([]*p.InternalWorkflowMutableState, 0, len(states))
			//nolint:staticcheck // Model the row-at-a-time iterator used by ListConcreteExecutions.
			for _, state := range states {
				result = append(result, state)
			}
			benchmarkExecutionStateSink = result
		}
	})
}

func BenchmarkListConcreteExecutionsReadPage(b *testing.B) {
	rows := make([][]any, 100)
	for i := range rows {
		rows[i] = []any{
			"run-id",
			[]byte("execution"),
			enumspb.ENCODING_TYPE_PROTO3.String(),
			[]byte("execution-state"),
			enumspb.ENCODING_TYPE_PROTO3.String(),
			int64(i + 1),
		}
	}
	session := &recordingSession{
		t: b,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{
				iter: &recordingIter{
					scanRows: rows,
				},
			}
		},
	}
	store := &MutableStateStore{Session: session}
	request := &p.ListConcreteExecutionsRequest{
		ShardID:  7,
		PageSize: 100,
	}

	b.ResetTimer()
	for b.Loop() {
		response, err := store.ListConcreteExecutions(b.Context(), request)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkConcreteExecutionResponseSink = response
	}
}

func BenchmarkGetWorkflowExecutionRead(b *testing.B) {
	row := newWorkflowExecutionTestRow(b)
	session := &recordingSession{
		t: b,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{
				mapScanFn: func(dest map[string]any) error {
					for key, value := range row {
						dest[key] = value
					}
					return nil
				},
				scanFn: func(dest ...any) error {
					return scanWorkflowExecutionTestRow(row, dest)
				},
			}
		},
	}
	store := &MutableStateStore{Session: session}
	request := &p.GetWorkflowExecutionRequest{
		ShardID:     7,
		NamespaceID: "namespace-id",
		WorkflowID:  "workflow-id",
		RunID:       "run-id",
	}

	b.ResetTimer()
	for b.Loop() {
		response, err := store.GetWorkflowExecution(b.Context(), request)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkWorkflowExecutionResponseSink = response
	}
}

func BenchmarkGetCurrentExecutionRead(b *testing.B) {
	serializer := serialization.NewSerializer()
	executionStateBlob, err := serializer.WorkflowExecutionStateToBlob(&persistencespb.WorkflowExecutionState{
		RunId: "11111111-1111-1111-1111-111111111111",
		State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
	})
	require.NoError(b, err)
	currentRunID, err := gocql.ParseUUID("11111111-1111-1111-1111-111111111111")
	require.NoError(b, err)
	row := map[string]any{
		"current_run_id":              currentRunID,
		"execution":                   []byte("execution"),
		"execution_encoding":          enumspb.ENCODING_TYPE_PROTO3.String(),
		"execution_state":             executionStateBlob.Data,
		"execution_state_encoding":    executionStateBlob.EncodingType.String(),
		"workflow_last_write_version": int64(42),
	}
	session := &recordingSession{
		t: b,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{
				mapScanFn: func(dest map[string]any) error {
					for key, value := range row {
						dest[key] = value
					}
					return nil
				},
				scanFn: func(dest ...any) error {
					*dest[0].(*gocql.UUID) = row["current_run_id"].(gocql.UUID)
					*dest[1].(*[]byte) = row["execution_state"].([]byte)
					*dest[2].(*string) = row["execution_state_encoding"].(string)
					return nil
				},
			}
		},
	}
	store := NewMutableStateStore(session, serializer, log.NewNoopLogger())
	request := &p.GetCurrentExecutionRequest{
		ShardID:     7,
		NamespaceID: "namespace-id",
		WorkflowID:  "workflow-id",
	}

	b.ResetTimer()
	for b.Loop() {
		response, err := store.GetCurrentExecution(b.Context(), request)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCurrentExecutionResponseSink = response
	}
}

func TestGetCurrentExecutionUsesTypedProjection(t *testing.T) {
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
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			require.Equal(t, templateGetCurrentExecutionQuery, stmt)
			require.NotContains(t, stmt, "current_run_id, execution,")
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					require.Len(t, dest, 3)
					*dest[0].(*gocql.UUID) = currentRunID
					*dest[1].(*[]byte) = executionStateBlob.Data
					*dest[2].(*string) = executionStateBlob.EncodingType.String()
					return nil
				},
			}
		},
	}
	store := NewMutableStateStore(session, serializer, log.NewNoopLogger())

	response, err := store.GetCurrentExecution(t.Context(), &p.GetCurrentExecutionRequest{
		ShardID:     7,
		NamespaceID: "namespace-id",
		WorkflowID:  "workflow-id",
	})

	require.NoError(t, err)
	require.Equal(t, executionState.RunId, response.RunID)
	require.Equal(t, executionState.RunId, response.ExecutionState.RunId)
	require.Equal(t, executionState.State, response.ExecutionState.State)
}

func TestGetWorkflowExecutionTypedScan(t *testing.T) {
	row := newWorkflowExecutionTestRow(t)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			require.Equal(t, templateGetWorkflowExecutionQuery, stmt)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					return scanWorkflowExecutionTestRow(row, dest)
				},
			}
		},
	}
	store := &MutableStateStore{Session: session}

	response, err := store.GetWorkflowExecution(t.Context(), &p.GetWorkflowExecutionRequest{
		ShardID:     7,
		NamespaceID: "namespace-id",
		WorkflowID:  "workflow-id",
		RunID:       "run-id",
	})

	require.NoError(t, err)
	require.Equal(t, int64(7), response.DBRecordVersion)
	require.Equal(t, p.NewDataBlob([]byte("execution"), enumspb.ENCODING_TYPE_PROTO3.String()), response.State.ExecutionInfo)
	require.Equal(t, p.NewDataBlob([]byte("execution-state"), enumspb.ENCODING_TYPE_PROTO3.String()), response.State.ExecutionState)
	require.Equal(t, int64(42), response.State.NextEventID)
	require.Equal(t, p.NewDataBlob([]byte("activity"), enumspb.ENCODING_TYPE_PROTO3.String()), response.State.ActivityInfos[1])
	require.Equal(t, p.NewDataBlob([]byte("timer"), enumspb.ENCODING_TYPE_PROTO3.String()), response.State.TimerInfos["timer"])
	require.Equal(t, p.NewDataBlob([]byte("child"), enumspb.ENCODING_TYPE_PROTO3.String()), response.State.ChildExecutionInfos[2])
	require.Equal(t, p.NewDataBlob([]byte("cancel"), enumspb.ENCODING_TYPE_PROTO3.String()), response.State.RequestCancelInfos[3])
	require.Equal(t, p.NewDataBlob([]byte("signal"), enumspb.ENCODING_TYPE_PROTO3.String()), response.State.SignalInfos[4])
	require.Equal(t, []string{"11111111-1111-1111-1111-111111111111"}, response.State.SignalRequestedIDs)
	require.Equal(t, p.NewDataBlob([]byte("events"), enumspb.ENCODING_TYPE_PROTO3.String()), response.State.BufferedEvents[0])
	require.Equal(t, p.NewDataBlob([]byte("chasm"), enumspb.ENCODING_TYPE_PROTO3.String()), response.State.ChasmNodes["node"].CassandraBlob)
	require.Equal(t, p.NewDataBlob([]byte("checksum"), enumspb.ENCODING_TYPE_PROTO3.String()), response.State.Checksum)
}

func TestGetWorkflowExecutionTypedScanDefaultsNullDBRecordVersion(t *testing.T) {
	row := newWorkflowExecutionTestRow(t)
	delete(row, "db_record_version")
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					return scanWorkflowExecutionTestRow(row, dest)
				},
			}
		},
	}
	store := &MutableStateStore{Session: session}

	response, err := store.GetWorkflowExecution(t.Context(), &p.GetWorkflowExecutionRequest{})

	require.NoError(t, err)
	require.Zero(t, response.DBRecordVersion)
}

func TestListConcreteExecutionsClosesIteratorOnScanError(t *testing.T) {
	iter := &recordingIter{
		closeErr: errors.New("scan failed"),
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListWorkflowExecutionQuery, stmt)
			require.Equal(t, []any{int32(7), rowTypeExecution}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &MutableStateStore{Session: session}

	response, err := store.ListConcreteExecutions(t.Context(), &p.ListConcreteExecutionsRequest{
		ShardID:  7,
		PageSize: 100,
	})

	require.Error(t, err)
	require.Nil(t, response)
	require.ErrorContains(t, err, "scan failed")
	require.Equal(t, 1, iter.closeCalls)
}

func newWorkflowExecutionTestRow(t testing.TB) map[string]any {
	t.Helper()
	signalID, err := gocql.ParseUUID("11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)
	return map[string]any{
		"execution":                     []byte("execution"),
		"execution_encoding":            enumspb.ENCODING_TYPE_PROTO3.String(),
		"execution_state":               []byte("execution-state"),
		"execution_state_encoding":      enumspb.ENCODING_TYPE_PROTO3.String(),
		"next_event_id":                 int64(42),
		"activity_map":                  map[int64][]byte{1: []byte("activity")},
		"activity_map_encoding":         enumspb.ENCODING_TYPE_PROTO3.String(),
		"timer_map":                     map[string][]byte{"timer": []byte("timer")},
		"timer_map_encoding":            enumspb.ENCODING_TYPE_PROTO3.String(),
		"child_executions_map":          map[int64][]byte{2: []byte("child")},
		"child_executions_map_encoding": enumspb.ENCODING_TYPE_PROTO3.String(),
		"request_cancel_map":            map[int64][]byte{3: []byte("cancel")},
		"request_cancel_map_encoding":   enumspb.ENCODING_TYPE_PROTO3.String(),
		"signal_map":                    map[int64][]byte{4: []byte("signal")},
		"signal_map_encoding":           enumspb.ENCODING_TYPE_PROTO3.String(),
		"signal_requested":              []gocql.UUID{signalID},
		"buffered_events_list":          []map[string]any{{"encoding_type": enumspb.ENCODING_TYPE_PROTO3.String(), "data": []byte("events")}},
		"chasm_node_map":                map[string][]byte{"node": []byte("chasm")},
		"chasm_node_map_encoding":       enumspb.ENCODING_TYPE_PROTO3.String(),
		"checksum":                      []byte("checksum"),
		"checksum_encoding":             enumspb.ENCODING_TYPE_PROTO3.String(),
		"db_record_version":             int64(7),
	}
}

func scanWorkflowExecutionTestRow(row map[string]any, dest []any) error {
	*dest[0].(*[]byte) = row["execution"].([]byte)
	*dest[1].(*string) = row["execution_encoding"].(string)
	*dest[2].(*[]byte) = row["execution_state"].([]byte)
	*dest[3].(*string) = row["execution_state_encoding"].(string)
	*dest[4].(*int64) = row["next_event_id"].(int64)
	*dest[5].(*map[int64][]byte) = row["activity_map"].(map[int64][]byte)
	*dest[6].(*string) = row["activity_map_encoding"].(string)
	*dest[7].(*map[string][]byte) = row["timer_map"].(map[string][]byte)
	*dest[8].(*string) = row["timer_map_encoding"].(string)
	*dest[9].(*map[int64][]byte) = row["child_executions_map"].(map[int64][]byte)
	*dest[10].(*string) = row["child_executions_map_encoding"].(string)
	*dest[11].(*map[int64][]byte) = row["request_cancel_map"].(map[int64][]byte)
	*dest[12].(*string) = row["request_cancel_map_encoding"].(string)
	*dest[13].(*map[int64][]byte) = row["signal_map"].(map[int64][]byte)
	*dest[14].(*string) = row["signal_map_encoding"].(string)
	*dest[15].(*[]gocql.UUID) = row["signal_requested"].([]gocql.UUID)
	*dest[16].(*[]map[string]any) = row["buffered_events_list"].([]map[string]any)
	*dest[17].(*map[string][]byte) = row["chasm_node_map"].(map[string][]byte)
	*dest[18].(*string) = row["chasm_node_map_encoding"].(string)
	*dest[19].(*[]byte) = row["checksum"].([]byte)
	*dest[20].(*string) = row["checksum_encoding"].(string)
	if dbRecordVersion, ok := row["db_record_version"]; ok {
		*dest[21].(*nullableInt64) = nullableInt64{
			value: dbRecordVersion.(int64),
			valid: true,
		}
	}
	return nil
}

func TestListConcreteExecutionsTypedScanSkipsCurrentRow(t *testing.T) {
	pageToken := []byte("next-page")
	iter := &recordingIter{
		scanRows: [][]any{
			{"current-run", nil, nil, nil, nil, int64(0)},
			{
				"run-id",
				[]byte("execution"),
				enumspb.ENCODING_TYPE_PROTO3.String(),
				[]byte("execution-state"),
				enumspb.ENCODING_TYPE_PROTO3.String(),
				int64(42),
			},
		},
		pageState:               pageToken,
		pageStateAfterExhausted: true,
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListWorkflowExecutionQuery, stmt)
			require.Equal(t, []any{int32(7), rowTypeExecution}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &MutableStateStore{Session: session}

	response, err := store.ListConcreteExecutions(t.Context(), &p.ListConcreteExecutionsRequest{
		ShardID:  7,
		PageSize: 100,
	})

	require.NoError(t, err)
	require.Len(t, response.States, 1)
	require.Equal(t, []byte("execution"), response.States[0].ExecutionInfo.Data)
	require.Equal(t, enumspb.ENCODING_TYPE_PROTO3, response.States[0].ExecutionInfo.EncodingType)
	require.Equal(t, []byte("execution-state"), response.States[0].ExecutionState.Data)
	require.Equal(t, enumspb.ENCODING_TYPE_PROTO3, response.States[0].ExecutionState.EncodingType)
	require.Equal(t, int64(42), response.States[0].NextEventID)
	require.Equal(t, pageToken, response.NextPageToken)
	require.Equal(t, 1, iter.closeCalls)
}

func TestGetTasksV1ClosesIteratorOnScanError(t *testing.T) {
	iter := &recordingIter{
		closeErr: errors.New("scan failed"),
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetTasksQuery, stmt)
			require.Equal(t, []any{"namespace-id", "task-queue", enumspb.TASK_QUEUE_TYPE_WORKFLOW, rowTypeTask, int64(1), int64(10)}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &matchingTaskStoreV1{Session: session}

	response, err := store.GetTasks(t.Context(), &p.GetTasksRequest{
		NamespaceID:        "namespace-id",
		TaskQueue:          "task-queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: 10,
		PageSize:           10,
	})

	require.Error(t, err)
	require.Nil(t, response)
	require.Equal(t, 1, iter.closeCalls)
}

func TestGetTasksV1TypedScanSkipsStaticRow(t *testing.T) {
	pageToken := []byte("next-page")
	iter := &recordingIter{
		scanRows: [][]any{
			{nil, nil, nil},
			{int64(1), []byte("task"), enumspb.ENCODING_TYPE_PROTO3.String()},
		},
		pageState:               pageToken,
		pageStateAfterExhausted: true,
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetTasksQuery, stmt)
			require.Equal(t, []any{"namespace-id", "task-queue", enumspb.TASK_QUEUE_TYPE_WORKFLOW, rowTypeTask, int64(1), int64(10)}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &matchingTaskStoreV1{Session: session}

	response, err := store.GetTasks(t.Context(), &p.GetTasksRequest{
		NamespaceID:        "namespace-id",
		TaskQueue:          "task-queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: 10,
		PageSize:           10,
	})

	require.NoError(t, err)
	require.Len(t, response.Tasks, 1)
	require.Equal(t, []byte("task"), response.Tasks[0].Data)
	require.Equal(t, enumspb.ENCODING_TYPE_PROTO3, response.Tasks[0].EncodingType)
	require.Equal(t, pageToken, response.NextPageToken)
	require.Equal(t, 1, iter.closeCalls)
}

func TestGetTasksV2ClosesIteratorOnScanError(t *testing.T) {
	iter := &recordingIter{
		closeErr: errors.New("scan failed"),
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetTasksQuery_v2_limit, stmt)
			require.Equal(t, []any{
				"namespace-id",
				"task-queue",
				enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				rowTypeTask,
				int64(1),
				int64(1),
				rowTypeTask,
				int64(math.MaxInt64),
				int64(math.MaxInt64),
				10,
			}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &matchingTaskStoreV2{Session: session}

	response, err := store.GetTasks(t.Context(), &p.GetTasksRequest{
		NamespaceID:        "namespace-id",
		TaskQueue:          "task-queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinPass:   1,
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: math.MaxInt64,
		PageSize:           10,
		UseLimit:           true,
	})

	require.Error(t, err)
	require.Nil(t, response)
	require.Equal(t, 1, iter.closeCalls)
}

func TestGetTasksV2TypedScanSkipsStaticRow(t *testing.T) {
	pageToken := []byte("next-page")
	iter := &recordingIter{
		scanRows: [][]any{
			{nil, nil, nil},
			{int64(1), []byte("task"), enumspb.ENCODING_TYPE_PROTO3.String()},
		},
		pageState:               pageToken,
		pageStateAfterExhausted: true,
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetTasksQuery_v2_limit, stmt)
			require.Equal(t, []any{
				"namespace-id",
				"task-queue",
				enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				rowTypeTask,
				int64(1),
				int64(1),
				rowTypeTask,
				int64(math.MaxInt64),
				int64(math.MaxInt64),
				10,
			}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &matchingTaskStoreV2{Session: session}

	response, err := store.GetTasks(t.Context(), &p.GetTasksRequest{
		NamespaceID:        "namespace-id",
		TaskQueue:          "task-queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinPass:   1,
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: math.MaxInt64,
		PageSize:           10,
		UseLimit:           true,
	})

	require.NoError(t, err)
	require.Len(t, response.Tasks, 1)
	require.Equal(t, []byte("task"), response.Tasks[0].Data)
	require.Equal(t, enumspb.ENCODING_TYPE_PROTO3, response.Tasks[0].EncodingType)
	require.Equal(t, pageToken, response.NextPageToken)
	require.Equal(t, 1, iter.closeCalls)
}

func BenchmarkGetTasksV1ReadPage(b *testing.B) {
	rows := make([][]any, 100)
	for i := range rows {
		rows[i] = []any{
			int64(i + 1),
			[]byte("task"),
			enumspb.ENCODING_TYPE_PROTO3.String(),
		}
	}
	session := &recordingSession{
		t: b,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{
				iter: &recordingIter{
					scanRows: rows,
				},
			}
		},
	}
	store := &matchingTaskStoreV1{Session: session}
	request := &p.GetTasksRequest{
		NamespaceID:        "namespace-id",
		TaskQueue:          "task-queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: 101,
		PageSize:           100,
	}

	b.ResetTimer()
	for b.Loop() {
		response, err := store.GetTasks(b.Context(), request)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkTaskResponseSink = response
	}
}

func TestReadHistoryBranchReverseUsesBranchPartition(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateReadHistoryNodeReverse, stmt)
			require.Equal(t, []any{treeID, branchID, int64(10), int64(20)}, args)
			return &recordingQuery{
				iter: &recordingIter{},
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())
	branchToken, err := store.NewHistoryBranch("", "", "", treeID, util.Ptr(branchID), nil, 0, 0, 0)
	require.NoError(t, err)

	_, err = store.ReadHistoryBranch(t.Context(), &p.InternalReadHistoryBranchRequest{
		BranchToken:  branchToken,
		BranchID:     branchID,
		MinNodeID:    10,
		MaxNodeID:    20,
		PageSize:     100,
		ReverseOrder: true,
	})

	require.NoError(t, err)
	require.Len(t, session.queries, 1)
}

func TestReadHistoryBranchUsesHistoryNodeV2WhenEnabled(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateReadHistoryNodeV2, stmt)
			require.Equal(t, []any{treeID, branchID, int64(10), int64(20)}, args)
			return &recordingQuery{iter: &recordingIter{}}
		},
	}
	store := NewHistoryStore(
		session,
		serialization.NewSerializer(),
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
	)
	branchToken, err := store.NewHistoryBranch("", "", "", treeID, util.Ptr(branchID), nil, 0, 0, 0)
	require.NoError(t, err)

	_, err = store.ReadHistoryBranch(t.Context(), &p.InternalReadHistoryBranchRequest{
		BranchToken: branchToken,
		BranchID:    branchID,
		MinNodeID:   10,
		MaxNodeID:   20,
		PageSize:    100,
	})

	require.NoError(t, err)
	require.Len(t, session.queries, 1)
}

func TestBackfillHistoryNodeV2CopiesRows(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateScanHistoryNodeForV2Backfill:
				require.Equal(t, []any{int64(math.MinInt64), int64(math.MaxInt64)}, args)
				return &recordingQuery{
					iter: &recordingIter{
						scanRows: [][]any{{
							treeID,
							branchID,
							int64(10),
							int64(9),
							int64(11),
							[]byte("events"),
							enumspb.ENCODING_TYPE_PROTO3.String(),
							int64(123456),
						}},
					},
				}
			case templateBackfillHistoryNodeV2:
				require.Equal(t, []any{
					treeID,
					branchID,
					int64(10),
					int64(9),
					int64(11),
					[]byte("events"),
					enumspb.ENCODING_TYPE_PROTO3.String(),
					int64(123456),
				}, args)
				return &recordingQuery{}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	copied, err := BackfillHistoryNodeV2(t.Context(), session, HistoryNodeV2BackfillOptions{
		PageSize:        25,
		Concurrency:     1,
		TokenRangeCount: 1,
		Partitioner:     HistoryNodeBackfillMurmur3Partitioner,
	})

	require.NoError(t, err)
	require.Equal(t, int64(1), copied)
	require.Equal(t, []string{
		templateScanHistoryNodeForV2Backfill,
		templateBackfillHistoryNodeV2,
	}, recordedStatements(session.queries))
	require.Equal(t, 25, session.queries[0].query.pageSize)
	require.True(t, session.queries[1].query.idempotent)
}

func TestBackfillHistoryNodeV1CopiesRows(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateScanHistoryNodeV2ForV1Backfill:
				require.Equal(t, []any{int64(math.MinInt64), int64(math.MaxInt64)}, args)
				return &recordingQuery{
					iter: &recordingIter{
						scanRows: [][]any{{
							treeID,
							branchID,
							int64(10),
							int64(9),
							int64(11),
							[]byte("events"),
							enumspb.ENCODING_TYPE_PROTO3.String(),
							int64(123456),
						}},
					},
				}
			case templateBackfillHistoryNodeV1:
				require.Equal(t, []any{
					treeID,
					branchID,
					int64(10),
					int64(9),
					int64(11),
					[]byte("events"),
					enumspb.ENCODING_TYPE_PROTO3.String(),
					int64(123456),
				}, args)
				return &recordingQuery{}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	copied, err := BackfillHistoryNodeV1(t.Context(), session, HistoryNodeBackfillOptions{
		PageSize:        25,
		Concurrency:     1,
		TokenRangeCount: 1,
		Partitioner:     HistoryNodeBackfillMurmur3Partitioner,
	})

	require.NoError(t, err)
	require.Equal(t, int64(1), copied)
	require.Equal(t, []string{
		templateScanHistoryNodeV2ForV1Backfill,
		templateBackfillHistoryNodeV1,
	}, recordedStatements(session.queries))
	require.Equal(t, 25, session.queries[0].query.pageSize)
	require.True(t, session.queries[1].query.idempotent)
}

func TestBackfillHistoryNodeV2ValidatesOptions(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			t.Fatal("invalid options must not query Cassandra")
			return nil
		},
	}

	_, err := BackfillHistoryNodeV2(t.Context(), session, HistoryNodeV2BackfillOptions{
		Concurrency: 1,
	})
	require.ErrorContains(t, err, "page size")

	_, err = BackfillHistoryNodeV2(t.Context(), session, HistoryNodeV2BackfillOptions{
		PageSize: 1,
	})
	require.ErrorContains(t, err, "concurrency")

	_, err = BackfillHistoryNodeV2(t.Context(), session, HistoryNodeV2BackfillOptions{
		PageSize:        1,
		Concurrency:     1,
		TokenRangeCount: -1,
	})
	require.ErrorContains(t, err, "token range count")
}

func TestBackfillHistoryNodeV2RejectsNonMurmur3Partitioner(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetHistoryNodeBackfillPartitioner, stmt)
			require.Empty(t, args)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*string) = "org.apache.cassandra.dht.RandomPartitioner"
					return nil
				},
			}
		},
	}

	copied, err := BackfillHistoryNodeV2(t.Context(), session, HistoryNodeV2BackfillOptions{
		PageSize:        1,
		Concurrency:     1,
		TokenRangeCount: 1,
	})

	require.Zero(t, copied)
	require.ErrorContains(t, err, HistoryNodeBackfillMurmur3Partitioner)
	require.ErrorContains(t, err, "RandomPartitioner")
	require.Len(t, session.queries, 1)
}

func TestHistoryNodeBackfillTokenRangesCoverRing(t *testing.T) {
	tokenRanges, err := HistoryNodeBackfillTokenRanges(3)
	require.NoError(t, err)
	require.Len(t, tokenRanges, 3)
	require.Equal(t, int64(math.MinInt64), tokenRanges[0].StartToken)
	require.Equal(t, int64(math.MaxInt64), tokenRanges[2].EndToken)

	for index, tokenRange := range tokenRanges {
		require.Equal(t, index, tokenRange.Index)
		require.LessOrEqual(t, tokenRange.StartToken, tokenRange.EndToken)
		if index > 0 {
			require.Equal(t, tokenRanges[index-1].EndToken+1, tokenRange.StartToken)
		}
	}
}

func TestBackfillHistoryNodeV2UsesCompositeSourceToken(t *testing.T) {
	tokenRange := HistoryNodeBackfillTokenRange{
		Index:      7,
		StartToken: -100,
		EndToken:   100,
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateScanBranchHistoryNodeForV2Backfill, stmt)
			require.Equal(t, []any{tokenRange.StartToken, tokenRange.EndToken}, args)
			return &recordingQuery{iter: &recordingIter{}}
		},
	}

	copied, err := BackfillHistoryNodeV2Range(
		t.Context(),
		session,
		HistoryNodeBackfillOptions{
			PageSize:     16,
			Concurrency:  1,
			SourceLayout: HistoryNodeTableLayoutBranchV2,
			Partitioner:  HistoryNodeBackfillMurmur3Partitioner,
		},
		tokenRange,
	)

	require.NoError(t, err)
	require.Zero(t, copied)
	require.Len(t, session.queries, 1)
}

func TestBackfillHistoryNodeV2PropagatesWriteFailure(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	writeErr := errors.New("write failed")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			switch stmt {
			case templateScanHistoryNodeForV2Backfill:
				return &recordingQuery{
					iter: &recordingIter{
						scanRows: [][]any{{
							treeID,
							branchID,
							int64(10),
							int64(9),
							int64(11),
							[]byte("events"),
							enumspb.ENCODING_TYPE_PROTO3.String(),
							int64(123456),
						}},
					},
				}
			case templateBackfillHistoryNodeV2:
				return &recordingQuery{
					execFn: func() error {
						return writeErr
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	copied, err := BackfillHistoryNodeV2(t.Context(), session, HistoryNodeV2BackfillOptions{
		PageSize:        25,
		Concurrency:     1,
		TokenRangeCount: 1,
		Partitioner:     HistoryNodeBackfillMurmur3Partitioner,
	})

	require.Zero(t, copied)
	require.ErrorIs(t, err, writeErr)
	require.ErrorContains(t, err, "node_id=10")
	require.ErrorContains(t, err, "txn_id=11")
}

func TestBackfillHistoryNodeV2PropagatesScanFailure(t *testing.T) {
	scanErr := errors.New("scan failed")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateScanHistoryNodeForV2Backfill, stmt)
			require.Equal(t, []any{int64(math.MinInt64), int64(math.MaxInt64)}, args)
			return &recordingQuery{
				iter: &recordingIter{
					closeErr: scanErr,
				},
			}
		},
	}

	copied, err := BackfillHistoryNodeV2(t.Context(), session, HistoryNodeV2BackfillOptions{
		PageSize:        25,
		Concurrency:     1,
		TokenRangeCount: 1,
		Partitioner:     HistoryNodeBackfillMurmur3Partitioner,
	})

	require.Zero(t, copied)
	require.ErrorIs(t, err, scanErr)
	require.ErrorContains(t, err, "scan history_node")
}

func TestReadHistoryBranchMetadataOnlyUsesTypedScan(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateReadHistoryNodeMetadata, stmt)
			require.Equal(t, []any{treeID, branchID, int64(1), int64(10)}, args)
			return &recordingQuery{
				iter: &recordingIter{
					scanRows: [][]any{
						{int64(1), int64(2), int64(3)},
					},
				},
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())
	branchToken, err := store.NewHistoryBranch("", "", "", treeID, util.Ptr(branchID), nil, 0, 0, 0)
	require.NoError(t, err)

	response, err := store.ReadHistoryBranch(t.Context(), &p.InternalReadHistoryBranchRequest{
		BranchToken:  branchToken,
		BranchID:     branchID,
		MinNodeID:    1,
		MaxNodeID:    10,
		PageSize:     1,
		MetadataOnly: true,
	})

	require.NoError(t, err)
	require.Len(t, response.Nodes, 1)
	require.Equal(t, int64(1), response.Nodes[0].NodeID)
	require.Equal(t, int64(2), response.Nodes[0].PrevTransactionID)
	require.Equal(t, int64(3), response.Nodes[0].TransactionID)
	require.Empty(t, response.Nodes[0].Events.Data)
	require.Equal(t, enumspb.ENCODING_TYPE_UNSPECIFIED, response.Nodes[0].Events.EncodingType)
}

func TestReadHistoryBranchUsesReturnedRowCountAndPageTokenAfterScan(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	pageToken := []byte("next-page")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateReadHistoryNode, stmt)
			require.Equal(t, []any{treeID, branchID, int64(1), int64(10)}, args)
			return &recordingQuery{
				iter: &recordingIter{
					scanRows: [][]any{
						{
							int64(1),
							int64(0),
							int64(1),
							[]byte("events"),
							enumspb.ENCODING_TYPE_PROTO3.String(),
						},
					},
					pageState:               pageToken,
					pageStateAfterExhausted: true,
				},
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())
	branchToken, err := store.NewHistoryBranch("", "", "", treeID, util.Ptr(branchID), nil, 0, 0, 0)
	require.NoError(t, err)

	response, err := store.ReadHistoryBranch(t.Context(), &p.InternalReadHistoryBranchRequest{
		BranchToken: branchToken,
		BranchID:    branchID,
		MinNodeID:   1,
		MaxNodeID:   10,
		PageSize:    256,
	})

	require.NoError(t, err)
	require.Equal(t, pageToken, response.NextPageToken)
	require.Equal(
		t,
		store.encodeHistoryNodePageTokenMetadata(historyNodeReadLayoutLegacyV1),
		response.NextPageTokenMetadata,
	)
	require.Len(t, response.Nodes, 1)
	require.Equal(t, 1, cap(response.Nodes))
}

func TestReadHistoryBranchClosesIteratorOnScanError(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	iter := &recordingIter{
		closeErr: errors.New("scan failed"),
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateReadHistoryNode, stmt)
			require.Equal(t, []any{treeID, branchID, int64(1), int64(10)}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())
	branchToken, err := store.NewHistoryBranch("", "", "", treeID, util.Ptr(branchID), nil, 0, 0, 0)
	require.NoError(t, err)

	response, err := store.ReadHistoryBranch(t.Context(), &p.InternalReadHistoryBranchRequest{
		BranchToken: branchToken,
		BranchID:    branchID,
		MinNodeID:   1,
		MaxNodeID:   10,
		PageSize:    1,
	})

	require.Error(t, err)
	require.Nil(t, response)
	require.ErrorContains(t, err, "scan failed")
	require.Equal(t, 1, iter.closeCalls)
}

func BenchmarkReadHistoryBranchPage(b *testing.B) {
	benchmarkReadHistoryBranchPage(b, 100, 100)
}

func BenchmarkReadHistoryBranchSparsePage(b *testing.B) {
	benchmarkReadHistoryBranchPage(b, 256, 1)
}

func benchmarkReadHistoryBranchPage(b *testing.B, pageSize int, rowCount int) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	rows := make([][]any, rowCount)
	for i := range rows {
		rows[i] = []any{
			int64(i + 1),
			int64(i),
			int64(i + 1),
			[]byte("events"),
			enumspb.ENCODING_TYPE_PROTO3.String(),
		}
	}
	session := &recordingSession{
		t: b,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{
				iter: &recordingIter{
					scanRows: rows,
				},
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())
	branchToken, err := store.NewHistoryBranch("", "", "", treeID, util.Ptr(branchID), nil, 0, 0, 0)
	require.NoError(b, err)
	request := &p.InternalReadHistoryBranchRequest{
		BranchToken: branchToken,
		BranchID:    branchID,
		MinNodeID:   1,
		MaxNodeID:   101,
		PageSize:    pageSize,
	}

	b.ResetTimer()
	for b.Loop() {
		response, err := store.ReadHistoryBranch(b.Context(), request)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkHistoryBranchResponseSink = response
	}
}

func TestGetAllHistoryTreeBranchesReturnsPageTokenAfterScan(t *testing.T) {
	pageToken := []byte("next-page")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateScanAllTreeBranches, stmt)
			return &recordingQuery{
				iter: &recordingIter{
					scanRows: [][]any{
						{"tree-id", "branch-id", []byte("branch"), enumspb.ENCODING_TYPE_PROTO3.String()},
					},
					pageState:               pageToken,
					pageStateAfterExhausted: true,
				},
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())

	response, err := store.GetAllHistoryTreeBranches(t.Context(), &p.GetAllHistoryTreeBranchesRequest{
		PageSize: 1,
	})

	require.NoError(t, err)
	require.Equal(t, pageToken, response.NextPageToken)
	require.Len(t, response.Branches, 1)
}

func TestGetHistoryTreeContainingBranchUsesPostScanPageToken(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	pageToken := []byte("next-page")
	firstPage := true
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateReadAllBranches, stmt)
			require.Equal(t, []any{treeID}, args)
			return &recordingQuery{
				iterFn: func(*recordingQuery) cgocql.Iter {
					if firstPage {
						firstPage = false
						return &recordingIter{
							scanRows: [][]any{
								{"branch-1", []byte("branch-1-data"), enumspb.ENCODING_TYPE_PROTO3.String()},
							},
							pageState:               pageToken,
							pageStateAfterExhausted: true,
						}
					}
					return &recordingIter{
						scanRows: [][]any{
							{"branch-2", []byte("branch-2-data"), enumspb.ENCODING_TYPE_PROTO3.String()},
						},
					}
				},
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())
	branchToken, err := store.NewHistoryBranch("", "", "", treeID, util.Ptr(branchID), nil, 0, 0, 0)
	require.NoError(t, err)

	response, err := store.GetHistoryTreeContainingBranch(t.Context(), &p.InternalGetHistoryTreeContainingBranchRequest{
		BranchToken: branchToken,
	})

	require.NoError(t, err)
	require.Len(t, response.TreeInfos, 2)
	require.Len(t, session.queries, 1)
	require.Equal(t, pageToken, session.queries[0].query.pageState)
}

func TestQueueEnqueueReadsMaxForEveryConditionalInsert(t *testing.T) {
	maxMessageID := int64(40)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateGetLastMessageIDQuery:
				maxMessageID++
				return &recordingQuery{
					mapScanFn: func(dest map[string]any) error {
						dest["message_id"] = maxMessageID
						return nil
					},
				}
			case templateEnqueueMessageQuery:
				return &recordingQuery{
					mapScanCASFn: func(map[string]any) (bool, error) {
						return true, nil
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store, err := NewQueueStore(p.NamespaceReplicationQueueType, session, log.NewNoopLogger())
	require.NoError(t, err)

	for range 2 {
		err = store.EnqueueMessage(t.Context(), &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		})
		require.NoError(t, err)
	}

	require.Equal(t, []string{
		templateGetLastMessageIDQuery,
		templateEnqueueMessageQuery,
		templateGetLastMessageIDQuery,
		templateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
	require.Equal(t, int64(42), session.queries[1].args[1])
	require.Equal(t, int64(43), session.queries[3].args[1])
}

func TestQueueEnqueueMessageIDConflictReturnsError(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateGetLastMessageIDQuery:
				return &recordingQuery{
					mapScanFn: func(dest map[string]any) error {
						dest["message_id"] = int64(41)
						return nil
					},
				}
			case templateEnqueueMessageQuery:
				require.Equal(t, int64(42), args[1])
				return &recordingQuery{
					mapScanCASFn: func(map[string]any) (bool, error) {
						return false, nil
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store, err := NewQueueStore(p.NamespaceReplicationQueueType, session, log.NewNoopLogger())
	require.NoError(t, err)

	err = store.EnqueueMessage(t.Context(), &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	})

	require.ErrorIs(t, err, ErrEnqueueMessageConflict)
}

func TestQueueV2EnqueueStopsWaitingForLockWhenContextCanceled(t *testing.T) {
	const queueName = "test-queue"
	queryCalls := 0
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			queryCalls++
			return &recordingQuery{}
		},
	}
	store := &queueV2Store{session: session}
	unlock, err := store.lockQueue(t.Context(), p.QueueTypeHistoryNormal, queueName)
	require.NoError(t, err)
	defer unlock()

	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		_, err := store.EnqueueMessage(ctx, &p.InternalEnqueueMessageRequest{
			QueueType: p.QueueTypeHistoryNormal,
			QueueName: queueName,
		})
		result <- err
	}()
	<-started
	cancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("enqueue did not stop waiting for the queue lock")
	}
	require.Zero(t, queryCalls)
}

func TestQueueV2EnqueueCachesKnownQueueAndReadsMaxForEveryInsert(t *testing.T) {
	maxReads := 0
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateCreateQueueQuery, TemplateEnqueueMessageQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetQueueQuery:
			t.Fatal("enqueue should use queue metadata cache after CreateQueue")
		case TemplateGetMaxMessageIDQuery:
			maxReads++
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					if maxReads == 1 {
						return gocql.ErrNotFound
					}
					*dest[0].(*int64) = int64(0)
					return nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err := store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
	})
	require.NoError(t, err)
	for expectedID := range int64(2) {
		response, err := store.EnqueueMessage(t.Context(), &p.InternalEnqueueMessageRequest{
			QueueType: p.QueueTypeHistoryNormal,
			QueueName: "test-queue",
			Blob: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		})
		require.NoError(t, err)
		require.Equal(t, expectedID, response.Metadata.ID)
	}

	require.Equal(t, []string{
		TemplateCreateQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateEnqueueMessageQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2EnqueueMessageIDConflictReturnsError(t *testing.T) {
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateCreateQueueQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetMaxMessageIDQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*int64) = int64(41)
					return nil
				},
			}
		case TemplateEnqueueMessageQuery:
			require.Equal(t, int64(42), args[3])
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return false, nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err := store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
	})
	require.NoError(t, err)
	_, err = store.EnqueueMessage(t.Context(), &p.InternalEnqueueMessageRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
		Blob: &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
	})

	require.ErrorIs(t, err, ErrEnqueueMessageConflict)
}

func TestQueueV2ReadMessagesRefreshesQueueMetadata(t *testing.T) {
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: 2,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{
		t: t,
	}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateCreateQueueQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetQueueQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*[]byte) = queueBytes
					*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
					*dest[2].(*int64) = 1
					return nil
				},
			}
		case TemplateGetMessagesQuery:
			require.Equal(t, int64(2), args[3])
			return &recordingQuery{
				iter: &recordingIter{},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err = store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
	})
	require.NoError(t, err)
	_, err = store.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
		PageSize:  100,
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		TemplateCreateQueueQuery,
		TemplateGetQueueQuery,
		TemplateGetMessagesQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2ReadMessagesContinuationHonorsUpdatedMinimum(t *testing.T) {
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: 10,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*[]byte) = queueBytes
					*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
					*dest[2].(*int64) = 1
					return nil
				},
			}
		case TemplateGetMessagesQuery:
			require.Equal(t, int64(10), args[3])
			return &recordingQuery{iter: &recordingIter{}}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}
	store := NewQueueV2Store(session, log.NewNoopLogger()).(*queueV2Store)
	store.markKnownQueue(p.QueueTypeHistoryNormal, "test-queue")
	nextPageToken := p.GetNextPageTokenForReadMessages([]p.QueueV2Message{{
		MetaData: p.MessageMetadata{ID: 2},
	}})

	_, err = store.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType:     p.QueueTypeHistoryNormal,
		QueueName:     "test-queue",
		PageSize:      100,
		NextPageToken: nextPageToken,
	})

	require.NoError(t, err)
	require.Equal(t, []string{
		TemplateGetQueueQuery,
		TemplateGetMessagesQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2ReadMessagesColdContinuationValidatesQueue(t *testing.T) {
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: 2,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*[]byte) = queueBytes
					*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
					*dest[2].(*int64) = 1
					return nil
				},
			}
		case TemplateGetMessagesQuery:
			require.Equal(t, int64(8), args[3])
			return &recordingQuery{iter: &recordingIter{}}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}
	store := NewQueueV2Store(session, log.NewNoopLogger())
	nextPageToken := p.GetNextPageTokenForReadMessages([]p.QueueV2Message{{
		MetaData: p.MessageMetadata{ID: 7},
	}})

	_, err = store.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType:     p.QueueTypeHistoryNormal,
		QueueName:     "test-queue",
		PageSize:      100,
		NextPageToken: nextPageToken,
	})

	require.NoError(t, err)
	require.Equal(t, []string{
		TemplateGetQueueQuery,
		TemplateGetMessagesQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2ReadMessagesClosesIteratorOnMessageEncodingError(t *testing.T) {
	iter := &recordingIter{
		scanRows: [][]any{
			{int64(7), []byte("message"), "invalid-encoding"},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case TemplateGetQueueQuery:
				queueBytes, err := (&persistencespb.Queue{
					Partitions: map[int32]*persistencespb.QueuePartition{
						0: {
							MinMessageId: p.FirstQueueMessageID,
						},
					},
				}).Marshal()
				require.NoError(t, err)
				return &recordingQuery{
					scanFn: func(dest ...any) error {
						*dest[0].(*[]byte) = queueBytes
						*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
						*dest[2].(*int64) = 0
						return nil
					},
				}
			case TemplateGetMessagesQuery:
				return &recordingQuery{
					iter: iter,
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
			}
			return nil
		},
	}
	store := NewQueueV2Store(session, log.NewNoopLogger())

	resp, err := store.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
		PageSize:  100,
	})

	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, 1, iter.closeCalls)
}

func BenchmarkQueueV2CachedQueueLookup(b *testing.B) {
	store := &queueV2Store{}
	queueType := p.QueueTypeHistoryNormal
	queueName := "test-queue"
	store.markKnownQueue(queueType, queueName)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkBoolSink = store.isKnownQueue(queueType, queueName)
	}
}

func TestQueueV2KnownQueueCacheIsBounded(t *testing.T) {
	store := &queueV2Store{}
	for i := range queueV2KnownQueuesSize {
		store.markKnownQueue(p.QueueTypeHistoryNormal, fmt.Sprintf("queue-%d", i))
	}
	require.Len(t, store.knownQueues, queueV2KnownQueuesSize)

	store.markKnownQueue(p.QueueTypeHistoryNormal, "next-generation")

	require.Len(t, store.knownQueues, 1)
	require.False(t, store.isKnownQueue(p.QueueTypeHistoryNormal, "queue-0"))
	require.True(t, store.isKnownQueue(p.QueueTypeHistoryNormal, "next-generation"))
}

func TestQueueV2LockIndex(t *testing.T) {
	first := queueV2LockIndex(p.QueueTypeHistoryNormal, "test-queue")

	require.Less(t, first, uint32(queueV2LockStripes))
	require.Equal(t, first, queueV2LockIndex(p.QueueTypeHistoryNormal, "test-queue"))
	require.NotEqual(t, first, queueV2LockIndex(p.QueueTypeHistoryDLQ, "test-queue"))
	require.NotEqual(t, first, queueV2LockIndex(p.QueueTypeHistoryNormal, "other-queue"))
}

func TestQueueV2RangeDeleteMakesUpdatedMetadataVisibleToReads(t *testing.T) {
	const queueName = "test-queue"
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: p.FirstQueueMessageID,
			},
		},
	}).Marshal()
	require.NoError(t, err)
	updatedQueueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: 2,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{
		t: t,
	}
	getQueueCalls := 0
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			getQueueCalls++
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName}, args)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					if getQueueCalls == 1 {
						*dest[0].(*[]byte) = queueBytes
						*dest[2].(*int64) = 0
					} else {
						*dest[0].(*[]byte) = updatedQueueBytes
						*dest[2].(*int64) = 1
					}
					*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
					return nil
				},
			}
		case TemplateGetMaxMessageIDQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*int64) = 3
					return nil
				},
			}
		case TemplateRangeDeleteMessagesQuery:
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName, 0, int64(0), int64(1)}, args)
			return &recordingQuery{}
		case TemplateUpdateQueueMetadataQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetMessagesQuery:
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName, 0, int64(2), 100}, args)
			return &recordingQuery{
				iter: &recordingIter{},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	resp, err := store.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{
			ID: 1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), resp.MessagesDeleted)
	_, err = store.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		PageSize:  100,
	})
	require.NoError(t, err)
	require.Equal(t, 2, getQueueCalls)
	require.Equal(t, []string{
		TemplateGetQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateUpdateQueueMetadataQuery,
		TemplateRangeDeleteMessagesQuery,
		TemplateGetQueueQuery,
		TemplateGetMessagesQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2RangeDeleteRefreshesQueueMetadataAfterCreate(t *testing.T) {
	const queueName = "test-queue"
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: p.FirstQueueMessageID,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{
		t: t,
	}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateCreateQueueQuery, TemplateUpdateQueueMetadataQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetQueueQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*[]byte) = queueBytes
					*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
					*dest[2].(*int64) = 0
					return nil
				},
			}
		case TemplateGetMaxMessageIDQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*int64) = 3
					return nil
				},
			}
		case TemplateRangeDeleteMessagesQuery:
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName, 0, int64(0), int64(1)}, args)
			return &recordingQuery{}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err = store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
	})
	require.NoError(t, err)
	resp, err := store.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{
			ID: 1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), resp.MessagesDeleted)
	require.Equal(t, []string{
		TemplateCreateQueueQuery,
		TemplateGetQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateUpdateQueueMetadataQuery,
		TemplateRangeDeleteMessagesQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2RangeDeleteBelowMinRepairsPhysicalRange(t *testing.T) {
	const queueName = "test-queue"
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: 10,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{
		t: t,
	}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName}, args)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*[]byte) = queueBytes
					*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
					*dest[2].(*int64) = 3
					return nil
				},
			}
		case TemplateGetMaxMessageIDQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*int64) = 10
					return nil
				},
			}
		case TemplateRangeDeleteMessagesQuery:
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName, 0, int64(0), int64(9)}, args)
			return &recordingQuery{}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	resp, err := store.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{
			ID: 9,
		},
	})
	require.NoError(t, err)
	require.Zero(t, resp.MessagesDeleted)
	require.Equal(t, []string{
		TemplateGetQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateRangeDeleteMessagesQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2RangeDeleteRetryFinishesCleanupAfterDeleteFailure(t *testing.T) {
	const queueName = "test-queue"
	initialQueueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: 0,
			},
		},
	}).Marshal()
	require.NoError(t, err)
	updatedQueueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: 2,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	getQueueCalls := 0
	deleteCalls := 0
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case TemplateGetQueueQuery:
				getQueueCalls++
				return &recordingQuery{
					scanFn: func(dest ...any) error {
						if getQueueCalls == 1 {
							*dest[0].(*[]byte) = initialQueueBytes
							*dest[2].(*int64) = 0
						} else {
							*dest[0].(*[]byte) = updatedQueueBytes
							*dest[2].(*int64) = 1
						}
						*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
						return nil
					},
				}
			case TemplateGetMaxMessageIDQuery:
				return &recordingQuery{
					scanFn: func(dest ...any) error {
						*dest[0].(*int64) = 3
						return nil
					},
				}
			case TemplateUpdateQueueMetadataQuery:
				return &recordingQuery{
					mapScanCASFn: func(map[string]any) (bool, error) {
						return true, nil
					},
				}
			case TemplateRangeDeleteMessagesQuery:
				require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName, 0, int64(0), int64(1)}, args)
				deleteCalls++
				return &recordingQuery{
					execFn: func() error {
						if deleteCalls == 1 {
							return errors.New("delete failed")
						}
						return nil
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
			}
			return nil
		},
	}
	store := NewQueueV2Store(session, log.NewNoopLogger())
	request := &p.InternalRangeDeleteMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{
			ID: 1,
		},
	}

	resp, err := store.RangeDeleteMessages(t.Context(), request)
	require.ErrorContains(t, err, "delete failed")
	require.Nil(t, resp)

	resp, err = store.RangeDeleteMessages(t.Context(), request)
	require.NoError(t, err)
	require.Zero(t, resp.MessagesDeleted)
	require.Equal(t, []string{
		TemplateGetQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateUpdateQueueMetadataQuery,
		TemplateRangeDeleteMessagesQuery,
		TemplateGetQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateRangeDeleteMessagesQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2UpdateConflictInvalidatesCachedQueue(t *testing.T) {
	const queueName = "test-queue"
	initialQueueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: p.FirstQueueMessageID,
			},
		},
	}).Marshal()
	require.NoError(t, err)
	refreshedQueueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: 2,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{
		t: t,
	}
	getQueueCalls := 0
	updateCalls := 0
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			getQueueCalls++
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					switch getQueueCalls {
					case 1:
						*dest[0].(*[]byte) = initialQueueBytes
						*dest[2].(*int64) = 0
					case 2:
						*dest[0].(*[]byte) = refreshedQueueBytes
						*dest[2].(*int64) = 1
					default:
						t.Fatalf("unexpected get queue call: %d", getQueueCalls)
					}
					*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
					return nil
				},
			}
		case TemplateGetMaxMessageIDQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*int64) = 4
					return nil
				},
			}
		case TemplateRangeDeleteMessagesQuery:
			return &recordingQuery{}
		case TemplateUpdateQueueMetadataQuery:
			updateCalls++
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return updateCalls == 2, nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err = store.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{
			ID: 1,
		},
	})
	require.ErrorIs(t, err, ErrUpdateQueueConflict)
	_, err = store.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{
			ID: 2,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, getQueueCalls)
	require.Equal(t, 2, updateCalls)
	require.Equal(t, []string{
		TemplateGetQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateUpdateQueueMetadataQuery,
		TemplateGetQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateUpdateQueueMetadataQuery,
		TemplateRangeDeleteMessagesQuery,
	}, recordedStatements(session.queries))
}

type recordedQuery struct {
	stmt  string
	args  []any
	query *recordingQuery
}

type historyNodeRequestError struct {
	code    int
	message string
}

func (e historyNodeRequestError) Code() int       { return e.code }
func (e historyNodeRequestError) Message() string { return e.message }
func (e historyNodeRequestError) Error() string   { return e.message }

func testHistoryNodeAppendRequest(events []byte) *p.InternalAppendHistoryNodesRequest {
	return &p.InternalAppendHistoryNodesRequest{
		BranchInfo: &persistencespb.HistoryBranch{
			TreeId:   "11111111-1111-1111-1111-111111111111",
			BranchId: "22222222-2222-2222-2222-222222222222",
		},
		Node: p.InternalHistoryNode{
			NodeID:            10,
			PrevTransactionID: 9,
			TransactionID:     11,
			Events: &commonpb.DataBlob{
				Data:         events,
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
	}
}

type recordingSession struct {
	t                      testing.TB
	queryFn                func(stmt string, args ...any) cgocql.Query
	awaitSchemaAgreementFn func(context.Context) error
	mu                     sync.Mutex
	queries                []recordedQuery
}

func (s *recordingSession) Query(stmt string, args ...any) cgocql.Query {
	q := s.queryFn(stmt, args...)
	rq, ok := q.(*recordingQuery)
	require.True(s.t, ok)
	s.mu.Lock()
	s.queries = append(s.queries, recordedQuery{stmt: stmt, args: args, query: rq})
	s.mu.Unlock()
	return q
}

func (s *recordingSession) NewBatch(cgocql.BatchType) *cgocql.Batch {
	s.t.Fatal("unexpected NewBatch")
	return nil
}

func (s *recordingSession) ExecuteBatch(*cgocql.Batch) error {
	s.t.Fatal("unexpected ExecuteBatch")
	return nil
}

func (s *recordingSession) MapExecuteBatchCAS(*cgocql.Batch, map[string]any) (bool, cgocql.Iter, error) {
	s.t.Fatal("unexpected MapExecuteBatchCAS")
	return false, nil, nil
}

func (s *recordingSession) AwaitSchemaAgreement(ctx context.Context) error {
	if s.awaitSchemaAgreementFn != nil {
		return s.awaitSchemaAgreementFn(ctx)
	}
	s.t.Fatal("unexpected AwaitSchemaAgreement")
	return nil
}

func (s *recordingSession) Close() {}

type recordingQuery struct {
	iter         cgocql.Iter
	iterFn       func(*recordingQuery) cgocql.Iter
	pageSize     int
	pageState    []byte
	idempotent   bool
	timestamp    int64
	execFn       func() error
	scanFn       func(dest ...any) error
	mapScanFn    func(map[string]any) error
	mapScanCASFn func(map[string]any) (bool, error)
}

func (q *recordingQuery) Exec() error {
	if q.execFn != nil {
		return q.execFn()
	}
	return nil
}

func (q *recordingQuery) Scan(dest ...any) error {
	return q.scanFn(dest...)
}

func (q *recordingQuery) ScanCAS(...any) (bool, error) {
	return false, nil
}

func (q *recordingQuery) MapScan(dest map[string]any) error {
	if q.mapScanFn != nil {
		return q.mapScanFn(dest)
	}
	return nil
}

func (q *recordingQuery) MapScanCAS(dest map[string]any) (bool, error) {
	if q.mapScanCASFn != nil {
		return q.mapScanCASFn(dest)
	}
	return false, nil
}

func (q *recordingQuery) Iter() cgocql.Iter {
	if q.iterFn != nil {
		return q.iterFn(q)
	}
	return q.iter
}

func (q *recordingQuery) PageSize(pageSize int) cgocql.Query {
	q.pageSize = pageSize
	return q
}

func (q *recordingQuery) PageState(pageState []byte) cgocql.Query {
	q.pageState = pageState
	return q
}

func (q *recordingQuery) WithContext(context.Context) cgocql.Query {
	return q
}

func (q *recordingQuery) WithTimestamp(timestamp int64) cgocql.Query {
	q.timestamp = timestamp
	return q
}

func (q *recordingQuery) Consistency(cgocql.Consistency) cgocql.Query {
	return q
}

func (q *recordingQuery) Bind(...any) cgocql.Query {
	return q
}

func (q *recordingQuery) Idempotent(idempotent bool) cgocql.Query {
	q.idempotent = idempotent
	return q
}

func (q *recordingQuery) SetSpeculativeExecutionPolicy(cgocql.SpeculativeExecutionPolicy) cgocql.Query {
	return q
}

func recordedStatements(queries []recordedQuery) []string {
	statements := make([]string, len(queries))
	for i, query := range queries {
		statements[i] = query.stmt
	}
	return statements
}

func recordGetClusterMembersQuery(t *testing.T, request *p.GetClusterMembersRequest) string {
	t.Helper()

	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{
				iter: &recordingIter{},
			}
		},
	}
	store := &ClusterMetadataStore{session: session, logger: log.NewNoopLogger()}

	_, err := store.GetClusterMembers(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, session.queries, 1)
	return session.queries[0].stmt
}

type recordingIter struct {
	scanRows                [][]any
	mapRows                 []map[string]any
	pageState               []byte
	pageStateAfterExhausted bool
	scanIdx                 int
	mapIdx                  int
	closeErr                error
	closeCalls              int
}

func (i *recordingIter) Scan(dest ...any) bool {
	if i.scanIdx >= len(i.scanRows) {
		return false
	}
	row := i.scanRows[i.scanIdx]
	i.scanIdx++
	for idx := range dest {
		if dest[idx] == nil {
			continue
		}
		switch d := dest[idx].(type) {
		case *string:
			if row[idx] == nil {
				*d = ""
			} else {
				*d = row[idx].(string)
			}
		case *[]byte:
			if row[idx] == nil {
				*d = nil
			} else {
				*d = row[idx].([]byte)
			}
		case *int64:
			*d = row[idx].(int64)
		case *int:
			*d = row[idx].(int)
		case *nullableInt64:
			if row[idx] == nil {
				*d = nullableInt64{}
			} else {
				*d = nullableInt64{
					value: row[idx].(int64),
					valid: true,
				}
			}
		default:
			panic("unsupported scan destination")
		}
	}
	return true
}

func (i *recordingIter) MapScan(dest map[string]any) bool {
	if i.mapIdx >= len(i.mapRows) {
		return false
	}
	for key, value := range i.mapRows[i.mapIdx] {
		dest[key] = value
	}
	i.mapIdx++
	return true
}

func (i *recordingIter) NumRows() int {
	return len(i.scanRows)
}

func (i *recordingIter) PageState() []byte {
	if i.pageStateAfterExhausted && (i.scanIdx < len(i.scanRows) || i.mapIdx < len(i.mapRows)) {
		return nil
	}
	return i.pageState
}

func (i *recordingIter) Close() error {
	i.closeCalls++
	return i.closeErr
}
