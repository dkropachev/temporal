package cassandra

import (
	"math"
	"os"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func TestValidateLegacyQueueMigrationMode(t *testing.T) {
	require.NoError(t, ValidateLegacyQueueMigrationMode(""))
	require.NoError(t, ValidateLegacyQueueMigrationMode(config.CassandraLegacyQueueMigrationModeSourceDual))
	require.NoError(t, ValidateLegacyQueueMigrationMode(config.CassandraLegacyQueueMigrationModeTargetDual))
	require.NoError(t, ValidateLegacyQueueMigrationMode(config.CassandraLegacyQueueMigrationModeTargetOnly))
	require.Error(t, ValidateLegacyQueueMigrationMode("invalid"))
}

func TestLegacyQueueV2ReverseReconcileRequiresTrafficFenceConfirmation(t *testing.T) {
	session := &recordingSession{t: t}
	_, err := ReconcileSourceLegacyQueueFromV2(
		t.Context(),
		session,
		p.NamespaceReplicationQueueType,
		LegacyQueueV2MigrationOptions{
			PageSize:          16,
			MessageBucketSize: 4096,
		},
	)
	require.ErrorContains(t, err, "externally fenced, quiescent per-queue traffic")
	require.Empty(t, session.queries)
}

func TestLegacyQueueV2MessageBucketBoundaries(t *testing.T) {
	const bucketSize = int64(4)
	for messageID, expected := range map[int64]int64{
		0: 0,
		3: 0,
		4: 1,
		7: 1,
		8: 2,
	} {
		bucket, err := legacyQueueV2MessageBucket(messageID, bucketSize)
		require.NoError(t, err)
		require.Equal(t, expected, bucket)
	}
	_, err := legacyQueueV2MessageBucket(-1, bucketSize)
	require.Error(t, err)
	_, err = legacyQueueV2MessageBucket(0, 0)
	require.Error(t, err)
}

func TestLegacyQueueV2PageTokenRoundTrip(t *testing.T) {
	token := encodeLegacyQueueV2PageToken(42)
	messageID, err := decodeLegacyQueueV2PageToken(token)
	require.NoError(t, err)
	require.Equal(t, int64(42), messageID)

	_, err = decodeLegacyQueueV2PageToken([]byte("old-cassandra-page-state"))
	require.Error(t, err)
	token[len(legacyQueueV2PageTokenPrefix)]++
	_, err = decodeLegacyQueueV2PageToken(token)
	require.Error(t, err)
}

func TestLegacyQueueV2SchemaUsesBoundedMessagePartitions(t *testing.T) {
	schema, err := os.ReadFile("../../../schema/cassandra/temporal/versioned/v1.16/legacy_queue_v2.cql")
	require.NoError(t, err)
	schemaText := string(schema)
	require.Contains(t, schemaText, "PRIMARY KEY ((queue_type, bucket_id), row_type, message_id)")
	require.Contains(t, schemaText, "cleanup_message_id")
	require.Contains(t, schemaText, "last_message_id")
	require.Contains(t, schemaText, "legacy_queue_v2_delete_ranges")
	require.Contains(t, schemaText, "ALTER TABLE queue ADD migration_authority int")
	require.Contains(t, schemaText, "ALTER TABLE queue ADD migration_generation uuid")
	require.Contains(t, schemaText, "ALTER TABLE queue ADD message_bucket_size bigint")
	require.Contains(t, schemaText, "migration_authority int")
	require.Contains(t, schemaText, "migration_generation uuid")
	require.Contains(t, schemaText, "message_bucket_size bigint")
	require.Contains(t, schemaText, "migration_authority int static")
	require.Contains(t, schemaText, "migration_generation uuid static")
	require.NotContains(t, templateInsertLegacyQueueV2Message, "IF ")
	require.Contains(t, templateAdvanceLegacyQueueV2BucketTail, "IF last_message_id")
}

func TestLegacyQueueSourceQueriesExcludeAuthoritySentinel(t *testing.T) {
	require.Equal(t, legacyQueueSourceAuthorityMessageID, int64(math.MinInt64))
	require.Contains(t, templateGetLastMessageIDQuery, "message_id >= ?")
	require.Contains(t, templateGetMessagesQuery, "message_id >= ?")
	require.Contains(t, templateGetMessagesFromDLQQuery, "message_id >= ?")
	require.Contains(t, templateDeleteMessagesBeforeQuery, "message_id >= ?")
	require.Contains(t, templateScanSourceLegacyQueueForV2, "message_id >= ?")
	require.Contains(t, templateDeleteMessagesQuery, "message_id > ?")
}

func TestLegacyQueueV2TargetOnlyRejectsMissingPhysicalQueueWithoutSourceRead(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			require.Equal(t, templateGetLegacyQueueV2State, stmt)
			return &recordingQuery{scanFn: func(...any) error { return gocql.ErrNotFound }}
		},
	}
	store := &QueueStore{
		queueType:         p.NamespaceReplicationQueueType,
		session:           session,
		migrationMode:     config.CassandraLegacyQueueMigrationModeTargetOnly,
		messageBucketSize: 4,
		activeBuckets:     make(map[p.QueueType]int64),
	}

	err := store.initializeLegacyQueueV2StateForMode(t.Context())
	require.ErrorContains(t, err, "target state is missing")
	require.Len(t, session.queries, 1)
	requireLegacyQueueV2HasNoSourceQueries(t, session.queries)
}

func TestLegacyQueueV2TargetOnlyRejectsSourceAuthority(t *testing.T) {
	state := legacyQueueV2State{
		activeBucket:      0,
		minimumMessageID:  0,
		cleanupMessageID:  0,
		authority:         legacyQueueMigrationAuthoritySource,
		messageBucketSize: 4,
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			require.Equal(t, templateGetLegacyQueueV2State, stmt)
			return legacyQueueV2StateRecordingQuery(&state)
		},
	}
	store := &QueueStore{
		queueType:         p.NamespaceReplicationQueueType,
		session:           session,
		migrationMode:     config.CassandraLegacyQueueMigrationModeTargetOnly,
		messageBucketSize: 4,
		activeBuckets:     make(map[p.QueueType]int64),
	}

	err := store.initializeLegacyQueueV2StateForMode(t.Context())
	require.ErrorContains(t, err, "requires target authority")
	require.Len(t, session.queries, 1)
	requireLegacyQueueV2HasNoSourceQueries(t, session.queries)
}

func TestValidateLegacyQueueMigrationModeSchemaSourceOnlySkipsTargetSchema(t *testing.T) {
	queryCount := 0
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			queryCount++
			return &recordingQuery{}
		},
	}

	err := ValidateLegacyQueueMigrationModeSchema(
		t.Context(),
		session,
		"temporal",
		config.CassandraLegacyQueueMigrationModeSourceOnly,
	)
	require.NoError(t, err)
	require.Zero(t, queryCount)
}

func TestValidateLegacyQueueMigrationModeSchemaRequiresTargetTables(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{iter: &recordingIter{}}
		},
	}

	err := ValidateLegacyQueueMigrationModeSchema(
		t.Context(),
		session,
		"temporal",
		config.CassandraLegacyQueueMigrationModeTargetOnly,
	)
	require.Error(t, err)
}

func TestLegacyQueueV2TargetOnlyReadMergesBucketsAndFiltersDeleteRanges(t *testing.T) {
	state := legacyQueueV2State{
		activeBucket:     1,
		minimumMessageID: 0,
		cleanupMessageID: 0,
	}
	bucketState := legacyQueueV2BucketState{lastMessageID: 5}
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case templateGetLegacyQueueV2State:
			return legacyQueueV2StateRecordingQuery(&state)
		case templateGetLegacyQueueV2BucketState:
			return legacyQueueV2BucketStateRecordingQuery(&bucketState)
		case templateGetLegacyQueueV2DeleteRanges:
			return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
				{"delete-1", int64(1), int64(2)},
			}}}
		case templateGetLegacyQueueV2Messages:
			firstMessageID := args[3].(int64)
			lastMessageID := args[4].(int64)
			var rows []map[string]any
			for messageID := firstMessageID; messageID <= lastMessageID; messageID++ {
				rows = append(rows, legacyQueueV2MessageRow(messageID))
			}
			return &recordingQuery{iter: &recordingIter{mapRows: rows}}
		default:
			t.Fatalf("unexpected query: %s", stmt)
			return nil
		}
	}
	store, err := NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetOnly,
		4,
	)
	require.NoError(t, err)

	messages, nextToken, err := store.ReadMessagesFromDLQ(t.Context(), -1, 5, 4, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 3, 4}, legacyQueueV2MessageIDs(messages))
	require.NotEmpty(t, nextToken)

	messages, nextToken, err = store.ReadMessagesFromDLQ(t.Context(), -1, 5, 4, nextToken)
	require.NoError(t, err)
	require.Equal(t, []int64{5}, legacyQueueV2MessageIDs(messages))
	require.Empty(t, nextToken)
	requireLegacyQueueV2HasNoSourceQueries(t, session.queries)
}

func TestLegacyQueueV2TargetOnlyPrefixDeleteAdvancesLogicalCursorBeforeCleanup(t *testing.T) {
	state := legacyQueueV2State{
		activeBucket:     2,
		minimumMessageID: 0,
		cleanupMessageID: 0,
	}
	bucketState := legacyQueueV2BucketState{lastMessageID: 9}
	var statements []string
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		statements = append(statements, stmt)
		switch stmt {
		case templateGetLegacyQueueV2State:
			return legacyQueueV2StateRecordingQuery(&state)
		case templateGetLegacyQueueV2BucketState:
			return legacyQueueV2BucketStateRecordingQuery(&bucketState)
		case templateAdvanceLegacyQueueV2Minimum:
			return &recordingQuery{mapScanCASFn: func(map[string]any) (bool, error) {
				state.minimumMessageID = args[0].(int64)
				state.version = args[1].(int64)
				return true, nil
			}}
		case templateDeleteLegacyQueueV2MessageRange:
			return &recordingQuery{}
		case templateAdvanceLegacyQueueV2Cleanup:
			return &recordingQuery{mapScanCASFn: func(map[string]any) (bool, error) {
				state.cleanupMessageID = args[0].(int64)
				return true, nil
			}}
		default:
			t.Fatalf("unexpected query: %s", stmt)
			return nil
		}
	}
	store, err := NewQueueStoreWithMigrationMode(
		p.NamespaceReplicationQueueType,
		session,
		log.NewNoopLogger(),
		config.CassandraLegacyQueueMigrationModeTargetOnly,
		4,
	)
	require.NoError(t, err)

	err = store.DeleteMessagesBefore(t.Context(), 6)
	require.NoError(t, err)
	require.Equal(t, int64(6), state.minimumMessageID)
	require.Equal(t, int64(6), state.cleanupMessageID)
	minimumIndex := statementIndex(statements, templateAdvanceLegacyQueueV2Minimum)
	cleanupIndex := statementIndex(statements, templateDeleteLegacyQueueV2MessageRange)
	require.Greater(t, cleanupIndex, minimumIndex)
	requireLegacyQueueV2HasNoSourceQueries(t, session.queries)
}

func TestLegacyQueueV2DLQRangeDeletePersistsIntentBeforePhysicalCleanup(t *testing.T) {
	var statements []string
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			statements = append(statements, stmt)
			switch stmt {
			case templateInsertLegacyQueueV2DeleteRange,
				templateDeleteLegacyQueueV2MessageRange,
				templateDeleteLegacyQueueV2DeleteRange:
				return &recordingQuery{}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store := &QueueStore{
		session:           session,
		migrationMode:     config.CassandraLegacyQueueMigrationModeTargetOnly,
		messageBucketSize: 4,
	}

	err := store.rangeDeleteMessagesLegacyQueueV2(t.Context(), -p.NamespaceReplicationQueueType, 1, 5)
	require.NoError(t, err)
	require.Equal(t, templateInsertLegacyQueueV2DeleteRange, statements[0])
	require.Equal(t, templateDeleteLegacyQueueV2DeleteRange, statements[len(statements)-1])
	require.Equal(t, 2, statementCount(statements, templateDeleteLegacyQueueV2MessageRange))
	requireLegacyQueueV2HasNoSourceQueries(t, session.queries)
}

func legacyQueueV2StateRecordingQuery(state *legacyQueueV2State) *recordingQuery {
	return &recordingQuery{scanFn: func(dest ...any) error {
		*dest[0].(*int64) = state.activeBucket
		*dest[1].(*int64) = state.minimumMessageID
		*dest[2].(*int64) = state.cleanupMessageID
		*dest[3].(*int64) = state.version
		if len(dest) > 4 {
			authority := int(state.authority)
			generation := state.generation
			if generation == (gocql.UUID{}) {
				generation = gocql.UUID{1}
			}
			messageBucketSize := state.messageBucketSize
			*dest[4].(**int) = &authority
			*dest[5].(**gocql.UUID) = &generation
			*dest[6].(**int64) = &messageBucketSize
		}
		return nil
	}}
}

func legacyQueueV2BucketStateRecordingQuery(state *legacyQueueV2BucketState) *recordingQuery {
	return &recordingQuery{scanFn: func(dest ...any) error {
		*dest[0].(*int64) = state.lastMessageID
		*dest[1].(*int64) = state.version
		return nil
	}}
}

func legacyQueueV2MessageRow(messageID int64) map[string]any {
	return map[string]any{
		"message_id":       messageID,
		"message_payload":  []byte{byte(messageID)},
		"message_encoding": enumspb.ENCODING_TYPE_PROTO3.String(),
	}
}

func legacyQueueV2MessageIDs(messages []*p.QueueMessage) []int64 {
	result := make([]int64, len(messages))
	for index, message := range messages {
		result[index] = message.ID
	}
	return result
}

func requireLegacyQueueV2HasNoSourceQueries(t *testing.T, queries []recordedQuery) {
	t.Helper()
	for _, query := range queries {
		require.NotContains(t, query.stmt, "FROM queue ", query.stmt)
		require.NotContains(t, query.stmt, "INTO queue ", query.stmt)
		require.NotContains(t, query.stmt, "UPDATE queue ", query.stmt)
		require.NotContains(t, query.stmt, "DELETE FROM queue ", query.stmt)
	}
}

func statementIndex(statements []string, statement string) int {
	for index, candidate := range statements {
		if candidate == statement {
			return index
		}
	}
	return -1
}

func statementCount(statements []string, statement string) int {
	count := 0
	for _, candidate := range statements {
		if candidate == statement {
			count++
		}
	}
	return count
}
