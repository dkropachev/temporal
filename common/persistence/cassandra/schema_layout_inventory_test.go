package cassandra

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func TestAuditSchemaLayoutTargetOnlyInventoryAllowsEmptySource(t *testing.T) {
	tests := []struct {
		name      SchemaLayoutName
		parameter int64
		queries   int
	}{
		{name: SchemaLayoutMatchingTasks, parameter: 16, queries: 2},
		{name: SchemaLayoutMatchingTasksFair, parameter: 16, queries: 2},
		{name: SchemaLayoutTaskQueueUserData, parameter: 16, queries: 3},
		{name: SchemaLayoutLegacyQueue, parameter: 4096, queries: 4},
	}
	for _, test := range tests {
		t.Run(string(test.name), func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(string, ...any) commongocql.Query {
					return &recordingQuery{iter: &recordingIter{}}
				},
			}
			result, err := AuditSchemaLayoutTargetOnlyInventory(
				t.Context(),
				session,
				testTargetOnlyInventorySpec(t, test.name, test.parameter),
				SchemaLayoutInventoryAuditOptions{PageSize: 7, ExpectedExecutionShards: 1},
			)
			require.NoError(t, err)
			require.Zero(t, result.SourceEntities)
			require.Zero(t, result.TargetEntities)
			require.Len(t, session.queries, test.queries)
			for _, query := range session.queries {
				require.Equal(t, 7, query.query.pageSize)
			}
		})
	}
}

func TestAuditExecutionTargetOnlyInventoryRequiresCompleteFixedShardSet(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) commongocql.Query {
			return &recordingQuery{iter: &recordingIter{}}
		},
	}

	result, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		testTargetOnlyInventorySpec(t, SchemaLayoutExecutions, 16),
		SchemaLayoutInventoryAuditOptions{PageSize: 7, ExpectedExecutionShards: 2},
	)
	require.ErrorContains(t, err, "source inventory is incomplete")
	require.Zero(t, result.SourceEntities)
	require.Zero(t, result.TargetEntities)
}

func TestAuditHistoryLayoutsTargetOnlyInventoryAllowsEmptyLayouts(t *testing.T) {
	for _, name := range []SchemaLayoutName{SchemaLayoutHistoryNode, SchemaLayoutHistoryTree} {
		t.Run(string(name), func(t *testing.T) {
			parameter := int64(16)
			if name == SchemaLayoutHistoryNode {
				parameter = 0
			}
			session := &recordingSession{
				t: t,
				queryFn: func(string, ...any) commongocql.Query {
					return &recordingQuery{iter: &recordingIter{}}
				},
			}
			result, err := AuditSchemaLayoutTargetOnlyInventory(
				t.Context(),
				session,
				testTargetOnlyInventorySpec(t, name, parameter),
				SchemaLayoutInventoryAuditOptions{PageSize: 7},
			)
			require.NoError(t, err)
			require.Zero(t, result.SourceEntities)
			require.Zero(t, result.TargetEntities)
			for _, query := range session.queries {
				require.Equal(t, 7, query.query.pageSize)
			}
		})
	}
}

func TestAuditHistoryTreeTargetOnlyInventoryRejectsSourceAuthority(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) commongocql.Query {
			switch stmt {
			case templateScanHistoryTreeInventory:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{
					"11111111-1111-1111-1111-111111111111",
				}}}}
			case templateGetHistoryTreeAuthority:
				return &recordingQuery{scanFn: func(dest ...any) error {
					authority := int(historyTreeMigrationAuthoritySource)
					timestamp := int64(10)
					*dest[0].(**int) = &authority
					*dest[1].(**int64) = &timestamp
					return nil
				}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	result, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		testTargetOnlyInventorySpec(t, SchemaLayoutHistoryTree, 16),
		SchemaLayoutInventoryAuditOptions{PageSize: 3},
	)
	require.ErrorContains(t, err, "not target-authoritative")
	require.Equal(t, int64(1), result.SourceEntities)
	require.Zero(t, result.TargetEntities)
}

func TestAuditHistoryNodeTargetOnlyInventoryRejectsMissingTargetRow(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) commongocql.Query {
			switch stmt {
			case templateValidateScanHistoryNodeV1:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{
					"11111111-1111-1111-1111-111111111111",
					"22222222-2222-2222-2222-222222222222",
					int64(3),
					int64(2),
					int64(4),
					[]byte("events"),
					"Proto3",
					int64(5),
				}}}}
			case templateValidateHistoryNodeV2Row:
				return &recordingQuery{scanFn: func(...any) error { return gocql.ErrNotFound }}
			case templateValidateScanHistoryNodeV2:
				return &recordingQuery{iter: &recordingIter{}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	result, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		testTargetOnlyInventorySpec(t, SchemaLayoutHistoryNode, 0),
		SchemaLayoutInventoryAuditOptions{PageSize: 3},
	)
	require.ErrorContains(t, err, "missing-target=1")
	require.Equal(t, int64(1), result.SourceEntities)
	require.Zero(t, result.TargetEntities)
}

func TestAuditSchemaLayoutTargetOnlyInventoryRejectsUnsupportedAndInvalidOptions(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) commongocql.Query {
			t.Fatal("unexpected query")
			return nil
		},
	}

	unsupported := testTargetOnlyInventorySpec(t, SchemaLayoutHistoryNode, 16)
	unsupported.Name = SchemaLayoutName("unknown")
	_, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		unsupported,
		SchemaLayoutInventoryAuditOptions{PageSize: 1},
	)
	require.ErrorContains(t, err, "unsupported Cassandra schema layout name")

	_, err = AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		testTargetOnlyInventorySpec(t, SchemaLayoutExecutions, 16),
		SchemaLayoutInventoryAuditOptions{},
	)
	require.ErrorContains(t, err, "page size must be positive")
}

func TestAuditExecutionTargetOnlyInventoryRejectsSourceAuthority(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) commongocql.Query {
			if stmt == templateScanExecutionShardInventory {
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{1}}}}
			}
			require.Contains(t, stmt, "FROM executions WHERE")
			return &recordingQuery{scanFn: func(dest ...any) error {
				setExecutionInventoryAuthority(dest, 3, int(executionShardAuthoritySource), 16)
				return nil
			}}
		},
	}

	result, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		testTargetOnlyInventorySpec(t, SchemaLayoutExecutions, 16),
		SchemaLayoutInventoryAuditOptions{PageSize: 3, ExpectedExecutionShards: 1},
	)
	require.ErrorContains(t, err, "not target-authoritative")
	require.Equal(t, int64(1), result.SourceEntities)
	require.Zero(t, result.TargetEntities)
}

func TestAuditMatchingTargetOnlyInventoryRejectsSourceAuthority(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) commongocql.Query {
			if strings.HasPrefix(stmt, "SELECT DISTINCT namespace_id") {
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{
					"11111111-1111-1111-1111-111111111111", "queue", 1,
				}}}}
			}
			return &recordingQuery{scanFn: func(dest ...any) error {
				setMatchingInventoryAuthority(dest, 4, matchingTaskAuthoritySource, 16)
				return nil
			}}
		},
	}

	result, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		testTargetOnlyInventorySpec(t, SchemaLayoutMatchingTasks, 16),
		SchemaLayoutInventoryAuditOptions{PageSize: 3},
	)
	require.ErrorContains(t, err, "not target-authoritative")
	require.Equal(t, int64(1), result.SourceEntities)
	require.Zero(t, result.TargetEntities)
}

func TestAuditTaskQueueUserDataTargetOnlyInventoryRejectsGenerationMismatch(t *testing.T) {
	spec := testTargetOnlyInventorySpec(t, SchemaLayoutTaskQueueUserData, 16)
	otherGeneration := mustParseSchemaLayoutGeneration(t, "22222222-2222-2222-2222-222222222222")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) commongocql.Query {
			if stmt == templateScanUserDataInventory {
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{
					"11111111-1111-1111-1111-111111111111",
				}}}}
			}
			return &recordingQuery{scanFn: func(dest ...any) error {
				authority := int(taskQueueUserDataAuthorityTarget)
				bucketCount := int16(16)
				*dest[0].(**int) = &authority
				*dest[1].(**int16) = &bucketCount
				*dest[2].(**gocql.UUID) = &otherGeneration
				return nil
			}}
		},
	}

	result, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		spec,
		SchemaLayoutInventoryAuditOptions{PageSize: 3},
	)
	require.ErrorContains(t, err, "generation")
	require.Equal(t, int64(1), result.SourceEntities)
	require.Zero(t, result.TargetEntities)
}

func TestAuditLegacyQueueTargetOnlyInventoryRejectsSourceAuthority(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) commongocql.Query {
			if stmt == templateScanLegacyQueueInventory {
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{-1}}}}
			}
			return &recordingQuery{scanFn: func(dest ...any) error {
				authority := int(legacyQueueMigrationAuthoritySource)
				generation := mustParseSchemaLayoutGeneration(t, "11111111-1111-4111-8111-111111111111")
				bucketSize := int64(4096)
				*dest[0].(**int) = &authority
				*dest[1].(**gocql.UUID) = &generation
				*dest[2].(**int64) = &bucketSize
				return nil
			}}
		},
	}

	result, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		testTargetOnlyInventorySpec(t, SchemaLayoutLegacyQueue, 4096),
		SchemaLayoutInventoryAuditOptions{PageSize: 3},
	)
	require.ErrorContains(t, err, "requires target authority")
	require.Equal(t, int64(1), result.SourceEntities)
	require.Zero(t, result.TargetEntities)
}

func TestAuditLegacyQueueTargetOnlyInventoryRejectsInactiveBucketGeneration(t *testing.T) {
	generation := mustParseSchemaLayoutGeneration(t, "11111111-1111-4111-8111-111111111111")
	otherGeneration := mustParseSchemaLayoutGeneration(t, "22222222-2222-4222-8222-222222222222")
	state := legacyQueueV2State{
		activeBucket:      1,
		minimumMessageID:  0,
		cleanupMessageID:  0,
		authority:         legacyQueueMigrationAuthorityTarget,
		generation:        generation,
		messageBucketSize: 4096,
	}
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) commongocql.Query {
		switch stmt {
		case templateScanLegacyQueueInventory,
			templateScanLegacyQueueTargetStateInventory,
			templateScanLegacyQueueTargetDeleteInventory:
			return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{-1}}}}
		case templateGetLegacyQueueSourceAuthority:
			return legacyQueueAuthorityRecordingQuery(legacyQueueAuthorityRecord{
				authority:         legacyQueueMigrationAuthorityTarget,
				generation:        generation,
				messageBucketSize: 4096,
			})
		case templateGetLegacyQueueV2State:
			return legacyQueueV2StateRecordingQuery(&state)
		case templateGetLegacyQueueTargetBucketAuthority:
			bucketGeneration := generation
			if args[1].(int64) == 0 {
				bucketGeneration = otherGeneration
			}
			return legacyQueueAuthorityRecordingQuery(legacyQueueAuthorityRecord{
				authority:         legacyQueueMigrationAuthorityTarget,
				generation:        bucketGeneration,
				messageBucketSize: 4096,
			})
		case templateGetLegacyQueueV2BucketState:
			bucket := args[1].(int64)
			return legacyQueueV2BucketStateRecordingQuery(&legacyQueueV2BucketState{lastMessageID: bucket*4096 - 1})
		case templateGetLegacyQueueTargetDeleteAuthority:
			return legacyQueueAuthorityRecordingQuery(state.authorityRecord())
		case templateScanLegacyQueueTargetBucketInventory:
			return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{-1, int64(0)}, {-1, int64(1)}}}}
		default:
			t.Fatalf("unexpected query: %s", stmt)
			return nil
		}
	}

	result, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		testTargetOnlyInventorySpec(t, SchemaLayoutLegacyQueue, 4096),
		SchemaLayoutInventoryAuditOptions{PageSize: 3},
	)
	require.ErrorContains(t, err, "target bucket 0")
	require.ErrorContains(t, err, "generation")
	require.Equal(t, int64(1), result.SourceEntities)
	require.Equal(t, int64(1), result.TargetEntities)
}

func TestAuditExecutionTargetOnlyInventoryChecksReversePartitions(t *testing.T) {
	tests := []struct {
		name       string
		targetRows [][]any
		wantError  string
	}{
		{name: "current", targetRows: [][]any{{1}}},
		{name: "orphan", targetRows: [][]any{{1}, {2}}, wantError: "orphan partition 2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			targetLayout, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, 1)
			require.NoError(t, err)
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, _ ...any) commongocql.Query {
					switch stmt {
					case templateScanExecutionShardInventory:
						return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{1}}}}
					case templateScanExecutionTargetShardInventory:
						return &recordingQuery{iter: &recordingIter{scanRows: test.targetRows}}
					case templateGetExecutionShardAuthority, targetLayout.query(templateGetExecutionShardAuthority):
						return &recordingQuery{scanFn: func(dest ...any) error {
							setExecutionInventoryAuthority(dest, 3, int(executionShardAuthorityTarget), 1)
							return nil
						}}
					default:
						t.Fatalf("unexpected query: %s", stmt)
						return nil
					}
				},
			}

			result, err := AuditSchemaLayoutTargetOnlyInventory(
				t.Context(),
				session,
				testTargetOnlyInventorySpec(t, SchemaLayoutExecutions, 1),
				SchemaLayoutInventoryAuditOptions{PageSize: 3, ExpectedExecutionShards: 1},
			)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, SchemaLayoutInventoryAuditResult{SourceEntities: 1, TargetEntities: 1}, result)
		})
	}
}

func TestAuditMatchingTargetOnlyInventoryChecksReversePartitions(t *testing.T) {
	for _, fair := range []bool{false, true} {
		name := "classic"
		sourceTable := "tasks"
		targetTable := matchingTaskV3TableName
		layoutName := SchemaLayoutMatchingTasks
		version := matchingTaskVersion1
		if fair {
			name = "fair"
			sourceTable = "tasks_v2"
			targetTable = matchingTaskV3FairTableName
			layoutName = SchemaLayoutMatchingTasksFair
			version = matchingTaskVersion2
		}
		t.Run(name+"_current", func(t *testing.T) {
			session := &recordingSession{t: t}
			session.queryFn = func(stmt string, args ...any) commongocql.Query {
				switch stmt {
				case fmt.Sprintf(templateScanMatchingQueueInventory, sourceTable):
					return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{
						"11111111-1111-1111-1111-111111111111", "queue", 1,
					}}}}
				case fmt.Sprintf(templateScanMatchingTargetQueueInventory, targetTable):
					return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
						{"11111111-1111-1111-1111-111111111111", "queue", 1, 0},
						{"11111111-1111-1111-1111-111111111111", "queue", 1, 1},
					}}}
				case switchTasksTable(templateGetMatchingTaskSourceAuthority, version):
					return matchingTaskSourceInventoryRecordingQuery(3, matchingTaskAuthorityTarget, 2)
				default:
					if strings.Contains(stmt, "FROM "+targetTable+" WHERE") {
						if args[3].(int16) == 1 {
							return &recordingQuery{scanFn: func(...any) error { return gocql.ErrNotFound }}
						}
						return matchingTaskTargetInventoryRecordingQuery(3, matchingTaskMetadataStateActive, matchingTaskAuthorityTarget, 2)
					}
					t.Fatalf("unexpected query: %s", stmt)
					return nil
				}
			}

			result, err := AuditSchemaLayoutTargetOnlyInventory(
				t.Context(),
				session,
				testTargetOnlyInventorySpec(t, layoutName, 2),
				SchemaLayoutInventoryAuditOptions{PageSize: 3},
			)
			require.NoError(t, err)
			require.Equal(t, SchemaLayoutInventoryAuditResult{SourceEntities: 1, TargetEntities: 1}, result)
		})

		t.Run(name+"_orphan", func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, _ ...any) commongocql.Query {
					switch stmt {
					case fmt.Sprintf(templateScanMatchingQueueInventory, sourceTable):
						return &recordingQuery{iter: &recordingIter{}}
					case fmt.Sprintf(templateScanMatchingTargetQueueInventory, targetTable):
						return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{
							"22222222-2222-2222-2222-222222222222", "orphan", 1, 0,
						}}}}
					case switchTasksTable(templateGetMatchingTaskSourceAuthority, version):
						return &recordingQuery{scanFn: func(...any) error { return gocql.ErrNotFound }}
					default:
						t.Fatalf("unexpected query: %s", stmt)
						return nil
					}
				},
			}

			_, err := AuditSchemaLayoutTargetOnlyInventory(
				t.Context(),
				session,
				testTargetOnlyInventorySpec(t, layoutName, 2),
				SchemaLayoutInventoryAuditOptions{PageSize: 3},
			)
			require.ErrorContains(t, err, "has no valid source")
		})
	}
}

func TestAuditTaskQueueUserDataTargetOnlyInventoryChecksReversePartitions(t *testing.T) {
	const namespaceID = "11111111-1111-1111-1111-111111111111"
	spec := testTargetOnlyInventorySpec(t, SchemaLayoutTaskQueueUserData, 16)
	identity := taskQueueUserDataAuthorityRecord{
		authority:   taskQueueUserDataAuthorityTarget,
		bucketCount: 16,
		generation:  spec.Generation,
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) commongocql.Query {
			switch stmt {
			case templateScanUserDataInventory, templateScanUserDataTargetTxnInventory:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{namespaceID}}}}
			case templateScanUserDataTargetInventory:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{namespaceID, 3}}}}
			case templateGetTaskQueueUserDataSourceAuthority, templateGetTaskQueueUserDataTargetAuthority:
				return taskQueueUserDataAuthorityRecordingQuery(identity)
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	result, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		spec,
		SchemaLayoutInventoryAuditOptions{PageSize: 3},
	)
	require.NoError(t, err)
	require.Equal(t, SchemaLayoutInventoryAuditResult{SourceEntities: 1, TargetEntities: 1}, result)
}

func TestAuditTaskQueueUserDataTargetOnlyInventoryRejectsOrphanTargets(t *testing.T) {
	const namespaceID = "22222222-2222-2222-2222-222222222222"
	for _, target := range []string{"data", "transaction"} {
		t.Run(target, func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, _ ...any) commongocql.Query {
					switch stmt {
					case templateScanUserDataInventory:
						return &recordingQuery{iter: &recordingIter{}}
					case templateScanUserDataTargetInventory:
						rows := [][]any(nil)
						if target == "data" {
							rows = [][]any{{namespaceID, 3}}
						}
						return &recordingQuery{iter: &recordingIter{scanRows: rows}}
					case templateScanUserDataTargetTxnInventory:
						rows := [][]any(nil)
						if target == "transaction" {
							rows = [][]any{{namespaceID}}
						}
						return &recordingQuery{iter: &recordingIter{scanRows: rows}}
					case templateGetTaskQueueUserDataSourceAuthority:
						return &recordingQuery{scanFn: func(...any) error { return gocql.ErrNotFound }}
					default:
						t.Fatalf("unexpected query: %s", stmt)
						return nil
					}
				},
			}

			_, err := AuditSchemaLayoutTargetOnlyInventory(
				t.Context(),
				session,
				testTargetOnlyInventorySpec(t, SchemaLayoutTaskQueueUserData, 16),
				SchemaLayoutInventoryAuditOptions{PageSize: 3},
			)
			require.ErrorContains(t, err, "has no valid source")
		})
	}
}

func TestAuditLegacyQueueTargetOnlyInventoryChecksReversePartitions(t *testing.T) {
	generation := mustParseSchemaLayoutGeneration(t, "11111111-1111-4111-8111-111111111111")
	state := legacyQueueV2State{
		activeBucket:      0,
		minimumMessageID:  0,
		cleanupMessageID:  0,
		version:           0,
		authority:         legacyQueueMigrationAuthorityTarget,
		generation:        generation,
		messageBucketSize: 4096,
	}
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, _ ...any) commongocql.Query {
		switch stmt {
		case templateScanLegacyQueueInventory,
			templateScanLegacyQueueTargetStateInventory,
			templateScanLegacyQueueTargetDeleteInventory:
			return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{-1}}}}
		case templateScanLegacyQueueTargetBucketInventory:
			return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{-1, int64(0)}}}}
		case templateGetLegacyQueueSourceAuthority,
			templateGetLegacyQueueTargetBucketAuthority,
			templateGetLegacyQueueTargetDeleteAuthority:
			return legacyQueueAuthorityRecordingQuery(state.authorityRecord())
		case templateGetLegacyQueueV2State:
			return legacyQueueV2StateRecordingQuery(&state)
		case templateGetLegacyQueueV2BucketState:
			return legacyQueueV2BucketStateRecordingQuery(&legacyQueueV2BucketState{lastMessageID: -1})
		default:
			t.Fatalf("unexpected query: %s", stmt)
			return nil
		}
	}

	result, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		testTargetOnlyInventorySpec(t, SchemaLayoutLegacyQueue, 4096),
		SchemaLayoutInventoryAuditOptions{PageSize: 3},
	)
	require.NoError(t, err)
	require.Equal(t, SchemaLayoutInventoryAuditResult{SourceEntities: 1, TargetEntities: 1}, result)
}

func TestAuditLegacyQueueTargetOnlyInventoryRejectsOrphanTargets(t *testing.T) {
	for _, target := range []string{"state", "messages", "delete-ranges"} {
		t.Run(target, func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, _ ...any) commongocql.Query {
					switch stmt {
					case templateScanLegacyQueueInventory:
						return &recordingQuery{iter: &recordingIter{}}
					case templateScanLegacyQueueTargetStateInventory:
						rows := [][]any(nil)
						if target == "state" {
							rows = [][]any{{-1}}
						}
						return &recordingQuery{iter: &recordingIter{scanRows: rows}}
					case templateScanLegacyQueueTargetBucketInventory:
						rows := [][]any(nil)
						if target == "messages" {
							rows = [][]any{{-1, int64(0)}}
						}
						return &recordingQuery{iter: &recordingIter{scanRows: rows}}
					case templateScanLegacyQueueTargetDeleteInventory:
						rows := [][]any(nil)
						if target == "delete-ranges" {
							rows = [][]any{{-1}}
						}
						return &recordingQuery{iter: &recordingIter{scanRows: rows}}
					case templateGetLegacyQueueSourceAuthority:
						return &recordingQuery{scanFn: func(...any) error { return gocql.ErrNotFound }}
					default:
						t.Fatalf("unexpected query: %s", stmt)
						return nil
					}
				},
			}

			_, err := AuditSchemaLayoutTargetOnlyInventory(
				t.Context(),
				session,
				testTargetOnlyInventorySpec(t, SchemaLayoutLegacyQueue, 4096),
				SchemaLayoutInventoryAuditOptions{PageSize: 3},
			)
			require.ErrorContains(t, err, "has no source authority")
		})
	}
}

func legacyQueueAuthorityRecordingQuery(record legacyQueueAuthorityRecord) commongocql.Query {
	return &recordingQuery{scanFn: func(dest ...any) error {
		authority := int(record.authority)
		generation := record.generation
		bucketSize := record.messageBucketSize
		*dest[0].(**int) = &authority
		*dest[1].(**gocql.UUID) = &generation
		*dest[2].(**int64) = &bucketSize
		return nil
	}}
}

func TestAuditQueueV2TargetOnlyInventoryRejectsEntityGenerationMismatch(t *testing.T) {
	metadataSpec := testTargetOnlyInventorySpec(t, SchemaLayoutQueueV2Metadata, queueV2MetadataBucketCount)
	messagesSpec := testTargetOnlyInventorySpec(t, SchemaLayoutQueueV2Messages, DefaultQueueV2MessageBucketSpan)
	messagesSpec.Generation = mustParseSchemaLayoutGeneration(t, "22222222-2222-2222-2222-222222222222")
	wrongGeneration := mustParseSchemaLayoutGeneration(t, "33333333-3333-3333-3333-333333333333")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) commongocql.Query {
			switch stmt {
			case templateGetSchemaLayoutMetadata:
				return &recordingQuery{scanFn: func(dest ...any) error {
					spec := metadataSpec
					if args[0] == SchemaLayoutQueueV2Messages {
						spec = messagesSpec
					}
					setSchemaLayoutInventoryMetadata(dest, spec, SchemaLayoutAuthorityTargetReady, 1)
					return nil
				}}
			case templateScanQueueV2Inventory:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{1, "queue-a"}}}}
			case templateGetQueueV2SourceMetadataAuthority, templateGetQueueV2SourceMessageAuthority:
				return &recordingQuery{scanFn: func(dest ...any) error {
					setQueueV2InventoryAuthority(
						dest,
						queueV2MigrationAuthorityTarget,
						wrongGeneration,
						3,
						DefaultQueueV2MessageBucketSpan,
					)
					return nil
				}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	result, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		metadataSpec,
		SchemaLayoutInventoryAuditOptions{
			PageSize:            3,
			QueueV2MetadataSpec: metadataSpec,
			QueueV2MessagesSpec: messagesSpec,
		},
	)
	require.ErrorContains(t, err, "generation")
	require.Equal(t, int64(1), result.SourceEntities)
	require.Zero(t, result.TargetEntities)
	require.Equal(t, 3, session.queries[2].query.pageSize)
}

func TestAuditQueueV2TargetOnlyInventoryRejectsMetadataFirstGlobalTransition(t *testing.T) {
	metadataSpec := testTargetOnlyInventorySpec(t, SchemaLayoutQueueV2Metadata, queueV2MetadataBucketCount)
	messagesSpec := testTargetOnlyInventorySpec(t, SchemaLayoutQueueV2Messages, DefaultQueueV2MessageBucketSpan)
	messagesSpec.Generation = mustParseSchemaLayoutGeneration(t, "22222222-2222-2222-2222-222222222222")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) commongocql.Query {
			require.Equal(t, templateGetSchemaLayoutMetadata, stmt)
			return &recordingQuery{scanFn: func(dest ...any) error {
				spec := metadataSpec
				authority := SchemaLayoutAuthorityTargetOnly
				epoch := int64(2)
				if args[0] == SchemaLayoutQueueV2Messages {
					spec = messagesSpec
					authority = SchemaLayoutAuthorityTargetReady
					epoch = 1
				}
				setSchemaLayoutInventoryMetadata(dest, spec, authority, epoch)
				return nil
			}}
		},
	}

	_, err := AuditSchemaLayoutTargetOnlyInventory(
		t.Context(),
		session,
		metadataSpec,
		SchemaLayoutInventoryAuditOptions{
			PageSize:            3,
			QueueV2MetadataSpec: metadataSpec,
			QueueV2MessagesSpec: messagesSpec,
		},
	)
	require.ErrorContains(t, err, "metadata is target-only before messages")
	require.Len(t, session.queries, 2)
}

func TestAuditQueueV2TargetMetadataPlacementRequiresOneCanonicalRow(t *testing.T) {
	queueType := persistence.QueueV2Type(1)
	queueName := "queue-a"
	canonicalBucket := queueV2MetadataBucket(queueType, queueName)
	tests := []struct {
		name          string
		rows          [][]any
		errorContains string
	}{
		{name: "canonical", rows: [][]any{{canonicalBucket}}},
		{name: "missing", errorContains: "expected exactly one"},
		{name: "wrong bucket", rows: [][]any{{canonicalBucket + 1}}, errorContains: "expected"},
		{name: "duplicate", rows: [][]any{{canonicalBucket}, {canonicalBucket}}, errorContains: "expected exactly one"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, args ...any) commongocql.Query {
					require.Equal(t, templateScanQueueV2TargetMetadataBuckets, stmt)
					require.Equal(t, []any{queueType, queueName}, args)
					return &recordingQuery{iter: &recordingIter{scanRows: test.rows}}
				},
			}
			err := auditQueueV2TargetMetadataPlacement(
				t.Context(),
				&queueV2Store{session: session},
				queueType,
				queueName,
				3,
			)
			if test.errorContains == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.errorContains)
			}
			require.Equal(t, 3, session.queries[0].query.pageSize)
		})
	}
}

func TestAuditQueueV2TargetMessagePartitionsValidatesEveryBucket(t *testing.T) {
	queueType := persistence.QueueV2Type(1)
	queueName := "queue-a"
	record := queueV2AuthorityRecord{
		authority:   queueV2MigrationAuthorityTarget,
		generation:  mustParseSchemaLayoutGeneration(t, "11111111-1111-1111-1111-111111111111"),
		epoch:       3,
		messageSpan: DefaultQueueV2MessageBucketSpan,
	}
	authorityBuckets := make([]int64, 0, 3)
	stateBuckets := make([]int64, 0, 2)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) commongocql.Query {
			switch stmt {
			case templateScanQueueV2MessageBucketInventory:
				require.Equal(t, []any{queueType, queueName}, args)
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{int(queueType), queueName, queueV2MessageDirectoryBucket},
					{int(queueType), queueName, int64(0)},
					{int(queueType), queueName, int64(1)},
				}}}
			case templateGetQueueV2TargetMessageAuthority:
				bucket := args[2].(int64)
				authorityBuckets = append(authorityBuckets, bucket)
				return &recordingQuery{scanFn: func(dest ...any) error {
					setQueueV2InventoryAuthority(dest, record.authority, record.generation, record.epoch, record.messageSpan)
					return nil
				}}
			case templateGetQueueV2MessageDirectory:
				return &recordingQuery{scanFn: func(dest ...any) error {
					*dest[0].(*int64) = 1
					*dest[1].(*int64) = record.messageSpan
					*dest[2].(*int64) = 4
					return nil
				}}
			case templateGetQueueV2MessageBucketState:
				bucket := args[2].(int64)
				stateBuckets = append(stateBuckets, bucket)
				return &recordingQuery{scanFn: func(dest ...any) error {
					tail := bucket*record.messageSpan - 1
					if bucket == 0 {
						tail = record.messageSpan - 1
					}
					*dest[0].(*int64) = tail
					*dest[1].(*int64) = 2
					return nil
				}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	err := auditQueueV2TargetMessagePartitions(
		t.Context(),
		&queueV2Store{session: session, messageSpan: record.messageSpan},
		queueType,
		queueName,
		record,
		0,
		1,
		3,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{-1, 0, 1}, authorityBuckets)
	require.Equal(t, []int64{0, 1}, stateBuckets)
	require.Equal(t, 3, session.queries[0].query.pageSize)
}

func TestAuditQueueV2TargetMessagePartitionsRejectsInvalidInactiveBucket(t *testing.T) {
	queueType := persistence.QueueV2Type(1)
	queueName := "queue-a"
	record := queueV2AuthorityRecord{
		authority:   queueV2MigrationAuthorityTarget,
		generation:  mustParseSchemaLayoutGeneration(t, "11111111-1111-1111-1111-111111111111"),
		epoch:       3,
		messageSpan: DefaultQueueV2MessageBucketSpan,
	}
	wrongGeneration := mustParseSchemaLayoutGeneration(t, "22222222-2222-2222-2222-222222222222")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) commongocql.Query {
			switch stmt {
			case templateScanQueueV2MessageBucketInventory:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{int(queueType), queueName, int64(0)},
				}}}
			case templateGetQueueV2TargetMessageAuthority:
				return &recordingQuery{scanFn: func(dest ...any) error {
					setQueueV2InventoryAuthority(
						dest,
						record.authority,
						wrongGeneration,
						record.epoch,
						record.messageSpan,
					)
					return nil
				}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	err := auditQueueV2TargetMessagePartitions(
		t.Context(),
		&queueV2Store{session: session, messageSpan: record.messageSpan},
		queueType,
		queueName,
		record,
		0,
		0,
		3,
	)
	require.ErrorContains(t, err, "bucket 0 inventory is incomplete")
	require.ErrorContains(t, err, "generation")
}

func TestAuditQueueV2TargetMessagePartitionsRequiresBucketState(t *testing.T) {
	queueType := persistence.QueueV2Type(1)
	queueName := "queue-a"
	record := queueV2AuthorityRecord{
		authority:   queueV2MigrationAuthorityTarget,
		generation:  mustParseSchemaLayoutGeneration(t, "11111111-1111-1111-1111-111111111111"),
		epoch:       3,
		messageSpan: DefaultQueueV2MessageBucketSpan,
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) commongocql.Query {
			switch stmt {
			case templateScanQueueV2MessageBucketInventory:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{int(queueType), queueName, int64(0)},
				}}}
			case templateGetQueueV2TargetMessageAuthority:
				return &recordingQuery{scanFn: func(dest ...any) error {
					setQueueV2InventoryAuthority(dest, record.authority, record.generation, record.epoch, record.messageSpan)
					return nil
				}}
			case templateGetQueueV2MessageBucketState:
				return &recordingQuery{scanFn: func(...any) error { return gocql.ErrNotFound }}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	err := auditQueueV2TargetMessagePartitions(
		t.Context(),
		&queueV2Store{session: session, messageSpan: record.messageSpan},
		queueType,
		queueName,
		record,
		0,
		0,
		3,
	)
	require.ErrorContains(t, err, "bucket state")
}

func TestAuditQueueV2TargetMessagePartitionsRequiresCompleteActiveRange(t *testing.T) {
	queueType := persistence.QueueV2Type(1)
	queueName := "queue-a"
	record := queueV2AuthorityRecord{
		authority:   queueV2MigrationAuthorityTarget,
		generation:  mustParseSchemaLayoutGeneration(t, "11111111-1111-1111-1111-111111111111"),
		epoch:       3,
		messageSpan: DefaultQueueV2MessageBucketSpan,
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) commongocql.Query {
			switch stmt {
			case templateScanQueueV2MessageBucketInventory:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{int(queueType), queueName, queueV2MessageDirectoryBucket},
					{int(queueType), queueName, int64(0)},
					{int(queueType), queueName, int64(2)},
				}}}
			case templateGetQueueV2TargetMessageAuthority:
				return &recordingQuery{scanFn: func(dest ...any) error {
					setQueueV2InventoryAuthority(dest, record.authority, record.generation, record.epoch, record.messageSpan)
					return nil
				}}
			case templateGetQueueV2MessageDirectory:
				return &recordingQuery{scanFn: func(dest ...any) error {
					*dest[0].(*int64) = 2
					*dest[1].(*int64) = record.messageSpan
					*dest[2].(*int64) = 4
					return nil
				}}
			case templateGetQueueV2MessageBucketState:
				bucket := args[2].(int64)
				return &recordingQuery{scanFn: func(dest ...any) error {
					tail := (bucket+1)*record.messageSpan - 1
					if bucket == 2 {
						tail = bucket*record.messageSpan - 1
					}
					*dest[0].(*int64) = tail
					*dest[1].(*int64) = 2
					return nil
				}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	err := auditQueueV2TargetMessagePartitions(
		t.Context(),
		&queueV2Store{session: session, messageSpan: record.messageSpan},
		queueType,
		queueName,
		record,
		0,
		2,
		3,
	)
	require.ErrorContains(t, err, "inventory is incomplete")
	require.ErrorContains(t, err, "found=2 expected=3")
}

func TestAuditQueueV2TargetMessagePartitionsRejectsFutureBucket(t *testing.T) {
	queueType := persistence.QueueV2Type(1)
	queueName := "queue-a"
	record := queueV2AuthorityRecord{
		authority:   queueV2MigrationAuthorityTarget,
		generation:  mustParseSchemaLayoutGeneration(t, "11111111-1111-1111-1111-111111111111"),
		epoch:       3,
		messageSpan: DefaultQueueV2MessageBucketSpan,
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) commongocql.Query {
			switch stmt {
			case templateScanQueueV2MessageBucketInventory:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{int(queueType), queueName, int64(1)},
				}}}
			case templateGetQueueV2TargetMessageAuthority:
				return &recordingQuery{scanFn: func(dest ...any) error {
					setQueueV2InventoryAuthority(dest, record.authority, record.generation, record.epoch, record.messageSpan)
					return nil
				}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}

	err := auditQueueV2TargetMessagePartitions(
		t.Context(),
		&queueV2Store{session: session, messageSpan: record.messageSpan},
		queueType,
		queueName,
		record,
		0,
		0,
		3,
	)
	require.ErrorContains(t, err, "future bucket 1 beyond active bucket 0")
}

func testTargetOnlyInventorySpec(t *testing.T, name SchemaLayoutName, parameter int64) SchemaLayoutSpec {
	t.Helper()
	return SchemaLayoutSpec{
		Name:               name,
		Generation:         mustParseSchemaLayoutGeneration(t, "11111111-1111-1111-1111-111111111111"),
		Version:            1,
		ImmutableParameter: parameter,
	}
}

func setExecutionInventoryAuthority(dest []any, rangeID int64, authority int, bucketCount int) {
	*dest[0].(*int64) = rangeID
	*dest[1].(**int) = &authority
	*dest[2].(**int) = &bucketCount
}

func setMatchingInventoryAuthority(dest []any, rangeID int64, authority int, bucketCount int16) {
	*dest[0].(*int64) = rangeID
	*dest[1].(*[]byte) = []byte("metadata")
	*dest[2].(*string) = "Proto3"
	*dest[3].(**int) = &authority
	*dest[4].(**int16) = &bucketCount
}

func matchingTaskSourceInventoryRecordingQuery(
	rangeID int64,
	authority int,
	bucketCount int16,
) commongocql.Query {
	return &recordingQuery{scanFn: func(dest ...any) error {
		migrationTimestamp := int64(1)
		*dest[0].(*int64) = rangeID
		*dest[1].(*[]byte) = []byte("metadata")
		*dest[2].(*string) = "Proto3"
		*dest[3].(**int) = &authority
		*dest[4].(**int16) = &bucketCount
		*dest[5].(**int64) = &migrationTimestamp
		*dest[6].(*nullableInt64) = nullableInt64{}
		return nil
	}}
}

func matchingTaskTargetInventoryRecordingQuery(
	rangeID int64,
	state int,
	authority int,
	bucketCount int16,
) commongocql.Query {
	return &recordingQuery{scanFn: func(dest ...any) error {
		*dest[0].(*int64) = rangeID
		*dest[1].(*[]byte) = []byte("metadata")
		*dest[2].(*string) = "Proto3"
		*dest[3].(*int) = state
		*dest[4].(*int16) = bucketCount
		*dest[5].(*int) = authority
		*dest[6].(*nullableInt64) = nullableInt64{}
		*dest[7].(*[]byte) = nil
		*dest[8].(*string) = ""
		*dest[9].(*nullableInt64) = nullableInt64{}
		return nil
	}}
}

func taskQueueUserDataAuthorityRecordingQuery(
	record taskQueueUserDataAuthorityRecord,
) commongocql.Query {
	return &recordingQuery{scanFn: func(dest ...any) error {
		authority := int(record.authority)
		bucketCount := int16(record.bucketCount)
		generation := record.generation
		*dest[0].(**int) = &authority
		*dest[1].(**int16) = &bucketCount
		*dest[2].(**gocql.UUID) = &generation
		return nil
	}}
}

func setSchemaLayoutInventoryMetadata(
	dest []any,
	spec SchemaLayoutSpec,
	authority SchemaLayoutAuthorityState,
	epoch int64,
) {
	*dest[0].(*gocql.UUID) = spec.Generation
	*dest[1].(*int32) = spec.Version
	*dest[2].(*int64) = spec.ImmutableParameter
	*dest[3].(*SchemaLayoutAuthorityState) = authority
	*dest[4].(*int64) = epoch
	*dest[5].(*time.Time) = time.Unix(1, 0).UTC()
}

func setQueueV2InventoryAuthority(
	dest []any,
	authority queueV2MigrationAuthority,
	generation gocql.UUID,
	epoch int64,
	span int64,
) {
	authorityValue := int(authority)
	*dest[0].(**int) = &authorityValue
	*dest[1].(**gocql.UUID) = &generation
	*dest[2].(**int64) = &epoch
	*dest[3].(**int64) = &span
}
