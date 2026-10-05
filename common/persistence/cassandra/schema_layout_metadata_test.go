package cassandra

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type schemaLayoutMetadataTestSession struct {
	queryFn func(stmt string, args ...any) commongocql.Query
}

func (s *schemaLayoutMetadataTestSession) Query(stmt string, args ...any) commongocql.Query {
	return s.queryFn(stmt, args...)
}

func (*schemaLayoutMetadataTestSession) NewBatch(commongocql.BatchType) *commongocql.Batch {
	return nil
}

func (*schemaLayoutMetadataTestSession) ExecuteBatch(*commongocql.Batch) error {
	return nil
}

func (*schemaLayoutMetadataTestSession) MapExecuteBatchCAS(
	*commongocql.Batch,
	map[string]any,
) (bool, commongocql.Iter, error) {
	return false, nil, nil
}

func (*schemaLayoutMetadataTestSession) AwaitSchemaAgreement(context.Context) error {
	return nil
}

func (*schemaLayoutMetadataTestSession) Close() {}

type schemaLayoutMetadataTestQuery struct {
	scanFn       func(dest ...any) error
	mapScanCASFn func(map[string]any) (bool, error)
}

func (*schemaLayoutMetadataTestQuery) Exec() error { return nil }

func (q *schemaLayoutMetadataTestQuery) Scan(dest ...any) error {
	if q.scanFn == nil {
		return nil
	}
	return q.scanFn(dest...)
}

func (*schemaLayoutMetadataTestQuery) ScanCAS(...any) (bool, error) { return false, nil }

func (*schemaLayoutMetadataTestQuery) MapScan(map[string]any) error { return nil }

func (q *schemaLayoutMetadataTestQuery) MapScanCAS(dest map[string]any) (bool, error) {
	if q.mapScanCASFn == nil {
		return false, nil
	}
	return q.mapScanCASFn(dest)
}

func (*schemaLayoutMetadataTestQuery) Iter() commongocql.Iter { return nil }

func (q *schemaLayoutMetadataTestQuery) PageSize(int) commongocql.Query { return q }

func (q *schemaLayoutMetadataTestQuery) PageState([]byte) commongocql.Query { return q }

func (q *schemaLayoutMetadataTestQuery) WithContext(context.Context) commongocql.Query { return q }

func (q *schemaLayoutMetadataTestQuery) WithTimestamp(int64) commongocql.Query { return q }

func (q *schemaLayoutMetadataTestQuery) Consistency(commongocql.Consistency) commongocql.Query {
	return q
}

func (q *schemaLayoutMetadataTestQuery) Bind(...any) commongocql.Query { return q }

func (q *schemaLayoutMetadataTestQuery) Idempotent(bool) commongocql.Query { return q }

func (q *schemaLayoutMetadataTestQuery) SetSpeculativeExecutionPolicy(
	commongocql.SpeculativeExecutionPolicy,
) commongocql.Query {
	return q
}

func TestSchemaLayoutNames(t *testing.T) {
	require.Equal(t, []SchemaLayoutName{
		SchemaLayoutExecutions,
		SchemaLayoutHistoryNode,
		SchemaLayoutHistoryTree,
		SchemaLayoutQueueV2Metadata,
		SchemaLayoutQueueV2Messages,
		SchemaLayoutLegacyQueue,
		SchemaLayoutMatchingTasks,
		SchemaLayoutMatchingTasksFair,
		SchemaLayoutTaskQueueUserData,
	}, SchemaLayoutNames())

	names := SchemaLayoutNames()
	names[0] = "changed"
	require.Equal(t, SchemaLayoutExecutions, SchemaLayoutNames()[0])
}

func TestSchemaLayoutMetadataLifecycle(t *testing.T) {
	generation := mustParseSchemaLayoutGeneration(t, "11111111-1111-1111-1111-111111111111")
	spec := SchemaLayoutSpec{
		Name:               SchemaLayoutExecutions,
		Generation:         generation,
		Version:            2,
		ImmutableParameter: 16,
	}
	var persisted *SchemaLayoutMetadata
	store := NewSchemaLayoutMetadataStore(newSchemaLayoutMetadataTestSession(t, &persisted))

	preparing, err := store.InitializePreparing(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, SchemaLayoutAuthorityPreparing, preparing.AuthorityState)
	require.Equal(t, int64(0), preparing.Epoch)
	require.False(t, preparing.UpdatedAt.IsZero())

	idempotent, err := store.InitializePreparing(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, preparing, idempotent)

	_, err = store.RequireTargetReady(t.Context(), spec)
	var authorityErr *SchemaLayoutAuthorityError
	require.ErrorAs(t, err, &authorityErr)

	ready, err := store.MarkTargetReady(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, SchemaLayoutAuthorityTargetReady, ready.AuthorityState)
	require.Equal(t, int64(1), ready.Epoch)

	readyAgain, err := store.MarkTargetReady(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, ready, readyAgain)
	_, err = store.RequireTargetReady(t.Context(), spec)
	require.NoError(t, err)

	_, err = store.CompareAndSwapAuthority(t.Context(), preparing, SchemaLayoutAuthorityTargetReady)
	var conflictErr *SchemaLayoutConflictError
	require.ErrorAs(t, err, &conflictErr)
	require.Equal(t, SchemaLayoutAuthorityTargetReady, conflictErr.ActualState)
	require.Equal(t, int64(1), conflictErr.ActualEpoch)

	targetOnly, err := store.MarkTargetOnly(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, SchemaLayoutAuthorityTargetOnly, targetOnly.AuthorityState)
	require.Equal(t, int64(2), targetOnly.Epoch)
	_, err = store.RequireTargetReady(t.Context(), spec)
	require.NoError(t, err)
	_, err = store.RequireTargetOnly(t.Context(), spec)
	require.NoError(t, err)

	retired, err := store.MarkRetired(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, SchemaLayoutAuthorityRetired, retired.AuthorityState)
	require.Equal(t, int64(3), retired.Epoch)
	_, err = store.RequireTargetOnly(t.Context(), spec)
	require.ErrorAs(t, err, &authorityErr)
}

func TestSchemaLayoutMetadataRejectsImmutableMismatch(t *testing.T) {
	spec := SchemaLayoutSpec{
		Name:               SchemaLayoutTaskQueueUserData,
		Generation:         mustParseSchemaLayoutGeneration(t, "22222222-2222-2222-2222-222222222222"),
		Version:            2,
		ImmutableParameter: 64,
	}
	var persisted *SchemaLayoutMetadata
	store := NewSchemaLayoutMetadataStore(newSchemaLayoutMetadataTestSession(t, &persisted))
	_, err := store.InitializePreparing(t.Context(), spec)
	require.NoError(t, err)

	for name, mutate := range map[string]func(*SchemaLayoutSpec){
		"generation": func(other *SchemaLayoutSpec) {
			other.Generation = mustParseSchemaLayoutGeneration(t, "33333333-3333-3333-3333-333333333333")
		},
		"version": func(other *SchemaLayoutSpec) {
			other.Version++
		},
		"parameter": func(other *SchemaLayoutSpec) {
			other.ImmutableParameter++
		},
	} {
		t.Run(name, func(t *testing.T) {
			other := spec
			mutate(&other)
			_, err := store.InitializePreparing(t.Context(), other)
			var mismatchErr *SchemaLayoutMismatchError
			require.ErrorAs(t, err, &mismatchErr)
			require.Equal(t, mismatchErr.Expected, other)
			require.Equal(t, spec, mismatchErr.Actual)
		})
	}
}

func TestSchemaLayoutMetadataValidation(t *testing.T) {
	valid := SchemaLayoutSpec{
		Name:               SchemaLayoutHistoryNode,
		Generation:         mustParseSchemaLayoutGeneration(t, "44444444-4444-4444-4444-444444444444"),
		Version:            2,
		ImmutableParameter: 0,
	}

	for name, mutate := range map[string]func(*SchemaLayoutSpec){
		"name": func(spec *SchemaLayoutSpec) {
			spec.Name = "typo"
		},
		"generation": func(spec *SchemaLayoutSpec) {
			spec.Generation = gocql.UUID{}
		},
		"version": func(spec *SchemaLayoutSpec) {
			spec.Version = 0
		},
		"parameter": func(spec *SchemaLayoutSpec) {
			spec.ImmutableParameter = -1
		},
	} {
		t.Run(name, func(t *testing.T) {
			spec := valid
			mutate(&spec)
			require.Error(t, validateSchemaLayoutSpec(spec))
		})
	}

	require.False(t, isLegalSchemaLayoutTransition(
		SchemaLayoutAuthorityPreparing,
		SchemaLayoutAuthorityTargetOnly,
	))
	require.False(t, isLegalSchemaLayoutTransition(
		SchemaLayoutAuthorityTargetOnly,
		SchemaLayoutAuthorityTargetReady,
	))
}

func TestSchemaLayoutMetadataLoadRejectsCorruptState(t *testing.T) {
	spec := SchemaLayoutSpec{
		Name:               SchemaLayoutQueueV2Metadata,
		Generation:         mustParseSchemaLayoutGeneration(t, "55555555-5555-5555-5555-555555555555"),
		Version:            2,
		ImmutableParameter: 64,
	}
	persisted := &SchemaLayoutMetadata{
		SchemaLayoutSpec: spec,
		AuthorityState:   "corrupt",
		Epoch:            1,
		UpdatedAt:        time.Now().UTC(),
	}
	store := NewSchemaLayoutMetadataStore(newSchemaLayoutMetadataTestSession(t, &persisted))

	_, err := store.Load(t.Context(), spec.Name)
	require.ErrorContains(t, err, "unsupported authority state")
}

func TestSchemaLayoutMetadataLoadMissing(t *testing.T) {
	var persisted *SchemaLayoutMetadata
	store := NewSchemaLayoutMetadataStore(newSchemaLayoutMetadataTestSession(t, &persisted))

	_, err := store.Load(t.Context(), SchemaLayoutLegacyQueue)
	var notFoundErr *SchemaLayoutNotFoundError
	require.ErrorAs(t, err, &notFoundErr)
}

func mustParseSchemaLayoutGeneration(t *testing.T, value string) gocql.UUID {
	t.Helper()
	generation, err := gocql.ParseUUID(value)
	require.NoError(t, err)
	return generation
}

func newSchemaLayoutMetadataTestSession(
	t *testing.T,
	persisted **SchemaLayoutMetadata,
) commongocql.Session {
	t.Helper()
	return &schemaLayoutMetadataTestSession{
		queryFn: func(stmt string, args ...any) commongocql.Query {
			switch stmt {
			case templateInitializeSchemaLayoutMetadata:
				return &schemaLayoutMetadataTestQuery{mapScanCASFn: func(map[string]any) (bool, error) {
					if *persisted != nil {
						return false, nil
					}
					*persisted = &SchemaLayoutMetadata{
						SchemaLayoutSpec: SchemaLayoutSpec{
							Name:               args[0].(SchemaLayoutName),
							Generation:         args[1].(gocql.UUID),
							Version:            args[2].(int32),
							ImmutableParameter: args[3].(int64),
						},
						AuthorityState: args[4].(SchemaLayoutAuthorityState),
						Epoch:          args[5].(int64),
						UpdatedAt:      time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
					}
					return true, nil
				}}
			case templateGetSchemaLayoutMetadata:
				return &schemaLayoutMetadataTestQuery{scanFn: func(dest ...any) error {
					if *persisted == nil || (*persisted).Name != args[0].(SchemaLayoutName) {
						return gocql.ErrNotFound
					}
					*dest[0].(*gocql.UUID) = (*persisted).Generation
					*dest[1].(*int32) = (*persisted).Version
					*dest[2].(*int64) = (*persisted).ImmutableParameter
					*dest[3].(*SchemaLayoutAuthorityState) = (*persisted).AuthorityState
					*dest[4].(*int64) = (*persisted).Epoch
					*dest[5].(*time.Time) = (*persisted).UpdatedAt
					return nil
				}}
			case templateUpdateSchemaLayoutAuthority:
				return &schemaLayoutMetadataTestQuery{mapScanCASFn: func(map[string]any) (bool, error) {
					if *persisted == nil ||
						(*persisted).Name != args[2].(SchemaLayoutName) ||
						(*persisted).Generation != args[3].(gocql.UUID) ||
						(*persisted).Version != args[4].(int32) ||
						(*persisted).ImmutableParameter != args[5].(int64) ||
						(*persisted).AuthorityState != args[6].(SchemaLayoutAuthorityState) ||
						(*persisted).Epoch != args[7].(int64) {
						return false, nil
					}
					updated := **persisted
					updated.AuthorityState = args[0].(SchemaLayoutAuthorityState)
					updated.Epoch = args[1].(int64)
					updated.UpdatedAt = updated.UpdatedAt.Add(time.Second)
					*persisted = &updated
					return true, nil
				}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
}

func TestSchemaLayoutMetadataQueryFailure(t *testing.T) {
	session := &schemaLayoutMetadataTestSession{
		queryFn: func(string, ...any) commongocql.Query {
			return &schemaLayoutMetadataTestQuery{mapScanCASFn: func(map[string]any) (bool, error) {
				return false, errors.New("injected failure")
			}}
		},
	}
	store := NewSchemaLayoutMetadataStore(session)
	_, err := store.InitializePreparing(context.Background(), SchemaLayoutSpec{
		Name:               SchemaLayoutHistoryTree,
		Generation:         mustParseSchemaLayoutGeneration(t, "66666666-6666-6666-6666-666666666666"),
		Version:            2,
		ImmutableParameter: 64,
	})
	require.ErrorContains(t, err, "injected failure")
}
