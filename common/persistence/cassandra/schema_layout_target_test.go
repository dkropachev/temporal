package cassandra

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func TestSchemaLayoutSpecForTableUsesPhysicalGeneration(t *testing.T) {
	generation, err := gocql.ParseUUID("11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetHistoryNodeTableID, stmt)
			require.Equal(t, []any{"temporal", "executions_v2"}, args)
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*gocql.UUID) = generation
				return nil
			}}
		},
	}

	spec, err := SchemaLayoutSpecForTable(
		t.Context(),
		session,
		"temporal",
		SchemaLayoutExecutions,
		"executions_v2",
		16,
	)
	require.NoError(t, err)
	require.Equal(t, generation, spec.Generation)
	require.Equal(t, int64(16), spec.ImmutableParameter)
}

func TestSchemaLayoutTargetDualStartupAuthorityOverlap(t *testing.T) {
	generation, err := gocql.ParseUUID("11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)
	spec := SchemaLayoutSpec{
		Name:               SchemaLayoutExecutions,
		Generation:         generation,
		Version:            1,
		ImmutableParameter: 16,
	}
	newSession := func() *recordingSession {
		return &recordingSession{
			t: t,
			queryFn: func(stmt string, _ ...any) cgocql.Query {
				switch stmt {
				case templateGetHistoryNodeTableID:
					return &recordingQuery{scanFn: func(dest ...any) error {
						*dest[0].(*gocql.UUID) = generation
						return nil
					}}
				case templateGetSchemaLayoutMetadata:
					return &recordingQuery{scanFn: func(dest ...any) error {
						setSchemaLayoutInventoryMetadata(dest, spec, SchemaLayoutAuthorityTargetOnly, 2)
						return nil
					}}
				default:
					t.Fatalf("unexpected query: %s", stmt)
					return nil
				}
			},
		}
	}

	err = RequireSchemaLayoutTargetReady(
		t.Context(),
		newSession(),
		"temporal",
		spec.Name,
		"executions_v2",
		spec.ImmutableParameter,
	)
	require.NoError(t, err)

	err = RequireSchemaLayoutSourceCompatible(
		t.Context(),
		newSession(),
		"temporal",
		spec.Name,
		"executions_v2",
		spec.ImmutableParameter,
	)
	var authorityErr *SchemaLayoutAuthorityError
	require.ErrorAs(t, err, &authorityErr)
	require.Equal(t, SchemaLayoutAuthorityTargetOnly, authorityErr.Actual)
}
