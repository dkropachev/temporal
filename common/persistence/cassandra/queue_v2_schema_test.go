package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func TestValidateQueueV2MigrationModeSchema(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetQueueV2SchemaColumns, stmt)
			require.Len(t, args, 2)
			require.Equal(t, "temporal", args[0])
			switch args[1] {
			case "queues_v2":
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{"queue_type", "partition_key", 0},
					{"metadata_bucket", "partition_key", 1},
					{"queue_name", "clustering", 0},
					{"metadata_payload", "regular", -1},
					{"metadata_encoding", "regular", -1},
					{"version", "regular", -1},
					{"migration_authority", "regular", -1},
					{"migration_generation", "regular", -1},
					{"migration_epoch", "regular", -1},
					{"message_bucket_span", "regular", -1},
				}}}
			case "queues":
				t.Fatal("target-only validation queried source queues schema")
				return nil
			case "queue_messages":
				t.Fatal("target-only validation queried source queue_messages schema")
				return nil
			case "queue_messages_v3":
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{"queue_type", "partition_key", 0},
					{"queue_name", "partition_key", 1},
					{"message_bucket", "partition_key", 2},
					{"row_type", "clustering", 0},
					{"message_id", "clustering", 1},
					{"message_payload", "regular", -1},
					{"message_encoding", "regular", -1},
					{"active_message_bucket", "regular", -1},
					{"bucket_span", "static", -1},
					{"last_message_id", "regular", -1},
					{"version", "regular", -1},
					{"migration_authority", "static", -1},
					{"migration_generation", "static", -1},
					{"migration_epoch", "static", -1},
				}}}
			default:
				t.Fatalf("unexpected QueueV2 schema table %v", args[1])
				return nil
			}
		},
	}

	require.NoError(t, ValidateQueueV2MigrationModeSchema(
		t.Context(),
		session,
		"temporal",
		config.CassandraQueueV2MigrationModeTargetOnly,
	))
}

func TestValidateQueueV2SourceOnlyDoesNotReadTargetSchema(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			t.Fatal("source-only validation queried target schema")
			return nil
		},
	}
	require.NoError(t, ValidateQueueV2MigrationModeSchema(
		t.Context(),
		session,
		"temporal",
		config.CassandraQueueV2MigrationModeSourceOnly,
	))
}
