package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"go.temporal.io/server/common/config"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const templateGetLegacyQueueV2SchemaColumns = `SELECT column_name, kind, position FROM system_schema.columns
	WHERE keyspace_name = ? AND table_name = ?`

type legacyQueueV2SchemaColumn struct {
	name     string
	kind     string
	position int
}

// ValidateLegacyQueueMigrationModeSchema verifies tables required by the configured mode.
func ValidateLegacyQueueMigrationModeSchema(
	ctx context.Context,
	session cgocql.Session,
	keyspace string,
	mode config.CassandraLegacyQueueMigrationMode,
) error {
	mode = normalizeLegacyQueueMigrationMode(mode)
	if err := ValidateLegacyQueueMigrationMode(mode); err != nil {
		return err
	}
	if mode == config.CassandraLegacyQueueMigrationModeSourceOnly {
		return nil
	}
	if keyspace == "" {
		return errors.New("legacy queue v2 schema validation requires a keyspace")
	}
	tables := []struct {
		name                string
		partitionKeys       []string
		clusteringKeys      []string
		requiredRegularKeys []string
		requiredStaticKeys  []string
	}{
		{
			name:          "legacy_queue_v2_state",
			partitionKeys: []string{"queue_type"},
			requiredRegularKeys: []string{
				"active_bucket", "minimum_message_id", "cleanup_message_id", "version",
				"migration_authority", "migration_generation", "message_bucket_size",
			},
		},
		{
			name:                "legacy_queue_v2_messages",
			partitionKeys:       []string{"queue_type", "bucket_id"},
			clusteringKeys:      []string{"row_type", "message_id"},
			requiredRegularKeys: []string{"message_payload", "message_encoding", "last_message_id", "version"},
			requiredStaticKeys:  []string{"migration_authority", "migration_generation", "message_bucket_size"},
		},
		{
			name:                "legacy_queue_v2_delete_ranges",
			partitionKeys:       []string{"queue_type"},
			clusteringKeys:      []string{"deletion_id"},
			requiredRegularKeys: []string{"first_message_id", "last_message_id"},
			requiredStaticKeys:  []string{"migration_authority", "migration_generation", "message_bucket_size"},
		},
	}
	if mode != config.CassandraLegacyQueueMigrationModeTargetOnly {
		tables = append(tables, struct {
			name                string
			partitionKeys       []string
			clusteringKeys      []string
			requiredRegularKeys []string
			requiredStaticKeys  []string
		}{
			name:                "queue",
			partitionKeys:       []string{"queue_type"},
			clusteringKeys:      []string{"message_id"},
			requiredRegularKeys: []string{"migration_authority", "migration_generation", "message_bucket_size"},
		})
	}
	for _, table := range tables {
		if err := validateLegacyQueueV2Table(ctx, session, keyspace, table); err != nil {
			return err
		}
	}
	return nil
}

func validateLegacyQueueV2Table(
	ctx context.Context,
	session cgocql.Session,
	keyspace string,
	table struct {
		name                string
		partitionKeys       []string
		clusteringKeys      []string
		requiredRegularKeys []string
		requiredStaticKeys  []string
	},
) error {
	iter := session.Query(templateGetLegacyQueueV2SchemaColumns, keyspace, table.name).WithContext(ctx).Iter()
	var partitionKeys []legacyQueueV2SchemaColumn
	var clusteringKeys []legacyQueueV2SchemaColumn
	regularKeys := make(map[string]string)
	for {
		var column legacyQueueV2SchemaColumn
		if !iter.Scan(&column.name, &column.kind, &column.position) {
			break
		}
		switch column.kind {
		case "partition_key":
			partitionKeys = append(partitionKeys, column)
		case "clustering":
			clusteringKeys = append(clusteringKeys, column)
		case "regular", "static":
			regularKeys[column.name] = column.kind
		default:
			continue
		}
	}
	if err := iter.Close(); err != nil {
		return cgocql.ConvertError("GetLegacyQueueV2SchemaColumns", err)
	}
	sort.Slice(partitionKeys, func(i, j int) bool { return partitionKeys[i].position < partitionKeys[j].position })
	sort.Slice(clusteringKeys, func(i, j int) bool { return clusteringKeys[i].position < clusteringKeys[j].position })
	if !legacyQueueV2SchemaColumnsEqual(partitionKeys, table.partitionKeys) ||
		!legacyQueueV2SchemaColumnsEqual(clusteringKeys, table.clusteringKeys) {
		return fmt.Errorf("cassandra table %s.%s has incompatible primary key", keyspace, table.name)
	}
	for _, column := range table.requiredRegularKeys {
		if kind, ok := regularKeys[column]; !ok || kind != "regular" {
			return fmt.Errorf("cassandra table %s.%s is missing required column %q", keyspace, table.name, column)
		}
	}
	for _, column := range table.requiredStaticKeys {
		if kind, ok := regularKeys[column]; !ok || kind != "static" {
			return fmt.Errorf("cassandra table %s.%s is missing required static column %q", keyspace, table.name, column)
		}
	}
	return nil
}

func legacyQueueV2SchemaColumnsEqual(columns []legacyQueueV2SchemaColumn, expected []string) bool {
	if len(columns) != len(expected) {
		return false
	}
	for index, column := range columns {
		if column.name != expected[index] || column.position != index {
			return false
		}
	}
	return true
}
