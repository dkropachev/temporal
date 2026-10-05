package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const templateGetQueueV2SchemaColumns = `SELECT column_name, kind, position FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?`

// ValidateQueueV2MigrationModeSchema verifies the bucketed metadata table before it receives traffic.
func ValidateQueueV2MigrationModeSchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	mode config.CassandraQueueV2MigrationMode,
) error {
	mode = normalizeQueueV2MigrationMode(mode)
	if err := ValidateQueueV2MigrationMode(mode); err != nil {
		return err
	}
	if mode == config.CassandraQueueV2MigrationModeSourceOnly {
		return nil
	}
	if keyspace == "" {
		return errors.New("queueV2 schema validation requires a keyspace")
	}

	type column struct {
		name     string
		kind     string
		position int
	}
	iter := session.Query(templateGetQueueV2SchemaColumns, keyspace, "queues_v2").WithContext(ctx).Iter()
	var partitionKeys []column
	var clusteringKeys []column
	regular := make(map[string]struct{})
	for {
		var current column
		if !iter.Scan(&current.name, &current.kind, &current.position) {
			break
		}
		switch current.kind {
		case "partition_key":
			partitionKeys = append(partitionKeys, current)
		case "clustering":
			clusteringKeys = append(clusteringKeys, current)
		case "regular", "static":
			regular[current.name] = struct{}{}
		default:
		}
	}
	if err := iter.Close(); err != nil {
		return gocql.ConvertError("ValidateQueueV2MigrationSchema", err)
	}
	sort.Slice(partitionKeys, func(i, j int) bool { return partitionKeys[i].position < partitionKeys[j].position })
	sort.Slice(clusteringKeys, func(i, j int) bool { return clusteringKeys[i].position < clusteringKeys[j].position })
	if len(partitionKeys) != 2 || partitionKeys[0].name != "queue_type" || partitionKeys[1].name != "metadata_bucket" ||
		len(clusteringKeys) != 1 || clusteringKeys[0].name != "queue_name" {
		return fmt.Errorf("cassandra table %s.queues_v2 has incompatible primary key", keyspace)
	}
	for _, required := range []string{"metadata_payload", "metadata_encoding", "version"} {
		if _, ok := regular[required]; !ok {
			return fmt.Errorf("cassandra table %s.queues_v2 is missing required column %s", keyspace, required)
		}
	}
	for _, required := range []string{
		"migration_authority",
		"migration_generation",
		"migration_epoch",
		"message_bucket_span",
	} {
		if _, ok := regular[required]; !ok {
			return fmt.Errorf("cassandra table %s.queues_v2 is missing required column %s", keyspace, required)
		}
	}
	if mode != config.CassandraQueueV2MigrationModeTargetOnly {
		if err := validateQueueV2SourceAuthoritySchema(ctx, session, keyspace); err != nil {
			return err
		}
	}
	return validateQueueV2MessagesV3Schema(ctx, session, keyspace)
}

func validateQueueV2SourceAuthoritySchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
) error {
	tables := []struct {
		name     string
		required map[string]string
	}{
		{
			name: "queues",
			required: map[string]string{
				"migration_authority":  "regular",
				"migration_generation": "regular",
				"migration_epoch":      "regular",
				"message_bucket_span":  "regular",
			},
		},
		{
			name: "queue_messages",
			required: map[string]string{
				"migration_authority":  "static",
				"migration_generation": "static",
				"migration_epoch":      "static",
				"bucket_span":          "static",
			},
		},
	}
	for _, table := range tables {
		columns := make(map[string]string)
		iter := session.Query(templateGetQueueV2SchemaColumns, keyspace, table.name).WithContext(ctx).Iter()
		for {
			var name string
			var kind string
			var position int
			if !iter.Scan(&name, &kind, &position) {
				break
			}
			columns[name] = kind
		}
		if err := iter.Close(); err != nil {
			return gocql.ConvertError("ValidateQueueV2SourceAuthoritySchema", err)
		}
		for name, kind := range table.required {
			if columns[name] != kind {
				return fmt.Errorf(
					"cassandra table %s.%s requires %s column %s",
					keyspace,
					table.name,
					kind,
					name,
				)
			}
		}
	}
	return nil
}

func validateQueueV2MessagesV3Schema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
) error {
	type column struct {
		name     string
		kind     string
		position int
	}
	iter := session.Query(templateGetQueueV2SchemaColumns, keyspace, "queue_messages_v3").WithContext(ctx).Iter()
	var partitionKeys []column
	var clusteringKeys []column
	regular := make(map[string]struct{})
	for {
		var current column
		if !iter.Scan(&current.name, &current.kind, &current.position) {
			break
		}
		switch current.kind {
		case "partition_key":
			partitionKeys = append(partitionKeys, current)
		case "clustering":
			clusteringKeys = append(clusteringKeys, current)
		case "regular", "static":
			regular[current.name] = struct{}{}
		default:
		}
	}
	if err := iter.Close(); err != nil {
		return gocql.ConvertError("ValidateQueueV2MessagesV3Schema", err)
	}
	sort.Slice(partitionKeys, func(i, j int) bool { return partitionKeys[i].position < partitionKeys[j].position })
	sort.Slice(clusteringKeys, func(i, j int) bool { return clusteringKeys[i].position < clusteringKeys[j].position })
	if len(partitionKeys) != 3 || partitionKeys[0].name != "queue_type" ||
		partitionKeys[1].name != "queue_name" || partitionKeys[2].name != "message_bucket" ||
		len(clusteringKeys) != 2 || clusteringKeys[0].name != "row_type" || clusteringKeys[1].name != "message_id" {
		return fmt.Errorf("cassandra table %s.queue_messages_v3 has incompatible primary key", keyspace)
	}
	for _, required := range []string{
		"message_payload",
		"message_encoding",
		"active_message_bucket",
		"bucket_span",
		"last_message_id",
		"version",
		"migration_authority",
		"migration_generation",
		"migration_epoch",
	} {
		if _, ok := regular[required]; !ok {
			return fmt.Errorf("cassandra table %s.queue_messages_v3 is missing required column %s", keyspace, required)
		}
	}
	return nil
}
