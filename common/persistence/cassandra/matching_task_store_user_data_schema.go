package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	taskQueueUserDataV1TableName    = "task_queue_user_data"
	taskQueueUserDataV2TableName    = "task_queue_user_data_v2"
	taskQueueUserDataV2TxnTableName = "task_queue_user_data_v2_txn"

	templateGetTaskQueueUserDataV2SchemaColumns = `SELECT column_name, kind, position FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ?`
)

type taskQueueUserDataV2SchemaColumn struct {
	name     string
	kind     string
	position int
}

func ValidateTaskQueueUserDataMigrationModeSchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	mode TaskQueueUserDataMigrationMode,
) error {
	if err := ValidateTaskQueueUserDataMigrationMode(mode); err != nil {
		return err
	}
	if keyspace == "" {
		return errors.New("task queue user data schema validation requires a keyspace")
	}
	if normalizeTaskQueueUserDataMigrationMode(mode) != TaskQueueUserDataMigrationModeTargetOnly {
		if err := validateTaskQueueUserDataV2Table(
			ctx,
			session,
			keyspace,
			taskQueueUserDataV1TableName,
			[]string{"namespace_id"},
			[]string{"build_id", "task_queue_name"},
			[]string{
				"data", "data_encoding", "version", "migration_authority", "migration_bucket_count",
				"migration_generation",
			},
		); err != nil {
			return err
		}
	}
	if normalizeTaskQueueUserDataMigrationMode(mode) == TaskQueueUserDataMigrationModeSourceOnly {
		return nil
	}
	if err := validateTaskQueueUserDataV2Table(
		ctx,
		session,
		keyspace,
		taskQueueUserDataV2TableName,
		[]string{"namespace_id", "bucket_id"},
		[]string{"build_id", "task_queue_name"},
		[]string{
			"data", "data_encoding", "version", "present", "pending_txn_id", "pending_data",
			"pending_data_encoding", "pending_version", "pending_present",
		},
	); err != nil {
		return err
	}
	return validateTaskQueueUserDataV2Table(
		ctx,
		session,
		keyspace,
		taskQueueUserDataV2TxnTableName,
		[]string{"namespace_id"},
		[]string{"txn_id"},
		[]string{
			"state", "expires_at", "active_txn_id", "epoch", "migration_authority",
			"migration_bucket_count", "migration_generation",
		},
	)
}

func validateTaskQueueUserDataV2Table(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	table string,
	expectedPartitionKeys []string,
	expectedClusteringKeys []string,
	requiredRegularColumns []string,
) error {
	iter := session.Query(
		templateGetTaskQueueUserDataV2SchemaColumns,
		keyspace,
		table,
	).WithContext(ctx).Iter()
	var partitionKeys []taskQueueUserDataV2SchemaColumn
	var clusteringKeys []taskQueueUserDataV2SchemaColumn
	regularColumns := make(map[string]struct{})
	for {
		var column taskQueueUserDataV2SchemaColumn
		if !iter.Scan(&column.name, &column.kind, &column.position) {
			break
		}
		switch column.kind {
		case "partition_key":
			partitionKeys = append(partitionKeys, column)
		case "clustering":
			clusteringKeys = append(clusteringKeys, column)
		case "regular", "static":
			regularColumns[column.name] = struct{}{}
		default:
			continue
		}
	}
	if err := iter.Close(); err != nil {
		return gocql.ConvertError("ValidateTaskQueueUserDataV2Schema", err)
	}
	sort.Slice(partitionKeys, func(i, j int) bool { return partitionKeys[i].position < partitionKeys[j].position })
	sort.Slice(clusteringKeys, func(i, j int) bool { return clusteringKeys[i].position < clusteringKeys[j].position })
	if !taskQueueUserDataV2ColumnsEqual(partitionKeys, expectedPartitionKeys) ||
		!taskQueueUserDataV2ColumnsEqual(clusteringKeys, expectedClusteringKeys) {
		return fmt.Errorf(
			"cassandra table %s.%s has incompatible primary key: partition=%v clustering=%v",
			keyspace,
			table,
			taskQueueUserDataV2ColumnNames(partitionKeys),
			taskQueueUserDataV2ColumnNames(clusteringKeys),
		)
	}
	for _, column := range requiredRegularColumns {
		if _, ok := regularColumns[column]; !ok {
			return fmt.Errorf("cassandra table %s.%s is missing required column %s", keyspace, table, column)
		}
	}
	return nil
}

func taskQueueUserDataV2ColumnsEqual(columns []taskQueueUserDataV2SchemaColumn, expected []string) bool {
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

func taskQueueUserDataV2ColumnNames(columns []taskQueueUserDataV2SchemaColumn) []string {
	names := make([]string, len(columns))
	for index, column := range columns {
		names[index] = column.name
	}
	return names
}
