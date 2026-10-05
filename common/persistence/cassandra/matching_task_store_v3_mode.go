package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	cgocql "github.com/gocql/gocql"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	DefaultMatchingTaskStorageBucketCount = 16
	matchingTaskV3TableName               = "tasks_v3"
	matchingTaskV3FairTableName           = "tasks_v3_fair"
)

type MatchingTaskMigrationMode = config.CassandraMatchingTaskMigrationMode

const (
	MatchingTaskMigrationModeSourceOnly = config.CassandraMatchingTaskMigrationModeSourceOnly
	MatchingTaskMigrationModeSourceDual = config.CassandraMatchingTaskMigrationModeSourceDual
	MatchingTaskMigrationModeTargetDual = config.CassandraMatchingTaskMigrationModeTargetDual
	MatchingTaskMigrationModeTargetOnly = config.CassandraMatchingTaskMigrationModeTargetOnly
)

func normalizeMatchingTaskMigrationMode(mode MatchingTaskMigrationMode) MatchingTaskMigrationMode {
	if mode == "" {
		return MatchingTaskMigrationModeSourceOnly
	}
	return mode
}

func ValidateMatchingTaskMigrationMode(mode MatchingTaskMigrationMode) error {
	switch normalizeMatchingTaskMigrationMode(mode) {
	case MatchingTaskMigrationModeSourceOnly,
		MatchingTaskMigrationModeSourceDual,
		MatchingTaskMigrationModeTargetDual,
		MatchingTaskMigrationModeTargetOnly:
		return nil
	default:
		return fmt.Errorf("unsupported cassandra matching task migration mode %q", mode)
	}
}

func validateMatchingTaskStorageBucketCount(bucketCount int) error {
	if bucketCount < 1 || bucketCount > 256 {
		return fmt.Errorf("cassandra matching task storage bucket count must be between 1 and 256, got %d", bucketCount)
	}
	return nil
}

func matchingTaskStorageBucket(rangeID int64, bucketCount int) (int16, error) {
	if rangeID < 1 {
		return 0, fmt.Errorf("cassandra matching task range ID must be positive, got %d", rangeID)
	}
	if err := validateMatchingTaskStorageBucketCount(bucketCount); err != nil {
		return 0, err
	}
	return int16((rangeID - 1) % int64(bucketCount)), nil
}

// NewMatchingTaskStoreWithMigrations creates independently selectable task and user-data layouts.
func NewMatchingTaskStoreWithMigrations(
	session gocql.Session,
	logger log.Logger,
	enableFairness bool,
	userDataMode TaskQueueUserDataMigrationMode,
	userDataBucketCount int,
	taskMode MatchingTaskMigrationMode,
	taskBucketCount int,
	userDataGeneration ...cgocql.UUID,
) (p.TaskStore, error) {
	if err := ValidateMatchingTaskMigrationMode(taskMode); err != nil {
		return nil, err
	}
	if taskBucketCount == 0 {
		taskBucketCount = DefaultMatchingTaskStorageBucketCount
	}
	if err := validateMatchingTaskStorageBucketCount(taskBucketCount); err != nil {
		return nil, err
	}

	source, err := NewMatchingTaskStoreWithUserDataMigration(
		session,
		logger,
		enableFairness,
		userDataMode,
		userDataBucketCount,
		userDataGeneration...,
	)
	if err != nil {
		return nil, err
	}
	mode := normalizeMatchingTaskMigrationMode(taskMode)
	if mode == MatchingTaskMigrationModeSourceOnly {
		return source, nil
	}
	sourceAuthority := matchingTaskAuthorityTarget
	if mode == MatchingTaskMigrationModeSourceDual {
		sourceAuthority = matchingTaskAuthoritySource
	}
	sourceQueue, err := configureMatchingTaskSourceMigration(source, sourceAuthority, taskBucketCount)
	if err != nil {
		return nil, err
	}

	target := newMatchingTaskStoreV3(session, enableFairness, taskBucketCount)
	if mode == MatchingTaskMigrationModeSourceDual {
		target.authority = matchingTaskAuthoritySource
	}
	target.enforceAuthority = mode == MatchingTaskMigrationModeTargetOnly
	return &matchingTaskMigrationStore{
		TaskStore:   source,
		sourceQueue: sourceQueue,
		target:      target,
		mode:        mode,
	}, nil
}

const templateGetMatchingTaskV3Schema = `SELECT column_name, kind, position, clustering_order, type ` +
	`FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?`

// ValidateMatchingTaskMigrationModeSchema rejects target modes before their table exists.
func ValidateMatchingTaskMigrationModeSchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	mode MatchingTaskMigrationMode,
	enableFairness bool,
) error {
	mode = normalizeMatchingTaskMigrationMode(mode)
	if err := ValidateMatchingTaskMigrationMode(mode); err != nil {
		return err
	}
	if mode == MatchingTaskMigrationModeSourceOnly {
		return nil
	}
	if keyspace == "" {
		return errors.New("cassandra matching task schema validation requires a keyspace")
	}
	if mode != MatchingTaskMigrationModeTargetOnly {
		sourceTable := "tasks"
		if enableFairness {
			sourceTable = "tasks_v2"
		}
		_, _, sourceColumns, err := getMatchingTaskV3Schema(ctx, session, keyspace, sourceTable)
		if err != nil {
			return err
		}
		for column, expectedType := range map[string]string{
			"migration_authority":    "int",
			"migration_bucket_count": "smallint",
			"migration_timestamp":    "bigint",
		} {
			if sourceColumns[column] != expectedType {
				return fmt.Errorf(
					"cassandra matching task source table %s.%s requires column %s %s, got %q",
					keyspace,
					sourceTable,
					column,
					expectedType,
					sourceColumns[column],
				)
			}
		}
	}
	table := matchingTaskV3TableName
	if enableFairness {
		table = matchingTaskV3FairTableName
	}
	partitionKeys, clusteringKeys, columns, err := getMatchingTaskV3Schema(ctx, session, keyspace, table)
	if err != nil {
		return err
	}
	if len(partitionKeys) == 0 && len(clusteringKeys) == 0 {
		return fmt.Errorf("cassandra matching task migration mode %q requires table %s.%s", mode, keyspace, table)
	}
	expectedClustering := []string{"type", "task_id"}
	if enableFairness {
		expectedClustering = []string{"type", "pass", "task_id"}
	}
	if !matchingTaskV3KeyColumnsEqual(
		partitionKeys,
		[]string{"namespace_id", "task_queue_name", "task_queue_type", "storage_bucket"},
	) || !matchingTaskV3KeyColumnsEqual(clusteringKeys, expectedClustering) {
		return fmt.Errorf("cassandra matching task table %s.%s has an incompatible primary key", keyspace, table)
	}
	for column, expectedType := range map[string]string{
		"bucket_count":        "smallint",
		"migration_authority": "int",
	} {
		if columns[column] != expectedType {
			return fmt.Errorf(
				"cassandra matching task table %s.%s requires column %s %s, got %q",
				keyspace,
				table,
				column,
				expectedType,
				columns[column],
			)
		}
	}
	return nil
}

func getMatchingTaskV3Schema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	table string,
) (partitionKeys []historyNodeKeyColumn, clusteringKeys []historyNodeKeyColumn, columns map[string]string, err error) {
	iter := session.Query(templateGetMatchingTaskV3Schema, keyspace, table).WithContext(ctx).Iter()
	columns = make(map[string]string)
	for {
		var name string
		var kind string
		var position int
		var order string
		var columnType string
		if !iter.Scan(&name, &kind, &position, &order, &columnType) {
			break
		}
		columns[name] = columnType
		column := historyNodeKeyColumn{name: name, position: position, clusteringOrder: strings.ToLower(order)}
		switch kind {
		case "partition_key":
			partitionKeys = append(partitionKeys, column)
		case "clustering":
			clusteringKeys = append(clusteringKeys, column)
		default:
			continue
		}
	}
	if err := iter.Close(); err != nil {
		return nil, nil, nil, fmt.Errorf("read cassandra matching task schema for %s.%s: %w", keyspace, table, err)
	}
	sort.Slice(partitionKeys, func(i, j int) bool { return partitionKeys[i].position < partitionKeys[j].position })
	sort.Slice(clusteringKeys, func(i, j int) bool { return clusteringKeys[i].position < clusteringKeys[j].position })
	return partitionKeys, clusteringKeys, columns, nil
}

func matchingTaskV3KeyColumnsEqual(columns []historyNodeKeyColumn, expected []string) bool {
	if len(columns) != len(expected) {
		return false
	}
	for index, name := range expected {
		if columns[index].name != name || columns[index].position != index {
			return false
		}
		if len(columns) == 2 || len(columns) == 3 {
			if columns[index].clusteringOrder != "asc" {
				return false
			}
		}
	}
	return true
}
