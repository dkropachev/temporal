package cassandra

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type executionShardAuthority int

const (
	executionShardAuthorityUnspecified executionShardAuthority = iota
	executionShardAuthoritySource
	executionShardAuthoritySealing
	executionShardAuthorityTarget
)

const (
	templateGetExecutionShardAuthority = `SELECT range_id, migration_authority, storage_bucket_count ` +
		`FROM executions WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? ` +
		`AND run_id = ? AND visibility_ts = ? AND task_id = ?`
	templateInitializeExecutionShardAuthority = `UPDATE executions ` +
		`SET migration_authority = ?, storage_bucket_count = ? ` +
		`WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? ` +
		`AND run_id = ? AND visibility_ts = ? AND task_id = ? ` +
		`IF migration_authority = null AND storage_bucket_count = null`
	templateGuardExecutionShardAuthority = `UPDATE executions SET migration_authority = ? ` +
		`WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? ` +
		`AND run_id = ? AND visibility_ts = ? AND task_id = ? ` +
		`IF migration_authority = ? AND storage_bucket_count = ?`
	templateSealExecutionSourceShard = `UPDATE executions SET range_id = ?, migration_authority = ? ` +
		`WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? ` +
		`AND run_id = ? AND visibility_ts = ? AND task_id = ? ` +
		`IF range_id = ? AND migration_authority = ? AND storage_bucket_count = ?`
	templateSealExecutionTargetShard = `UPDATE executions SET migration_authority = ? ` +
		`WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? ` +
		`AND run_id = ? AND visibility_ts = ? AND task_id = ? ` +
		`IF migration_authority = ? AND storage_bucket_count = ?`
	templateActivateExecutionShard = `UPDATE executions ` +
		`SET shard = ?, shard_encoding = ?, range_id = ?, migration_authority = ? ` +
		`WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? ` +
		`AND run_id = ? AND visibility_ts = ? AND task_id = ? ` +
		`IF range_id = ? AND migration_authority = ? AND storage_bucket_count = ?`
)

type executionShardAuthorityRecord struct {
	rangeID     int64
	authority   executionShardAuthority
	bucketCount int32
}

func (a executionShardAuthority) String() string {
	switch a {
	case executionShardAuthoritySource:
		return "source"
	case executionShardAuthoritySealing:
		return "sealing"
	case executionShardAuthorityTarget:
		return "target"
	default:
		return fmt.Sprintf("unknown(%d)", a)
	}
}

func executionShardRowKey(partition int32) []any {
	return []any{
		partition,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
	}
}

func (b *executionBatch) addShardAuthorityGuard(partition int32) {
	if !b.layout.requiresAuthority() {
		return
	}
	args := []any{int(b.layout.authority)}
	args = append(args, executionShardRowKey(partition)...)
	args = append(args, int(b.layout.authority), int(b.layout.buckets))
	b.Query(templateGuardExecutionShardAuthority, args...)
}

func readExecutionShardAuthority(
	ctx context.Context,
	session gocql.Session,
	layout executionLayout,
	partition int32,
) (executionShardAuthorityRecord, error) {
	var (
		rangeID     int64
		authority   *int
		bucketCount *int
	)
	err := session.Query(
		layout.query(templateGetExecutionShardAuthority),
		executionShardRowKey(partition)...,
	).WithContext(ctx).Scan(&rangeID, &authority, &bucketCount)
	if err != nil {
		return executionShardAuthorityRecord{}, err
	}
	if authority == nil || bucketCount == nil {
		return executionShardAuthorityRecord{rangeID: rangeID}, nil
	}
	return executionShardAuthorityRecord{
		rangeID:     rangeID,
		authority:   executionShardAuthority(*authority),
		bucketCount: int32(*bucketCount),
	}, nil
}

func validateExecutionShardAuthority(
	logicalShardID int32,
	partition int32,
	layout executionLayout,
	record executionShardAuthorityRecord,
) error {
	if record.authority != layout.authority || record.bucketCount != layout.buckets {
		return &p.ShardOwnershipLostError{
			ShardID: logicalShardID,
			Msg: fmt.Sprintf(
				"execution storage partition %d requires authority %s and %d buckets; persisted authority is %s with %d buckets",
				partition,
				layout.authority,
				layout.buckets,
				record.authority,
				record.bucketCount,
			),
		}
	}
	return nil
}

func initializeExecutionShardAuthority(
	ctx context.Context,
	session gocql.Session,
	layout executionLayout,
	logicalShardID int32,
	partition int32,
) error {
	args := []any{int(layout.authority), int(layout.buckets)}
	args = append(args, executionShardRowKey(partition)...)
	applied, err := session.Query(
		layout.query(templateInitializeExecutionShardAuthority),
		args...,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("InitializeExecutionShardAuthority", err)
	}
	if !applied {
		record, readErr := readExecutionShardAuthority(ctx, session, layout, partition)
		if readErr != nil {
			return gocql.ConvertError("ReadExecutionShardAuthority", readErr)
		}
		return validateExecutionShardAuthority(logicalShardID, partition, layout, record)
	}
	return nil
}

func ensureExecutionShardAuthority(
	ctx context.Context,
	session gocql.Session,
	layout executionLayout,
	logicalShardID int32,
	partition int32,
	record executionShardAuthorityRecord,
) error {
	if !layout.requiresAuthority() {
		return nil
	}
	if record.authority == executionShardAuthorityUnspecified && record.bucketCount == 0 {
		return initializeExecutionShardAuthority(ctx, session, layout, logicalShardID, partition)
	}
	return validateExecutionShardAuthority(logicalShardID, partition, layout, record)
}

func resolveTargetDualExecutionLayout(
	ctx context.Context,
	session gocql.Session,
	layout executionLayout,
	logicalShardID int32,
) (executionLayout, error) {
	if layout.mode != config.CassandraExecutionMigrationModeTargetDual {
		return layout, nil
	}
	sourceLayout := executionLayout{
		mode:    config.CassandraExecutionMigrationModeLegacy,
		buckets: layout.buckets,
	}
	record, err := readExecutionShardAuthority(ctx, session, sourceLayout, logicalShardID)
	if gocql.IsNotFoundError(err) {
		return executionLayout{
			mode:      config.CassandraExecutionMigrationModeSourceDual,
			buckets:   layout.buckets,
			authority: executionShardAuthoritySource,
		}, nil
	}
	if err != nil {
		return executionLayout{}, gocql.ConvertError("ResolveExecutionShardAuthority", err)
	}
	if record.bucketCount != layout.buckets {
		return executionLayout{}, fmt.Errorf(
			"execution shard %d has persisted bucket count %d; configured %d",
			logicalShardID,
			record.bucketCount,
			layout.buckets,
		)
	}
	switch record.authority {
	case executionShardAuthoritySource, executionShardAuthoritySealing:
		return executionLayout{
			mode:      config.CassandraExecutionMigrationModeSourceDual,
			buckets:   layout.buckets,
			authority: executionShardAuthoritySource,
		}, nil
	case executionShardAuthorityTarget:
		return layout, nil
	default:
		return executionLayout{}, fmt.Errorf(
			"execution shard %d has invalid authority %s",
			logicalShardID,
			record.authority,
		)
	}
}

func executeGuardedExecutionMutation(
	ctx context.Context,
	session gocql.Session,
	layout executionLayout,
	logicalShardID int32,
	partition int32,
	operation string,
	query string,
	args []any,
	nonAuthorityConditionIsNoop bool,
) error {
	if !layout.requiresAuthority() {
		return gocql.ConvertError(
			operation,
			session.Query(layout.query(query), args...).WithContext(ctx).Exec(),
		)
	}

	batch := newExecutionBatch(session.NewBatch(gocql.LoggedBatch).WithContext(ctx), layout)
	batch.Query(query, args...)
	batch.addShardAuthorityGuard(partition)
	previous := make(map[string]any)
	applied, iter, err := session.MapExecuteBatchCAS(batch.Batch, previous)
	if err != nil {
		return gocql.ConvertError(operation, err)
	}
	if iter != nil {
		defer func() {
			_ = iter.Close()
		}()
	}
	if applied {
		return nil
	}

	record, err := readExecutionShardAuthority(ctx, session, layout, partition)
	if err != nil {
		return gocql.ConvertError("ReadExecutionShardAuthorityAfterConflict", err)
	}
	if authorityErr := validateExecutionShardAuthority(logicalShardID, partition, layout, record); authorityErr != nil {
		return authorityErr
	}
	if nonAuthorityConditionIsNoop {
		return nil
	}
	return fmt.Errorf("%s conditional batch failed while execution shard authority remained valid", operation)
}
