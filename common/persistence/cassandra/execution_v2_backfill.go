package cassandra

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync/atomic"

	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultExecutionBackfillTokenRangeCount = 4096

	templateScanExecutionsForV2Backfill = `SELECT JSON ` +
		`shard_id, type, namespace_id, workflow_id, run_id, current_run_id, visibility_ts, task_id, ` +
		`shard, shard_encoding, execution, execution_encoding, execution_state, execution_state_encoding, ` +
		`transfer, transfer_encoding, replication, replication_encoding, timer, timer_encoding, ` +
		`visibility_task_data, visibility_task_encoding, task_data, task_encoding, next_event_id, range_id, ` +
		`activity_map, activity_map_encoding, timer_map, timer_map_encoding, child_executions_map, ` +
		`child_executions_map_encoding, request_cancel_map, request_cancel_map_encoding, signal_map, ` +
		`signal_map_encoding, signal_requested, chasm_node_map, chasm_node_map_encoding, buffered_events_list, ` +
		`workflow_last_write_version, workflow_state, checksum, checksum_encoding, db_record_version, ` +
		`writetime(shard) AS migration_wt_shard, writetime(range_id) AS migration_wt_range_id, ` +
		`writetime(execution) AS migration_wt_execution, writetime(db_record_version) AS migration_wt_db_record_version, ` +
		`writetime(current_run_id) AS migration_wt_current, writetime(transfer) AS migration_wt_transfer, ` +
		`writetime(replication) AS migration_wt_replication, writetime(timer) AS migration_wt_timer, ` +
		`writetime(visibility_task_data) AS migration_wt_visibility, writetime(task_data) AS migration_wt_task ` +
		`FROM executions WHERE token(shard_id) >= ? AND token(shard_id) <= ?`
	templateBackfillExecutionV2 = `INSERT INTO executions_v2 JSON ? DEFAULT UNSET USING TIMESTAMP ?`
)

var executionBackfillWriteTimeFields = []string{
	"migration_wt_shard",
	"migration_wt_range_id",
	"migration_wt_execution",
	"migration_wt_db_record_version",
	"migration_wt_current",
	"migration_wt_transfer",
	"migration_wt_replication",
	"migration_wt_timer",
	"migration_wt_visibility",
	"migration_wt_task",
}

type ExecutionBackfillOptions struct {
	PageSize        int
	Concurrency     int
	TokenRangeCount int
	StorageBuckets  int
	Partitioner     string
}

type executionBackfillRow struct {
	data      []byte
	timestamp int64
}

func BackfillExecutionsV2(
	ctx context.Context,
	session gocql.Session,
	options ExecutionBackfillOptions,
) (int64, error) {
	if err := validateExecutionBackfillOptions(options); err != nil {
		return 0, err
	}
	rangeCount := options.TokenRangeCount
	if rangeCount == 0 {
		rangeCount = DefaultExecutionBackfillTokenRangeCount
	}
	ranges, err := HistoryNodeBackfillTokenRanges(rangeCount)
	if err != nil {
		return 0, err
	}
	if err := validateHistoryNodeBackfillPartitioner(ctx, session, options.Partitioner); err != nil {
		return 0, err
	}
	var copied int64
	for _, tokenRange := range ranges {
		rangeCopied, err := BackfillExecutionsV2Range(ctx, session, options, tokenRange)
		copied += rangeCopied
		if err != nil {
			return copied, fmt.Errorf(
				"backfill executions token range %d [%d, %d]: %w",
				tokenRange.Index,
				tokenRange.StartToken,
				tokenRange.EndToken,
				err,
			)
		}
	}
	return copied, nil
}

func BackfillExecutionsV2Range(
	ctx context.Context,
	session gocql.Session,
	options ExecutionBackfillOptions,
	tokenRange HistoryNodeBackfillTokenRange,
) (int64, error) {
	if err := validateExecutionBackfillOptions(options); err != nil {
		return 0, err
	}
	if tokenRange.Index < 0 || tokenRange.StartToken > tokenRange.EndToken {
		return 0, fmt.Errorf("invalid executions backfill token range: %+v", tokenRange)
	}
	layout, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, options.StorageBuckets)
	if err != nil {
		return 0, err
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(options.Concurrency)
	iter := session.Query(
		templateScanExecutionsForV2Backfill,
		tokenRange.StartToken,
		tokenRange.EndToken,
	).WithContext(groupCtx).PageSize(options.PageSize).Iter()
	var copied atomic.Int64
	for groupCtx.Err() == nil {
		var sourceJSON string
		if !iter.Scan(&sourceJSON) {
			break
		}
		targetRows, err := executionBackfillTargetRows([]byte(sourceJSON), layout)
		if err != nil {
			_ = iter.Close()
			return copied.Load(), err
		}
		for _, targetRow := range targetRows {
			targetRow := targetRow
			group.Go(func() error {
				if err := session.Query(
					templateBackfillExecutionV2,
					string(targetRow.data),
					targetRow.timestamp,
				).WithContext(groupCtx).Idempotent(true).Exec(); err != nil {
					return fmt.Errorf("backfill executions_v2 row: %w", err)
				}
				copied.Add(1)
				return nil
			})
		}
	}
	closeErr := iter.Close()
	groupErr := group.Wait()
	if groupErr != nil {
		return copied.Load(), groupErr
	}
	if closeErr != nil {
		return copied.Load(), gocql.ConvertError("ScanExecutionsForV2Backfill", closeErr)
	}
	return copied.Load(), nil
}

func executionBackfillTargetRows(sourceJSON []byte, layout executionLayout) ([]executionBackfillRow, error) {
	decoder := json.NewDecoder(bytes.NewReader(sourceJSON))
	decoder.UseNumber()
	row := make(map[string]any)
	if err := decoder.Decode(&row); err != nil {
		return nil, fmt.Errorf("decode executions backfill row: %w", err)
	}
	logicalShardID, err := executionBackfillInt32(row, "shard_id")
	if err != nil {
		return nil, err
	}
	rowType, err := executionBackfillInt(row, "type")
	if err != nil {
		return nil, err
	}
	writeTimestamp := int64(0)
	for _, field := range executionBackfillWriteTimeFields {
		value, ok := row[field]
		delete(row, field)
		if !ok || value == nil {
			continue
		}
		number, ok := value.(json.Number)
		if !ok {
			return nil, fmt.Errorf("executions backfill field %s has type %T", field, value)
		}
		timestamp, err := number.Int64()
		if err != nil {
			return nil, fmt.Errorf("parse executions backfill field %s: %w", field, err)
		}
		writeTimestamp = max(writeTimestamp, timestamp)
	}
	if writeTimestamp == 0 {
		return nil, fmt.Errorf("executions backfill row type %d has no source write timestamp", rowType)
	}

	partitions, err := executionBackfillTargetPartitions(row, rowType, logicalShardID, layout)
	if err != nil {
		return nil, err
	}

	results := make([]executionBackfillRow, 0, len(partitions))
	for _, partition := range partitions {
		row["shard_id"] = partition
		encoded, err := json.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("encode executions_v2 backfill row: %w", err)
		}
		results = append(results, executionBackfillRow{data: encoded, timestamp: writeTimestamp})
	}
	return results, nil
}

func executionBackfillTargetPartitions(
	row map[string]any,
	rowType int,
	logicalShardID int32,
	layout executionLayout,
) ([]int32, error) {
	if rowType == rowTypeShard {
		return layout.partitions(logicalShardID)
	}
	var partition int32
	var err error
	switch rowType {
	case rowTypeExecution:
		partition, err = layout.workflowPartition(
			logicalShardID,
			executionBackfillString(row, "namespace_id"),
			executionBackfillString(row, "workflow_id"),
		)
	case rowTypeDLQ:
		if executionBackfillString(row, "namespace_id") == rowTypeDLQNamespaceID {
			partition, err = layout.workflowPartition(
				logicalShardID,
				rowTypeDLQNamespaceID,
				executionBackfillString(row, "workflow_id"),
			)
		} else {
			partition, err = executionBackfillTaskPartition(row, rowType, logicalShardID, layout)
		}
	default:
		partition, err = executionBackfillTaskPartition(row, rowType, logicalShardID, layout)
	}
	if err != nil {
		return nil, err
	}
	return []int32{partition}, nil
}

func executionBackfillTaskPartition(
	row map[string]any,
	rowType int,
	logicalShardID int32,
	layout executionLayout,
) (int32, error) {
	category, dataField, encodingField, err := executionBackfillTaskCategory(
		rowType,
		executionBackfillString(row, "namespace_id"),
	)
	if err != nil {
		return 0, err
	}
	encoded := executionBackfillString(row, dataField)
	if len(encoded) < 2 || encoded[:2] != "0x" {
		return 0, fmt.Errorf("executions backfill task row type %d has invalid %s blob", rowType, dataField)
	}
	data, err := hex.DecodeString(encoded[2:])
	if err != nil {
		return 0, fmt.Errorf("decode executions backfill task row type %d: %w", rowType, err)
	}
	task, err := serialization.NewSerializer().DeserializeTask(
		category,
		p.NewDataBlob(data, executionBackfillString(row, encodingField)),
	)
	if err != nil {
		return 0, fmt.Errorf("deserialize executions backfill task row type %d: %w", rowType, err)
	}
	return layout.workflowPartition(logicalShardID, task.GetNamespaceID(), task.GetWorkflowID())
}

func executionBackfillTaskCategory(
	rowType int,
	namespaceID string,
) (category tasks.Category, dataField string, encodingField string, err error) {
	switch rowType {
	case rowTypeTransferTask:
		return tasks.CategoryTransfer, "transfer", "transfer_encoding", nil
	case rowTypeTimerTask:
		return tasks.CategoryTimer, "timer", "timer_encoding", nil
	case rowTypeReplicationTask:
		return tasks.CategoryReplication, "replication", "replication_encoding", nil
	case rowTypeVisibilityTask:
		if namespaceID == rowTypeVisibilityTaskNamespaceID {
			return tasks.CategoryVisibility, "visibility_task_data", "visibility_task_encoding", nil
		}
	case tasks.CategoryIDArchival:
		return tasks.CategoryArchival, "task_data", "task_encoding", nil
	case tasks.CategoryIDOutbound:
		return tasks.CategoryOutbound, "task_data", "task_encoding", nil
	default:
		var unknown tasks.Category
		return unknown, "", "", fmt.Errorf("executions backfill cannot decode persisted task category %d", rowType)
	}
	var unknown tasks.Category
	return unknown, "", "", fmt.Errorf("executions backfill cannot decode persisted task category %d", rowType)
}

func executionBackfillString(row map[string]any, field string) string {
	value, ok := row[field].(string)
	if !ok {
		return ""
	}
	return value
}

func executionBackfillInt(row map[string]any, field string) (int, error) {
	value, err := executionBackfillInt64(row, field)
	if err != nil {
		return 0, err
	}
	if value < math.MinInt || value > math.MaxInt {
		return 0, fmt.Errorf("executions backfill field %s overflows int: %d", field, value)
	}
	return int(value), nil
}

func executionBackfillInt32(row map[string]any, field string) (int32, error) {
	value, err := executionBackfillInt64(row, field)
	if err != nil {
		return 0, err
	}
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, fmt.Errorf("executions backfill field %s overflows int32: %d", field, value)
	}
	return int32(value), nil
}

func executionBackfillInt64(row map[string]any, field string) (int64, error) {
	value, ok := row[field]
	if !ok {
		return 0, fmt.Errorf("executions backfill row missing %s", field)
	}
	number, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("executions backfill field %s has type %T", field, value)
	}
	parsed, err := number.Int64()
	if err != nil {
		return 0, fmt.Errorf("parse executions backfill field %s: %w", field, err)
	}
	return parsed, nil
}

func validateExecutionBackfillOptions(options ExecutionBackfillOptions) error {
	if options.PageSize <= 0 {
		return errors.New("executions backfill page size must be positive")
	}
	if options.Concurrency <= 0 {
		return errors.New("executions backfill concurrency must be positive")
	}
	_, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, options.StorageBuckets)
	return err
}
