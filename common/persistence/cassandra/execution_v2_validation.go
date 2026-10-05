package cassandra

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	templateTruncateExecutionsV2          = `TRUNCATE %s.executions_v2`
	templateScanExecutionsV2ForValidation = `SELECT JSON * FROM executions_v2 ` +
		`WHERE token(shard_id) >= ? AND token(shard_id) <= ?`
	templateGetExecutionForValidation = `SELECT JSON * FROM executions ` +
		`WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ? ` +
		`AND visibility_ts = ? AND task_id = ?`
	templateGetExecutionV2ForValidation = `SELECT JSON * FROM executions_v2 ` +
		`WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ? ` +
		`AND visibility_ts = ? AND task_id = ?`
	templateScanExecutionV2Partition = `SELECT JSON * FROM executions_v2 WHERE shard_id = ?`
	templateDeleteExecutionV2Row     = `DELETE FROM executions_v2 ` +
		`WHERE shard_id = ? AND type = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ? ` +
		`AND visibility_ts = ? AND task_id = ?`
	templateReconcileExecutionV2 = `INSERT INTO executions_v2 JSON ? DEFAULT NULL`
)

var templateScanExecutionShardForReconcile = strings.Replace(
	templateScanExecutionsForV2Backfill,
	"WHERE token(shard_id) >= ? AND token(shard_id) <= ?",
	"WHERE shard_id = ?",
	1,
)

type ExecutionValidationOptions struct {
	PageSize        int
	TokenRangeCount int
	StorageBuckets  int
	MaxMismatches   int
	Partitioner     string
}

type ExecutionValidationResult struct {
	SourceRows int64
	TargetRows int64
	Mismatches []string
}

func (r ExecutionValidationResult) Matches() bool {
	return len(r.Mismatches) == 0
}

func PrepareExecutionsV2Backfill(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	storageBuckets int,
	confirmSourceRebuild bool,
) error {
	if !confirmSourceRebuild {
		return errors.New("truncating executions_v2 requires confirmation that every Temporal writer uses executionMigrationMode source-rebuild")
	}
	layout, err := newExecutionLayout(config.CassandraExecutionMigrationModeSourceRebuild, storageBuckets)
	if err != nil {
		return err
	}
	if err := validateExecutionLayoutSchema(ctx, session, keyspace, layout); err != nil {
		return err
	}
	if err := session.Query(fmt.Sprintf(templateTruncateExecutionsV2, quoteCQLIdentifier(keyspace))).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("truncate executions_v2 before backfill: %w", err)
	}
	return nil
}

//nolint:revive // Bidirectional reconciliation keeps source and target scans together to enforce exact parity.
func ReconcileExecutionShardV2(
	ctx context.Context,
	session gocql.Session,
	logicalShardID int32,
	storageBuckets int,
) error {
	layout, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, storageBuckets)
	if err != nil {
		return err
	}
	iter := session.Query(templateScanExecutionShardForReconcile, logicalShardID).WithContext(ctx).PageSize(128).Iter()
	for {
		var sourceJSON string
		if !iter.Scan(&sourceJSON) {
			break
		}
		rows, err := executionBackfillTargetRows([]byte(sourceJSON), layout)
		if err != nil {
			_ = iter.Close()
			return err
		}
		for _, row := range rows {
			reconciled, err := executionReconcileTargetRow(row.data, layout)
			if err != nil {
				_ = iter.Close()
				return err
			}
			if err := session.Query(templateReconcileExecutionV2, string(reconciled)).
				WithContext(ctx).
				Idempotent(true).
				Exec(); err != nil {
				_ = iter.Close()
				return gocql.ConvertError("ReconcileExecutionShardV2", err)
			}
		}
	}
	if err := iter.Close(); err != nil {
		return gocql.ConvertError("ScanExecutionShardForV2Reconcile", err)
	}

	partitions, err := layout.partitions(logicalShardID)
	if err != nil {
		return err
	}
	for _, partition := range partitions {
		targetIter := session.Query(templateScanExecutionV2Partition, partition).WithContext(ctx).PageSize(128).Iter()
		for {
			var targetJSON string
			if !targetIter.Scan(&targetJSON) {
				break
			}
			sourceKey, err := executionValidationSourceKey([]byte(targetJSON), layout)
			if err != nil {
				_ = targetIter.Close()
				return err
			}
			source, found, err := readExecutionValidationRow(ctx, session, templateGetExecutionForValidation, sourceKey)
			if err != nil {
				_ = targetIter.Close()
				return err
			}
			validPartition := false
			if found {
				validPartition, err = executionValidationTargetPartition([]byte(targetJSON), source, layout)
				if err != nil {
					_ = targetIter.Close()
					return err
				}
			}
			if !found || !validPartition {
				if err := deleteExecutionV2ValidationRow(ctx, session, []byte(targetJSON)); err != nil {
					_ = targetIter.Close()
					return err
				}
				continue
			}
			if !executionJSONEqual(sourceKey, source) {
				_ = targetIter.Close()
				return fmt.Errorf(
					"reconciled executions_v2 row differs from source for %s",
					executionValidationKey(sourceKey),
				)
			}
		}
		if err := targetIter.Close(); err != nil {
			return gocql.ConvertError("ScanExecutionV2PartitionForReconcile", err)
		}
	}
	return nil
}

func executionReconcileTargetRow(rowJSON []byte, layout executionLayout) ([]byte, error) {
	row, err := decodeExecutionJSON(rowJSON)
	if err != nil {
		return nil, err
	}
	rowType, err := executionBackfillInt(row, "type")
	if err != nil {
		return nil, err
	}
	if rowType == rowTypeShard {
		row["migration_authority"] = int(executionShardAuthoritySealing)
		row["storage_bucket_count"] = layout.buckets
	}
	return json.Marshal(row)
}

func ValidateExecutionsV2(
	ctx context.Context,
	session gocql.Session,
	options ExecutionValidationOptions,
) (ExecutionValidationResult, error) {
	if err := validateExecutionValidationOptions(options); err != nil {
		return ExecutionValidationResult{}, err
	}
	rangeCount := options.TokenRangeCount
	if rangeCount == 0 {
		rangeCount = DefaultExecutionBackfillTokenRangeCount
	}
	ranges, err := HistoryNodeBackfillTokenRanges(rangeCount)
	if err != nil {
		return ExecutionValidationResult{}, err
	}
	if err := validateHistoryNodeBackfillPartitioner(ctx, session, options.Partitioner); err != nil {
		return ExecutionValidationResult{}, err
	}
	result := ExecutionValidationResult{}
	for _, tokenRange := range ranges {
		rangeResult, err := ValidateExecutionsV2Range(ctx, session, options, tokenRange)
		result.SourceRows += rangeResult.SourceRows
		result.TargetRows += rangeResult.TargetRows
		for _, mismatch := range rangeResult.Mismatches {
			result.addMismatch(options.MaxMismatches, mismatch)
		}
		if err != nil {
			return result, fmt.Errorf("validate executions token range %d: %w", tokenRange.Index, err)
		}
	}
	return result, nil
}

//nolint:revive // Validation deliberately performs both source-to-target and target-to-source scans.
func ValidateExecutionsV2Range(
	ctx context.Context,
	session gocql.Session,
	options ExecutionValidationOptions,
	tokenRange HistoryNodeBackfillTokenRange,
) (ExecutionValidationResult, error) {
	if err := validateExecutionValidationOptions(options); err != nil {
		return ExecutionValidationResult{}, err
	}
	if tokenRange.Index < 0 || tokenRange.StartToken > tokenRange.EndToken {
		return ExecutionValidationResult{}, fmt.Errorf("invalid executions validation token range: %+v", tokenRange)
	}
	layout, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, options.StorageBuckets)
	if err != nil {
		return ExecutionValidationResult{}, err
	}
	result := ExecutionValidationResult{}

	sourceIter := session.Query(
		templateScanExecutionsForV2Backfill,
		tokenRange.StartToken,
		tokenRange.EndToken,
	).WithContext(ctx).PageSize(options.PageSize).Iter()
	for {
		var sourceJSON string
		if !sourceIter.Scan(&sourceJSON) {
			break
		}
		result.SourceRows++
		expectedRows, err := executionBackfillTargetRows([]byte(sourceJSON), layout)
		if err != nil {
			_ = sourceIter.Close()
			return result, err
		}
		for _, expected := range expectedRows {
			actual, found, err := readExecutionValidationRow(ctx, session, templateGetExecutionV2ForValidation, expected.data)
			if err != nil {
				_ = sourceIter.Close()
				return result, err
			}
			if !found {
				result.addMismatch(options.MaxMismatches, "target missing "+executionValidationKey(expected.data))
				continue
			}
			if !executionJSONEqual(expected.data, actual) {
				result.addMismatch(options.MaxMismatches, "target differs for "+executionValidationKey(expected.data))
			}
		}
	}
	if err := sourceIter.Close(); err != nil {
		return result, gocql.ConvertError("ValidateExecutionsV2SourceScan", err)
	}

	targetIter := session.Query(
		templateScanExecutionsV2ForValidation,
		tokenRange.StartToken,
		tokenRange.EndToken,
	).WithContext(ctx).PageSize(options.PageSize).Iter()
	for {
		var targetJSON string
		if !targetIter.Scan(&targetJSON) {
			break
		}
		result.TargetRows++
		sourceJSON, err := executionValidationSourceKey([]byte(targetJSON), layout)
		if err != nil {
			_ = targetIter.Close()
			return result, err
		}
		actual, found, err := readExecutionValidationRow(ctx, session, templateGetExecutionForValidation, sourceJSON)
		if err != nil {
			_ = targetIter.Close()
			return result, err
		}
		if !found {
			result.addMismatch(options.MaxMismatches, "target has extra "+executionValidationKey([]byte(targetJSON)))
			continue
		}
		validPartition, err := executionValidationTargetPartition([]byte(targetJSON), actual, layout)
		if err != nil {
			_ = targetIter.Close()
			return result, err
		}
		if !validPartition {
			result.addMismatch(options.MaxMismatches, "target row uses wrong storage partition for "+executionValidationKey([]byte(targetJSON)))
			continue
		}
		if !executionJSONEqual(sourceJSON, actual) {
			result.addMismatch(options.MaxMismatches, "source differs for "+executionValidationKey(sourceJSON))
		}
	}
	if err := targetIter.Close(); err != nil {
		return result, gocql.ConvertError("ValidateExecutionsV2TargetScan", err)
	}
	return result, nil
}

func executionValidationTargetPartition(
	targetJSON []byte,
	sourceJSON []byte,
	layout executionLayout,
) (bool, error) {
	target, err := decodeExecutionJSON(targetJSON)
	if err != nil {
		return false, err
	}
	physicalShardID, err := executionBackfillInt32(target, "shard_id")
	if err != nil {
		return false, err
	}
	source, err := decodeExecutionJSON(sourceJSON)
	if err != nil {
		return false, err
	}
	logicalShardID, err := executionBackfillInt32(source, "shard_id")
	if err != nil {
		return false, err
	}
	rowType, err := executionBackfillInt(source, "type")
	if err != nil {
		return false, err
	}
	partitions, err := executionBackfillTargetPartitions(source, rowType, logicalShardID, layout)
	if err != nil {
		return false, err
	}
	for _, partition := range partitions {
		if partition == physicalShardID {
			return true, nil
		}
	}
	return false, nil
}

func executionValidationSourceKey(targetJSON []byte, layout executionLayout) ([]byte, error) {
	row, err := decodeExecutionJSON(targetJSON)
	if err != nil {
		return nil, err
	}
	physicalShardID, err := executionBackfillInt32(row, "shard_id")
	if err != nil {
		return nil, err
	}
	logicalShardID := (physicalShardID-1)/layout.buckets + 1
	row["shard_id"] = logicalShardID
	return json.Marshal(row)
}

func readExecutionValidationRow(
	ctx context.Context,
	session gocql.Session,
	query string,
	keyJSON []byte,
) ([]byte, bool, error) {
	row, err := decodeExecutionJSON(keyJSON)
	if err != nil {
		return nil, false, err
	}
	shardID, err := executionBackfillInt32(row, "shard_id")
	if err != nil {
		return nil, false, err
	}
	rowType, err := executionBackfillInt(row, "type")
	if err != nil {
		return nil, false, err
	}
	taskID, err := executionBackfillInt64(row, "task_id")
	if err != nil {
		return nil, false, err
	}
	visibility, err := time.Parse("2006-01-02 15:04:05.999999999Z07:00", executionBackfillString(row, "visibility_ts"))
	if err != nil {
		return nil, false, fmt.Errorf("parse executions visibility timestamp: %w", err)
	}
	var encoded string
	err = session.Query(
		query,
		shardID,
		rowType,
		executionBackfillString(row, "namespace_id"),
		executionBackfillString(row, "workflow_id"),
		executionBackfillString(row, "run_id"),
		visibility,
		taskID,
	).WithContext(ctx).Scan(&encoded)
	if gocql.IsNotFoundError(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, gocql.ConvertError("ReadExecutionForV2Validation", err)
	}
	return []byte(encoded), true, nil
}

func deleteExecutionV2ValidationRow(
	ctx context.Context,
	session gocql.Session,
	keyJSON []byte,
) error {
	row, err := decodeExecutionJSON(keyJSON)
	if err != nil {
		return err
	}
	shardID, err := executionBackfillInt32(row, "shard_id")
	if err != nil {
		return err
	}
	rowType, err := executionBackfillInt(row, "type")
	if err != nil {
		return err
	}
	taskID, err := executionBackfillInt64(row, "task_id")
	if err != nil {
		return err
	}
	visibility, err := time.Parse("2006-01-02 15:04:05.999999999Z07:00", executionBackfillString(row, "visibility_ts"))
	if err != nil {
		return fmt.Errorf("parse executions visibility timestamp: %w", err)
	}
	if err := session.Query(
		templateDeleteExecutionV2Row,
		shardID,
		rowType,
		executionBackfillString(row, "namespace_id"),
		executionBackfillString(row, "workflow_id"),
		executionBackfillString(row, "run_id"),
		visibility,
		taskID,
	).WithContext(ctx).Exec(); err != nil {
		return gocql.ConvertError("DeleteExecutionV2RowDuringReconcile", err)
	}
	return nil
}

func executionJSONEqual(left []byte, right []byte) bool {
	leftRow, leftErr := decodeExecutionJSON(left)
	rightRow, rightErr := decodeExecutionJSON(right)
	delete(leftRow, "migration_authority")
	delete(leftRow, "storage_bucket_count")
	delete(rightRow, "migration_authority")
	delete(rightRow, "storage_bucket_count")
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(leftRow, rightRow)
}

func executionValidationKey(encoded []byte) string {
	row, err := decodeExecutionJSON(encoded)
	if err != nil {
		return string(encoded)
	}
	return fmt.Sprintf(
		"shard=%v/type=%v/namespace=%v/workflow=%v/run=%v/visibility=%v/task=%v",
		row["shard_id"], row["type"], row["namespace_id"], row["workflow_id"], row["run_id"], row["visibility_ts"], row["task_id"],
	)
}

func decodeExecutionJSON(encoded []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	row := make(map[string]any)
	if err := decoder.Decode(&row); err != nil {
		return nil, fmt.Errorf("decode executions JSON: %w", err)
	}
	return row, nil
}

func validateExecutionValidationOptions(options ExecutionValidationOptions) error {
	if options.PageSize <= 0 {
		return errors.New("executions validation page size must be positive")
	}
	if options.MaxMismatches < 0 {
		return errors.New("executions validation maximum mismatches must not be negative")
	}
	_, err := newExecutionLayout(config.CassandraExecutionMigrationModeTargetOnly, options.StorageBuckets)
	return err
}

func (r *ExecutionValidationResult) addMismatch(limit int, mismatch string) {
	if limit == 0 || len(r.Mismatches) < limit {
		r.Mismatches = append(r.Mismatches, mismatch)
	}
}
