package cassandra

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultMatchingTaskBackfillTokenRangeCount = 4096
	DefaultMatchingTaskRangeSize               = int64(100000)
)

type MatchingTaskBackfillOptions struct {
	PageSize        int
	Concurrency     int
	TokenRangeCount int
	BucketCount     int
	RangeSize       int64
	Fair            bool
	Partitioner     string
}

type matchingTaskBackfillRow struct {
	namespaceID       string
	taskQueue         string
	taskType          enumspb.TaskQueueType
	rowType           int
	pass              int64
	taskID            int64
	rangeID           int64
	rangeIDValid      bool
	task              []byte
	taskEncoding      string
	taskQueueData     []byte
	taskQueueEncoding string
	ttl               int64
	ttlValid          bool
	writeTime         int64
}

func BackfillMatchingTasksV3(
	ctx context.Context,
	session gocql.Session,
	options MatchingTaskBackfillOptions,
) (int64, error) {
	if err := validateMatchingTaskBackfillOptions(options); err != nil {
		return 0, err
	}
	tokenRangeCount := options.TokenRangeCount
	if tokenRangeCount == 0 {
		tokenRangeCount = DefaultMatchingTaskBackfillTokenRangeCount
	}
	ranges, err := HistoryNodeBackfillTokenRanges(tokenRangeCount)
	if err != nil {
		return 0, err
	}
	if err := validateHistoryNodeBackfillPartitioner(ctx, session, options.Partitioner); err != nil {
		return 0, err
	}

	var copied int64
	for _, tokenRange := range ranges {
		rangeCopied, err := BackfillMatchingTasksV3Range(ctx, session, options, tokenRange)
		copied += rangeCopied
		if err != nil {
			return copied, fmt.Errorf(
				"backfill matching tasks token range %d [%d, %d]: %w",
				tokenRange.Index,
				tokenRange.StartToken,
				tokenRange.EndToken,
				err,
			)
		}
	}
	return copied, nil
}

func BackfillMatchingTasksV3Range(
	ctx context.Context,
	session gocql.Session,
	options MatchingTaskBackfillOptions,
	tokenRange HistoryNodeBackfillTokenRange,
) (int64, error) {
	if err := validateMatchingTaskBackfillOptions(options); err != nil {
		return 0, err
	}
	if tokenRange.Index < 0 || tokenRange.StartToken > tokenRange.EndToken {
		return 0, fmt.Errorf("invalid matching task backfill token range: %+v", tokenRange)
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(options.Concurrency)
	iter := session.Query(
		matchingTaskBackfillScanQuery(options.Fair),
		tokenRange.StartToken,
		tokenRange.EndToken,
	).WithContext(groupCtx).PageSize(options.PageSize).Iter()
	var copied atomic.Int64
	for groupCtx.Err() == nil {
		row, ok := scanMatchingTaskBackfillRow(iter, options.Fair)
		if !ok {
			break
		}
		row.task = bytes.Clone(row.task)
		row.taskQueueData = bytes.Clone(row.taskQueueData)
		group.Go(func() error {
			if err := backfillMatchingTaskV3Row(groupCtx, session, options, row); err != nil {
				return err
			}
			copied.Add(1)
			return nil
		})
	}
	closeErr := iter.Close()
	groupErr := group.Wait()
	if groupErr != nil {
		return copied.Load(), groupErr
	}
	if closeErr != nil {
		return copied.Load(), gocql.ConvertError("ScanMatchingTasksV3Backfill", closeErr)
	}
	return copied.Load(), nil
}

func matchingTaskBackfillScanQuery(fair bool) string {
	if fair {
		return "SELECT namespace_id, task_queue_name, task_queue_type, type, pass, task_id, range_id, " +
			"task, task_encoding, task_queue, task_queue_encoding, TTL(task), TTL(task_queue), " +
			"WRITETIME(task), WRITETIME(task_encoding), WRITETIME(range_id), WRITETIME(task_queue), WRITETIME(task_queue_encoding) FROM tasks_v2 " +
			"WHERE token(namespace_id, task_queue_name, task_queue_type) >= ? " +
			"AND token(namespace_id, task_queue_name, task_queue_type) <= ?"
	}
	return "SELECT namespace_id, task_queue_name, task_queue_type, type, task_id, range_id, " +
		"task, task_encoding, task_queue, task_queue_encoding, TTL(task), TTL(task_queue), " +
		"WRITETIME(task), WRITETIME(task_encoding), WRITETIME(range_id), WRITETIME(task_queue), WRITETIME(task_queue_encoding) FROM tasks " +
		"WHERE token(namespace_id, task_queue_name, task_queue_type) >= ? " +
		"AND token(namespace_id, task_queue_name, task_queue_type) <= ?"
}

func matchingTaskQueueBackfillScanQuery(fair bool) string {
	if fair {
		return "SELECT namespace_id, task_queue_name, task_queue_type, type, pass, task_id, range_id, " +
			"task, task_encoding, task_queue, task_queue_encoding, TTL(task), TTL(task_queue), " +
			"WRITETIME(task), WRITETIME(task_encoding), WRITETIME(range_id), WRITETIME(task_queue), WRITETIME(task_queue_encoding) FROM tasks_v2 " +
			"WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ?"
	}
	return "SELECT namespace_id, task_queue_name, task_queue_type, type, task_id, range_id, " +
		"task, task_encoding, task_queue, task_queue_encoding, TTL(task), TTL(task_queue), " +
		"WRITETIME(task), WRITETIME(task_encoding), WRITETIME(range_id), WRITETIME(task_queue), WRITETIME(task_queue_encoding) FROM tasks " +
		"WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ?"
}

func readMatchingTaskQueueForV3Backfill(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	fair bool,
) ([]matchingTaskBackfillRow, error) {
	iter := session.Query(
		matchingTaskQueueBackfillScanQuery(fair),
		namespaceID,
		taskQueue,
		taskType,
	).WithContext(ctx).Iter()
	var rows []matchingTaskBackfillRow
	for {
		row, ok := scanMatchingTaskBackfillRow(iter, fair)
		if !ok {
			break
		}
		row.task = bytes.Clone(row.task)
		row.taskQueueData = bytes.Clone(row.taskQueueData)
		rows = append(rows, row)
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ScanMatchingTaskQueueV3Backfill", err)
	}
	return rows, nil
}

func scanMatchingTaskBackfillRow(iter gocql.Iter, fair bool) (matchingTaskBackfillRow, bool) {
	var row matchingTaskBackfillRow
	var rangeID nullableInt64
	var taskTTL nullableInt64
	var metadataTTL nullableInt64
	var taskWriteTime nullableInt64
	var taskEncodingWriteTime nullableInt64
	var rangeWriteTime nullableInt64
	var taskQueueWriteTime nullableInt64
	var taskQueueEncodingWriteTime nullableInt64
	var taskType int
	var ok bool
	if fair {
		ok = iter.Scan(
			&row.namespaceID,
			&row.taskQueue,
			&taskType,
			&row.rowType,
			&row.pass,
			&row.taskID,
			&rangeID,
			&row.task,
			&row.taskEncoding,
			&row.taskQueueData,
			&row.taskQueueEncoding,
			&taskTTL,
			&metadataTTL,
			&taskWriteTime,
			&taskEncodingWriteTime,
			&rangeWriteTime,
			&taskQueueWriteTime,
			&taskQueueEncodingWriteTime,
		)
	} else {
		ok = iter.Scan(
			&row.namespaceID,
			&row.taskQueue,
			&taskType,
			&row.rowType,
			&row.taskID,
			&rangeID,
			&row.task,
			&row.taskEncoding,
			&row.taskQueueData,
			&row.taskQueueEncoding,
			&taskTTL,
			&metadataTTL,
			&taskWriteTime,
			&taskEncodingWriteTime,
			&rangeWriteTime,
			&taskQueueWriteTime,
			&taskQueueEncodingWriteTime,
		)
	}
	if !ok {
		return matchingTaskBackfillRow{}, false
	}
	row.taskType = enumspb.TaskQueueType(taskType)
	row.rangeID = rangeID.value
	row.rangeIDValid = rangeID.valid
	if row.rowType == rowTypeTaskQueue {
		row.ttl = metadataTTL.value
		row.ttlValid = metadataTTL.valid
		row.writeTime = max(rangeWriteTime.value, taskQueueWriteTime.value, taskQueueEncodingWriteTime.value)
	} else {
		row.ttl = taskTTL.value
		row.ttlValid = taskTTL.valid
		row.writeTime = max(taskWriteTime.value, taskEncodingWriteTime.value)
	}
	return row, true
}

func backfillMatchingTaskV3Row(
	ctx context.Context,
	session gocql.Session,
	options MatchingTaskBackfillOptions,
	row matchingTaskBackfillRow,
) error {
	return backfillMatchingTaskV3RowAtTimestamp(ctx, session, options, row, row.writeTime)
}

func backfillMatchingTaskV3RowAtTimestamp(
	ctx context.Context,
	session gocql.Session,
	options MatchingTaskBackfillOptions,
	row matchingTaskBackfillRow,
	writeTime int64,
) error {
	if writeTime <= 0 {
		return fmt.Errorf(
			"backfill matching task namespace=%s queue=%q type=%d task=%d has no source write timestamp",
			row.namespaceID,
			row.taskQueue,
			row.rowType,
			row.taskID,
		)
	}
	table := matchingTaskV3TableName
	if options.Fair {
		table = matchingTaskV3FairTableName
	}
	rangeID := row.rangeID
	if row.rowType != rowTypeTaskQueue {
		if row.taskID < 1 {
			return fmt.Errorf("backfill matching task with invalid task ID %d", row.taskID)
		}
		rangeID = (row.taskID-1)/effectiveMatchingTaskRangeSize(options) + 1
	} else if !row.rangeIDValid || row.rangeID < 1 {
		return fmt.Errorf("backfill matching task queue %q with invalid range ID", row.taskQueue)
	}
	bucket, err := matchingTaskStorageBucket(rangeID, effectiveMatchingTaskBucketCount(options))
	if err != nil {
		return err
	}

	var query string
	var args []any
	if options.Fair {
		query = fmt.Sprintf(
			"INSERT INTO %s (namespace_id, task_queue_name, task_queue_type, storage_bucket, type, pass, task_id, "+
				"range_id, metadata_state, bucket_count, migration_authority, task, task_encoding, task_queue, task_queue_encoding) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			table,
		)
		args = []any{
			row.namespaceID, row.taskQueue, row.taskType, bucket, row.rowType, row.pass, row.taskID,
			matchingTaskBackfillRangeID(row), matchingTaskBackfillMetadataState(row),
			matchingTaskBackfillBucketCount(row, effectiveMatchingTaskBucketCount(options)), matchingTaskBackfillAuthority(row),
			row.task, row.taskEncoding, row.taskQueueData, row.taskQueueEncoding,
		}
	} else {
		query = fmt.Sprintf(
			"INSERT INTO %s (namespace_id, task_queue_name, task_queue_type, storage_bucket, type, task_id, "+
				"range_id, metadata_state, bucket_count, migration_authority, task, task_encoding, task_queue, task_queue_encoding) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			table,
		)
		args = []any{
			row.namespaceID, row.taskQueue, row.taskType, bucket, row.rowType, row.taskID,
			matchingTaskBackfillRangeID(row), matchingTaskBackfillMetadataState(row),
			matchingTaskBackfillBucketCount(row, effectiveMatchingTaskBucketCount(options)), matchingTaskBackfillAuthority(row),
			row.task, row.taskEncoding, row.taskQueueData, row.taskQueueEncoding,
		}
	}
	if row.ttlValid && row.ttl > 0 {
		query += " USING TTL ?"
		args = append(args, row.ttl)
	}
	if err := session.Query(query, args...).
		WithContext(ctx).
		WithTimestamp(writeTime).
		Idempotent(true).
		Exec(); err != nil {
		return fmt.Errorf(
			"backfill matching task namespace=%s queue=%q type=%d task=%d: %w",
			row.namespaceID,
			row.taskQueue,
			row.rowType,
			row.taskID,
			err,
		)
	}
	return nil
}

func matchingTaskBackfillRangeID(row matchingTaskBackfillRow) any {
	if row.rowType != rowTypeTaskQueue {
		return nil
	}
	return row.rangeID
}

func matchingTaskBackfillMetadataState(row matchingTaskBackfillRow) any {
	if row.rowType != rowTypeTaskQueue {
		return nil
	}
	return matchingTaskMetadataStateActive
}

func matchingTaskBackfillBucketCount(row matchingTaskBackfillRow, bucketCount int) any {
	if row.rowType != rowTypeTaskQueue {
		return nil
	}
	return int16(bucketCount)
}

func matchingTaskBackfillAuthority(row matchingTaskBackfillRow) any {
	if row.rowType != rowTypeTaskQueue {
		return nil
	}
	return matchingTaskAuthoritySource
}

func validateMatchingTaskBackfillOptions(options MatchingTaskBackfillOptions) error {
	if options.PageSize < 1 {
		return errors.New("matching task backfill page size must be positive")
	}
	if options.Concurrency < 1 {
		return errors.New("matching task backfill concurrency must be positive")
	}
	if options.TokenRangeCount < 0 {
		return errors.New("matching task backfill token range count cannot be negative")
	}
	if options.RangeSize < 0 {
		return errors.New("matching task backfill range size cannot be negative")
	}
	return validateMatchingTaskStorageBucketCount(effectiveMatchingTaskBucketCount(options))
}

func effectiveMatchingTaskBucketCount(options MatchingTaskBackfillOptions) int {
	if options.BucketCount == 0 {
		return DefaultMatchingTaskStorageBucketCount
	}
	return options.BucketCount
}

func effectiveMatchingTaskRangeSize(options MatchingTaskBackfillOptions) int64 {
	if options.RangeSize == 0 {
		return DefaultMatchingTaskRangeSize
	}
	return options.RangeSize
}
