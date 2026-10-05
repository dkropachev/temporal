package cassandra

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type MatchingTaskValidationResult struct {
	SourceTasks         int
	TargetTasks         int
	TargetUniqueTasks   int
	DuplicateTargetRows int
	TargetOnlyReady     bool
	Mismatches          []string
}

type matchingTaskV3ValidationRow struct {
	rowType   int
	pass      int64
	taskID    int64
	bucket    int16
	writeTime int64
	blob      *commonpb.DataBlob
}

func (r MatchingTaskValidationResult) Matches() bool {
	return len(r.Mismatches) == 0
}

func (r MatchingTaskValidationResult) CanEnterTargetOnly() bool {
	return r.Matches() && r.TargetOnlyReady
}

func ValidateMatchingTasksV3Queue(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	fair bool,
	bucketCount int,
) (MatchingTaskValidationResult, error) {
	if err := validateMatchingTaskStorageBucketCount(bucketCount); err != nil {
		return MatchingTaskValidationResult{}, err
	}
	sourceTasks, sourceMetadata, err := readMatchingTaskSourceQueueForValidation(
		ctx,
		session,
		namespaceID,
		taskQueue,
		taskType,
		fair,
	)
	if err != nil {
		return MatchingTaskValidationResult{}, err
	}
	targetStore := newMatchingTaskStoreV3(session, fair, bucketCount)
	targetRows, err := targetStore.readQueueTasksForValidation(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return MatchingTaskValidationResult{}, err
	}
	targetTasks, duplicateMismatches, targetRowCount, err := collapseMatchingTaskV3ValidationRows(targetRows)
	if err != nil {
		return MatchingTaskValidationResult{}, err
	}
	targetMetadata, err := targetStore.getCurrentMetadata(ctx, namespaceID, taskQueue, taskType, true)
	if err != nil {
		return MatchingTaskValidationResult{}, err
	}

	result := MatchingTaskValidationResult{
		SourceTasks:         len(sourceTasks),
		TargetTasks:         targetRowCount,
		TargetUniqueTasks:   len(targetTasks),
		DuplicateTargetRows: targetRowCount - len(targetTasks),
		TargetOnlyReady: targetMetadata.authority == matchingTaskAuthorityTarget &&
			sourceMetadata.authority == matchingTaskAuthorityTarget &&
			targetMetadata.bucketCount == int16(bucketCount) &&
			sourceMetadata.bucketCount == int16(bucketCount),
		Mismatches: duplicateMismatches,
	}
	for key, source := range sourceTasks {
		target, ok := targetTasks[key]
		if !ok {
			result.Mismatches = append(result.Mismatches, "missing target task "+key)
			continue
		}
		if source.EncodingType != target.EncodingType || !bytes.Equal(source.Data, target.Data) {
			result.Mismatches = append(result.Mismatches, "different target task "+key)
		}
	}
	for key := range targetTasks {
		if _, ok := sourceTasks[key]; !ok {
			result.Mismatches = append(result.Mismatches, "extra target task "+key)
		}
	}
	if sourceMetadata.rangeID != targetMetadata.rangeID ||
		sourceMetadata.taskQueueEncoding != targetMetadata.taskQueueEncoding ||
		!bytes.Equal(sourceMetadata.taskQueue, targetMetadata.taskQueue) {
		result.Mismatches = append(result.Mismatches, "different target task queue metadata")
	}
	sort.Strings(result.Mismatches)
	return result, nil
}

func readMatchingTaskSourceQueueForValidation(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	fair bool,
) (map[string]*commonpb.DataBlob, *matchingTaskV3Metadata, error) {
	table := "tasks"
	columns := "type, task_id, range_id, task, task_encoding, task_queue, task_queue_encoding, migration_authority, migration_bucket_count"
	if fair {
		table = "tasks_v2"
		columns = "type, pass, task_id, range_id, task, task_encoding, task_queue, task_queue_encoding, migration_authority, migration_bucket_count"
	}
	iter := session.Query(
		fmt.Sprintf(
			"SELECT %s FROM %s WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ?",
			columns,
			table,
		),
		namespaceID,
		taskQueue,
		taskType,
	).WithContext(ctx).Iter()
	tasks := make(map[string]*commonpb.DataBlob)
	var metadata *matchingTaskV3Metadata
	for {
		var rowType int
		var pass int64
		var taskID int64
		var rangeID nullableInt64
		var task []byte
		var taskEncoding string
		var taskQueueData []byte
		var taskQueueEncoding string
		var authority *int
		var bucketCount *int16
		var ok bool
		if fair {
			ok = iter.Scan(
				&rowType,
				&pass,
				&taskID,
				&rangeID,
				&task,
				&taskEncoding,
				&taskQueueData,
				&taskQueueEncoding,
				&authority,
				&bucketCount,
			)
		} else {
			ok = iter.Scan(
				&rowType,
				&taskID,
				&rangeID,
				&task,
				&taskEncoding,
				&taskQueueData,
				&taskQueueEncoding,
				&authority,
				&bucketCount,
			)
		}
		if !ok {
			break
		}
		if rowType == rowTypeTaskQueue {
			actualAuthority := 0
			if authority != nil {
				actualAuthority = *authority
			}
			actualBucketCount := int16(0)
			if bucketCount != nil {
				actualBucketCount = *bucketCount
			}
			metadata = &matchingTaskV3Metadata{
				rangeID:           rangeID.value,
				taskQueue:         bytes.Clone(taskQueueData),
				taskQueueEncoding: taskQueueEncoding,
				authority:         actualAuthority,
				bucketCount:       actualBucketCount,
			}
			continue
		}
		tasks[matchingTaskValidationKey(rowType, pass, taskID)] = p.NewDataBlob(task, taskEncoding)
	}
	if err := iter.Close(); err != nil {
		return nil, nil, gocql.ConvertError("ValidateMatchingTaskSourceQueue", err)
	}
	if metadata == nil {
		return nil, nil, errors.New("source matching task queue metadata not found")
	}
	return tasks, metadata, nil
}

func (d *matchingTaskStoreV3) readQueueTasksForValidation(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
) (map[string][]matchingTaskV3ValidationRow, error) {
	rows := make(map[string][]matchingTaskV3ValidationRow)
	for bucket := range d.bucketCount {
		columns := "type, task_id, task, task_encoding, WRITETIME(task), WRITETIME(task_encoding)"
		if d.fair {
			columns = "type, pass, task_id, task, task_encoding, WRITETIME(task), WRITETIME(task_encoding)"
		}
		iter := d.session.Query(
			fmt.Sprintf(
				"SELECT %s FROM %s WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? AND storage_bucket = ?",
				columns,
				d.table,
			),
			namespaceID,
			taskQueue,
			taskType,
			int16(bucket),
		).WithContext(ctx).Iter()
		for {
			var rowType int
			var pass int64
			var taskID int64
			var task []byte
			var encoding string
			var writeTime nullableInt64
			var encodingWriteTime nullableInt64
			var ok bool
			if d.fair {
				ok = iter.Scan(&rowType, &pass, &taskID, &task, &encoding, &writeTime, &encodingWriteTime)
			} else {
				ok = iter.Scan(&rowType, &taskID, &task, &encoding, &writeTime, &encodingWriteTime)
			}
			if !ok {
				break
			}
			if rowType == rowTypeTaskQueue {
				continue
			}
			key := matchingTaskValidationKey(rowType, pass, taskID)
			rows[key] = append(rows[key], matchingTaskV3ValidationRow{
				rowType:   rowType,
				pass:      pass,
				taskID:    taskID,
				bucket:    int16(bucket),
				writeTime: max(writeTime.value, encodingWriteTime.value),
				blob:      p.NewDataBlob(task, encoding),
			})
		}
		if err := iter.Close(); err != nil {
			return nil, gocql.ConvertError("ValidateMatchingTaskTargetQueue", err)
		}
	}
	return rows, nil
}

func collapseMatchingTaskV3ValidationRows(
	rows map[string][]matchingTaskV3ValidationRow,
) (map[string]*commonpb.DataBlob, []string, int, error) {
	result := make(map[string]*commonpb.DataBlob, len(rows))
	var mismatches []string
	rowCount := 0
	for key, copies := range rows {
		rowCount += len(copies)
		winner := copies[0]
		for _, copy := range copies[1:] {
			if winner.blob.EncodingType != copy.blob.EncodingType || !bytes.Equal(winner.blob.Data, copy.blob.Data) {
				return nil, nil, 0, fmt.Errorf("conflicting duplicate target task %s", key)
			}
			if copy.writeTime > winner.writeTime ||
				(copy.writeTime == winner.writeTime && copy.bucket < winner.bucket) {
				winner = copy
			}
		}
		if len(copies) > 1 {
			mismatches = append(mismatches, fmt.Sprintf("duplicate target task %s has %d rows", key, len(copies)))
		}
		result[key] = winner.blob
	}
	return result, mismatches, rowCount, nil
}

func RepairMatchingTasksV3QueueDuplicates(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	fair bool,
	bucketCount int,
) (int, error) {
	store := newMatchingTaskStoreV3(session, fair, bucketCount)
	rows, err := store.readQueueTasksForValidation(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return 0, err
	}
	repaired := 0
	for key, copies := range rows {
		if len(copies) < 2 {
			continue
		}
		sort.Slice(copies, func(i, j int) bool {
			if copies[i].writeTime != copies[j].writeTime {
				return copies[i].writeTime > copies[j].writeTime
			}
			return copies[i].bucket < copies[j].bucket
		})
		winner := copies[0]
		for _, copy := range copies[1:] {
			if winner.blob.EncodingType != copy.blob.EncodingType || !bytes.Equal(winner.blob.Data, copy.blob.Data) {
				return repaired, fmt.Errorf("refusing to repair conflicting duplicate target task %s", key)
			}
			if err := store.deleteValidationTaskRow(ctx, namespaceID, taskQueue, taskType, copy); err != nil {
				return repaired, err
			}
			repaired++
		}
	}
	return repaired, nil
}

//nolint:revive // Exact repair compares metadata and de-duplicates every task across target buckets.
func (d *matchingTaskStoreV3) reconcileSealedSourceQueue(
	ctx context.Context,
	sourceQueue *taskQueueStore,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
) (int64, error) {
	sourceRows, err := readMatchingTaskQueueForV3Backfill(
		ctx, d.session, namespaceID, taskQueue, taskType, d.fair,
	)
	if err != nil {
		return 0, err
	}
	targetRows, err := d.readQueueTasksForValidation(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return 0, err
	}
	maximumWriteTime := int64(0)
	for _, row := range sourceRows {
		maximumWriteTime = max(maximumWriteTime, row.writeTime)
	}
	for _, copies := range targetRows {
		for _, row := range copies {
			maximumWriteTime = max(maximumWriteTime, row.writeTime)
		}
	}
	repairTimestamp, err := sourceQueue.reserveRepairTimestamp(
		ctx, namespaceID, taskQueue, taskType, maximumWriteTime,
	)
	if err != nil {
		return 0, err
	}
	for _, copies := range targetRows {
		for _, row := range copies {
			if err := d.deleteValidationTaskRowAtTimestamp(
				ctx, namespaceID, taskQueue, taskType, row, repairTimestamp,
			); err != nil {
				return 0, err
			}
		}
	}
	options := MatchingTaskBackfillOptions{
		PageSize: 1, Concurrency: 1, BucketCount: d.bucketCount,
		RangeSize: DefaultMatchingTaskRangeSize, Fair: d.fair,
	}
	for _, row := range sourceRows {
		if row.rowType == rowTypeTaskQueue {
			continue
		}
		if err := backfillMatchingTaskV3RowAtTimestamp(
			ctx, d.session, options, row, repairTimestamp+1,
		); err != nil {
			return 0, err
		}
	}
	sourceTasks, _, err := readMatchingTaskSourceQueueForValidation(
		ctx, d.session, namespaceID, taskQueue, taskType, d.fair,
	)
	if err != nil {
		return 0, err
	}
	targetRows, err = d.readQueueTasksForValidation(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return 0, err
	}
	targetTasks, duplicateMismatches, _, err := collapseMatchingTaskV3ValidationRows(targetRows)
	if err != nil {
		return 0, err
	}
	if len(duplicateMismatches) != 0 || len(sourceTasks) != len(targetTasks) {
		return 0, fmt.Errorf(
			"matching task reconciliation differs: source=%d target=%d duplicates=%v",
			len(sourceTasks), len(targetTasks), duplicateMismatches,
		)
	}
	for key, source := range sourceTasks {
		target, ok := targetTasks[key]
		if !ok || source.EncodingType != target.EncodingType || !bytes.Equal(source.Data, target.Data) {
			return 0, fmt.Errorf("matching task reconciliation differs for task %s", key)
		}
	}
	return repairTimestamp, nil
}

func (d *matchingTaskStoreV3) deleteValidationTaskRow(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	row matchingTaskV3ValidationRow,
) error {
	return d.deleteValidationTaskRowAtTimestamp(ctx, namespaceID, taskQueue, taskType, row, 0)
}

func (d *matchingTaskStoreV3) deleteValidationTaskRowAtTimestamp(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	row matchingTaskV3ValidationRow,
	writeTime int64,
) error {
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? "+
			"AND storage_bucket = ? AND type = ? AND task_id = ?",
		d.table,
	)
	args := []any{namespaceID, taskQueue, taskType, row.bucket, row.rowType, row.taskID}
	if d.fair {
		query = fmt.Sprintf(
			"DELETE FROM %s WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? "+
				"AND storage_bucket = ? AND type = ? AND pass = ? AND task_id = ?",
			d.table,
		)
		args = []any{namespaceID, taskQueue, taskType, row.bucket, row.rowType, row.pass, row.taskID}
	}
	mutation := d.session.Query(query, args...).WithContext(ctx)
	if writeTime > 0 {
		mutation = mutation.WithTimestamp(writeTime)
	}
	if err := mutation.Exec(); err != nil {
		return gocql.ConvertError("RepairMatchingTaskV3Duplicate", err)
	}
	return nil
}

func matchingTaskValidationKey(rowType int, pass int64, taskID int64) string {
	return fmt.Sprintf("%d/%d/%d", rowType, pass, taskID)
}
