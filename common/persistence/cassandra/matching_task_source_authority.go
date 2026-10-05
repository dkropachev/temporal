package cassandra

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	templateGetMatchingTaskSourceAuthority = `SELECT range_id, task_queue, task_queue_encoding,
		migration_authority, migration_bucket_count, migration_timestamp, TTL(task_queue)
		FROM tasks_v2 WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ?
		AND type = ? AND pass = 0 AND task_id = ?`
	templateInitializeMatchingTaskSourceAuthority = `UPDATE tasks_v2 SET migration_authority = ?, migration_bucket_count = ?
		WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ?
		AND type = ? AND pass = 0 AND task_id = ?
		IF migration_authority = null AND migration_bucket_count = null`
	templateGuardMatchingTaskSourceAuthority = `UPDATE tasks_v2 SET migration_authority = ?
		WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ?
		AND type = ? AND pass = 0 AND task_id = ?
		IF migration_authority = ? AND migration_bucket_count = ?`
	templateReserveMatchingTaskRepairTimestamp = `UPDATE tasks_v2 SET migration_timestamp = ?
		WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ?
		AND type = ? AND pass = 0 AND task_id = ?
		IF migration_authority = ? AND migration_bucket_count = ? AND migration_timestamp = null`
	templatePublishMatchingTaskTargetAuthority = `UPDATE tasks_v2 SET migration_authority = ?
		WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ?
		AND type = ? AND pass = 0 AND task_id = ?
		IF range_id = ? AND migration_authority = ? AND migration_bucket_count = ? AND migration_timestamp = ?`
)

type matchingTaskSourceAuthorityRecord struct {
	rangeID            int64
	taskQueue          []byte
	taskQueueEncoding  string
	authority          int
	bucketCount        int16
	migrationTimestamp int64
	ttl                int64
}

func configureMatchingTaskSourceMigration(
	store p.TaskStore,
	authority int,
	bucketCount int,
) (*taskQueueStore, error) {
	var queueStore *taskQueueStore
	switch store := store.(type) {
	case *matchingTaskStoreV1:
		queueStore = &store.taskQueueStore
	case *matchingTaskStoreV2:
		queueStore = &store.taskQueueStore
	default:
		return nil, fmt.Errorf("unsupported Cassandra matching task source store %T", store)
	}
	queueStore.migrationAuthority = authority
	queueStore.migrationBucketCount = int16(bucketCount)
	return queueStore, nil
}

func (d *taskQueueStore) migrationKeyArgs(
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
) []any {
	return []any{namespaceID, taskQueue, taskType, rowTypeTaskQueue, taskQueueTaskID}
}

func (d *taskQueueStore) readMigrationAuthority(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
) (matchingTaskSourceAuthorityRecord, error) {
	var (
		record             matchingTaskSourceAuthorityRecord
		authority          *int
		bucketCount        *int16
		migrationTimestamp *int64
		ttl                nullableInt64
	)
	err := d.Session.Query(
		switchTasksTable(templateGetMatchingTaskSourceAuthority, d.version),
		d.migrationKeyArgs(namespaceID, taskQueue, taskType)...,
	).WithContext(ctx).Scan(
		&record.rangeID,
		&record.taskQueue,
		&record.taskQueueEncoding,
		&authority,
		&bucketCount,
		&migrationTimestamp,
		&ttl,
	)
	if err != nil {
		return matchingTaskSourceAuthorityRecord{}, err
	}
	if authority != nil {
		record.authority = *authority
	}
	if bucketCount != nil {
		record.bucketCount = *bucketCount
	}
	if migrationTimestamp != nil {
		record.migrationTimestamp = *migrationTimestamp
	}
	if ttl.valid {
		record.ttl = ttl.value
	}
	record.taskQueue = bytes.Clone(record.taskQueue)
	return record, nil
}

func (d *taskQueueStore) ensureMigrationAuthority(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
) error {
	if d.migrationAuthority == 0 {
		return nil
	}
	record, err := d.readMigrationAuthority(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return gocql.ConvertError("ReadMatchingTaskSourceAuthority", err)
	}
	if record.authority == 0 && record.bucketCount == 0 && d.migrationAuthority == matchingTaskAuthoritySource {
		args := []any{matchingTaskAuthoritySource, d.migrationBucketCount}
		args = append(args, d.migrationKeyArgs(namespaceID, taskQueue, taskType)...)
		applied, err := d.Session.Query(
			switchTasksTable(templateInitializeMatchingTaskSourceAuthority, d.version),
			args...,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return gocql.ConvertError("InitializeMatchingTaskSourceAuthority", err)
		}
		if applied {
			return nil
		}
		record, err = d.readMigrationAuthority(ctx, namespaceID, taskQueue, taskType)
		if err != nil {
			return gocql.ConvertError("ReadMatchingTaskSourceAuthorityAfterInitialize", err)
		}
	}
	return d.validateMigrationAuthority(namespaceID, taskQueue, taskType, &record.authority, &record.bucketCount)
}

func (d *taskQueueStore) validateMigrationAuthority(
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	authority *int,
	bucketCount *int16,
) error {
	actualAuthority := 0
	if authority != nil {
		actualAuthority = *authority
	}
	actualBucketCount := int16(0)
	if bucketCount != nil {
		actualBucketCount = *bucketCount
	}
	if actualAuthority != d.migrationAuthority || actualBucketCount != d.migrationBucketCount {
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"Cassandra matching task source authority changed for namespace %s queue %q type %s: expected authority %d buckets %d, got authority %d buckets %d",
			namespaceID,
			taskQueue,
			taskType,
			d.migrationAuthority,
			d.migrationBucketCount,
			actualAuthority,
			actualBucketCount,
		)}
	}
	return nil
}

func (d *taskQueueStore) addMigrationAuthorityGuard(
	batch *gocql.Batch,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
) {
	if d.migrationAuthority == 0 {
		return
	}
	args := []any{d.migrationAuthority}
	args = append(args, d.migrationKeyArgs(namespaceID, taskQueue, taskType)...)
	args = append(args, d.migrationAuthority, d.migrationBucketCount)
	batch.Query(switchTasksTable(templateGuardMatchingTaskSourceAuthority, d.version), args...)
}

func (d *taskQueueStore) addCreateTasksGuard(
	batch *gocql.Batch,
	request *p.InternalCreateTasksRequest,
) {
	query := templateUpdateTaskQueueQuery
	args := []any{
		request.RangeID,
		request.TaskQueueInfo.Data,
		request.TaskQueueInfo.EncodingType.String(),
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		rowTypeTaskQueue,
		taskQueueTaskID,
		request.RangeID,
	}
	if d.migrationAuthority != 0 {
		query = templateUpdateTaskQueueWithMigrationQuery
		args = []any{
			request.RangeID,
			request.TaskQueueInfo.Data,
			request.TaskQueueInfo.EncodingType.String(),
			d.migrationAuthority,
			d.migrationBucketCount,
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			request.RangeID,
			d.migrationAuthority,
			d.migrationBucketCount,
		}
	}
	batch.Query(switchTasksTable(query, d.version), args...)
}

func (d *taskQueueStore) initializeSourceAuthorityForCutover(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
) (matchingTaskSourceAuthorityRecord, error) {
	record, err := d.readMigrationAuthority(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return record, gocql.ConvertError("ReadMatchingTaskSourceAuthorityForCutover", err)
	}
	if record.authority != 0 || record.bucketCount != 0 {
		return record, nil
	}
	args := []any{matchingTaskAuthoritySource, d.migrationBucketCount}
	args = append(args, d.migrationKeyArgs(namespaceID, taskQueue, taskType)...)
	if _, err := d.Session.Query(
		switchTasksTable(templateInitializeMatchingTaskSourceAuthority, d.version),
		args...,
	).WithContext(ctx).MapScanCAS(make(map[string]any)); err != nil {
		return record, gocql.ConvertError("InitializeMatchingTaskSourceAuthorityForCutover", err)
	}
	record, err = d.readMigrationAuthority(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return record, gocql.ConvertError("ReadMatchingTaskSourceAuthorityAfterCutoverInitialize", err)
	}
	return record, nil
}

func (d *taskQueueStore) sealForTargetCutover(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
) (matchingTaskSourceAuthorityRecord, error) {
	record, err := d.initializeSourceAuthorityForCutover(
		ctx,
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
	)
	if err != nil {
		return record, err
	}
	if record.authority == matchingTaskAuthoritySealing {
		if record.rangeID == request.RangeID &&
			record.taskQueueEncoding == request.TaskQueueInfo.EncodingType.String() &&
			bytes.Equal(record.taskQueue, request.TaskQueueInfo.Data) {
			return record, nil
		}
		return record, matchingTaskSourceCutoverConflict(request, record)
	}
	if record.authority == matchingTaskAuthorityTarget {
		return record, nil
	}
	if record.authority != matchingTaskAuthoritySource || record.bucketCount != d.migrationBucketCount {
		return record, matchingTaskSourceCutoverConflict(request, record)
	}

	usingTTL := ""
	args := make([]any, 0, 16)
	if record.ttl > 0 {
		usingTTL = " USING TTL ?"
		args = append(args, record.ttl)
	}
	query := `UPDATE tasks_v2` + usingTTL + ` SET range_id = ?, task_queue = ?, task_queue_encoding = ?,
		migration_authority = ?, migration_bucket_count = ?, migration_timestamp = null
		WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ?
		AND type = ? AND pass = 0 AND task_id = ?
		IF range_id = ? AND migration_authority = ? AND migration_bucket_count = ?`
	args = append(args,
		request.RangeID,
		request.TaskQueueInfo.Data,
		request.TaskQueueInfo.EncodingType.String(),
		matchingTaskAuthoritySealing,
		d.migrationBucketCount,
	)
	args = append(args, d.migrationKeyArgs(request.NamespaceID, request.TaskQueue, request.TaskType)...)
	expectedRangeID := request.PrevRangeID
	if record.rangeID == request.RangeID {
		expectedRangeID = record.rangeID
	}
	args = append(args, expectedRangeID, matchingTaskAuthoritySource, d.migrationBucketCount)
	applied, err := d.Session.Query(switchTasksTable(query, d.version), args...).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return record, gocql.ConvertError("SealMatchingTaskSourceAuthority", err)
	}
	record, readErr := d.readMigrationAuthority(ctx, request.NamespaceID, request.TaskQueue, request.TaskType)
	if readErr != nil {
		return record, gocql.ConvertError("ReadMatchingTaskSourceAuthorityAfterSeal", readErr)
	}
	if !applied && (record.authority != matchingTaskAuthoritySealing || record.rangeID != request.RangeID) {
		return record, matchingTaskSourceCutoverConflict(request, record)
	}
	return record, nil
}

func (d *taskQueueStore) reserveRepairTimestamp(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	minimum int64,
) (int64, error) {
	record, err := d.readMigrationAuthority(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return 0, gocql.ConvertError("ReadMatchingTaskRepairTimestamp", err)
	}
	if record.authority != matchingTaskAuthoritySealing || record.bucketCount != d.migrationBucketCount {
		return 0, &p.ConditionFailedError{Msg: "cassandra matching task source is not sealed for repair"}
	}
	if record.migrationTimestamp > 0 {
		return record.migrationTimestamp, nil
	}
	if minimum >= math.MaxInt64-2 {
		return 0, errors.New("cassandra matching task repair timestamp overflow")
	}
	timestamp := max(time.Now().UTC().UnixMicro(), minimum+1)
	if timestamp == math.MaxInt64 {
		return 0, errors.New("cassandra matching task repair timestamp overflow")
	}
	args := []any{timestamp}
	args = append(args, d.migrationKeyArgs(namespaceID, taskQueue, taskType)...)
	args = append(args, matchingTaskAuthoritySealing, d.migrationBucketCount)
	if _, err := d.Session.Query(
		switchTasksTable(templateReserveMatchingTaskRepairTimestamp, d.version),
		args...,
	).WithContext(ctx).MapScanCAS(make(map[string]any)); err != nil {
		return 0, gocql.ConvertError("ReserveMatchingTaskRepairTimestamp", err)
	}
	record, err = d.readMigrationAuthority(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return 0, gocql.ConvertError("ReadMatchingTaskRepairTimestampAfterReserve", err)
	}
	if record.authority != matchingTaskAuthoritySealing || record.migrationTimestamp <= minimum {
		return 0, &p.ConditionFailedError{Msg: "Cassandra matching task repair timestamp reservation conflicted"}
	}
	return record.migrationTimestamp, nil
}

func (d *taskQueueStore) publishTargetAuthority(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
	repairTimestamp int64,
) error {
	record, err := d.readMigrationAuthority(ctx, request.NamespaceID, request.TaskQueue, request.TaskType)
	if err != nil {
		return gocql.ConvertError("ReadMatchingTaskSourceAuthorityBeforePublish", err)
	}
	usingTTL := ""
	args := make([]any, 0, 16)
	if record.ttl > 0 {
		usingTTL = " USING TTL ?"
		args = append(args, record.ttl)
	}
	query := `UPDATE tasks_v2` + usingTTL + ` SET migration_authority = ?, migration_bucket_count = ?, migration_timestamp = ?
		WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ?
		AND type = ? AND pass = 0 AND task_id = ?
		IF range_id = ? AND migration_authority = ? AND migration_bucket_count = ? AND migration_timestamp = ?`
	args = append(args, matchingTaskAuthorityTarget, d.migrationBucketCount, repairTimestamp)
	args = append(args, d.migrationKeyArgs(request.NamespaceID, request.TaskQueue, request.TaskType)...)
	args = append(
		args,
		request.RangeID,
		matchingTaskAuthoritySealing,
		d.migrationBucketCount,
		repairTimestamp,
	)
	applied, err := d.Session.Query(
		switchTasksTable(query, d.version),
		args...,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("PublishMatchingTaskTargetAuthority", err)
	}
	if applied {
		return nil
	}
	record, err = d.readMigrationAuthority(ctx, request.NamespaceID, request.TaskQueue, request.TaskType)
	if err != nil {
		return gocql.ConvertError("ReadMatchingTaskSourceAuthorityAfterPublish", err)
	}
	if record.authority == matchingTaskAuthorityTarget && record.rangeID == request.RangeID &&
		record.bucketCount == d.migrationBucketCount && record.migrationTimestamp == repairTimestamp {
		return nil
	}
	return matchingTaskSourceCutoverConflict(request, record)
}

func matchingTaskSourceCutoverConflict(
	request *p.InternalUpdateTaskQueueRequest,
	record matchingTaskSourceAuthorityRecord,
) error {
	return &p.ConditionFailedError{Msg: fmt.Sprintf(
		"Cassandra matching task source cutover for queue %q expected range %d -> %d with source authority; got range %d authority %d buckets %d",
		request.TaskQueue,
		request.PrevRangeID,
		request.RangeID,
		record.rangeID,
		record.authority,
		record.bucketCount,
	)}
}

func matchingTaskBlob(data []byte, encoding string) *commonpb.DataBlob {
	return p.NewDataBlob(bytes.Clone(data), encoding)
}
