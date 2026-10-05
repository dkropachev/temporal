package cassandra

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/primitives/timestamp"
	"golang.org/x/sync/errgroup"
)

const (
	matchingTaskMetadataStateActive = 1
	matchingTaskMetadataStateFenced = 2
	matchingTaskAuthoritySource     = 1
	matchingTaskAuthoritySealing    = 2
	matchingTaskAuthorityTarget     = 3
	matchingTaskV3PageTokenVersion  = byte(1)
	matchingTaskV3PageTokenLength   = 22
	matchingTaskV3PageTokenPrefix   = "MTV3"
)

type matchingTaskStoreV3 struct {
	session          gocql.Session
	fair             bool
	bucketCount      int
	table            string
	authority        int
	enforceAuthority bool
}

type matchingTaskV3Metadata struct {
	bucket                int16
	rangeID               int64
	taskQueue             []byte
	taskQueueEncoding     string
	state                 int
	bucketCount           int16
	authority             int
	nextRangeID           int64
	nextRangeIDValid      bool
	nextTaskQueue         []byte
	nextTaskQueueEncoding string
	nextTTL               int64
	nextTTLValid          bool
}

type matchingTaskV3Row struct {
	pass int64
	id   int64
	blob *commonpb.DataBlob
}

type matchingTaskV3PageToken struct {
	fair bool
	pass int64
	id   int64
}

func newMatchingTaskStoreV3(
	session gocql.Session,
	fair bool,
	bucketCount int,
) *matchingTaskStoreV3 {
	table := matchingTaskV3TableName
	if fair {
		table = matchingTaskV3FairTableName
	}
	return &matchingTaskStoreV3{
		session:     session,
		fair:        fair,
		bucketCount: bucketCount,
		table:       table,
		authority:   matchingTaskAuthorityTarget,
	}
}

func (d *matchingTaskStoreV3) metadataKeyArgs(
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	bucket int16,
) []any {
	args := []any{namespaceID, taskQueue, taskType, bucket, rowTypeTaskQueue}
	if d.fair {
		args = append(args, int64(0))
	}
	return append(args, taskQueueTaskID)
}

func (d *matchingTaskStoreV3) metadataSelectQuery() string {
	clustering := "type = ? AND task_id = ?"
	if d.fair {
		clustering = "type = ? AND pass = ? AND task_id = ?"
	}
	return fmt.Sprintf(
		"SELECT range_id, task_queue, task_queue_encoding, metadata_state, bucket_count, migration_authority, next_range_id, next_task_queue, next_task_queue_encoding, TTL(next_task_queue) "+
			"FROM %s WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? AND storage_bucket = ? AND %s",
		d.table,
		clustering,
	)
}

func (d *matchingTaskStoreV3) metadataInsertQuery(ttl bool) string {
	columns := "namespace_id, task_queue_name, task_queue_type, storage_bucket, type, task_id, range_id, metadata_state, bucket_count, migration_authority, task_queue, task_queue_encoding"
	values := "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
	if d.fair {
		columns = "namespace_id, task_queue_name, task_queue_type, storage_bucket, type, pass, task_id, range_id, metadata_state, bucket_count, migration_authority, task_queue, task_queue_encoding"
		values = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
	}
	usingTTL := ""
	if ttl {
		usingTTL = " USING TTL ?"
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)%s IF NOT EXISTS", d.table, columns, values, usingTTL)
}

func (d *matchingTaskStoreV3) metadataUpdateQuery(ttl bool) string {
	clustering := "type = ? AND task_id = ?"
	if d.fair {
		clustering = "type = ? AND pass = ? AND task_id = ?"
	}
	usingTTL := ""
	if ttl {
		usingTTL = " USING TTL ?"
	}
	return fmt.Sprintf(
		"UPDATE %s%s SET range_id = ?, metadata_state = ?, migration_authority = ?, bucket_count = ?, task_queue = ?, task_queue_encoding = ?, "+
			"next_range_id = null, next_task_queue = null, next_task_queue_encoding = null "+
			"WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? AND storage_bucket = ? AND %s "+
			"IF range_id = ? AND metadata_state = ? AND bucket_count = ? AND migration_authority = ?",
		d.table,
		usingTTL,
		clustering,
	)
}

func (d *matchingTaskStoreV3) metadataFenceQuery(ttl bool) string {
	clustering := "type = ? AND task_id = ?"
	if d.fair {
		clustering = "type = ? AND pass = ? AND task_id = ?"
	}
	usingTTL := ""
	if ttl {
		usingTTL = " USING TTL ?"
	}
	return fmt.Sprintf(
		"UPDATE %s%s SET metadata_state = ?, next_range_id = ?, next_task_queue = ?, next_task_queue_encoding = ?, "+
			"bucket_count = ?, migration_authority = ? "+
			"WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? AND storage_bucket = ? AND %s "+
			"IF range_id = ? AND metadata_state = ? AND bucket_count = ? AND migration_authority = ?",
		d.table,
		usingTTL,
		clustering,
	)
}

func (d *matchingTaskStoreV3) metadataDeleteQuery() string {
	clustering := "type = ? AND task_id = ?"
	if d.fair {
		clustering = "type = ? AND pass = ? AND task_id = ?"
	}
	return fmt.Sprintf(
		"DELETE FROM %s WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? "+
			"AND storage_bucket = ? AND %s IF range_id = ? AND metadata_state = ? AND bucket_count = ? AND migration_authority = ?",
		d.table,
		clustering,
	)
}

func (d *matchingTaskStoreV3) metadataClearTransitionQuery() string {
	clustering := "type = ? AND task_id = ?"
	if d.fair {
		clustering = "type = ? AND pass = ? AND task_id = ?"
	}
	return fmt.Sprintf(
		"DELETE FROM %s WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? AND storage_bucket = ? AND %s "+
			"IF range_id = ? AND metadata_state = ? AND next_range_id = ? AND bucket_count = ? AND migration_authority = ?",
		d.table,
		clustering,
	)
}

func (d *matchingTaskStoreV3) metadataPromoteAuthorityQuery() string {
	clustering := "type = ? AND task_id = ?"
	if d.fair {
		clustering = "type = ? AND pass = ? AND task_id = ?"
	}
	return fmt.Sprintf(
		"UPDATE %s SET migration_authority = ? WHERE namespace_id = ? AND task_queue_name = ? "+
			"AND task_queue_type = ? AND storage_bucket = ? AND %s "+
			"IF range_id = ? AND metadata_state = ? AND bucket_count = ? AND migration_authority = ?",
		d.table,
		clustering,
	)
}

func (d *matchingTaskStoreV3) metadataTTL(
	kind enumspb.TaskQueueKind,
	expiryTime *time.Time,
) int64 {
	if kind != enumspb.TASK_QUEUE_KIND_STICKY || expiryTime == nil {
		return 0
	}
	ttl := int64(math.Ceil(time.Until(*expiryTime).Seconds()))
	if ttl < 1 {
		return 1
	}
	if ttl > maxCassandraTTL {
		return maxCassandraTTL
	}
	return ttl
}

func (d *matchingTaskStoreV3) CreateTaskQueue(
	ctx context.Context,
	request *p.InternalCreateTaskQueueRequest,
) error {
	bucket, err := matchingTaskStorageBucket(request.RangeID, d.bucketCount)
	if err != nil {
		return err
	}
	args := []any{request.NamespaceID, request.TaskQueue, request.TaskType, bucket, rowTypeTaskQueue}
	if d.fair {
		args = append(args, int64(0))
	}
	args = append(args,
		taskQueueTaskID,
		request.RangeID,
		matchingTaskMetadataStateActive,
		int16(d.bucketCount),
		d.authority,
		request.TaskQueueInfo.Data,
		request.TaskQueueInfo.EncodingType.String(),
	)
	previous := make(map[string]any)
	applied, err := d.session.Query(d.metadataInsertQuery(false), args...).WithContext(ctx).MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("CreateTaskQueueV3", err)
	}
	if !applied {
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"CreateTaskQueueV3: TaskQueue:%v, TaskQueueType:%v, PreviousRangeID:%v",
			request.TaskQueue,
			request.TaskType,
			previous["range_id"],
		)}
	}
	return nil
}

func (d *matchingTaskStoreV3) GetTaskQueue(
	ctx context.Context,
	request *p.InternalGetTaskQueueRequest,
) (*p.InternalGetTaskQueueResponse, error) {
	metadata, err := d.getCurrentMetadata(ctx, request.NamespaceID, request.TaskQueue, request.TaskType, true)
	if err != nil {
		return nil, err
	}
	if d.enforceAuthority && metadata.authority != matchingTaskAuthorityTarget {
		return nil, serviceerror.NewUnavailablef(
			"Cassandra matching task queue %q has not completed target cutover",
			request.TaskQueue,
		)
	}
	return &p.InternalGetTaskQueueResponse{
		RangeID:       metadata.rangeID,
		TaskQueueInfo: p.NewDataBlob(metadata.taskQueue, metadata.taskQueueEncoding),
	}, nil
}

func (d *matchingTaskStoreV3) ensureActiveMetadata(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	rangeID int64,
	blob *commonpb.DataBlob,
) error {
	metadata, err := d.getCurrentMetadata(ctx, namespaceID, taskQueue, taskType, true)
	if err == nil {
		if metadata.rangeID == rangeID {
			return nil
		}
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"Cassandra matching task target has range ID %d, expected %d",
			metadata.rangeID,
			rangeID,
		)}
	}
	var notFound *serviceerror.NotFound
	if !errors.As(err, &notFound) {
		return err
	}
	ttl, err := d.sourceMetadataTTL(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return err
	}
	bucket, err := matchingTaskStorageBucket(rangeID, d.bucketCount)
	if err != nil {
		return err
	}
	return d.activateMetadata(ctx, namespaceID, taskQueue, taskType, bucket, rangeID, blob, ttl)
}

func (d *matchingTaskStoreV3) sourceMetadataTTL(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
) (int64, error) {
	query := `SELECT TTL(task_queue) FROM tasks_v2 WHERE namespace_id = ? AND task_queue_name = ? ` +
		`AND task_queue_type = ? AND type = ? AND pass = 0 AND task_id = ?`
	version := matchingTaskVersion1
	if d.fair {
		version = matchingTaskVersion2
	}
	var ttl nullableInt64
	if err := d.session.Query(
		switchTasksTable(query, version),
		namespaceID,
		taskQueue,
		taskType,
		rowTypeTaskQueue,
		taskQueueTaskID,
	).WithContext(ctx).Scan(&ttl); err != nil {
		return 0, gocql.ConvertError("GetSourceTaskQueueTTL", err)
	}
	if !ttl.valid {
		return 0, nil
	}
	return ttl.value, nil
}

func (d *matchingTaskStoreV3) UpdateTaskQueue(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
) (*p.UpdateTaskQueueResponse, error) {
	oldBucket, err := matchingTaskStorageBucket(request.PrevRangeID, d.bucketCount)
	if err != nil {
		return nil, err
	}
	newBucket, err := matchingTaskStorageBucket(request.RangeID, d.bucketCount)
	if err != nil {
		return nil, err
	}
	ttl := int64(0)
	if request.ExpiryTime != nil {
		expiry := timestamp.TimeValue(request.ExpiryTime)
		ttl = d.metadataTTL(request.TaskQueueKind, &expiry)
	}

	if oldBucket == newBucket {
		if err := d.updateMetadata(
			ctx,
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			oldBucket,
			request.RangeID,
			request.TaskQueueInfo,
			request.PrevRangeID,
			matchingTaskMetadataStateActive,
			ttl,
		); err != nil {
			return nil, err
		}
		return &p.UpdateTaskQueueResponse{}, nil
	}

	if err := d.fenceMetadata(ctx, request, oldBucket, ttl); err != nil {
		return nil, err
	}
	if err := d.activateMetadata(
		ctx,
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		newBucket,
		request.RangeID,
		request.TaskQueueInfo,
		ttl,
	); err != nil {
		return nil, err
	}
	if err := d.clearTransition(ctx, request.NamespaceID, request.TaskQueue, request.TaskType, oldBucket, request.PrevRangeID, request.RangeID); err != nil {
		return nil, err
	}
	return &p.UpdateTaskQueueResponse{}, nil
}

func (d *matchingTaskStoreV3) ListTaskQueue(
	_ context.Context,
	_ *p.ListTaskQueueRequest,
) (*p.InternalListTaskQueueResponse, error) {
	return nil, serviceerror.NewUnavailable("unsupported operation")
}

func (d *matchingTaskStoreV3) DeleteTaskQueue(
	ctx context.Context,
	request *p.DeleteTaskQueueRequest,
) error {
	metadata, err := d.getCurrentMetadata(
		ctx,
		request.TaskQueue.NamespaceID,
		request.TaskQueue.TaskQueueName,
		request.TaskQueue.TaskQueueType,
		true,
	)
	if err != nil {
		return err
	}
	if metadata.rangeID != request.RangeID {
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"DeleteTaskQueueV3 failed: expected_range_id=%v but found %v",
			request.RangeID,
			metadata.rangeID,
		)}
	}
	bucket, err := matchingTaskStorageBucket(request.RangeID, d.bucketCount)
	if err != nil {
		return err
	}
	args := d.metadataKeyArgs(
		request.TaskQueue.NamespaceID,
		request.TaskQueue.TaskQueueName,
		request.TaskQueue.TaskQueueType,
		bucket,
	)
	args = append(args, request.RangeID, matchingTaskMetadataStateActive, int16(d.bucketCount), d.authority)
	previous := make(map[string]any)
	applied, err := d.session.Query(d.metadataDeleteQuery(), args...).WithContext(ctx).MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("DeleteTaskQueueV3", err)
	}
	if !applied {
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"DeleteTaskQueueV3 failed: expected_range_id=%v but found %+v",
			request.RangeID,
			previous,
		)}
	}
	return nil
}

func (d *matchingTaskStoreV3) CreateTasks(
	ctx context.Context,
	request *p.InternalCreateTasksRequest,
) (*p.CreateTasksResponse, error) {
	bucket, err := matchingTaskStorageBucket(request.RangeID, d.bucketCount)
	if err != nil {
		return nil, err
	}
	batch := d.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	for _, task := range request.Tasks {
		if d.fair && task.TaskPass == 0 {
			return nil, serviceerror.NewInternal("invalid fair queue task missing pass number")
		}
		if !d.fair && task.TaskPass != 0 {
			return nil, serviceerror.NewInternal("invalid non-fair queue task with pass number")
		}
		d.addCreateTaskQuery(batch, request, task, bucket)
	}

	batch.Query(
		d.createTasksMetadataQuery(),
		append(
			[]any{
				request.TaskQueueInfo.Data,
				request.TaskQueueInfo.EncodingType.String(),
			},
			append(
				d.metadataKeyArgs(request.NamespaceID, request.TaskQueue, request.TaskType, bucket),
				request.RangeID,
				matchingTaskMetadataStateActive,
				int16(d.bucketCount),
				d.authority,
			)...,
		)...,
	)
	previous := make(map[string]any)
	applied, _, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return nil, gocql.ConvertError("CreateTasksV3", err)
	}
	if !applied {
		return nil, &p.ConditionFailedError{Msg: fmt.Sprintf(
			"CreateTasksV3 failed. TaskQueue: %v, taskQueueType: %v, rangeID: %v, db rangeID: %v",
			request.TaskQueue,
			request.TaskType,
			request.RangeID,
			previous["range_id"],
		)}
	}
	return &p.CreateTasksResponse{UpdatedMetadata: true}, nil
}

func (d *matchingTaskStoreV3) GetTasks(
	ctx context.Context,
	request *p.GetTasksRequest,
) (*p.InternalGetTasksResponse, error) {
	if request.PageSize < 1 {
		return nil, serviceerror.NewInternal("invalid GetTasks request: PageSize must be positive")
	}
	if d.fair {
		if request.InclusiveMinPass < 1 {
			return nil, serviceerror.NewInternal("invalid GetTasks request on fair queue: InclusiveMinPass must be >= 1")
		}
		if request.ExclusiveMaxTaskID != math.MaxInt64 {
			return nil, serviceerror.NewInternal("invalid GetTasks request on fair queue: ExclusiveMaxTaskID is not supported")
		}
	} else if request.InclusiveMinPass != 0 {
		return nil, serviceerror.NewInternal("invalid GetTasks request on queue: InclusiveMinPass is not supported")
	}

	lowerPass := request.InclusiveMinPass
	lowerID := request.InclusiveMinTaskID
	if len(request.NextPageToken) != 0 {
		token, err := decodeMatchingTaskV3PageToken(request.NextPageToken, d.fair)
		if err != nil {
			return nil, err
		}
		lowerPass = token.pass
		lowerID = token.id
	}

	buckets := d.readBuckets(request)
	rowsByBucket := make([][]matchingTaskV3Row, len(buckets))
	group, groupCtx := errgroup.WithContext(ctx)
	for index, bucket := range buckets {
		index := index
		bucket := bucket
		group.Go(func() error {
			rows, err := d.readTaskBucket(groupCtx, request, bucket, lowerPass, lowerID, request.PageSize+1)
			if err != nil {
				return err
			}
			rowsByBucket[index] = rows
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	var rows []matchingTaskV3Row
	for _, bucketRows := range rowsByBucket {
		rows = append(rows, bucketRows...)
	}
	sort.Slice(rows, func(i, j int) bool {
		if d.fair && rows[i].pass != rows[j].pass {
			return rows[i].pass < rows[j].pass
		}
		return rows[i].id < rows[j].id
	})
	deduplicatedRows, err := deduplicateMatchingTaskV3Rows(rows, d.fair)
	if err != nil {
		return nil, err
	}
	rows = deduplicatedRows

	response := &p.InternalGetTasksResponse{
		Tasks: make([]*commonpb.DataBlob, 0, min(len(rows), request.PageSize)),
	}
	pageLength := min(len(rows), request.PageSize)
	for _, row := range rows[:pageLength] {
		response.Tasks = append(response.Tasks, row.blob)
	}
	if len(rows) > pageLength {
		last := rows[pageLength-1]
		nextPass, nextID, err := nextMatchingTaskV3Level(last.pass, last.id, d.fair)
		if err != nil {
			return nil, err
		}
		response.NextPageToken = encodeMatchingTaskV3PageToken(matchingTaskV3PageToken{
			fair: d.fair,
			pass: nextPass,
			id:   nextID,
		})
	}
	return response, nil
}

func (d *matchingTaskStoreV3) readBuckets(request *p.GetTasksRequest) []int16 {
	all := func() []int16 {
		buckets := make([]int16, d.bucketCount)
		for bucket := range d.bucketCount {
			buckets[bucket] = int16(bucket)
		}
		return buckets
	}
	if d.fair || request.TaskIDRangeSize < 1 || request.TaskIDMaxBatchSize < 1 ||
		int64(request.TaskIDMaxBatchSize) > request.TaskIDRangeSize || request.InclusiveMinTaskID < 1 ||
		request.ExclusiveMaxTaskID <= request.InclusiveMinTaskID {
		return all()
	}
	firstRange := (request.InclusiveMinTaskID-1)/request.TaskIDRangeSize + 1
	lastTaskID := request.ExclusiveMaxTaskID - 1
	if request.ExclusiveMaxTaskID == math.MaxInt64 {
		lastTaskID = math.MaxInt64 - 1
	}
	lastRange := (lastTaskID-1)/request.TaskIDRangeSize + 1
	if lastRange-firstRange+2 >= int64(d.bucketCount) {
		return all()
	}
	// A batch may renew its lease midway through ID assignment, placing tail IDs
	// from one allocation range in the following range's storage bucket.
	selected := make(map[int16]struct{}, int(lastRange-firstRange+2))
	endRange := lastRange + 1
	for rangeID := firstRange; ; rangeID++ {
		bucket, err := matchingTaskStorageBucket(rangeID, d.bucketCount)
		if err != nil {
			return all()
		}
		selected[bucket] = struct{}{}
		if rangeID == endRange {
			break
		}
	}
	buckets := make([]int16, 0, len(selected))
	for bucket := range selected {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
	return buckets
}

func (d *matchingTaskStoreV3) CompleteTasksLessThan(
	ctx context.Context,
	request *p.CompleteTasksLessThanRequest,
) (int, error) {
	if d.fair && request.ExclusiveMaxPass < 1 {
		return 0, serviceerror.NewInternal("invalid CompleteTasksLessThan request on fair queue")
	}
	if !d.fair && request.ExclusiveMaxPass != 0 {
		return 0, serviceerror.NewInternal("invalid CompleteTasksLessThan request on queue")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	for bucket := range d.bucketCount {
		bucket := int16(bucket)
		group.Go(func() error {
			query, args := d.completeTasksQuery(request, bucket)
			if err := d.session.Query(query, args...).WithContext(groupCtx).Exec(); err != nil {
				return gocql.ConvertError("CompleteTasksLessThanV3", err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return 0, err
	}
	return p.UnknownNumRowsAffected, nil
}

func (d *matchingTaskStoreV3) getMetadataAtBucket(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	bucket int16,
) (*matchingTaskV3Metadata, error) {
	metadata := &matchingTaskV3Metadata{bucket: bucket}
	var nextRangeID nullableInt64
	var nextTTL nullableInt64
	err := d.session.Query(
		d.metadataSelectQuery(),
		d.metadataKeyArgs(namespaceID, taskQueue, taskType, bucket)...,
	).WithContext(ctx).Scan(
		&metadata.rangeID,
		&metadata.taskQueue,
		&metadata.taskQueueEncoding,
		&metadata.state,
		&metadata.bucketCount,
		&metadata.authority,
		&nextRangeID,
		&metadata.nextTaskQueue,
		&metadata.nextTaskQueueEncoding,
		&nextTTL,
	)
	if err != nil {
		return nil, err
	}
	metadata.nextRangeID = nextRangeID.value
	metadata.nextRangeIDValid = nextRangeID.valid
	metadata.nextTTL = nextTTL.value
	metadata.nextTTLValid = nextTTL.valid
	return metadata, nil
}

//nolint:revive // Metadata resolution validates all buckets and repairs resumable range transitions.
func (d *matchingTaskStoreV3) getCurrentMetadata(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	recoverTransition bool,
) (*matchingTaskV3Metadata, error) {
	rows := make([]*matchingTaskV3Metadata, d.bucketCount)
	group, groupCtx := errgroup.WithContext(ctx)
	for bucket := range d.bucketCount {
		bucket := bucket
		group.Go(func() error {
			metadata, err := d.getMetadataAtBucket(groupCtx, namespaceID, taskQueue, taskType, int16(bucket))
			if gocql.IsNotFoundError(err) {
				return nil
			}
			if err != nil {
				return gocql.ConvertError("GetTaskQueueV3", err)
			}
			rows[bucket] = metadata
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	var active *matchingTaskV3Metadata
	var pending *matchingTaskV3Metadata
	var pendingRows []*matchingTaskV3Metadata
	for _, row := range rows {
		if row == nil {
			continue
		}
		if row.bucketCount != int16(d.bucketCount) {
			return nil, serviceerror.NewUnavailablef(
				"Cassandra matching task bucket count changed for task queue %q: stored %d configured %d",
				taskQueue,
				row.bucketCount,
				d.bucketCount,
			)
		}
		if row.authority != matchingTaskAuthoritySource && row.authority != matchingTaskAuthorityTarget {
			return nil, serviceerror.NewUnavailablef(
				"Cassandra matching task authority is invalid for task queue %q: %d",
				taskQueue,
				row.authority,
			)
		}
		if row.state == matchingTaskMetadataStateActive {
			if active != nil {
				return nil, serviceerror.NewUnavailablef(
					"multiple active Cassandra matching task fences for task queue %q: ranges %d and %d",
					taskQueue,
					active.rangeID,
					row.rangeID,
				)
			}
			active = row
		}
		if row.state == matchingTaskMetadataStateFenced && row.nextRangeIDValid &&
			(pending == nil || row.nextRangeID > pending.nextRangeID) {
			pending = row
		}
		if row.state == matchingTaskMetadataStateFenced && row.nextRangeIDValid {
			pendingRows = append(pendingRows, row)
		}
	}
	if active != nil {
		for _, row := range pendingRows {
			if row.nextRangeID > active.rangeID {
				return nil, serviceerror.NewUnavailablef(
					"Cassandra matching task fence transition to range %d conflicts with active range %d",
					row.nextRangeID,
					active.rangeID,
				)
			}
			if err := d.clearTransition(
				ctx,
				namespaceID,
				taskQueue,
				taskType,
				row.bucket,
				row.rangeID,
				row.nextRangeID,
			); err != nil {
				return nil, err
			}
		}
		return active, nil
	}
	if pending != nil && recoverTransition {
		bucket, err := matchingTaskStorageBucket(pending.nextRangeID, d.bucketCount)
		if err != nil {
			return nil, err
		}
		ttl := int64(0)
		if pending.nextTTLValid {
			ttl = pending.nextTTL
		}
		if err := d.activateMetadata(
			ctx,
			namespaceID,
			taskQueue,
			taskType,
			bucket,
			pending.nextRangeID,
			p.NewDataBlob(pending.nextTaskQueue, pending.nextTaskQueueEncoding),
			ttl,
		); err != nil {
			return nil, err
		}
		return d.getCurrentMetadata(ctx, namespaceID, taskQueue, taskType, false)
	}
	return nil, serviceerror.NewNotFound("Cassandra matching task queue metadata not found")
}

func (d *matchingTaskStoreV3) clearTransition(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	bucket int16,
	rangeID int64,
	nextRangeID int64,
) error {
	args := d.metadataKeyArgs(namespaceID, taskQueue, taskType, bucket)
	args = append(args, rangeID, matchingTaskMetadataStateFenced, nextRangeID, int16(d.bucketCount), d.authority)
	previous := make(map[string]any)
	_, err := d.session.Query(d.metadataClearTransitionQuery(), args...).WithContext(ctx).MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("ClearTaskQueueV3Transition", err)
	}
	return nil
}

func (d *matchingTaskStoreV3) updateMetadata(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	bucket int16,
	newRangeID int64,
	blob *commonpb.DataBlob,
	expectedRangeID int64,
	expectedState int,
	ttl int64,
) error {
	args := make([]any, 0, 16)
	if ttl > 0 {
		args = append(args, ttl)
	}
	args = append(args,
		newRangeID,
		matchingTaskMetadataStateActive,
		d.authority,
		int16(d.bucketCount),
		blob.Data,
		blob.EncodingType.String(),
	)
	args = append(args, d.metadataKeyArgs(namespaceID, taskQueue, taskType, bucket)...)
	args = append(args, expectedRangeID, expectedState, int16(d.bucketCount), d.authority)
	previous := make(map[string]any)
	applied, err := d.session.Query(d.metadataUpdateQuery(ttl > 0), args...).WithContext(ctx).MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("UpdateTaskQueueV3", err)
	}
	if !applied {
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"UpdateTaskQueueV3 failed. name: %v, type: %v, expected rangeID: %v, found: %+v",
			taskQueue,
			taskType,
			expectedRangeID,
			previous,
		)}
	}
	return nil
}

func (d *matchingTaskStoreV3) promoteSourceAuthority(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
) error {
	bucket, err := matchingTaskStorageBucket(request.RangeID, d.bucketCount)
	if err != nil {
		return err
	}
	args := []any{matchingTaskAuthorityTarget}
	args = append(args, d.metadataKeyArgs(request.NamespaceID, request.TaskQueue, request.TaskType, bucket)...)
	args = append(
		args,
		request.RangeID,
		matchingTaskMetadataStateActive,
		int16(d.bucketCount),
		matchingTaskAuthoritySource,
	)
	applied, err := d.session.Query(d.metadataPromoteAuthorityQuery(), args...).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("PromoteTaskQueueV3Authority", err)
	}
	if applied {
		return nil
	}
	metadata, err := d.getCurrentMetadata(ctx, request.NamespaceID, request.TaskQueue, request.TaskType, true)
	if err != nil {
		return err
	}
	if metadata.rangeID == request.RangeID && metadata.state == matchingTaskMetadataStateActive &&
		metadata.bucketCount == int16(d.bucketCount) && metadata.authority == matchingTaskAuthorityTarget &&
		metadata.taskQueueEncoding == request.TaskQueueInfo.EncodingType.String() &&
		bytes.Equal(metadata.taskQueue, request.TaskQueueInfo.Data) {
		return nil
	}
	return &p.ConditionFailedError{Msg: fmt.Sprintf(
		"PromoteTaskQueueV3Authority failed for queue %q at range %d; found %+v",
		request.TaskQueue,
		request.RangeID,
		metadata,
	)}
}

func (d *matchingTaskStoreV3) fenceMetadata(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
	bucket int16,
	ttl int64,
) error {
	args := make([]any, 0, 16)
	if ttl > 0 {
		args = append(args, ttl)
	}
	args = append(args,
		matchingTaskMetadataStateFenced,
		request.RangeID,
		request.TaskQueueInfo.Data,
		request.TaskQueueInfo.EncodingType.String(),
		int16(d.bucketCount),
		d.authority,
	)
	args = append(args, d.metadataKeyArgs(request.NamespaceID, request.TaskQueue, request.TaskType, bucket)...)
	args = append(args, request.PrevRangeID, matchingTaskMetadataStateActive, int16(d.bucketCount), d.authority)
	previous := make(map[string]any)
	applied, err := d.session.Query(d.metadataFenceQuery(ttl > 0), args...).WithContext(ctx).MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("FenceTaskQueueV3", err)
	}
	if applied {
		return nil
	}
	metadata, readErr := d.getMetadataAtBucket(ctx, request.NamespaceID, request.TaskQueue, request.TaskType, bucket)
	if readErr == nil && metadata.rangeID == request.PrevRangeID &&
		metadata.state == matchingTaskMetadataStateFenced &&
		metadata.authority == d.authority &&
		metadata.nextRangeIDValid && metadata.nextRangeID == request.RangeID &&
		bytes.Equal(metadata.nextTaskQueue, request.TaskQueueInfo.Data) &&
		metadata.nextTaskQueueEncoding == request.TaskQueueInfo.EncodingType.String() {
		return nil
	}
	return &p.ConditionFailedError{Msg: fmt.Sprintf(
		"FenceTaskQueueV3 failed. name: %v, type: %v, expected rangeID: %v, found: %+v",
		request.TaskQueue,
		request.TaskType,
		request.PrevRangeID,
		previous,
	)}
}

func (d *matchingTaskStoreV3) activateMetadata(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	bucket int16,
	rangeID int64,
	blob *commonpb.DataBlob,
	ttl int64,
) error {
	metadata, err := d.getMetadataAtBucket(ctx, namespaceID, taskQueue, taskType, bucket)
	if gocql.IsNotFoundError(err) {
		args := []any{namespaceID, taskQueue, taskType, bucket, rowTypeTaskQueue}
		if d.fair {
			args = append(args, int64(0))
		}
		args = append(args,
			taskQueueTaskID,
			rangeID,
			matchingTaskMetadataStateActive,
			int16(d.bucketCount),
			d.authority,
			blob.Data,
			blob.EncodingType.String(),
		)
		if ttl > 0 {
			args = append(args, ttl)
		}
		previous := make(map[string]any)
		applied, insertErr := d.session.Query(d.metadataInsertQuery(ttl > 0), args...).WithContext(ctx).MapScanCAS(previous)
		if insertErr != nil {
			return gocql.ConvertError("ActivateTaskQueueV3", insertErr)
		}
		if applied {
			return nil
		}
		metadata, err = d.getMetadataAtBucket(ctx, namespaceID, taskQueue, taskType, bucket)
	}
	if err != nil {
		return gocql.ConvertError("ActivateTaskQueueV3", err)
	}
	if metadata.state == matchingTaskMetadataStateActive && metadata.rangeID == rangeID {
		if metadata.authority != d.authority {
			return &p.ConditionFailedError{Msg: fmt.Sprintf(
				"ActivateTaskQueueV3 cannot change authority from %d to %d for queue %q",
				metadata.authority,
				d.authority,
				taskQueue,
			)}
		}
		if bytes.Equal(metadata.taskQueue, blob.Data) && metadata.taskQueueEncoding == blob.EncodingType.String() {
			return nil
		}
		return d.updateMetadata(
			ctx,
			namespaceID,
			taskQueue,
			taskType,
			bucket,
			rangeID,
			blob,
			rangeID,
			matchingTaskMetadataStateActive,
			ttl,
		)
	}
	if metadata.state != matchingTaskMetadataStateFenced || metadata.rangeID >= rangeID {
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"ActivateTaskQueueV3 failed. name: %v, type: %v, rangeID: %v, current rangeID: %v, state: %v",
			taskQueue,
			taskType,
			rangeID,
			metadata.rangeID,
			metadata.state,
		)}
	}
	if metadata.nextRangeIDValid && metadata.nextRangeID > rangeID {
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"ActivateTaskQueueV3 rejected stale range. name: %v, type: %v, rangeID: %v, pending rangeID: %v",
			taskQueue,
			taskType,
			rangeID,
			metadata.nextRangeID,
		)}
	}
	return d.updateMetadata(
		ctx,
		namespaceID,
		taskQueue,
		taskType,
		bucket,
		rangeID,
		blob,
		metadata.rangeID,
		matchingTaskMetadataStateFenced,
		ttl,
	)
}

func (d *matchingTaskStoreV3) addCreateTaskQuery(
	batch *gocql.Batch,
	request *p.InternalCreateTasksRequest,
	task *p.InternalCreateTask,
	bucket int16,
) {
	columns := "namespace_id, task_queue_name, task_queue_type, storage_bucket, type, task_id, task, task_encoding"
	values := "?, ?, ?, ?, ?, ?, ?, ?"
	args := []any{
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		bucket,
		rowTypeTaskInSubqueue(task.Subqueue),
		task.TaskId,
		task.Task.Data,
		task.Task.EncodingType.String(),
	}
	if d.fair {
		columns = "namespace_id, task_queue_name, task_queue_type, storage_bucket, type, pass, task_id, task, task_encoding"
		values = "?, ?, ?, ?, ?, ?, ?, ?, ?"
		args = []any{
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			bucket,
			rowTypeTaskInSubqueue(task.Subqueue),
			task.TaskPass,
			task.TaskId,
			task.Task.Data,
			task.Task.EncodingType.String(),
		}
	}
	ttl := getTaskTTL(task.ExpiryTime)
	usingTTL := ""
	if !d.fair && ttl > 0 && ttl <= maxCassandraTTL {
		usingTTL = " USING TTL ?"
		args = append(args, ttl)
	}
	batch.Query(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)%s", d.table, columns, values, usingTTL), args...)
}

func (d *matchingTaskStoreV3) createTasksMetadataQuery() string {
	clustering := "type = ? AND task_id = ?"
	if d.fair {
		clustering = "type = ? AND pass = ? AND task_id = ?"
	}
	return fmt.Sprintf(
		"UPDATE %s SET task_queue = ?, task_queue_encoding = ? WHERE namespace_id = ? AND task_queue_name = ? "+
			"AND task_queue_type = ? AND storage_bucket = ? AND %s IF range_id = ? AND metadata_state = ? AND bucket_count = ? AND migration_authority = ?",
		d.table,
		clustering,
	)
}

func (d *matchingTaskStoreV3) readTaskBucket(
	ctx context.Context,
	request *p.GetTasksRequest,
	bucket int16,
	lowerPass int64,
	lowerID int64,
	limit int,
) ([]matchingTaskV3Row, error) {
	rowType := rowTypeTaskInSubqueue(request.Subqueue)
	var query string
	var args []any
	if d.fair {
		query = fmt.Sprintf(
			"SELECT pass, task_id, task, task_encoding FROM %s WHERE namespace_id = ? AND task_queue_name = ? "+
				"AND task_queue_type = ? AND storage_bucket = ? AND (type, pass, task_id) >= (?, ?, ?) "+
				"AND (type, pass, task_id) < (?, ?, ?) LIMIT ?",
			d.table,
		)
		args = []any{
			request.NamespaceID, request.TaskQueue, request.TaskType, bucket,
			rowType, lowerPass, lowerID,
			rowType, int64(math.MaxInt64), int64(math.MaxInt64), limit,
		}
	} else {
		query = fmt.Sprintf(
			"SELECT task_id, task, task_encoding FROM %s WHERE namespace_id = ? AND task_queue_name = ? "+
				"AND task_queue_type = ? AND storage_bucket = ? AND type = ? AND task_id >= ? AND task_id < ? LIMIT ?",
			d.table,
		)
		args = []any{
			request.NamespaceID, request.TaskQueue, request.TaskType, bucket,
			rowType, lowerID, request.ExclusiveMaxTaskID, limit,
		}
	}
	iter := d.session.Query(query, args...).WithContext(ctx).Iter()
	rows := make([]matchingTaskV3Row, 0, limit)
	for {
		var row matchingTaskV3Row
		var data []byte
		var encoding string
		var scanned bool
		if d.fair {
			scanned = iter.Scan(&row.pass, &row.id, &data, &encoding)
		} else {
			scanned = iter.Scan(&row.id, &data, &encoding)
		}
		if !scanned {
			break
		}
		row.blob = p.NewDataBlob(bytes.Clone(data), encoding)
		rows = append(rows, row)
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetTasksV3", err)
	}
	return rows, nil
}

func (d *matchingTaskStoreV3) completeTasksQuery(
	request *p.CompleteTasksLessThanRequest,
	bucket int16,
) (string, []any) {
	rowType := rowTypeTaskInSubqueue(request.Subqueue)
	if d.fair {
		return fmt.Sprintf(
				"DELETE FROM %s WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? AND storage_bucket = ? "+
					"AND (type, pass, task_id) >= (?, ?, ?) AND (type, pass, task_id) < (?, ?, ?)",
				d.table,
			), []any{
				request.NamespaceID, request.TaskQueueName, request.TaskType, bucket,
				rowType, int64(0), int64(0),
				rowType, request.ExclusiveMaxPass, request.ExclusiveMaxTaskID,
			}
	}
	return fmt.Sprintf(
			"DELETE FROM %s WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? AND storage_bucket = ? "+
				"AND type = ? AND task_id < ?",
			d.table,
		), []any{
			request.NamespaceID, request.TaskQueueName, request.TaskType, bucket,
			rowType, request.ExclusiveMaxTaskID,
		}
}

func deduplicateMatchingTaskV3Rows(rows []matchingTaskV3Row, fair bool) ([]matchingTaskV3Row, error) {
	if len(rows) < 2 {
		return rows, nil
	}
	result := rows[:1]
	for _, row := range rows[1:] {
		previous := result[len(result)-1]
		same := row.id == previous.id && (!fair || row.pass == previous.pass)
		if !same {
			result = append(result, row)
			continue
		}
		if row.blob.EncodingType != previous.blob.EncodingType || !bytes.Equal(row.blob.Data, previous.blob.Data) {
			return nil, serviceerror.NewUnavailablef("conflicting duplicate matching task row at pass %d task ID %d", row.pass, row.id)
		}
	}
	return result, nil
}

func nextMatchingTaskV3Level(pass int64, id int64, fair bool) (nextPass int64, nextID int64, err error) {
	if id < math.MaxInt64 {
		return pass, id + 1, nil
	}
	if fair && pass < math.MaxInt64 {
		return pass + 1, math.MinInt64, nil
	}
	return 0, 0, serviceerror.NewInternal("matching task pagination level overflow")
}

func encodeMatchingTaskV3PageToken(token matchingTaskV3PageToken) []byte {
	result := make([]byte, matchingTaskV3PageTokenLength)
	copy(result, matchingTaskV3PageTokenPrefix)
	result[4] = matchingTaskV3PageTokenVersion
	if token.fair {
		result[5] = 1
	}
	binary.BigEndian.PutUint64(result[6:14], uint64(token.pass))
	binary.BigEndian.PutUint64(result[14:22], uint64(token.id))
	return result
}

func decodeMatchingTaskV3PageToken(data []byte, fair bool) (matchingTaskV3PageToken, error) {
	if len(data) != matchingTaskV3PageTokenLength || string(data[:4]) != matchingTaskV3PageTokenPrefix ||
		data[4] != matchingTaskV3PageTokenVersion {
		return matchingTaskV3PageToken{}, serviceerror.NewInternal("invalid Cassandra matching task v3 page token")
	}
	tokenFair := data[5] == 1
	if data[5] > 1 || tokenFair != fair {
		return matchingTaskV3PageToken{}, serviceerror.NewInternal("Cassandra matching task v3 page token layout mismatch")
	}
	return matchingTaskV3PageToken{
		fair: tokenFair,
		pass: int64(binary.BigEndian.Uint64(data[6:14])),
		id:   int64(binary.BigEndian.Uint64(data[14:22])),
	}, nil
}
