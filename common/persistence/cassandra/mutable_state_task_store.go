package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
	"golang.org/x/sync/errgroup"
)

const (
	templateCreateTransferTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, transfer, transfer_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateReplicationTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, replication, replication_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateVisibilityTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, visibility_task_data, visibility_task_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateTimerTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, timer, timer_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateHistoryTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, task_data, task_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateGetHistoryImmediateTasksQuery = `SELECT task_id, task_data, task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateGetHistoryScheduledTasksQuery = `SELECT visibility_ts, task_id, task_data, task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`

	templateGetHistoryScheduledTasksTargetQuery = `SELECT visibility_ts, task_id, task_data, task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and (type, namespace_id, workflow_id, run_id, visibility_ts, task_id) >= (?, ?, ?, ?, ?, ?) ` +
		`and (type, namespace_id, workflow_id, run_id, visibility_ts, task_id) < (?, ?, ?, ?, ?, ?)`

	templateGetTransferTasksQuery = `SELECT task_id, transfer, transfer_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateGetVisibilityTasksQuery = `SELECT task_id, visibility_task_data, visibility_task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateGetReplicationTasksQuery = `SELECT task_id, replication, replication_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateIsQueueEmptyQuery = `SELECT task_id ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`limit 1`

	templateCompleteTransferTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateRangeCompleteTransferTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateCompleteVisibilityTaskQuery = templateCompleteTransferTaskQuery

	templateRangeCompleteVisibilityTaskQuery = templateRangeCompleteTransferTaskQuery

	templateCompleteReplicationTaskQuery = templateCompleteTransferTaskQuery

	templateRangeCompleteReplicationTaskQuery = templateRangeCompleteTransferTaskQuery

	templateCompleteHistoryTaskQuery = templateCompleteTransferTaskQuery

	templateRangeCompleteHistoryImmediateTasksQuery = templateRangeCompleteTransferTaskQuery

	templateRangeCompleteHistoryScheduledTasksQuery = templateRangeCompleteTimerTaskQuery

	templateGetTimerTasksQuery = `SELECT visibility_ts, task_id, timer, timer_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`

	templateGetTimerTasksTargetQuery = `SELECT visibility_ts, task_id, timer, timer_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and (type, namespace_id, workflow_id, run_id, visibility_ts, task_id) >= (?, ?, ?, ?, ?, ?) ` +
		`and (type, namespace_id, workflow_id, run_id, visibility_ts, task_id) < (?, ?, ?, ?, ?, ?)`

	templateCompleteTimerTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateRangeCompleteTimerTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`
)

type (
	MutableStateTaskStore struct {
		Session    gocql.Session
		serializer serialization.Serializer
		layout     executionLayout
		logger     log.Logger
	}
)

func NewMutableStateTaskStore(session gocql.Session, serializer serialization.Serializer) *MutableStateTaskStore {
	return newMutableStateTaskStore(
		session,
		serializer,
		executionLayout{mode: config.CassandraExecutionMigrationModeLegacy, buckets: 1},
		log.NewNoopLogger(),
	)
}

func newMutableStateTaskStore(
	session gocql.Session,
	serializer serialization.Serializer,
	layout executionLayout,
	logger log.Logger,
) *MutableStateTaskStore {
	return &MutableStateTaskStore{
		Session:    session,
		serializer: serializer,
		layout:     layout,
		logger:     logger,
	}
}

func (d *MutableStateTaskStore) AddHistoryTasks(
	ctx context.Context,
	request *p.InternalAddHistoryTasksRequest,
) error {
	resolved, err := d.forExecutionShard(ctx, request.ShardID)
	if err != nil {
		return err
	}
	if resolved != d {
		return resolved.AddHistoryTasks(ctx, request)
	}
	if primary, mirror, ok := d.migrationStores(); ok {
		if err := primary.AddHistoryTasks(ctx, request); err != nil {
			return err
		}
		return d.handleMirrorError("AddHistoryTasks", mirror.AddHistoryTasks(ctx, request))
	}
	batch := newExecutionBatch(d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx), d.layout)
	shardID, err := d.layout.workflowPartition(request.ShardID, request.NamespaceID, request.WorkflowID)
	if err != nil {
		return err
	}

	if err := applyTasks(
		batch,
		shardID,
		request.Tasks,
	); err != nil {
		return err
	}

	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		shardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)
	batch.addShardAuthorityGuard(shardID)

	previous := make(map[string]any)
	applied, iter, err := d.Session.MapExecuteBatchCAS(batch.Batch, previous)
	if err != nil {
		return gocql.ConvertError("AddTasks", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	if !applied {
		if previousRangeID, ok := previous["range_id"].(int64); ok && previousRangeID != request.RangeID {
			// CreateWorkflowExecution failed because rangeID was modified
			return &p.ShardOwnershipLostError{
				ShardID: request.ShardID,
				Msg:     fmt.Sprintf("Failed to add tasks.  Request RangeID: %v, Actual RangeID: %v", request.RangeID, previousRangeID),
			}
		} else {
			return serviceerror.NewUnavailable("AddTasks operation failed because of conditional failure.")
		}
	}
	return nil
}

func (d *MutableStateTaskStore) GetHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	resolved, err := d.forExecutionShard(ctx, request.ShardID)
	if err != nil {
		return nil, err
	}
	if resolved != d {
		return resolved.GetHistoryTasks(ctx, request)
	}
	if d.layout.isTarget() {
		return d.getBucketedHistoryTasks(ctx, request)
	}
	return d.getHistoryTasksFromPartition(ctx, request)
}

func (d *MutableStateTaskStore) getHistoryTasksFromPartition(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.getTransferTasks(ctx, request)
	case tasks.CategoryIDTimer:
		return d.getTimerTasks(ctx, request)
	case tasks.CategoryIDVisibility:
		return d.getVisibilityTasks(ctx, request)
	case tasks.CategoryIDReplication:
		return d.getReplicationTasks(ctx, request)
	default:
		return d.getHistoryTasks(ctx, request)
	}
}

func (d *MutableStateTaskStore) CompleteHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	// Ignore the request if it is best effort
	if request.BestEffort {
		return nil
	}
	resolved, err := d.forExecutionShard(ctx, request.ShardID)
	if err != nil {
		return err
	}
	if resolved != d {
		return resolved.CompleteHistoryTask(ctx, request)
	}
	if primary, mirror, ok := d.migrationStores(); ok {
		if err := primary.CompleteHistoryTask(ctx, request); err != nil {
			return err
		}
		return d.handleMirrorError("CompleteHistoryTask", mirror.CompleteHistoryTask(ctx, request))
	}
	if d.layout.isTarget() {
		partitions, err := d.layout.partitions(request.ShardID)
		if err != nil {
			return err
		}
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(min(len(partitions), 16))
		for _, partition := range partitions {
			partition := partition
			group.Go(func() error {
				partitionRequest := *request
				partitionRequest.ShardID = partition
				return d.completeHistoryTaskFromPartition(groupCtx, &partitionRequest)
			})
		}
		return group.Wait()
	}
	return d.completeHistoryTaskFromPartition(ctx, request)
}

func (d *MutableStateTaskStore) completeHistoryTaskFromPartition(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.completeTransferTask(ctx, request)
	case tasks.CategoryIDTimer:
		return d.completeTimerTask(ctx, request)
	case tasks.CategoryIDVisibility:
		return d.completeVisibilityTask(ctx, request)
	case tasks.CategoryIDReplication:
		return d.completeReplicationTask(ctx, request)
	default:
		return d.completeHistoryTask(ctx, request)
	}
}

func (d *MutableStateTaskStore) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	resolved, err := d.forExecutionShard(ctx, request.ShardID)
	if err != nil {
		return err
	}
	if resolved != d {
		return resolved.RangeCompleteHistoryTasks(ctx, request)
	}
	if primary, mirror, ok := d.migrationStores(); ok {
		if err := primary.RangeCompleteHistoryTasks(ctx, request); err != nil {
			return err
		}
		return d.handleMirrorError("RangeCompleteHistoryTasks", mirror.RangeCompleteHistoryTasks(ctx, request))
	}
	if d.layout.isTarget() {
		partitions, err := d.layout.partitions(request.ShardID)
		if err != nil {
			return err
		}
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(min(len(partitions), 16))
		for _, partition := range partitions {
			partition := partition
			group.Go(func() error {
				partitionRequest := *request
				partitionRequest.ShardID = partition
				return d.rangeCompleteHistoryTasksFromPartition(groupCtx, &partitionRequest)
			})
		}
		return group.Wait()
	}
	return d.rangeCompleteHistoryTasksFromPartition(ctx, request)
}

func (d *MutableStateTaskStore) rangeCompleteHistoryTasksFromPartition(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.rangeCompleteTransferTasks(ctx, request)
	case tasks.CategoryIDTimer:
		return d.rangeCompleteTimerTasks(ctx, request)
	case tasks.CategoryIDVisibility:
		return d.rangeCompleteVisibilityTasks(ctx, request)
	case tasks.CategoryIDReplication:
		return d.rangeCompleteReplicationTasks(ctx, request)
	default:
		return d.rangeCompleteHistoryTasks(ctx, request)
	}
}

type executionTaskPageToken struct {
	Version      int   `json:"version"`
	CategoryID   int   `json:"categoryId"`
	FireTimeNano int64 `json:"fireTimeNano"`
	TaskID       int64 `json:"taskId"`
}

func (d *MutableStateTaskStore) getBucketedHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	if request.BatchSize <= 0 {
		return nil, serviceerror.NewInvalidArgument("GetHistoryTasks batch size must be positive")
	}
	minKey := request.InclusiveMinTaskKey
	if len(request.NextPageToken) > 0 {
		var token executionTaskPageToken
		if err := json.Unmarshal(request.NextPageToken, &token); err != nil ||
			token.Version != 1 || token.CategoryID != request.TaskCategory.ID() {
			return nil, serviceerror.NewInvalidArgument("invalid executions_v2 history-task page token")
		}
		lastKey := tasks.NewKey(time.Unix(0, token.FireTimeNano).UTC(), token.TaskID)
		if minKey.CompareTo(lastKey) <= 0 {
			minKey = lastKey.Next()
		}
	}

	partitions, err := d.layout.partitions(request.ShardID)
	if err != nil {
		return nil, err
	}
	responses := make([]*p.InternalGetHistoryTasksResponse, len(partitions))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(min(len(partitions), 16))
	for index, partition := range partitions {
		index, partition := index, partition
		group.Go(func() error {
			partitionRequest := *request
			partitionRequest.ShardID = partition
			partitionRequest.InclusiveMinTaskKey = minKey
			partitionRequest.BatchSize = request.BatchSize + 1
			partitionRequest.NextPageToken = nil
			response, err := d.getHistoryTasksFromPartition(groupCtx, &partitionRequest)
			if err != nil {
				return err
			}
			responses[index] = response
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	allTasks := make([]p.InternalHistoryTask, 0, preallocatedResultCapacity(request.BatchSize))
	hasMore := false
	for _, response := range responses {
		allTasks = append(allTasks, response.Tasks...)
		hasMore = hasMore || len(response.NextPageToken) > 0
	}

	sort.Slice(allTasks, func(i, j int) bool {
		return allTasks[i].Key.CompareTo(allTasks[j].Key) < 0
	})
	if len(allTasks) > request.BatchSize {
		allTasks = allTasks[:request.BatchSize]
		hasMore = true
	}
	response := &p.InternalGetHistoryTasksResponse{Tasks: allTasks}
	if hasMore && len(allTasks) > 0 {
		lastKey := allTasks[len(allTasks)-1].Key
		response.NextPageToken, err = json.Marshal(executionTaskPageToken{
			Version:      1,
			CategoryID:   request.TaskCategory.ID(),
			FireTimeNano: lastKey.FireTime.UnixNano(), //nolint:forbidigo // This is page-token precision, not a Cassandra timestamp.
			TaskID:       lastKey.TaskID,
		})
		if err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (d *MutableStateTaskStore) getTransferTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {

	// Reading transfer tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(d.layout.query(templateGetTransferTasksQuery),
		request.ShardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{
		Tasks: make([]p.InternalHistoryTask, 0, preallocatedResultCapacity(request.BatchSize)),
	}
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetTransferTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeTransferTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	args := []any{
		request.ShardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.TaskKey.TaskID,
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, request.ShardID,
		"CompleteTransferTask", templateCompleteTransferTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) rangeCompleteTransferTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	args := []any{
		request.ShardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, request.ShardID,
		"RangeCompleteTransferTask", templateRangeCompleteTransferTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) getTimerTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// Reading timer tasks need to be quorum level consistent, otherwise we could lose tasks
	minTimestamp := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
	maxTimestamp := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
	var query gocql.Query
	if d.layout.isTarget() {
		query = d.Session.Query(d.layout.query(templateGetTimerTasksTargetQuery),
			request.ShardID,
			rowTypeTimerTask,
			rowTypeTimerNamespaceID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			minTimestamp,
			request.InclusiveMinTaskKey.TaskID,
			rowTypeTimerTask,
			rowTypeTimerNamespaceID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			maxTimestamp,
			request.ExclusiveMaxTaskKey.TaskID,
		).WithContext(ctx)
	} else {
		query = d.Session.Query(d.layout.query(templateGetTimerTasksQuery),
			request.ShardID,
			rowTypeTimerTask,
			rowTypeTimerNamespaceID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			minTimestamp,
			maxTimestamp,
		).WithContext(ctx)
	}
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{
		Tasks: make([]p.InternalHistoryTask, 0, preallocatedResultCapacity(request.BatchSize)),
	}
	var timestamp time.Time
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&timestamp, &taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewKey(timestamp, taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		timestamp = time.Time{}
		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetTimerTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeTimerTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	ts := p.UnixMilliseconds(request.TaskKey.FireTime)
	args := []any{
		request.ShardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		ts,
		request.TaskKey.TaskID,
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, request.ShardID,
		"CompleteTimerTask", templateCompleteTimerTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) rangeCompleteTimerTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	start := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
	end := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
	args := []any{
		request.ShardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		start,
		end,
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, request.ShardID,
		"RangeCompleteTimerTask", templateRangeCompleteTimerTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) getReplicationTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {

	// Reading replication tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(d.layout.query(templateGetReplicationTasksQuery),
		request.ShardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasks")
}

func (d *MutableStateTaskStore) completeReplicationTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	args := []any{
		request.ShardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.TaskKey.TaskID,
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, request.ShardID,
		"CompleteReplicationTask", templateCompleteReplicationTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) rangeCompleteReplicationTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	args := []any{
		request.ShardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, request.ShardID,
		"RangeCompleteReplicationTask", templateRangeCompleteReplicationTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *p.PutReplicationTaskToDLQRequest,
) error {
	resolved, err := d.forExecutionShard(ctx, request.ShardID)
	if err != nil {
		return err
	}
	if resolved != d {
		return resolved.PutReplicationTaskToDLQ(ctx, request)
	}
	if primary, mirror, ok := d.migrationStores(); ok {
		if err := primary.PutReplicationTaskToDLQ(ctx, request); err != nil {
			return err
		}
		return d.handleMirrorError("PutReplicationTaskToDLQ", mirror.PutReplicationTaskToDLQ(ctx, request))
	}
	task := request.TaskInfo
	datablob, err := d.serializer.ReplicationTaskInfoToBlob(task)
	if err != nil {
		return gocql.ConvertError("PutReplicationTaskToDLQ", err)
	}
	shardID, err := d.layout.workflowPartition(request.ShardID, rowTypeDLQNamespaceID, request.SourceClusterName)
	if err != nil {
		return err
	}

	// Use source cluster name as the workflow id for replication dlq
	args := []any{
		shardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		datablob.Data,
		datablob.EncodingType.String(),
		defaultVisibilityTimestamp,
		task.GetTaskId(),
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, shardID,
		"PutReplicationTaskToDLQ", templateCreateReplicationTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	resolved, err := d.forExecutionShard(ctx, request.ShardID)
	if err != nil {
		return nil, err
	}
	if resolved != d {
		return resolved.GetReplicationTasksFromDLQ(ctx, request)
	}
	// Reading replication tasks need to be quorum level consistent, otherwise we could lose tasks
	shardID, err := d.layout.workflowPartition(request.ShardID, rowTypeDLQNamespaceID, request.SourceClusterName)
	if err != nil {
		return nil, err
	}
	query := d.Session.Query(d.layout.query(templateGetReplicationTasksQuery),
		shardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasksFromDLQ")
}

func (d *MutableStateTaskStore) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *p.DeleteReplicationTaskFromDLQRequest,
) error {
	resolved, err := d.forExecutionShard(ctx, request.ShardID)
	if err != nil {
		return err
	}
	if resolved != d {
		return resolved.DeleteReplicationTaskFromDLQ(ctx, request)
	}
	if primary, mirror, ok := d.migrationStores(); ok {
		if err := primary.DeleteReplicationTaskFromDLQ(ctx, request); err != nil {
			return err
		}
		return d.handleMirrorError("DeleteReplicationTaskFromDLQ", mirror.DeleteReplicationTaskFromDLQ(ctx, request))
	}
	shardID, err := d.layout.workflowPartition(request.ShardID, rowTypeDLQNamespaceID, request.SourceClusterName)
	if err != nil {
		return err
	}

	args := []any{
		shardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.TaskKey.TaskID,
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, shardID,
		"DeleteReplicationTaskFromDLQ", templateCompleteReplicationTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *p.RangeDeleteReplicationTaskFromDLQRequest,
) error {
	resolved, err := d.forExecutionShard(ctx, request.ShardID)
	if err != nil {
		return err
	}
	if resolved != d {
		return resolved.RangeDeleteReplicationTaskFromDLQ(ctx, request)
	}
	if primary, mirror, ok := d.migrationStores(); ok {
		if err := primary.RangeDeleteReplicationTaskFromDLQ(ctx, request); err != nil {
			return err
		}
		return d.handleMirrorError("RangeDeleteReplicationTaskFromDLQ", mirror.RangeDeleteReplicationTaskFromDLQ(ctx, request))
	}
	shardID, err := d.layout.workflowPartition(request.ShardID, rowTypeDLQNamespaceID, request.SourceClusterName)
	if err != nil {
		return err
	}

	args := []any{
		shardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, shardID,
		"RangeDeleteReplicationTaskFromDLQ", templateRangeCompleteReplicationTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) IsReplicationDLQEmpty(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (bool, error) {
	resolved, err := d.forExecutionShard(ctx, request.ShardID)
	if err != nil {
		return true, err
	}
	if resolved != d {
		return resolved.IsReplicationDLQEmpty(ctx, request)
	}
	shardID, err := d.layout.workflowPartition(request.ShardID, rowTypeDLQNamespaceID, request.SourceClusterName)
	if err != nil {
		return true, err
	}

	query := d.Session.Query(d.layout.query(templateIsQueueEmptyQuery),
		shardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
	).WithContext(ctx)

	if err := query.Scan(nil); err != nil {
		if gocql.IsNotFoundError(err) {
			return true, nil
		}
		return true, gocql.ConvertError("IsReplicationDLQEmpty", err)
	}
	return false, nil
}

func (d *MutableStateTaskStore) getVisibilityTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {

	// Reading Visibility tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(d.layout.query(templateGetVisibilityTasksQuery),
		request.ShardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{
		Tasks: make([]p.InternalHistoryTask, 0, preallocatedResultCapacity(request.BatchSize)),
	}
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetVisibilityTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeVisibilityTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	args := []any{
		request.ShardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.TaskKey.TaskID,
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, request.ShardID,
		"CompleteVisibilityTask", templateCompleteVisibilityTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) rangeCompleteVisibilityTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	args := []any{
		request.ShardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, request.ShardID,
		"RangeCompleteVisibilityTask", templateRangeCompleteVisibilityTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) populateGetReplicationTasksResponse(
	query gocql.Query,
	operation string,
) (*p.InternalGetHistoryTasksResponse, error) {
	iter := query.Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError(operation, err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) getHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	switch request.TaskCategory.Type() {
	case tasks.CategoryTypeImmediate:
		return d.getHistoryImmedidateTasks(ctx, request)
	case tasks.CategoryTypeScheduled:
		return d.getHistoryScheduledTasks(ctx, request)
	default:
		panic(fmt.Sprintf("Unknown task category type: %v", request.TaskCategory.Type().String()))
	}
}

func (d *MutableStateTaskStore) getHistoryImmedidateTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// execution manager should already validated the request
	// Reading history tasks need to be quorum level consistent, otherwise we could lose task

	query := d.Session.Query(d.layout.query(templateGetHistoryImmediateTasksQuery),
		request.ShardID,
		request.TaskCategory.ID(),
		rowTypeHistoryTaskNamespaceID,
		rowTypeHistoryTaskWorkflowID,
		rowTypeHistoryTaskRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)

	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{
		Tasks: make([]p.InternalHistoryTask, 0, preallocatedResultCapacity(request.BatchSize)),
	}
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetHistoryImmediateTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) getHistoryScheduledTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// execution manager should already validated the request
	// Reading history tasks need to be quorum level consistent, otherwise we could lose task

	minTimestamp := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
	maxTimestamp := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
	var query gocql.Query
	if d.layout.isTarget() {
		query = d.Session.Query(d.layout.query(templateGetHistoryScheduledTasksTargetQuery),
			request.ShardID,
			request.TaskCategory.ID(),
			rowTypeHistoryTaskNamespaceID,
			rowTypeHistoryTaskWorkflowID,
			rowTypeHistoryTaskRunID,
			minTimestamp,
			request.InclusiveMinTaskKey.TaskID,
			request.TaskCategory.ID(),
			rowTypeHistoryTaskNamespaceID,
			rowTypeHistoryTaskWorkflowID,
			rowTypeHistoryTaskRunID,
			maxTimestamp,
			request.ExclusiveMaxTaskKey.TaskID,
		).WithContext(ctx)
	} else {
		query = d.Session.Query(d.layout.query(templateGetHistoryScheduledTasksQuery),
			request.ShardID,
			request.TaskCategory.ID(),
			rowTypeHistoryTaskNamespaceID,
			rowTypeHistoryTaskWorkflowID,
			rowTypeHistoryTaskRunID,
			minTimestamp,
			maxTimestamp,
		).WithContext(ctx)
	}

	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{
		Tasks: make([]p.InternalHistoryTask, 0, preallocatedResultCapacity(request.BatchSize)),
	}
	var timestamp time.Time
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&timestamp, &taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewKey(timestamp, taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		timestamp = time.Time{}
		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetHistoryScheduledTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	ts := defaultVisibilityTimestamp
	if request.TaskCategory.Type() == tasks.CategoryTypeScheduled {
		ts = p.UnixMilliseconds(request.TaskKey.FireTime)
	}
	args := []any{
		request.ShardID,
		request.TaskCategory.ID(),
		rowTypeHistoryTaskNamespaceID,
		rowTypeHistoryTaskWorkflowID,
		rowTypeHistoryTaskRunID,
		ts,
		request.TaskKey.TaskID,
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, request.ShardID,
		"CompleteHistoryTask", templateCompleteHistoryTaskQuery, args, false,
	)
}

func (d *MutableStateTaskStore) rangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	// execution manager should already validated the request
	var (
		query string
		args  []any
	)
	if request.TaskCategory.Type() == tasks.CategoryTypeImmediate {
		query = templateRangeCompleteHistoryImmediateTasksQuery
		args = []any{
			request.ShardID,
			request.TaskCategory.ID(),
			rowTypeHistoryTaskNamespaceID,
			rowTypeHistoryTaskWorkflowID,
			rowTypeHistoryTaskRunID,
			defaultVisibilityTimestamp,
			request.InclusiveMinTaskKey.TaskID,
			request.ExclusiveMaxTaskKey.TaskID,
		}
	} else {
		minTimestamp := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
		maxTimestamp := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
		query = templateRangeCompleteHistoryScheduledTasksQuery
		args = []any{
			request.ShardID,
			request.TaskCategory.ID(),
			rowTypeHistoryTaskNamespaceID,
			rowTypeHistoryTaskWorkflowID,
			rowTypeHistoryTaskRunID,
			minTimestamp,
			maxTimestamp,
		}
	}
	return executeGuardedExecutionMutation(
		ctx, d.Session, d.layout, request.ShardID, request.ShardID,
		"RangeCompleteHistoryTasks", query, args, false,
	)
}

func (d *MutableStateTaskStore) migrationStores() (primaryStore *MutableStateTaskStore, mirrorStore *MutableStateTaskStore, hasMirror bool) {
	mirrorLayout, ok := d.layout.mirrorLayout()
	if !ok {
		return nil, nil, false
	}
	primary := *d
	primary.layout = d.layout.authoritativeLayout()
	mirror := *d
	mirror.layout = mirrorLayout
	return &primary, &mirror, true
}

func (d *MutableStateTaskStore) forExecutionShard(
	ctx context.Context,
	shardID int32,
) (*MutableStateTaskStore, error) {
	layout, err := resolveTargetDualExecutionLayout(ctx, d.Session, d.layout, shardID)
	if err != nil {
		return nil, err
	}
	if layout == d.layout {
		return d, nil
	}
	resolved := *d
	resolved.layout = layout
	return &resolved, nil
}

func (d *MutableStateTaskStore) handleMirrorError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if d.layout.mirrorRequired() {
		return fmt.Errorf("mirror %s: %w", operation, err)
	}
	if d.logger != nil {
		d.logger.Warn("Cassandra executions source-rebuild task mirror failed", tag.Operation(operation), tag.Error(err))
	}
	return nil
}
