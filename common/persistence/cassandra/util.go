package cassandra

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/convert"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/service/history/tasks"
)

func applyWorkflowMutationBatch(
	batch *gocql.Batch,
	shardID int32,
	workflowMutation *p.InternalWorkflowMutation,
	compressor *blobCompressor,
) error {

	// TODO update all call sites to update LastUpdatetime
	// cqlNowTimestampMillis := p.UnixMilliseconds(time.Now().UTC())

	namespaceID := workflowMutation.NamespaceID
	workflowID := workflowMutation.WorkflowID
	runID := workflowMutation.RunID

	if err := updateExecution(
		batch,
		shardID,
		namespaceID,
		workflowID,
		runID,
		workflowMutation.ExecutionInfoBlob,
		workflowMutation.ExecutionState,
		workflowMutation.ExecutionStateBlob,
		workflowMutation.NextEventID,
		workflowMutation.Condition,
		workflowMutation.DBRecordVersion,
		workflowMutation.Checksum,
		compressor,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(
		batch,
		workflowMutation.UpsertActivityInfos,
		workflowMutation.DeleteActivityInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := updateTimerInfos(
		batch,
		workflowMutation.UpsertTimerInfos,
		workflowMutation.DeleteTimerInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := updateChildExecutionInfos(
		batch,
		workflowMutation.UpsertChildExecutionInfos,
		workflowMutation.DeleteChildExecutionInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := updateRequestCancelInfos(
		batch,
		workflowMutation.UpsertRequestCancelInfos,
		workflowMutation.DeleteRequestCancelInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := updateSignalInfos(
		batch,
		workflowMutation.UpsertSignalInfos,
		workflowMutation.DeleteSignalInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := updateChasmNodes(
		batch,
		workflowMutation.UpsertChasmNodes,
		workflowMutation.DeleteChasmNodes,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	updateSignalsRequested(
		batch,
		workflowMutation.UpsertSignalRequestedIDs,
		workflowMutation.DeleteSignalRequestedIDs,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	if err := updateBufferedEvents(
		batch,
		workflowMutation.NewBufferedEvents,
		workflowMutation.ClearBufferedEvents,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		workflowMutation.Tasks,
		compressor,
	)
}

func applyWorkflowSnapshotBatchAsReset(
	batch *gocql.Batch,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
	compressor *blobCompressor,
) error {

	// TODO: update call site
	// cqlNowTimestampMillis := p.UnixMilliseconds(time.Now().UTC())

	namespaceID := workflowSnapshot.NamespaceID
	workflowID := workflowSnapshot.WorkflowID
	runID := workflowSnapshot.RunID

	if err := updateExecution(
		batch,
		shardID,
		namespaceID,
		workflowID,
		runID,
		workflowSnapshot.ExecutionInfoBlob,
		workflowSnapshot.ExecutionState,
		workflowSnapshot.ExecutionStateBlob,
		workflowSnapshot.NextEventID,
		workflowSnapshot.Condition,
		workflowSnapshot.DBRecordVersion,
		workflowSnapshot.Checksum,
		compressor,
	); err != nil {
		return err
	}

	if err := resetActivityInfos(
		batch,
		workflowSnapshot.ActivityInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := resetTimerInfos(
		batch,
		workflowSnapshot.TimerInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := resetChildExecutionInfos(
		batch,
		workflowSnapshot.ChildExecutionInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := resetRequestCancelInfos(
		batch,
		workflowSnapshot.RequestCancelInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := resetSignalInfos(
		batch,
		workflowSnapshot.SignalInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := resetChasmNodes(
		batch,
		workflowSnapshot.ChasmNodes,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	resetSignalRequested(
		batch,
		workflowSnapshot.SignalRequestedIDs,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	deleteBufferedEvents(
		batch,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		workflowSnapshot.Tasks,
		compressor,
	)
}

func applyWorkflowSnapshotBatchAsNew(
	batch *gocql.Batch,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
	compressor *blobCompressor,
) error {
	namespaceID := workflowSnapshot.NamespaceID
	workflowID := workflowSnapshot.WorkflowID
	runID := workflowSnapshot.RunID

	if err := createExecution(
		batch,
		shardID,
		workflowSnapshot,
		compressor,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(
		batch,
		workflowSnapshot.ActivityInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := updateTimerInfos(
		batch,
		workflowSnapshot.TimerInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := updateChildExecutionInfos(
		batch,
		workflowSnapshot.ChildExecutionInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := updateRequestCancelInfos(
		batch,
		workflowSnapshot.RequestCancelInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := updateSignalInfos(
		batch,
		workflowSnapshot.SignalInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	if err := updateChasmNodes(
		batch,
		workflowSnapshot.ChasmNodes,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
		compressor,
	); err != nil {
		return err
	}

	updateSignalsRequested(
		batch,
		workflowSnapshot.SignalRequestedIDs,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		batch,
		shardID,
		workflowSnapshot.Tasks,
		compressor,
	)
}

func createExecution(
	batch *gocql.Batch,
	shardID int32,
	snapshot *p.InternalWorkflowSnapshot,
	compressor *blobCompressor,
) error {
	// validate workflow state & close status
	if err := p.ValidateCreateWorkflowStateStatus(
		snapshot.ExecutionState.State,
		snapshot.ExecutionState.Status); err != nil {
		return err
	}
	executionInfoData, executionInfoEncoding, err := compressor.compressBlob(snapshot.ExecutionInfoBlob)
	if err != nil {
		return err
	}
	executionStateData, executionStateEncoding, err := compressor.compressBlob(snapshot.ExecutionStateBlob)
	if err != nil {
		return err
	}
	checksumData, checksumEncoding, err := compressor.compressBlob(snapshot.Checksum)
	if err != nil {
		return err
	}

	// TODO also need to set the start / current / last write version
	batch.Query(templateCreateWorkflowExecutionQuery,
		shardID,
		snapshot.NamespaceID,
		snapshot.WorkflowID,
		snapshot.RunID,
		rowTypeExecution,
		executionInfoData,
		executionInfoEncoding,
		executionStateData,
		executionStateEncoding,
		snapshot.NextEventID,
		snapshot.DBRecordVersion,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		checksumData,
		checksumEncoding,
	)

	return nil
}

func updateExecution(
	batch *gocql.Batch,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	executionInfoBlob *commonpb.DataBlob,
	executionState *persistencespb.WorkflowExecutionState,
	executionStateBlob *commonpb.DataBlob,
	nextEventID int64,
	condition int64,
	dbRecordVersion int64,
	checksumBlob *commonpb.DataBlob,
	compressor *blobCompressor,
) error {

	// validate workflow state & close status
	if err := p.ValidateUpdateWorkflowStateStatus(
		executionState.State,
		executionState.Status); err != nil {
		return err
	}
	executionInfoData, executionInfoEncoding, err := compressor.compressBlob(executionInfoBlob)
	if err != nil {
		return err
	}
	executionStateData, executionStateEncoding, err := compressor.compressBlob(executionStateBlob)
	if err != nil {
		return err
	}
	checksumData, checksumEncoding, err := compressor.compressBlob(checksumBlob)
	if err != nil {
		return err
	}

	if dbRecordVersion == 0 {
		batch.Query(templateUpdateWorkflowExecutionQueryDeprecated,
			executionInfoData,
			executionInfoEncoding,
			executionStateData,
			executionStateEncoding,
			nextEventID,
			dbRecordVersion,
			checksumData,
			checksumEncoding,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition,
		)
	} else {
		batch.Query(templateUpdateWorkflowExecutionQuery,
			executionInfoData,
			executionInfoEncoding,
			executionStateData,
			executionStateEncoding,
			nextEventID,
			dbRecordVersion,
			checksumData,
			checksumEncoding,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			dbRecordVersion-1,
		)
	}

	return nil
}

func applyTasks(
	batch *gocql.Batch,
	shardID int32,
	insertTasks map[tasks.Category][]p.InternalHistoryTask,
	compressor *blobCompressor,
) error {

	var err error
	for category, tasksByCategory := range insertTasks {
		switch category.ID() {
		case tasks.CategoryIDTransfer:
			err = createTransferTasks(batch, tasksByCategory, shardID, compressor)
		case tasks.CategoryIDTimer:
			err = createTimerTasks(batch, tasksByCategory, shardID, compressor)
		case tasks.CategoryIDVisibility:
			err = createVisibilityTasks(batch, tasksByCategory, shardID, compressor)
		case tasks.CategoryIDReplication:
			err = createReplicationTasks(batch, tasksByCategory, shardID, compressor)
		default:
			err = createHistoryTasks(batch, category, tasksByCategory, shardID, compressor)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func createTransferTasks(
	batch *gocql.Batch,
	transferTasks []p.InternalHistoryTask,
	shardID int32,
	compressor *blobCompressor,
) error {
	for _, task := range transferTasks {
		data, encoding, err := compressor.compressBlob(task.Blob)
		if err != nil {
			return err
		}
		batch.Query(templateCreateTransferTaskQuery,
			shardID,
			rowTypeTransferTask,
			rowTypeTransferNamespaceID,
			rowTypeTransferWorkflowID,
			rowTypeTransferRunID,
			data,
			encoding,
			defaultVisibilityTimestamp,
			task.Key.TaskID,
		)
	}
	return nil
}

func createTimerTasks(
	batch *gocql.Batch,
	timerTasks []p.InternalHistoryTask,
	shardID int32,
	compressor *blobCompressor,
) error {
	for _, task := range timerTasks {
		data, encoding, err := compressor.compressBlob(task.Blob)
		if err != nil {
			return err
		}
		batch.Query(templateCreateTimerTaskQuery,
			shardID,
			rowTypeTimerTask,
			rowTypeTimerNamespaceID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			data,
			encoding,
			p.UnixMilliseconds(task.Key.FireTime),
			task.Key.TaskID,
		)
	}
	return nil
}

func createReplicationTasks(
	batch *gocql.Batch,
	replicationTasks []p.InternalHistoryTask,
	shardID int32,
	compressor *blobCompressor,
) error {
	for _, task := range replicationTasks {
		data, encoding, err := compressor.compressBlob(task.Blob)
		if err != nil {
			return err
		}
		batch.Query(templateCreateReplicationTaskQuery,
			shardID,
			rowTypeReplicationTask,
			rowTypeReplicationNamespaceID,
			rowTypeReplicationWorkflowID,
			rowTypeReplicationRunID,
			data,
			encoding,
			defaultVisibilityTimestamp,
			task.Key.TaskID,
		)
	}
	return nil
}

func createVisibilityTasks(
	batch *gocql.Batch,
	visibilityTasks []p.InternalHistoryTask,
	shardID int32,
	compressor *blobCompressor,
) error {
	for _, task := range visibilityTasks {
		data, encoding, err := compressor.compressBlob(task.Blob)
		if err != nil {
			return err
		}
		batch.Query(templateCreateVisibilityTaskQuery,
			shardID,
			rowTypeVisibilityTask,
			rowTypeVisibilityTaskNamespaceID,
			rowTypeVisibilityTaskWorkflowID,
			rowTypeVisibilityTaskRunID,
			data,
			encoding,
			defaultVisibilityTimestamp,
			task.Key.TaskID,
		)
	}
	return nil
}

func createHistoryTasks(
	batch *gocql.Batch,
	category tasks.Category,
	historyTasks []p.InternalHistoryTask,
	shardID int32,
	compressor *blobCompressor,
) error {
	isScheduledTask := category.Type() == tasks.CategoryTypeScheduled
	for _, task := range historyTasks {
		data, encoding, err := compressor.compressBlob(task.Blob)
		if err != nil {
			return err
		}
		visibilityTimestamp := defaultVisibilityTimestamp
		if isScheduledTask {
			visibilityTimestamp = p.UnixMilliseconds(task.Key.FireTime)
		}
		batch.Query(templateCreateHistoryTaskQuery,
			shardID,
			category.ID(),
			rowTypeHistoryTaskNamespaceID,
			rowTypeHistoryTaskWorkflowID,
			rowTypeHistoryTaskRunID,
			data,
			encoding,
			visibilityTimestamp,
			task.Key.TaskID,
		)
	}
	return nil
}

func updateActivityInfos(
	batch *gocql.Batch,
	activityInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {

	for scheduledEventID, blob := range activityInfos {
		data, encoding, err := compressor.compressBlob(blob)
		if err != nil {
			return err
		}
		batch.Query(templateUpdateActivityInfoQuery,
			scheduledEventID,
			data,
			encoding,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
		batch.Query(templateDeleteActivityInfoQuery,
			deleteID,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
	return nil
}

func deleteBufferedEvents(
	batch *gocql.Batch,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {
	batch.Query(templateDeleteBufferedEventsQuery,
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
	)
}

func resetActivityInfos(
	batch *gocql.Batch,
	activityInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {
	infoMap, encoding, err := convertBlobMapToByteMap(activityInfos, compressor)
	if err != nil {
		return err
	}

	batch.Query(templateResetActivityInfoQuery,
		infoMap,
		encoding.String(),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func updateTimerInfos(
	batch *gocql.Batch,
	timerInfos map[string]*commonpb.DataBlob,
	deleteInfos map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {
	for timerID, blob := range timerInfos {
		data, encoding, err := compressor.compressBlob(blob)
		if err != nil {
			return err
		}
		batch.Query(templateUpdateTimerInfoQuery,
			timerID,
			data,
			encoding,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteInfoID := range deleteInfos {
		batch.Query(templateDeleteTimerInfoQuery,
			deleteInfoID,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	return nil
}

func resetTimerInfos(
	batch *gocql.Batch,
	timerInfos map[string]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {
	timerMap, timerMapEncoding, err := convertBlobMapToByteMap(timerInfos, compressor)
	if err != nil {
		return err
	}

	batch.Query(templateResetTimerInfoQuery,
		timerMap,
		timerMapEncoding.String(),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func updateChildExecutionInfos(
	batch *gocql.Batch,
	childExecutionInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {

	for initiatedId, blob := range childExecutionInfos {
		data, encoding, err := compressor.compressBlob(blob)
		if err != nil {
			return err
		}
		batch.Query(templateUpdateChildExecutionInfoQuery,
			initiatedId,
			data,
			encoding,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
		batch.Query(templateDeleteChildExecutionInfoQuery,
			deleteID,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
	return nil
}

func resetChildExecutionInfos(
	batch *gocql.Batch,
	childExecutionInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {
	infoMap, encoding, err := convertBlobMapToByteMap(childExecutionInfos, compressor)
	if err != nil {
		return err
	}

	batch.Query(templateResetChildExecutionInfoQuery,
		infoMap,
		encoding.String(),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func updateRequestCancelInfos(
	batch *gocql.Batch,
	requestCancelInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {

	for initiatedId, blob := range requestCancelInfos {
		data, encoding, err := compressor.compressBlob(blob)
		if err != nil {
			return err
		}
		batch.Query(templateUpdateRequestCancelInfoQuery,
			initiatedId,
			data,
			encoding,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
		batch.Query(templateDeleteRequestCancelInfoQuery,
			deleteID,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
	return nil
}

func resetRequestCancelInfos(
	batch *gocql.Batch,
	requestCancelInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {
	rciMap, rciMapEncoding, err := convertBlobMapToByteMap(requestCancelInfos, compressor)
	if err != nil {
		return err
	}

	batch.Query(templateResetRequestCancelInfoQuery,
		rciMap,
		rciMapEncoding.String(),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func updateSignalInfos(
	batch *gocql.Batch,
	signalInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {

	for initiatedId, blob := range signalInfos {
		data, encoding, err := compressor.compressBlob(blob)
		if err != nil {
			return err
		}
		batch.Query(templateUpdateSignalInfoQuery,
			initiatedId,
			data,
			encoding,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for deleteID := range deleteIDs {
		batch.Query(templateDeleteSignalInfoQuery,
			deleteID,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
	return nil
}

func resetSignalInfos(
	batch *gocql.Batch,
	signalInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {
	sMap, sMapEncoding, err := convertBlobMapToByteMap(signalInfos, compressor)
	if err != nil {
		return err
	}

	batch.Query(templateResetSignalInfoQuery,
		sMap,
		sMapEncoding.String(),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func resetChasmNodes(
	batch *gocql.Batch,
	nodes map[string]p.InternalChasmNode,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {
	blobMap := make(map[string][]byte, len(nodes))
	var encoding enumspb.EncodingType
	for path, node := range nodes {
		data, _, err := compressor.compressBlob(node.CassandraBlob)
		if err != nil {
			return err
		}
		blobMap[path] = data
		encoding = node.CassandraBlob.EncodingType // TODO - we only support a single encoding
	}

	batch.Query(templateResetChasmNodeQuery,
		blobMap,
		encoding.String(),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	return nil
}

func updateChasmNodes(
	batch *gocql.Batch,
	upsertNodes map[string]p.InternalChasmNode,
	deleteNodes map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {
	for deletePath := range deleteNodes {
		batch.Query(templateDeleteChasmNodeQuery,
			deletePath,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	for upsertPath, node := range upsertNodes {
		data, encoding, err := compressor.compressBlob(node.CassandraBlob)
		if err != nil {
			return err
		}
		batch.Query(templateUpdateChasmNodeQuery,
			upsertPath,
			data,
			encoding,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	return nil
}

func updateSignalsRequested(
	batch *gocql.Batch,
	signalReqIDs map[string]struct{},
	deleteSignalReqIDs map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {

	if len(signalReqIDs) > 0 {
		batch.Query(templateUpdateSignalRequestedQuery,
			convert.StringSetToSlice(signalReqIDs),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}

	if len(deleteSignalReqIDs) > 0 {
		batch.Query(templateDeleteWorkflowExecutionSignalRequestedQuery,
			convert.StringSetToSlice(deleteSignalReqIDs),
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
}

func resetSignalRequested(
	batch *gocql.Batch,
	signalRequested map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {

	batch.Query(templateResetSignalRequestedQuery,
		convert.StringSetToSlice(signalRequested),
		shardID,
		rowTypeExecution,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func updateBufferedEvents(
	batch *gocql.Batch,
	newBufferedEvents *commonpb.DataBlob,
	clearBufferedEvents bool,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	compressor *blobCompressor,
) error {

	if clearBufferedEvents {
		batch.Query(templateDeleteBufferedEventsQuery,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	} else if newBufferedEvents != nil {
		data, encoding, err := compressor.compressBlob(newBufferedEvents)
		if err != nil {
			return err
		}
		values := make(map[string]any)
		values["encoding_type"] = encoding
		values["version"] = int64(0)
		values["data"] = data
		newEventValues := []map[string]any{values}
		batch.Query(templateAppendBufferedEventsQuery,
			newEventValues,
			shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
	return nil
}

func convertBlobMapToByteMap[T comparable](
	input map[T]*commonpb.DataBlob,
	compressor *blobCompressor,
) (map[T][]byte, enumspb.EncodingType, error) {
	sMap := make(map[T][]byte)

	var encoding enumspb.EncodingType
	for key, blob := range input {
		data, _, err := compressor.compressBlob(blob)
		if err != nil {
			return nil, encoding, err
		}
		encoding = blob.EncodingType
		sMap[key] = data
	}

	return sMap, encoding, nil
}

func createHistoryEventBatchBlob(
	result map[string]any,
	compressor *blobCompressor,
) (*commonpb.DataBlob, error) {
	eventBatch := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_UNSPECIFIED}
	for k, v := range result {
		switch k {
		case "encoding_type":
			encodingStr := v.(string)
			if encoding, err := enumspb.EncodingTypeFromString(encodingStr); err == nil {
				eventBatch.EncodingType = enumspb.EncodingType(encoding)
			}
		case "data":
			data, err := compressor.decompressData(v.([]byte))
			if err != nil {
				return nil, err
			}
			eventBatch.Data = data
		}
	}

	return eventBatch, nil
}
