package cassandra

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type matchingTaskMigrationStore struct {
	p.TaskStore
	sourceQueue *taskQueueStore
	target      *matchingTaskStoreV3
	mode        MatchingTaskMigrationMode
}

type matchingTaskQueueMutationStore interface {
	CreateTaskQueue(context.Context, *p.InternalCreateTaskQueueRequest) error
	GetTaskQueue(context.Context, *p.InternalGetTaskQueueRequest) (*p.InternalGetTaskQueueResponse, error)
	UpdateTaskQueue(context.Context, *p.InternalUpdateTaskQueueRequest) (*p.UpdateTaskQueueResponse, error)
	DeleteTaskQueue(context.Context, *p.DeleteTaskQueueRequest) error
}

func (d *matchingTaskMigrationStore) CreateTaskQueue(
	ctx context.Context,
	request *p.InternalCreateTaskQueueRequest,
) error {
	switch d.mode {
	case MatchingTaskMigrationModeSourceDual:
		if err := createMatchingTaskQueueIdempotently(ctx, d.TaskStore, request); err != nil {
			return err
		}
		return d.ensureTargetQueue(ctx, request)
	case MatchingTaskMigrationModeTargetDual:
		if err := createMatchingTaskQueueIdempotently(ctx, d.target, request); err != nil {
			return err
		}
		return createMatchingTaskQueueIdempotently(ctx, d.TaskStore, request)
	case MatchingTaskMigrationModeTargetOnly:
		return d.target.CreateTaskQueue(ctx, request)
	default:
		return d.TaskStore.CreateTaskQueue(ctx, request)
	}
}

func (d *matchingTaskMigrationStore) GetTaskQueue(
	ctx context.Context,
	request *p.InternalGetTaskQueueRequest,
) (*p.InternalGetTaskQueueResponse, error) {
	switch d.mode {
	case MatchingTaskMigrationModeTargetDual, MatchingTaskMigrationModeTargetOnly:
		return d.target.GetTaskQueue(ctx, request)
	default:
		return d.TaskStore.GetTaskQueue(ctx, request)
	}
}

//nolint:revive // Dual-layout metadata updates include idempotent repair for every authority mode.
func (d *matchingTaskMigrationStore) UpdateTaskQueue(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
) (*p.UpdateTaskQueueResponse, error) {
	switch d.mode {
	case MatchingTaskMigrationModeSourceDual:
		response, err := updateMatchingTaskQueueIdempotently(ctx, d.TaskStore, request)
		if err != nil {
			return nil, err
		}
		if _, err := updateMatchingTaskQueueIdempotently(ctx, d.target, request); err != nil {
			var conditionFailed *p.ConditionFailedError
			if !errors.As(err, &conditionFailed) {
				return nil, err
			}
			if err := d.target.ensureActiveMetadata(
				ctx,
				request.NamespaceID,
				request.TaskQueue,
				request.TaskType,
				request.PrevRangeID,
				request.TaskQueueInfo,
			); err != nil {
				return nil, err
			}
			if _, err := updateMatchingTaskQueueIdempotently(ctx, d.target, request); err != nil {
				return nil, err
			}
		}
		return response, nil
	case MatchingTaskMigrationModeTargetDual:
		source, err := d.sourceQueue.initializeSourceAuthorityForCutover(
			ctx, request.NamespaceID, request.TaskQueue, request.TaskType,
		)
		if err != nil {
			return nil, err
		}
		switch source.authority {
		case matchingTaskAuthoritySource, matchingTaskAuthoritySealing:
			if err := d.cutOverTaskQueue(ctx, request); err != nil {
				return nil, err
			}
			return &p.UpdateTaskQueueResponse{}, nil
		case matchingTaskAuthorityTarget:
			response, err := updateMatchingTaskQueueIdempotently(ctx, d.target, request)
			if err != nil {
				return nil, err
			}
			if _, err := updateMatchingTaskQueueIdempotently(ctx, d.TaskStore, request); err != nil {
				return nil, err
			}
			return response, nil
		default:
			return nil, &p.ConditionFailedError{Msg: fmt.Sprintf(
				"Cassandra matching task source queue %q has invalid authority %d",
				request.TaskQueue,
				source.authority,
			)}
		}
	case MatchingTaskMigrationModeTargetOnly:
		return d.target.UpdateTaskQueue(ctx, request)
	default:
		return d.TaskStore.UpdateTaskQueue(ctx, request)
	}
}

func (d *matchingTaskMigrationStore) ListTaskQueue(
	ctx context.Context,
	request *p.ListTaskQueueRequest,
) (*p.InternalListTaskQueueResponse, error) {
	switch d.mode {
	case MatchingTaskMigrationModeTargetDual, MatchingTaskMigrationModeTargetOnly:
		return d.target.ListTaskQueue(ctx, request)
	default:
		return d.TaskStore.ListTaskQueue(ctx, request)
	}
}

func (d *matchingTaskMigrationStore) DeleteTaskQueue(
	ctx context.Context,
	request *p.DeleteTaskQueueRequest,
) error {
	switch d.mode {
	case MatchingTaskMigrationModeSourceDual:
		if err := deleteMatchingTaskQueueIdempotently(ctx, d.TaskStore, request); err != nil {
			return err
		}
		_, err := d.target.GetTaskQueue(ctx, &p.InternalGetTaskQueueRequest{
			NamespaceID: request.TaskQueue.NamespaceID,
			TaskQueue:   request.TaskQueue.TaskQueueName,
			TaskType:    request.TaskQueue.TaskQueueType,
		})
		if err != nil {
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) {
				return nil
			}
			return err
		}
		return d.target.DeleteTaskQueue(ctx, request)
	case MatchingTaskMigrationModeTargetDual:
		if err := d.requireTargetAuthority(
			ctx,
			request.TaskQueue.NamespaceID,
			request.TaskQueue.TaskQueueName,
			request.TaskQueue.TaskQueueType,
		); err != nil {
			return err
		}
		if err := deleteMatchingTaskQueueIdempotently(ctx, d.target, request); err != nil {
			return err
		}
		return deleteMatchingTaskQueueIdempotently(ctx, d.TaskStore, request)
	case MatchingTaskMigrationModeTargetOnly:
		return d.target.DeleteTaskQueue(ctx, request)
	default:
		return d.TaskStore.DeleteTaskQueue(ctx, request)
	}
}

func (d *matchingTaskMigrationStore) CreateTasks(
	ctx context.Context,
	request *p.InternalCreateTasksRequest,
) (*p.CreateTasksResponse, error) {
	switch d.mode {
	case MatchingTaskMigrationModeSourceDual:
		response, err := d.TaskStore.CreateTasks(ctx, request)
		if err != nil {
			return nil, err
		}
		if err := d.target.ensureActiveMetadata(
			ctx,
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			request.RangeID,
			request.TaskQueueInfo,
		); err != nil {
			return nil, err
		}
		if _, err := d.target.CreateTasks(ctx, request); err != nil {
			return nil, err
		}
		return response, nil
	case MatchingTaskMigrationModeTargetDual:
		if err := d.requireTargetAuthority(ctx, request.NamespaceID, request.TaskQueue, request.TaskType); err != nil {
			return nil, err
		}
		response, err := d.target.CreateTasks(ctx, request)
		if err != nil {
			return nil, err
		}
		if _, err := d.TaskStore.CreateTasks(ctx, request); err != nil {
			return nil, err
		}
		return response, nil
	case MatchingTaskMigrationModeTargetOnly:
		return d.target.CreateTasks(ctx, request)
	default:
		return d.TaskStore.CreateTasks(ctx, request)
	}
}

func (d *matchingTaskMigrationStore) ensureTargetQueue(
	ctx context.Context,
	request *p.InternalCreateTaskQueueRequest,
) error {
	return createMatchingTaskQueueIdempotently(ctx, d.target, request)
}

func (d *matchingTaskMigrationStore) GetTasks(
	ctx context.Context,
	request *p.GetTasksRequest,
) (*p.InternalGetTasksResponse, error) {
	switch d.mode {
	case MatchingTaskMigrationModeTargetDual:
		if err := d.requireTargetAuthority(ctx, request.NamespaceID, request.TaskQueue, request.TaskType); err != nil {
			return nil, err
		}
		return d.target.GetTasks(ctx, request)
	case MatchingTaskMigrationModeTargetOnly:
		return d.target.GetTasks(ctx, request)
	default:
		return d.TaskStore.GetTasks(ctx, request)
	}
}

func createMatchingTaskQueueIdempotently(
	ctx context.Context,
	store matchingTaskQueueMutationStore,
	request *p.InternalCreateTaskQueueRequest,
) error {
	err := store.CreateTaskQueue(ctx, request)
	if err == nil {
		return nil
	}
	var conditionFailed *p.ConditionFailedError
	if !errors.As(err, &conditionFailed) {
		return err
	}
	if matchingTaskQueueStateMatches(
		ctx,
		store,
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		request.RangeID,
		request.TaskQueueInfo,
	) {
		return nil
	}
	return err
}

func updateMatchingTaskQueueIdempotently(
	ctx context.Context,
	store matchingTaskQueueMutationStore,
	request *p.InternalUpdateTaskQueueRequest,
) (*p.UpdateTaskQueueResponse, error) {
	response, err := store.UpdateTaskQueue(ctx, request)
	if err == nil {
		return response, nil
	}
	var conditionFailed *p.ConditionFailedError
	if !errors.As(err, &conditionFailed) {
		return nil, err
	}
	if matchingTaskQueueStateMatches(
		ctx,
		store,
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		request.RangeID,
		request.TaskQueueInfo,
	) {
		return &p.UpdateTaskQueueResponse{}, nil
	}
	return nil, err
}

func matchingTaskQueueStateMatches(
	ctx context.Context,
	store matchingTaskQueueMutationStore,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	rangeID int64,
	blob *commonpb.DataBlob,
) bool {
	response, err := store.GetTaskQueue(ctx, &p.InternalGetTaskQueueRequest{
		NamespaceID: namespaceID,
		TaskQueue:   taskQueue,
		TaskType:    taskType,
	})
	return err == nil &&
		response.RangeID == rangeID &&
		response.TaskQueueInfo.EncodingType == blob.EncodingType &&
		bytes.Equal(response.TaskQueueInfo.Data, blob.Data)
}

func deleteMatchingTaskQueueIdempotently(
	ctx context.Context,
	store matchingTaskQueueMutationStore,
	request *p.DeleteTaskQueueRequest,
) error {
	err := store.DeleteTaskQueue(ctx, request)
	if err == nil {
		return nil
	}
	var conditionFailed *p.ConditionFailedError
	var notFound *serviceerror.NotFound
	if !errors.As(err, &conditionFailed) && !errors.As(err, &notFound) {
		return err
	}
	_, getErr := store.GetTaskQueue(ctx, &p.InternalGetTaskQueueRequest{
		NamespaceID: request.TaskQueue.NamespaceID,
		TaskQueue:   request.TaskQueue.TaskQueueName,
		TaskType:    request.TaskQueue.TaskQueueType,
	})
	if errors.As(getErr, &notFound) {
		return nil
	}
	return err
}

func (d *matchingTaskMigrationStore) CompleteTasksLessThan(
	ctx context.Context,
	request *p.CompleteTasksLessThanRequest,
) (int, error) {
	switch d.mode {
	case MatchingTaskMigrationModeTargetDual:
		if err := d.requireTargetAuthority(ctx, request.NamespaceID, request.TaskQueueName, request.TaskType); err != nil {
			return 0, err
		}
		count, err := d.target.CompleteTasksLessThan(ctx, request)
		if err != nil {
			return 0, err
		}
		if _, err := d.TaskStore.CompleteTasksLessThan(ctx, request); err != nil {
			return 0, err
		}
		return count, nil
	case MatchingTaskMigrationModeTargetOnly:
		return d.target.CompleteTasksLessThan(ctx, request)
	default:
		return d.TaskStore.CompleteTasksLessThan(ctx, request)
	}
}

func (d *matchingTaskMigrationStore) requireTargetAuthority(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
) error {
	record, err := d.sourceQueue.readMigrationAuthority(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return gocql.ConvertError("ReadMatchingTaskSourceAuthorityForTargetOperation", err)
	}
	if record.authority != matchingTaskAuthorityTarget ||
		record.bucketCount != int16(d.target.bucketCount) {
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"Cassandra matching task queue %q target authority is not active: authority %d buckets %d",
			taskQueue,
			record.authority,
			record.bucketCount,
		)}
	}
	return nil
}

func (d *matchingTaskMigrationStore) cutOverTaskQueue(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
) error {
	source, err := d.sourceQueue.sealForTargetCutover(ctx, request)
	if err != nil {
		return err
	}
	if source.authority == matchingTaskAuthorityTarget {
		return nil
	}
	repairTimestamp, err := d.target.reconcileSealedSourceQueue(
		ctx,
		d.sourceQueue,
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
	)
	if err != nil {
		return err
	}

	sourceTarget := *d.target
	sourceTarget.authority = matchingTaskAuthoritySource
	targetMetadata, err := sourceTarget.getCurrentMetadata(
		ctx, request.NamespaceID, request.TaskQueue, request.TaskType, true,
	)
	if err != nil {
		return err
	}
	if targetMetadata.authority == matchingTaskAuthoritySource {
		if _, err := updateMatchingTaskQueueIdempotently(ctx, &sourceTarget, request); err != nil {
			return err
		}
	} else if targetMetadata.authority != matchingTaskAuthorityTarget || targetMetadata.rangeID != request.RangeID {
		return &p.ConditionFailedError{Msg: fmt.Sprintf(
			"Cassandra matching target queue %q has range %d authority %d during cutover",
			request.TaskQueue,
			targetMetadata.rangeID,
			targetMetadata.authority,
		)}
	}
	if err := d.target.promoteSourceAuthority(ctx, request); err != nil {
		return err
	}
	return d.sourceQueue.publishTargetAuthority(ctx, request, repairTimestamp)
}
