package cassandra

import (
	"context"
	"fmt"
	"math"

	enumspb "go.temporal.io/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
)

// NewTaskStoreWithMatchingTaskMigration creates a task store with an explicit task-table mode.
// It is also used by migration validation and target-only benchmarks before cluster-wide cutover.
func (f *Factory) NewTaskStoreWithMatchingTaskMigration(
	enableFairness bool,
	mode MatchingTaskMigrationMode,
	bucketCount int,
) (p.TaskStore, error) {
	if err := ValidateMatchingTaskMigrationModeSchema(
		context.TODO(),
		f.session,
		f.cfg.Keyspace,
		mode,
		enableFairness,
	); err != nil {
		return nil, fmt.Errorf("validate Cassandra matching task migration schema: %w", err)
	}
	return NewMatchingTaskStoreWithMigrations(
		f.session,
		f.logger,
		enableFairness,
		normalizeTaskQueueUserDataMigrationMode(f.cfg.TaskQueueUserDataMigrationMode),
		f.cfg.TaskQueueUserDataBucketCount,
		mode,
		bucketCount,
	)
}

// CutoverMatchingTasksV3Queue fences one source queue, reconciles its tasks, and activates target authority.
func (f *Factory) CutoverMatchingTasksV3Queue(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	fair bool,
	bucketCount int,
) error {
	if bucketCount == 0 {
		bucketCount = DefaultMatchingTaskStorageBucketCount
	}
	if err := validateMatchingTaskStorageBucketCount(bucketCount); err != nil {
		return err
	}
	source, err := NewMatchingTaskStoreWithUserDataMigration(
		f.session,
		f.logger,
		fair,
		normalizeTaskQueueUserDataMigrationMode(f.cfg.TaskQueueUserDataMigrationMode),
		f.cfg.TaskQueueUserDataBucketCount,
	)
	if err != nil {
		return err
	}
	sourceQueue, err := configureMatchingTaskSourceMigration(source, matchingTaskAuthorityTarget, bucketCount)
	if err != nil {
		return err
	}
	sourceMetadata, err := sourceQueue.initializeSourceAuthorityForCutover(ctx, namespaceID, taskQueue, taskType)
	if err != nil {
		return err
	}
	target := newMatchingTaskStoreV3(f.session, fair, bucketCount)
	sourceTarget := *target
	sourceTarget.authority = matchingTaskAuthoritySource
	targetMetadata, err := sourceTarget.getCurrentMetadata(ctx, namespaceID, taskQueue, taskType, true)
	if err != nil {
		return err
	}
	if targetMetadata.authority == matchingTaskAuthorityTarget && sourceMetadata.authority == matchingTaskAuthorityTarget {
		return nil
	}
	if targetMetadata.rangeID == math.MaxInt64 {
		return fmt.Errorf("cannot cut over Cassandra matching task queue %q at maximum range ID", taskQueue)
	}
	newRangeID := targetMetadata.rangeID + 1
	if sourceMetadata.authority == matchingTaskAuthoritySealing {
		newRangeID = sourceMetadata.rangeID
	}
	if sourceMetadata.rangeID != targetMetadata.rangeID && sourceMetadata.rangeID != newRangeID {
		return fmt.Errorf(
			"cannot cut over Cassandra matching task queue %q: source range %d, target range %d",
			taskQueue,
			sourceMetadata.rangeID,
			targetMetadata.rangeID,
		)
	}
	queueInfo := p.NewDataBlob(sourceMetadata.taskQueue, sourceMetadata.taskQueueEncoding)
	info, err := f.serializer.TaskQueueInfoFromBlob(queueInfo)
	if err != nil {
		return fmt.Errorf("deserialize Cassandra matching task queue %q for cutover: %w", taskQueue, err)
	}
	store := &matchingTaskMigrationStore{
		TaskStore:   source,
		sourceQueue: sourceQueue,
		target:      target,
		mode:        MatchingTaskMigrationModeTargetDual,
	}
	_, err = store.UpdateTaskQueue(ctx, &p.InternalUpdateTaskQueueRequest{
		NamespaceID:   namespaceID,
		TaskQueue:     taskQueue,
		TaskType:      taskType,
		RangeID:       newRangeID,
		TaskQueueInfo: queueInfo,
		TaskQueueKind: info.Kind,
		ExpiryTime:    info.ExpiryTime,
		PrevRangeID:   targetMetadata.rangeID,
	})
	return err
}

func (f *Factory) BackfillMatchingTasksV3(
	ctx context.Context,
	options MatchingTaskBackfillOptions,
) (int64, error) {
	return BackfillMatchingTasksV3(ctx, f.session, options)
}

func (f *Factory) ValidateMatchingTasksV3Queue(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	fair bool,
	bucketCount int,
) (MatchingTaskValidationResult, error) {
	return ValidateMatchingTasksV3Queue(
		ctx,
		f.session,
		namespaceID,
		taskQueue,
		taskType,
		fair,
		bucketCount,
	)
}

func (f *Factory) RepairMatchingTasksV3QueueDuplicates(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
	fair bool,
	bucketCount int,
) (int, error) {
	return RepairMatchingTasksV3QueueDuplicates(
		ctx,
		f.session,
		namespaceID,
		taskQueue,
		taskType,
		fair,
		bucketCount,
	)
}
