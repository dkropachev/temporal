package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sync"

	cgocql "github.com/gocql/gocql"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type taskQueueUserDataAuthority int

const (
	taskQueueUserDataAuthorityUnspecified taskQueueUserDataAuthority = iota
	taskQueueUserDataAuthoritySource
	taskQueueUserDataAuthoritySealing
	taskQueueUserDataAuthorityTarget
)

const (
	templateGetTaskQueueUserDataSourceAuthority = `SELECT migration_authority, migration_bucket_count, migration_generation
		FROM task_queue_user_data WHERE namespace_id = ? LIMIT 1`
	templateInitializeTaskQueueUserDataSourceAuthority = `UPDATE task_queue_user_data SET
		migration_authority = ?, migration_bucket_count = ?, migration_generation = ? WHERE namespace_id = ?
		IF migration_authority = null AND migration_bucket_count = null AND migration_generation = null`
	templateTransitionTaskQueueUserDataSourceAuthority = `UPDATE task_queue_user_data SET migration_authority = ?
		WHERE namespace_id = ? IF migration_authority = ? AND migration_bucket_count = ? AND migration_generation = ?`
	templateGuardTaskQueueUserDataSourceAuthority = `UPDATE task_queue_user_data SET migration_authority = ?
		WHERE namespace_id = ? IF migration_authority = ? AND migration_bucket_count = ? AND migration_generation = ?`
	templateGuardUninitializedTaskQueueUserDataSourceAuthority = `UPDATE task_queue_user_data SET
		migration_authority = null, migration_bucket_count = null, migration_generation = null
		WHERE namespace_id = ?
		IF migration_authority = null AND migration_bucket_count = null AND migration_generation = null`

	templateGetTaskQueueUserDataTargetAuthority = `SELECT migration_authority, migration_bucket_count, migration_generation
		FROM task_queue_user_data_v2_txn WHERE namespace_id = ? AND txn_id = ''`
	templateInitializeTaskQueueUserDataTargetAuthority = `UPDATE task_queue_user_data_v2_txn SET
		migration_authority = ?, migration_bucket_count = ?, migration_generation = ?
		WHERE namespace_id = ? AND txn_id = ''
		IF migration_authority = null AND migration_bucket_count = null AND migration_generation = null`
	templateTransitionTaskQueueUserDataTargetAuthority = `UPDATE task_queue_user_data_v2_txn SET migration_authority = ?
		WHERE namespace_id = ? AND txn_id = ''
		IF migration_authority = ? AND migration_bucket_count = ? AND migration_generation = ?`
)

type taskQueueUserDataAuthorityRecord struct {
	authority   taskQueueUserDataAuthority
	bucketCount int
	generation  cgocql.UUID
}

type taskQueueUserDataAuthorityIdentityCache struct {
	mu         sync.Mutex
	loaded     bool
	generation cgocql.UUID
}

func (a taskQueueUserDataAuthority) String() string {
	switch a {
	case taskQueueUserDataAuthoritySource:
		return "source"
	case taskQueueUserDataAuthoritySealing:
		return "sealing"
	case taskQueueUserDataAuthorityTarget:
		return "target"
	default:
		return "unspecified"
	}
}

func (d *userDataStore) taskQueueUserDataAuthorityIdentity(
	ctx context.Context,
) (taskQueueUserDataAuthorityRecord, error) {
	cache := d.authorityIdentity
	if cache == nil {
		return taskQueueUserDataAuthorityRecord{}, serviceerror.NewUnavailable(
			"task queue user data migration authority is not initialized",
		)
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if !cache.loaded {
		metadata, err := NewSchemaLayoutMetadataStore(d.Session).Load(ctx, SchemaLayoutTaskQueueUserData)
		if err != nil {
			return taskQueueUserDataAuthorityRecord{}, fmt.Errorf(
				"load task queue user data schema layout identity: %w",
				err,
			)
		}
		if metadata.ImmutableParameter != int64(d.effectiveTaskQueueUserDataBucketCount()) {
			return taskQueueUserDataAuthorityRecord{}, fmt.Errorf(
				"task queue user data schema layout has %d buckets; configured %d",
				metadata.ImmutableParameter,
				d.effectiveTaskQueueUserDataBucketCount(),
			)
		}
		cache.generation = metadata.Generation
		cache.loaded = true
	}
	return taskQueueUserDataAuthorityRecord{
		bucketCount: d.effectiveTaskQueueUserDataBucketCount(),
		generation:  cache.generation,
	}, nil
}

func readTaskQueueUserDataSourceAuthority(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
) (taskQueueUserDataAuthorityRecord, error) {
	return readTaskQueueUserDataAuthority(
		ctx,
		session.Query(templateGetTaskQueueUserDataSourceAuthority, namespaceID),
		"ReadTaskQueueUserDataSourceAuthority",
	)
}

func readTaskQueueUserDataTargetAuthority(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
) (taskQueueUserDataAuthorityRecord, error) {
	return readTaskQueueUserDataAuthority(
		ctx,
		session.Query(templateGetTaskQueueUserDataTargetAuthority, namespaceID),
		"ReadTaskQueueUserDataTargetAuthority",
	)
}

func readTaskQueueUserDataAuthority(
	ctx context.Context,
	query gocql.Query,
	operation string,
) (taskQueueUserDataAuthorityRecord, error) {
	var authority *int
	var bucketCount *int16
	var generation *cgocql.UUID
	err := query.WithContext(ctx).Scan(&authority, &bucketCount, &generation)
	if gocql.IsNotFoundError(err) {
		return taskQueueUserDataAuthorityRecord{}, nil
	}
	if err != nil {
		return taskQueueUserDataAuthorityRecord{}, gocql.ConvertError(operation, err)
	}
	if authority == nil && bucketCount == nil && generation == nil {
		return taskQueueUserDataAuthorityRecord{}, nil
	}
	if authority == nil || bucketCount == nil || generation == nil {
		return taskQueueUserDataAuthorityRecord{}, serviceerror.NewDataLoss(
			"task queue user data migration authority is partially initialized",
		)
	}
	record := taskQueueUserDataAuthorityRecord{
		authority:   taskQueueUserDataAuthority(*authority),
		bucketCount: int(*bucketCount),
		generation:  *generation,
	}
	if record.authority != taskQueueUserDataAuthoritySource &&
		record.authority != taskQueueUserDataAuthoritySealing &&
		record.authority != taskQueueUserDataAuthorityTarget {
		return taskQueueUserDataAuthorityRecord{}, serviceerror.NewDataLossf(
			"task queue user data migration authority has invalid state %d",
			*authority,
		)
	}
	return record, nil
}

func validateTaskQueueUserDataAuthority(
	namespaceID string,
	record taskQueueUserDataAuthorityRecord,
	identity taskQueueUserDataAuthorityRecord,
	expected taskQueueUserDataAuthority,
) error {
	if record.authority != expected ||
		record.bucketCount != identity.bucketCount ||
		record.generation != identity.generation {
		return serviceerror.NewUnavailablef(
			"task queue user data namespace %s requires authority %s, %d buckets, generation %s; got authority %s, %d buckets, generation %s",
			namespaceID,
			expected,
			identity.bucketCount,
			identity.generation,
			record.authority,
			record.bucketCount,
			record.generation,
		)
	}
	return nil
}

func initializeTaskQueueUserDataTargetAuthority(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	identity taskQueueUserDataAuthorityRecord,
	authority taskQueueUserDataAuthority,
) error {
	if _, err := session.Query(
		templateInitializeTaskQueueUserDataV2NamespaceQuery,
		namespaceID,
	).WithContext(ctx).MapScanCAS(make(map[string]any)); err != nil {
		return gocql.ConvertError("InitializeTaskQueueUserDataV2Namespace", err)
	}
	applied, err := session.Query(
		templateInitializeTaskQueueUserDataTargetAuthority,
		int(authority),
		int16(identity.bucketCount),
		identity.generation,
		namespaceID,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("InitializeTaskQueueUserDataTargetAuthority", err)
	}
	if applied {
		return nil
	}
	record, err := readTaskQueueUserDataTargetAuthority(ctx, session, namespaceID)
	if err != nil {
		return err
	}
	return validateTaskQueueUserDataAuthority(namespaceID, record, identity, authority)
}

func initializeTaskQueueUserDataSourceAuthority(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	identity taskQueueUserDataAuthorityRecord,
) error {
	applied, err := session.Query(
		templateInitializeTaskQueueUserDataSourceAuthority,
		int(taskQueueUserDataAuthoritySource),
		int16(identity.bucketCount),
		identity.generation,
		namespaceID,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("InitializeTaskQueueUserDataSourceAuthority", err)
	}
	if applied {
		return nil
	}
	record, err := readTaskQueueUserDataSourceAuthority(ctx, session, namespaceID)
	if err != nil {
		return err
	}
	return validateTaskQueueUserDataAuthority(
		namespaceID,
		record,
		identity,
		taskQueueUserDataAuthoritySource,
	)
}

func activateTaskQueueUserDataSourceAuthority(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	identity taskQueueUserDataAuthorityRecord,
) error {
	if err := initializeTaskQueueUserDataTargetAuthority(
		ctx,
		session,
		namespaceID,
		identity,
		taskQueueUserDataAuthoritySource,
	); err != nil {
		return err
	}
	return initializeTaskQueueUserDataSourceAuthority(ctx, session, namespaceID, identity)
}

func transitionTaskQueueUserDataAuthority(
	ctx context.Context,
	session gocql.Session,
	query string,
	read func(context.Context, gocql.Session, string) (taskQueueUserDataAuthorityRecord, error),
	operation string,
	namespaceID string,
	identity taskQueueUserDataAuthorityRecord,
	from taskQueueUserDataAuthority,
	to taskQueueUserDataAuthority,
) error {
	if to <= from {
		return fmt.Errorf("illegal task queue user data authority transition %s -> %s", from, to)
	}
	applied, err := session.Query(
		query,
		int(to),
		namespaceID,
		int(from),
		int16(identity.bucketCount),
		identity.generation,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError(operation, err)
	}
	if applied {
		return nil
	}
	record, err := read(ctx, session, namespaceID)
	if err != nil {
		return err
	}
	if record.authority == to {
		return validateTaskQueueUserDataAuthority(namespaceID, record, identity, to)
	}
	if err := validateTaskQueueUserDataAuthority(namespaceID, record, identity, from); err != nil {
		return err
	}
	return serviceerror.NewUnavailablef(
		"task queue user data authority transition %s -> %s was not applied; retry",
		from,
		to,
	)
}

type TaskQueueUserDataCutoverOptions struct {
	NamespaceID string
	BucketCount int
	Concurrency int
	Generation  cgocql.UUID
}

func ActivateTaskQueueUserDataV2NamespaceFencing(
	ctx context.Context,
	session gocql.Session,
	options TaskQueueUserDataCutoverOptions,
) error {
	identity, err := validateTaskQueueUserDataCutoverOptions(options)
	if err != nil {
		return err
	}
	return ensureTaskQueueUserDataSourceAuthorityActivated(
		ctx,
		session,
		options.NamespaceID,
		identity,
	)
}

func CutoverTaskQueueUserDataV2Namespace(
	ctx context.Context,
	session gocql.Session,
	options TaskQueueUserDataCutoverOptions,
) (TaskQueueUserDataValidationResult, error) {
	identity, err := validateTaskQueueUserDataCutoverOptions(options)
	if err != nil {
		return TaskQueueUserDataValidationResult{}, err
	}
	if err := ensureTaskQueueUserDataSourceAuthorityActivated(
		ctx,
		session,
		options.NamespaceID,
		identity,
	); err != nil {
		return TaskQueueUserDataValidationResult{}, err
	}
	target, err := readTaskQueueUserDataTargetAuthority(ctx, session, options.NamespaceID)
	if err != nil {
		return TaskQueueUserDataValidationResult{}, err
	}
	source, err := readTaskQueueUserDataSourceAuthority(ctx, session, options.NamespaceID)
	if err != nil {
		return TaskQueueUserDataValidationResult{}, err
	}
	if err := validateTaskQueueUserDataCutoverState(options.NamespaceID, identity, source, target); err != nil {
		return TaskQueueUserDataValidationResult{}, err
	}
	if target.authority == taskQueueUserDataAuthorityTarget {
		return ValidateTaskQueueUserDataV2Namespace(ctx, session, options.NamespaceID, options.BucketCount)
	}
	if target.authority == taskQueueUserDataAuthoritySource {
		if err := transitionTaskQueueUserDataAuthority(
			ctx,
			session,
			templateTransitionTaskQueueUserDataTargetAuthority,
			readTaskQueueUserDataTargetAuthority,
			"SealTaskQueueUserDataTargetAuthority",
			options.NamespaceID,
			identity,
			taskQueueUserDataAuthoritySource,
			taskQueueUserDataAuthoritySealing,
		); err != nil {
			return TaskQueueUserDataValidationResult{}, err
		}
		target.authority = taskQueueUserDataAuthoritySealing
	}
	if source.authority == taskQueueUserDataAuthoritySource {
		if err := transitionTaskQueueUserDataAuthority(
			ctx,
			session,
			templateTransitionTaskQueueUserDataSourceAuthority,
			readTaskQueueUserDataSourceAuthority,
			"SealTaskQueueUserDataSourceAuthority",
			options.NamespaceID,
			identity,
			taskQueueUserDataAuthoritySource,
			taskQueueUserDataAuthoritySealing,
		); err != nil {
			return TaskQueueUserDataValidationResult{}, err
		}
		source.authority = taskQueueUserDataAuthoritySealing
	}
	if _, err := RecoverTaskQueueUserDataV2NamespaceTransactions(
		ctx,
		session,
		options.NamespaceID,
		options.BucketCount,
		options.Concurrency,
	); err != nil {
		return TaskQueueUserDataValidationResult{}, err
	}
	if source.authority == taskQueueUserDataAuthoritySealing {
		if _, err := ReconcileTaskQueueUserDataV2Namespace(
			ctx,
			session,
			options.NamespaceID,
			options.BucketCount,
			options.Concurrency,
		); err != nil {
			return TaskQueueUserDataValidationResult{}, err
		}
	}
	validation, err := ValidateTaskQueueUserDataV2Namespace(
		ctx,
		session,
		options.NamespaceID,
		options.BucketCount,
	)
	if err != nil {
		return TaskQueueUserDataValidationResult{}, err
	}
	if !validation.Matches() {
		return validation, fmt.Errorf(
			"task queue user data namespace %s differs after reconciliation: %v",
			options.NamespaceID,
			validation.Mismatches,
		)
	}
	if source.authority == taskQueueUserDataAuthoritySealing {
		if err := transitionTaskQueueUserDataAuthority(
			ctx,
			session,
			templateTransitionTaskQueueUserDataSourceAuthority,
			readTaskQueueUserDataSourceAuthority,
			"ActivateTaskQueueUserDataSourceTargetAuthority",
			options.NamespaceID,
			identity,
			taskQueueUserDataAuthoritySealing,
			taskQueueUserDataAuthorityTarget,
		); err != nil {
			return validation, err
		}
	}
	if err := transitionTaskQueueUserDataAuthority(
		ctx,
		session,
		templateTransitionTaskQueueUserDataTargetAuthority,
		readTaskQueueUserDataTargetAuthority,
		"PublishTaskQueueUserDataTargetAuthority",
		options.NamespaceID,
		identity,
		taskQueueUserDataAuthoritySealing,
		taskQueueUserDataAuthorityTarget,
	); err != nil {
		return validation, err
	}
	return validation, nil
}

func validateTaskQueueUserDataCutoverOptions(
	options TaskQueueUserDataCutoverOptions,
) (taskQueueUserDataAuthorityRecord, error) {
	if options.NamespaceID == "" {
		return taskQueueUserDataAuthorityRecord{}, errors.New("task queue user data cutover requires namespace ID")
	}
	if options.Concurrency <= 0 {
		return taskQueueUserDataAuthorityRecord{}, errors.New("task queue user data cutover concurrency must be positive")
	}
	if _, err := taskQueueUserDataBucket("", options.BucketCount); err != nil {
		return taskQueueUserDataAuthorityRecord{}, err
	}
	if options.Generation == (cgocql.UUID{}) {
		return taskQueueUserDataAuthorityRecord{}, errors.New("task queue user data cutover requires target generation")
	}
	return taskQueueUserDataAuthorityRecord{
		bucketCount: options.BucketCount,
		generation:  options.Generation,
	}, nil
}

func ensureTaskQueueUserDataSourceAuthorityActivated(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	identity taskQueueUserDataAuthorityRecord,
) error {
	target, err := readTaskQueueUserDataTargetAuthority(ctx, session, namespaceID)
	if err != nil {
		return err
	}
	source, err := readTaskQueueUserDataSourceAuthority(ctx, session, namespaceID)
	if err != nil {
		return err
	}
	if target.authority == taskQueueUserDataAuthorityUnspecified {
		if source.authority != taskQueueUserDataAuthorityUnspecified && source.authority != taskQueueUserDataAuthoritySource {
			return serviceerror.NewDataLoss("target authority is missing after source cutover began")
		}
		if err := initializeTaskQueueUserDataTargetAuthority(
			ctx,
			session,
			namespaceID,
			identity,
			taskQueueUserDataAuthoritySource,
		); err != nil {
			return err
		}
	}
	if source.authority == taskQueueUserDataAuthorityUnspecified {
		if target.authority != taskQueueUserDataAuthorityUnspecified && target.authority != taskQueueUserDataAuthoritySource {
			return serviceerror.NewDataLoss("source authority is missing after target cutover began")
		}
		if err := initializeTaskQueueUserDataSourceAuthority(ctx, session, namespaceID, identity); err != nil {
			return err
		}
	}
	return nil
}

func validateTaskQueueUserDataCutoverState(
	namespaceID string,
	identity taskQueueUserDataAuthorityRecord,
	source taskQueueUserDataAuthorityRecord,
	target taskQueueUserDataAuthorityRecord,
) error {
	if err := validateTaskQueueUserDataAuthority(namespaceID, source, identity, source.authority); err != nil {
		return err
	}
	if err := validateTaskQueueUserDataAuthority(namespaceID, target, identity, target.authority); err != nil {
		return err
	}
	switch target.authority {
	case taskQueueUserDataAuthoritySource:
		if source.authority != taskQueueUserDataAuthoritySource {
			return serviceerror.NewDataLossf(
				"task queue user data namespace %s has mixed source=%s target=%s authority",
				namespaceID,
				source.authority,
				target.authority,
			)
		}
	case taskQueueUserDataAuthoritySealing:
		if source.authority != taskQueueUserDataAuthoritySource &&
			source.authority != taskQueueUserDataAuthoritySealing &&
			source.authority != taskQueueUserDataAuthorityTarget {
			return serviceerror.NewDataLossf(
				"task queue user data namespace %s has invalid sealing source authority %s",
				namespaceID,
				source.authority,
			)
		}
	case taskQueueUserDataAuthorityTarget:
		if source.authority != taskQueueUserDataAuthorityTarget {
			return serviceerror.NewDataLossf(
				"task queue user data namespace %s published target authority before source fencing",
				namespaceID,
			)
		}
	default:
		return serviceerror.NewDataLossf(
			"task queue user data namespace %s has invalid target authority %s",
			namespaceID,
			target.authority,
		)
	}
	return nil
}

//nolint:revive // Authority resolution validates every supported migration state and caches only terminal target authority.
func (d *userDataStore) resolveTaskQueueUserDataAuthority(
	ctx context.Context,
	namespaceID string,
) (taskQueueUserDataAuthority, error) {
	mode := normalizeTaskQueueUserDataMigrationMode(d.migrationMode)
	if mode == TaskQueueUserDataMigrationModeSourceOnly {
		record, err := readTaskQueueUserDataSourceAuthority(ctx, d.Session, namespaceID)
		if err != nil {
			return taskQueueUserDataAuthorityUnspecified, err
		}
		if record.authority != taskQueueUserDataAuthorityUnspecified {
			return taskQueueUserDataAuthorityUnspecified, serviceerror.NewUnavailable(
				"task queue user data namespace has activated migration authority; source-only writer is fenced",
			)
		}
		return taskQueueUserDataAuthorityUnspecified, nil
	}

	if (mode == TaskQueueUserDataMigrationModeTargetDual || mode == TaskQueueUserDataMigrationModeTargetOnly) &&
		d.targetAuthorityCache != nil {
		if _, ok := d.targetAuthorityCache.Load(namespaceID); ok {
			return taskQueueUserDataAuthorityTarget, nil
		}
	}
	identity, err := d.taskQueueUserDataAuthorityIdentity(ctx)
	if err != nil {
		return taskQueueUserDataAuthorityUnspecified, err
	}
	target, err := readTaskQueueUserDataTargetAuthority(ctx, d.Session, namespaceID)
	if err != nil {
		return taskQueueUserDataAuthorityUnspecified, err
	}

	if mode == TaskQueueUserDataMigrationModeTargetOnly && target.authority == taskQueueUserDataAuthorityUnspecified {
		if err := initializeTaskQueueUserDataTargetAuthority(
			ctx,
			d.Session,
			namespaceID,
			identity,
			taskQueueUserDataAuthorityTarget,
		); err != nil {
			return taskQueueUserDataAuthorityUnspecified, err
		}
		target = taskQueueUserDataAuthorityRecord{
			authority:   taskQueueUserDataAuthorityTarget,
			bucketCount: identity.bucketCount,
			generation:  identity.generation,
		}
	}
	if target.authority != taskQueueUserDataAuthorityUnspecified {
		if err := validateTaskQueueUserDataAuthority(
			namespaceID,
			target,
			identity,
			target.authority,
		); err != nil {
			return taskQueueUserDataAuthorityUnspecified, err
		}
	}

	switch mode {
	case TaskQueueUserDataMigrationModeSourceDual, TaskQueueUserDataMigrationModeTargetShadow:
		if target.authority == taskQueueUserDataAuthorityUnspecified {
			source, err := readTaskQueueUserDataSourceAuthority(ctx, d.Session, namespaceID)
			if err != nil {
				return taskQueueUserDataAuthorityUnspecified, err
			}
			if source.authority == taskQueueUserDataAuthorityUnspecified {
				return taskQueueUserDataAuthorityUnspecified, nil
			}
			return taskQueueUserDataAuthorityUnspecified, serviceerror.NewUnavailable(
				"task queue user data migration authority is only partially activated",
			)
		}
		if target.authority != taskQueueUserDataAuthoritySource {
			return taskQueueUserDataAuthorityUnspecified, serviceerror.NewUnavailablef(
				"task queue user data namespace %s has authority %s; source writer is fenced",
				namespaceID,
				target.authority,
			)
		}
		source, err := readTaskQueueUserDataSourceAuthority(ctx, d.Session, namespaceID)
		if err != nil {
			return taskQueueUserDataAuthorityUnspecified, err
		}
		if err := validateTaskQueueUserDataAuthority(
			namespaceID,
			source,
			identity,
			taskQueueUserDataAuthoritySource,
		); err != nil {
			return taskQueueUserDataAuthorityUnspecified, err
		}
		return taskQueueUserDataAuthoritySource, nil
	case TaskQueueUserDataMigrationModeTargetDual:
		switch target.authority {
		case taskQueueUserDataAuthoritySource:
			source, err := readTaskQueueUserDataSourceAuthority(ctx, d.Session, namespaceID)
			if err != nil {
				return taskQueueUserDataAuthorityUnspecified, err
			}
			if err := validateTaskQueueUserDataAuthority(
				namespaceID,
				source,
				identity,
				taskQueueUserDataAuthoritySource,
			); err != nil {
				return taskQueueUserDataAuthorityUnspecified, err
			}
			return target.authority, nil
		case taskQueueUserDataAuthoritySealing:
			return taskQueueUserDataAuthorityUnspecified, serviceerror.NewUnavailable(
				"task queue user data namespace cutover is sealing writes; retry",
			)
		case taskQueueUserDataAuthorityTarget:
			d.targetAuthorityCache.Store(namespaceID, struct{}{})
			return target.authority, nil
		default:
			return taskQueueUserDataAuthorityUnspecified, serviceerror.NewUnavailable(
				"task queue user data namespace migration authority is not activated",
			)
		}
	case TaskQueueUserDataMigrationModeTargetOnly:
		if target.authority != taskQueueUserDataAuthorityTarget {
			return taskQueueUserDataAuthorityUnspecified, serviceerror.NewUnavailablef(
				"task queue user data namespace %s has not completed target cutover",
				namespaceID,
			)
		}
		d.targetAuthorityCache.Store(namespaceID, struct{}{})
		return target.authority, nil
	default:
		return taskQueueUserDataAuthorityUnspecified, unsupportedTaskQueueUserDataMigrationMode(mode)
	}
}

func addTaskQueueUserDataSourceAuthorityGuard(
	batch taskQueueUserDataV2Batch,
	namespaceID string,
	identity taskQueueUserDataAuthorityRecord,
	authority taskQueueUserDataAuthority,
) {
	if authority == taskQueueUserDataAuthorityUnspecified {
		batch.Query(templateGuardUninitializedTaskQueueUserDataSourceAuthority, namespaceID)
		return
	}
	batch.Query(
		templateGuardTaskQueueUserDataSourceAuthority,
		int(authority),
		namespaceID,
		int(authority),
		int16(identity.bucketCount),
		identity.generation,
	)
}

func (d *userDataStore) validateTaskQueueUserDataSourceGuard(
	ctx context.Context,
	namespaceID string,
	identity taskQueueUserDataAuthorityRecord,
	authority taskQueueUserDataAuthority,
) error {
	record, err := readTaskQueueUserDataSourceAuthority(ctx, d.Session, namespaceID)
	if err != nil {
		return err
	}
	if authority == taskQueueUserDataAuthorityUnspecified {
		if record.authority == taskQueueUserDataAuthorityUnspecified {
			return nil
		}
		return serviceerror.NewUnavailable("task queue user data source-only writer was fenced")
	}
	return validateTaskQueueUserDataAuthority(namespaceID, record, identity, authority)
}

func taskQueueUserDataAuthorityConditionFailed() error {
	return &p.ConditionFailedError{Msg: "task queue user data authority changed during update"}
}
