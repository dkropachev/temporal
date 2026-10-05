package cassandra

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/gocql/gocql"
	"go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type legacyQueueMigrationAuthority int

const (
	legacyQueueMigrationAuthorityUnspecified legacyQueueMigrationAuthority = iota
	legacyQueueMigrationAuthoritySource
	legacyQueueMigrationAuthoritySealing
	legacyQueueMigrationAuthorityTarget
)

const (
	legacyQueueSourceAuthorityMessageID = int64(math.MinInt64)
)

const (
	templateInitializeLegacyQueueSourceAuthority = `INSERT INTO queue
		(queue_type, message_id, migration_authority, migration_generation, message_bucket_size)
		VALUES (?, ?, ?, ?, ?) IF NOT EXISTS`
	templateGetLegacyQueueSourceAuthority = `SELECT migration_authority, migration_generation, message_bucket_size FROM queue
		WHERE queue_type = ? AND message_id = ?`
	templateGuardLegacyQueueSourceAuthority = `UPDATE queue SET migration_authority = ?
		WHERE queue_type = ? AND message_id = ?
		IF migration_authority = ? AND migration_generation = ? AND message_bucket_size = ?`
	templateTransitionLegacyQueueSourceAuthority = `UPDATE queue SET migration_authority = ?
		WHERE queue_type = ? AND message_id = ?
		IF migration_authority = ? AND migration_generation = ? AND message_bucket_size = ?`
	templateTransitionLegacyQueueTargetAuthority = `UPDATE legacy_queue_v2_state SET migration_authority = ?
		WHERE queue_type = ? IF migration_authority = ? AND migration_generation = ? AND message_bucket_size = ?`

	templateGetLegacyQueueTargetBucketAuthority = `SELECT migration_authority, migration_generation, message_bucket_size
		FROM legacy_queue_v2_messages WHERE queue_type = ? AND bucket_id = ? LIMIT 1`
	templateScanLegacyQueueTargetBuckets = `SELECT bucket_id FROM legacy_queue_v2_messages
		WHERE queue_type = ? ALLOW FILTERING`
	templateInitializeLegacyQueueTargetBucketAuthority = `UPDATE legacy_queue_v2_messages SET migration_authority = ?,
		migration_generation = ?, message_bucket_size = ? WHERE queue_type = ? AND bucket_id = ?
		IF migration_authority = null AND migration_generation = null AND message_bucket_size = null`
	templateGuardLegacyQueueTargetBucketAuthority = `UPDATE legacy_queue_v2_messages SET migration_authority = ?
		WHERE queue_type = ? AND bucket_id = ? IF migration_authority = ? AND migration_generation = ?
		AND message_bucket_size = ?`
	templateTransitionLegacyQueueTargetBucketAuthority = `UPDATE legacy_queue_v2_messages SET migration_authority = ?
		WHERE queue_type = ? AND bucket_id = ? IF migration_authority = ? AND migration_generation = ?
		AND message_bucket_size = ?`

	templateGetLegacyQueueTargetDeleteAuthority = `SELECT migration_authority, migration_generation, message_bucket_size
		FROM legacy_queue_v2_delete_ranges WHERE queue_type = ? LIMIT 1`
	templateInitializeLegacyQueueTargetDeleteAuthority = `UPDATE legacy_queue_v2_delete_ranges SET migration_authority = ?,
		migration_generation = ?, message_bucket_size = ? WHERE queue_type = ?
		IF migration_authority = null AND migration_generation = null AND message_bucket_size = null`
	templateGuardLegacyQueueTargetDeleteAuthority = `UPDATE legacy_queue_v2_delete_ranges SET migration_authority = ?
		WHERE queue_type = ? IF migration_authority = ? AND migration_generation = ? AND message_bucket_size = ?`
	templateTransitionLegacyQueueTargetDeleteAuthority = `UPDATE legacy_queue_v2_delete_ranges SET migration_authority = ?
		WHERE queue_type = ? IF migration_authority = ? AND migration_generation = ? AND message_bucket_size = ?`
)

type legacyQueueAuthorityRecord struct {
	authority         legacyQueueMigrationAuthority
	generation        gocql.UUID
	messageBucketSize int64
}

func (a legacyQueueMigrationAuthority) String() string {
	switch a {
	case legacyQueueMigrationAuthoritySource:
		return "source"
	case legacyQueueMigrationAuthoritySealing:
		return "sealing"
	case legacyQueueMigrationAuthorityTarget:
		return "target"
	default:
		return fmt.Sprintf("unknown(%d)", a)
	}
}

// InitializeLegacyQueueV2SourceAuthority activates fencing for one physical source queue.
func InitializeLegacyQueueV2SourceAuthority(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	messageBucketSize int64,
) error {
	if messageBucketSize <= 0 {
		return fmt.Errorf("legacy queue v2 message bucket size must be positive: %d", messageBucketSize)
	}
	generation, err := gocql.RandomUUID()
	if err != nil {
		return fmt.Errorf("generate legacy queue migration identity: %w", err)
	}
	applied, err := session.Query(
		templateInitializeLegacyQueueSourceAuthority,
		queueType,
		legacyQueueSourceAuthorityMessageID,
		int(legacyQueueMigrationAuthoritySource),
		generation,
		messageBucketSize,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return cgocql.ConvertError("InitializeLegacyQueueSourceAuthority", err)
	}
	if applied {
		return nil
	}
	record, err := readLegacyQueueSourceAuthority(ctx, session, queueType)
	if err != nil {
		return cgocql.ConvertError("ReadLegacyQueueSourceAuthorityAfterInitialization", err)
	}
	return validateLegacyQueueAuthority(queueType, messageBucketSize, legacyQueueMigrationAuthoritySource, record)
}

// InitializeEmptyLegacyQueueV2Target creates an explicitly target-authoritative empty physical queue.
func InitializeEmptyLegacyQueueV2Target(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	messageBucketSize int64,
) error {
	if messageBucketSize <= 0 {
		return fmt.Errorf("legacy queue v2 message bucket size must be positive: %d", messageBucketSize)
	}
	store := newLegacyQueueV2MigrationStore(session, messageBucketSize)
	generation, err := gocql.RandomUUID()
	if err != nil {
		return fmt.Errorf("generate empty legacy queue target identity: %w", err)
	}
	target := legacyQueueAuthorityRecord{
		authority:         legacyQueueMigrationAuthorityTarget,
		generation:        generation,
		messageBucketSize: messageBucketSize,
	}
	applied, err := session.Query(
		templateInsertLegacyQueueV2State,
		queueType,
		int64(0),
		int64(persistence.FirstQueueMessageID),
		int64(persistence.FirstQueueMessageID),
		int64(0),
		int(legacyQueueMigrationAuthorityTarget),
		generation,
		messageBucketSize,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return cgocql.ConvertError("InitializeEmptyLegacyQueueV2Target", err)
	}
	if !applied {
		state, err := store.getLegacyQueueV2State(ctx, queueType)
		if err != nil {
			return cgocql.ConvertError("ReadLegacyQueueV2TargetAfterInitialization", err)
		}
		if err := store.validateLegacyQueueV2State(queueType, state); err != nil {
			return err
		}
		if state.authority != legacyQueueMigrationAuthorityTarget {
			return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
				"legacy queue type %d already exists with %s authority",
				queueType,
				state.authority,
			)}
		}
		target = state.authorityRecord()
	}
	if err := initializeLegacyQueueTargetDeleteAuthority(ctx, session, queueType, target); err != nil {
		return err
	}
	return store.ensureLegacyQueueV2BucketStateWithAuthority(
		ctx,
		queueType,
		0,
		persistence.EmptyQueueMessageID,
		target,
	)
}

func readLegacyQueueSourceAuthority(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
) (legacyQueueAuthorityRecord, error) {
	var (
		authority         *int
		generation        *gocql.UUID
		messageBucketSize *int64
	)
	err := session.Query(
		templateGetLegacyQueueSourceAuthority,
		queueType,
		legacyQueueSourceAuthorityMessageID,
	).WithContext(ctx).Scan(&authority, &generation, &messageBucketSize)
	if err != nil {
		return legacyQueueAuthorityRecord{}, err
	}
	if authority == nil && generation == nil && messageBucketSize == nil {
		return legacyQueueAuthorityRecord{}, nil
	}
	if authority == nil || generation == nil || messageBucketSize == nil {
		return legacyQueueAuthorityRecord{}, fmt.Errorf("legacy queue type %d source authority is partially initialized", queueType)
	}
	return legacyQueueAuthorityRecord{
		authority:         legacyQueueMigrationAuthority(*authority),
		generation:        *generation,
		messageBucketSize: *messageBucketSize,
	}, nil
}

func validateLegacyQueueAuthority(
	queueType persistence.QueueType,
	messageBucketSize int64,
	expected legacyQueueMigrationAuthority,
	record legacyQueueAuthorityRecord,
) error {
	if record.authority == expected && record.generation != (gocql.UUID{}) &&
		record.messageBucketSize == messageBucketSize {
		return nil
	}
	return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
		"legacy queue type %d requires %s authority, a generation, and message bucket size %d; persisted authority is %s with generation %s and bucket size %d",
		queueType,
		expected,
		messageBucketSize,
		record.authority,
		record.generation,
		record.messageBucketSize,
	)}
}

func (q *QueueStore) requireLegacyQueueSourceAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
) error {
	record, err := readLegacyQueueSourceAuthority(ctx, q.session, queueType)
	if err != nil {
		if cgocql.IsNotFoundError(err) {
			return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
				"legacy queue type %d source authority is not initialized",
				queueType,
			)}
		}
		return cgocql.ConvertError("ReadLegacyQueueSourceAuthority", err)
	}
	return validateLegacyQueueAuthority(
		queueType,
		q.messageBucketSize,
		legacyQueueMigrationAuthoritySource,
		record,
	)
}

func (q *QueueStore) resolveLegacyQueueTargetDualAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
) (legacyQueueMigrationAuthority, error) {
	record, err := readLegacyQueueSourceAuthority(ctx, q.session, queueType)
	if err != nil {
		if cgocql.IsNotFoundError(err) {
			return legacyQueueMigrationAuthorityUnspecified, &persistence.ConditionFailedError{Msg: fmt.Sprintf(
				"legacy queue type %d source authority is not initialized",
				queueType,
			)}
		}
		return legacyQueueMigrationAuthorityUnspecified, cgocql.ConvertError("ResolveLegacyQueueAuthority", err)
	}
	if record.messageBucketSize != q.messageBucketSize {
		return legacyQueueMigrationAuthorityUnspecified, validateLegacyQueueAuthority(
			queueType,
			q.messageBucketSize,
			record.authority,
			record,
		)
	}
	switch record.authority {
	case legacyQueueMigrationAuthoritySource,
		legacyQueueMigrationAuthoritySealing,
		legacyQueueMigrationAuthorityTarget:
		return record.authority, nil
	default:
		return legacyQueueMigrationAuthorityUnspecified, fmt.Errorf(
			"legacy queue type %d has invalid migration authority %s",
			queueType,
			record.authority,
		)
	}
}

func (q *QueueStore) addLegacyQueueSourceAuthorityGuard(
	batch *cgocql.Batch,
	queueType persistence.QueueType,
	record legacyQueueAuthorityRecord,
) {
	batch.Query(
		templateGuardLegacyQueueSourceAuthority,
		int(record.authority),
		queueType,
		legacyQueueSourceAuthorityMessageID,
		int(record.authority),
		record.generation,
		record.messageBucketSize,
	)
}

func (q *QueueStore) executeGuardedLegacyQueueSourceMutation(
	ctx context.Context,
	queueType persistence.QueueType,
	operation string,
	query string,
	args ...any,
) (bool, error) {
	return q.executeGuardedLegacyQueueSourceMutationWithAuthority(
		ctx,
		queueType,
		legacyQueueMigrationAuthoritySource,
		operation,
		query,
		args...,
	)
}

func (q *QueueStore) executeGuardedLegacyQueueSourceMutationWithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	authority legacyQueueMigrationAuthority,
	operation string,
	query string,
	args ...any,
) (bool, error) {
	record, err := readLegacyQueueSourceAuthority(ctx, q.session, queueType)
	if err != nil {
		return false, cgocql.ConvertError("ReadLegacyQueueSourceAuthorityForMutation", err)
	}
	if err := validateLegacyQueueAuthority(queueType, q.messageBucketSize, authority, record); err != nil {
		return false, err
	}
	batch := q.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
	batch.Query(query, args...)
	q.addLegacyQueueSourceAuthorityGuard(batch, queueType, record)
	applied, iter, err := q.session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		return false, cgocql.ConvertError(operation, err)
	}
	if applied {
		return true, nil
	}
	record, readErr := readLegacyQueueSourceAuthority(ctx, q.session, queueType)
	if readErr != nil {
		return false, cgocql.ConvertError("ReadLegacyQueueSourceAuthorityAfterConflict", readErr)
	}
	if err := validateLegacyQueueAuthority(queueType, q.messageBucketSize, authority, record); err != nil {
		return false, err
	}
	return false, nil
}

func transitionLegacyQueueSourceAuthority(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	from legacyQueueAuthorityRecord,
	to legacyQueueMigrationAuthority,
) (bool, error) {
	applied, err := session.Query(
		templateTransitionLegacyQueueSourceAuthority,
		int(to),
		queueType,
		legacyQueueSourceAuthorityMessageID,
		int(from.authority),
		from.generation,
		from.messageBucketSize,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return false, cgocql.ConvertError("TransitionLegacyQueueSourceAuthority", err)
	}
	return applied, nil
}

func transitionLegacyQueueTargetAuthority(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	from legacyQueueAuthorityRecord,
	to legacyQueueMigrationAuthority,
) (bool, error) {
	applied, err := session.Query(
		templateTransitionLegacyQueueTargetAuthority,
		int(to),
		queueType,
		int(from.authority),
		from.generation,
		from.messageBucketSize,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return false, cgocql.ConvertError("TransitionLegacyQueueTargetAuthority", err)
	}
	return applied, nil
}

func readLegacyQueueTargetBucketAuthority(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	bucket int64,
) (legacyQueueAuthorityRecord, error) {
	return scanLegacyQueueTargetAuthority(
		ctx,
		session.Query(templateGetLegacyQueueTargetBucketAuthority, queueType, bucket),
		fmt.Sprintf("legacy queue type %d target bucket %d", queueType, bucket),
	)
}

func scanLegacyQueueTargetBuckets(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
) ([]int64, error) {
	iter := session.Query(templateScanLegacyQueueTargetBuckets, queueType).WithContext(ctx).Iter()
	seen := make(map[int64]struct{})
	var bucket int64
	for iter.Scan(&bucket) {
		seen[bucket] = struct{}{}
	}
	if err := iter.Close(); err != nil {
		return nil, cgocql.ConvertError("ScanLegacyQueueTargetBuckets", err)
	}
	buckets := make([]int64, 0, len(seen))
	for bucket := range seen {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
	return buckets, nil
}

func readLegacyQueueTargetDeleteAuthority(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
) (legacyQueueAuthorityRecord, error) {
	return scanLegacyQueueTargetAuthority(
		ctx,
		session.Query(templateGetLegacyQueueTargetDeleteAuthority, queueType),
		fmt.Sprintf("legacy queue type %d target delete-range partition", queueType),
	)
}

func scanLegacyQueueTargetAuthority(
	ctx context.Context,
	query cgocql.Query,
	partition string,
) (legacyQueueAuthorityRecord, error) {
	var (
		authority         *int
		generation        *gocql.UUID
		messageBucketSize *int64
	)
	err := query.WithContext(ctx).Scan(&authority, &generation, &messageBucketSize)
	if cgocql.IsNotFoundError(err) {
		return legacyQueueAuthorityRecord{}, nil
	}
	if err != nil {
		return legacyQueueAuthorityRecord{}, err
	}
	if authority == nil && generation == nil && messageBucketSize == nil {
		return legacyQueueAuthorityRecord{}, nil
	}
	if authority == nil || generation == nil || messageBucketSize == nil {
		return legacyQueueAuthorityRecord{}, fmt.Errorf("%s authority is partially initialized", partition)
	}
	return legacyQueueAuthorityRecord{
		authority:         legacyQueueMigrationAuthority(*authority),
		generation:        *generation,
		messageBucketSize: *messageBucketSize,
	}, nil
}

func initializeLegacyQueueTargetBucketAuthority(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	bucket int64,
	record legacyQueueAuthorityRecord,
) error {
	applied, err := session.Query(
		templateInitializeLegacyQueueTargetBucketAuthority,
		int(record.authority),
		record.generation,
		record.messageBucketSize,
		queueType,
		bucket,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return cgocql.ConvertError("InitializeLegacyQueueTargetBucketAuthority", err)
	}
	if applied {
		return nil
	}
	actual, err := readLegacyQueueTargetBucketAuthority(ctx, session, queueType, bucket)
	if err != nil {
		return cgocql.ConvertError("ReadLegacyQueueTargetBucketAuthorityAfterInitialization", err)
	}
	return validateLegacyQueueAuthorityRecord(queueType, fmt.Sprintf("target bucket %d", bucket), record, actual)
}

func initializeLegacyQueueTargetDeleteAuthority(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	record legacyQueueAuthorityRecord,
) error {
	applied, err := session.Query(
		templateInitializeLegacyQueueTargetDeleteAuthority,
		int(record.authority),
		record.generation,
		record.messageBucketSize,
		queueType,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return cgocql.ConvertError("InitializeLegacyQueueTargetDeleteAuthority", err)
	}
	if applied {
		return nil
	}
	actual, err := readLegacyQueueTargetDeleteAuthority(ctx, session, queueType)
	if err != nil {
		return cgocql.ConvertError("ReadLegacyQueueTargetDeleteAuthorityAfterInitialization", err)
	}
	return validateLegacyQueueAuthorityRecord(queueType, "target delete-range partition", record, actual)
}

func transitionLegacyQueueTargetBucketAuthority(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	bucket int64,
	from legacyQueueAuthorityRecord,
	to legacyQueueMigrationAuthority,
) error {
	applied, err := session.Query(
		templateTransitionLegacyQueueTargetBucketAuthority,
		int(to),
		queueType,
		bucket,
		int(from.authority),
		from.generation,
		from.messageBucketSize,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return cgocql.ConvertError("TransitionLegacyQueueTargetBucketAuthority", err)
	}
	if applied {
		return nil
	}
	actual, err := readLegacyQueueTargetBucketAuthority(ctx, session, queueType, bucket)
	if err != nil {
		return cgocql.ConvertError("ReadLegacyQueueTargetBucketAuthorityAfterTransition", err)
	}
	expected := from
	expected.authority = to
	return validateLegacyQueueAuthorityRecord(queueType, fmt.Sprintf("target bucket %d", bucket), expected, actual)
}

func transitionLegacyQueueTargetDeleteAuthority(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	from legacyQueueAuthorityRecord,
	to legacyQueueMigrationAuthority,
) error {
	applied, err := session.Query(
		templateTransitionLegacyQueueTargetDeleteAuthority,
		int(to),
		queueType,
		int(from.authority),
		from.generation,
		from.messageBucketSize,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return cgocql.ConvertError("TransitionLegacyQueueTargetDeleteAuthority", err)
	}
	if applied {
		return nil
	}
	actual, err := readLegacyQueueTargetDeleteAuthority(ctx, session, queueType)
	if err != nil {
		return cgocql.ConvertError("ReadLegacyQueueTargetDeleteAuthorityAfterTransition", err)
	}
	expected := from
	expected.authority = to
	return validateLegacyQueueAuthorityRecord(queueType, "target delete-range partition", expected, actual)
}

func validateLegacyQueueAuthorityRecord(
	queueType persistence.QueueType,
	partition string,
	expected legacyQueueAuthorityRecord,
	actual legacyQueueAuthorityRecord,
) error {
	if actual == expected && actual.generation != (gocql.UUID{}) {
		return nil
	}
	return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
		"legacy queue type %d %s requires authority %s, generation %s, and message bucket size %d; got authority %s, generation %s, and bucket size %d",
		queueType,
		partition,
		expected.authority,
		expected.generation,
		expected.messageBucketSize,
		actual.authority,
		actual.generation,
		actual.messageBucketSize,
	)}
}

func (q *QueueStore) requireLegacyQueueTargetBucketAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	bucket int64,
	expected legacyQueueAuthorityRecord,
) error {
	actual, err := readLegacyQueueTargetBucketAuthority(ctx, q.session, queueType, bucket)
	if err != nil {
		return cgocql.ConvertError("ReadLegacyQueueTargetBucketAuthority", err)
	}
	return validateLegacyQueueAuthorityRecord(
		queueType,
		fmt.Sprintf("target bucket %d", bucket),
		expected,
		actual,
	)
}

func (q *QueueStore) addLegacyQueueTargetBucketAuthorityGuard(
	batch *cgocql.Batch,
	queueType persistence.QueueType,
	bucket int64,
	record legacyQueueAuthorityRecord,
) {
	batch.Query(
		templateGuardLegacyQueueTargetBucketAuthority,
		int(record.authority),
		queueType,
		bucket,
		int(record.authority),
		record.generation,
		record.messageBucketSize,
	)
}

func (q *QueueStore) executeGuardedLegacyQueueTargetBucketMutation(
	ctx context.Context,
	queueType persistence.QueueType,
	bucket int64,
	record legacyQueueAuthorityRecord,
	operation string,
	query string,
	args ...any,
) (bool, error) {
	batch := q.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
	batch.Query(query, args...)
	q.addLegacyQueueTargetBucketAuthorityGuard(batch, queueType, bucket, record)
	applied, iter, err := q.session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		return false, cgocql.ConvertError(operation, err)
	}
	if applied {
		return true, nil
	}
	if err := q.requireLegacyQueueTargetBucketAuthority(ctx, queueType, bucket, record); err != nil {
		return false, err
	}
	return false, nil
}

func (q *QueueStore) executeGuardedLegacyQueueTargetDeleteMutation(
	ctx context.Context,
	queueType persistence.QueueType,
	record legacyQueueAuthorityRecord,
	operation string,
	query string,
	args ...any,
) (bool, error) {
	batch := q.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
	batch.Query(query, args...)
	batch.Query(
		templateGuardLegacyQueueTargetDeleteAuthority,
		int(record.authority),
		queueType,
		int(record.authority),
		record.generation,
		record.messageBucketSize,
	)
	applied, iter, err := q.session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		return false, cgocql.ConvertError(operation, err)
	}
	if applied {
		return true, nil
	}
	actual, readErr := readLegacyQueueTargetDeleteAuthority(ctx, q.session, queueType)
	if readErr != nil {
		return false, cgocql.ConvertError("ReadLegacyQueueTargetDeleteAuthorityAfterConflict", readErr)
	}
	if err := validateLegacyQueueAuthorityRecord(
		queueType,
		"target delete-range partition",
		record,
		actual,
	); err != nil {
		return false, err
	}
	return false, nil
}
