package cassandra

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	DefaultLegacyQueueV2MessageBucketSize = int64(4096)

	legacyQueueV2StateRowType   = int16(0)
	legacyQueueV2MessageRowType = int16(1)
	legacyQueueV2StateMessageID = int64(-1)

	legacyQueueV2PageTokenPrefix  = "\xffTEMPORAL-LEGACY-QUEUE-V2"
	legacyQueueV2PageTokenVersion = byte(1)

	templateInsertLegacyQueueV2State = `INSERT INTO legacy_queue_v2_state
		(queue_type, active_bucket, minimum_message_id, cleanup_message_id, version, migration_authority,
		migration_generation, message_bucket_size) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateGetLegacyQueueV2State = `SELECT active_bucket, minimum_message_id, cleanup_message_id, version,
		migration_authority, migration_generation, message_bucket_size
		FROM legacy_queue_v2_state WHERE queue_type = ?`
	templateAdvanceLegacyQueueV2ActiveBucket = `UPDATE legacy_queue_v2_state SET active_bucket = ?, version = ?
		WHERE queue_type = ? IF active_bucket = ? AND version = ?`
	templateAdvanceLegacyQueueV2Minimum = `UPDATE legacy_queue_v2_state SET minimum_message_id = ?, version = ?
		WHERE queue_type = ? IF version = ?`
	templateAdvanceLegacyQueueV2Cleanup = `UPDATE legacy_queue_v2_state SET cleanup_message_id = ?
		WHERE queue_type = ? IF cleanup_message_id = ?`
	templateResetLegacyQueueV2LogicalState = `UPDATE legacy_queue_v2_state
		SET minimum_message_id = ?, cleanup_message_id = ?, version = ? WHERE queue_type = ?
		IF version = ? AND migration_authority = ? AND migration_generation = ? AND message_bucket_size = ?`

	templateInsertLegacyQueueV2BucketState = `INSERT INTO legacy_queue_v2_messages
		(queue_type, bucket_id, row_type, message_id, last_message_id, version)
		VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateGetLegacyQueueV2BucketState = `SELECT last_message_id, version FROM legacy_queue_v2_messages
		WHERE queue_type = ? AND bucket_id = ? AND row_type = ? AND message_id = ?`
	templateAdvanceLegacyQueueV2BucketTail = `UPDATE legacy_queue_v2_messages SET last_message_id = ?, version = ?
		WHERE queue_type = ? AND bucket_id = ? AND row_type = ? AND message_id = ?
		IF last_message_id = ? AND version = ?`
	templateInsertLegacyQueueV2Message = `INSERT INTO legacy_queue_v2_messages
		(queue_type, bucket_id, row_type, message_id, message_payload, message_encoding)
		VALUES (?, ?, ?, ?, ?, ?)`
	templateGetLegacyQueueV2Message = `SELECT message_payload, message_encoding FROM legacy_queue_v2_messages
		WHERE queue_type = ? AND bucket_id = ? AND row_type = ? AND message_id = ?`
	templateGetLegacyQueueV2Messages = `SELECT message_id, message_payload, message_encoding
		FROM legacy_queue_v2_messages WHERE queue_type = ? AND bucket_id = ? AND row_type = ?
		AND message_id >= ? AND message_id <= ? ORDER BY message_id ASC`
	templateDeleteLegacyQueueV2Message = `DELETE FROM legacy_queue_v2_messages
		WHERE queue_type = ? AND bucket_id = ? AND row_type = ? AND message_id = ?`
	templateDeleteLegacyQueueV2MessageRange = `DELETE FROM legacy_queue_v2_messages
		WHERE queue_type = ? AND bucket_id = ? AND row_type = ? AND message_id >= ? AND message_id <= ?`

	templateInsertLegacyQueueV2DeleteRange = `INSERT INTO legacy_queue_v2_delete_ranges
		(queue_type, deletion_id, first_message_id, last_message_id) VALUES (?, ?, ?, ?)`
	templateGetLegacyQueueV2DeleteRanges = `SELECT deletion_id, first_message_id, last_message_id
		FROM legacy_queue_v2_delete_ranges WHERE queue_type = ?`
	templateDeleteLegacyQueueV2DeleteRange = `DELETE FROM legacy_queue_v2_delete_ranges
		WHERE queue_type = ? AND deletion_id = ?`

	templateGetSourceQueueMessage = `SELECT message_payload, message_encoding FROM queue
		WHERE queue_type = ? AND message_id = ?`
)

type legacyQueueV2State struct {
	activeBucket      int64
	minimumMessageID  int64
	cleanupMessageID  int64
	version           int64
	authority         legacyQueueMigrationAuthority
	generation        gocql.UUID
	messageBucketSize int64
}

func (s legacyQueueV2State) authorityRecord() legacyQueueAuthorityRecord {
	return legacyQueueAuthorityRecord{
		authority:         s.authority,
		generation:        s.generation,
		messageBucketSize: s.messageBucketSize,
	}
}

type legacyQueueV2BucketState struct {
	lastMessageID int64
	version       int64
}

type legacyQueueV2DeleteRange struct {
	id             string
	firstMessageID int64
	lastMessageID  int64
}

func normalizeLegacyQueueMigrationMode(
	mode config.CassandraLegacyQueueMigrationMode,
) config.CassandraLegacyQueueMigrationMode {
	if mode == "" {
		return config.CassandraLegacyQueueMigrationModeSourceOnly
	}
	return mode
}

// ValidateLegacyQueueMigrationMode checks whether a legacy queue migration mode is supported.
func ValidateLegacyQueueMigrationMode(mode config.CassandraLegacyQueueMigrationMode) error {
	switch normalizeLegacyQueueMigrationMode(mode) {
	case config.CassandraLegacyQueueMigrationModeSourceOnly,
		config.CassandraLegacyQueueMigrationModeSourceDual,
		config.CassandraLegacyQueueMigrationModeTargetDual,
		config.CassandraLegacyQueueMigrationModeTargetOnly:
		return nil
	default:
		return unsupportedLegacyQueueMigrationMode(mode)
	}
}

func unsupportedLegacyQueueMigrationMode(mode config.CassandraLegacyQueueMigrationMode) error {
	return fmt.Errorf("unsupported Cassandra legacy queue migration mode %q", mode)
}

func legacyQueueMessageBucketSize(configured int64) int64 {
	if configured == 0 {
		return DefaultLegacyQueueV2MessageBucketSize
	}
	return configured
}

func legacyQueueV2MessageBucket(messageID int64, bucketSize int64) (int64, error) {
	if messageID < persistence.FirstQueueMessageID {
		return 0, fmt.Errorf("legacy queue message ID must be non-negative: %d", messageID)
	}
	if bucketSize <= 0 {
		return 0, fmt.Errorf("legacy queue message bucket size must be positive: %d", bucketSize)
	}
	return messageID / bucketSize, nil
}

func legacyQueueV2BucketFirstMessageID(bucket int64, bucketSize int64) (int64, error) {
	if bucket < 0 || bucketSize <= 0 || bucket > math.MaxInt64/bucketSize {
		return 0, fmt.Errorf("invalid legacy queue v2 bucket %d with size %d", bucket, bucketSize)
	}
	return bucket * bucketSize, nil
}

func legacyQueueV2BucketLastMessageID(bucket int64, bucketSize int64) (int64, error) {
	firstMessageID, err := legacyQueueV2BucketFirstMessageID(bucket, bucketSize)
	if err != nil {
		return 0, err
	}
	if firstMessageID > math.MaxInt64-(bucketSize-1) {
		return math.MaxInt64, nil
	}
	return firstMessageID + bucketSize - 1, nil
}

//nolint:revive // Each migration mode deliberately validates a distinct persisted authority state.
func (q *QueueStore) initializeLegacyQueueV2StateForMode(ctx context.Context) error {
	switch q.migrationMode {
	case config.CassandraLegacyQueueMigrationModeSourceOnly:
		return nil
	case config.CassandraLegacyQueueMigrationModeSourceDual:
		for _, queueType := range []persistence.QueueType{q.queueType, q.getDLQTypeFromQueueType()} {
			record, err := readLegacyQueueSourceAuthority(ctx, q.session, queueType)
			if err != nil {
				return cgocql.ConvertError("ReadLegacyQueueSourceAuthorityForInitialization", err)
			}
			if err := validateLegacyQueueAuthority(
				queueType,
				q.messageBucketSize,
				legacyQueueMigrationAuthoritySource,
				record,
			); err != nil {
				return err
			}
			lastMessageID, err := q.getLastMessageID(ctx, queueType)
			if err != nil {
				return err
			}
			if err := q.ensureLegacyQueueV2Layout(ctx, queueType, lastMessageID, record); err != nil {
				return err
			}
		}
		return nil
	case config.CassandraLegacyQueueMigrationModeTargetDual:
		for _, queueType := range []persistence.QueueType{q.queueType, q.getDLQTypeFromQueueType()} {
			sourceRecord, err := readLegacyQueueSourceAuthority(ctx, q.session, queueType)
			if err != nil {
				return err
			}
			if err := validateLegacyQueueAuthority(
				queueType,
				q.messageBucketSize,
				sourceRecord.authority,
				sourceRecord,
			); err != nil {
				return err
			}
			state, err := q.getLegacyQueueV2State(ctx, queueType)
			if err != nil {
				return fmt.Errorf("legacy queue v2 state is missing for queue type %d; backfill is required before target-dual: %w", queueType, err)
			}
			if err := q.validateLegacyQueueV2State(queueType, state); err != nil {
				return err
			}
			if sourceRecord.authority == legacyQueueMigrationAuthorityTarget && state.authority != legacyQueueMigrationAuthorityTarget {
				return fmt.Errorf("legacy queue type %d source published target authority before target activation", queueType)
			}
			if state.generation != sourceRecord.generation {
				return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
					"legacy queue type %d source and target generations differ: source %s, target %s",
					queueType,
					sourceRecord.generation,
					state.generation,
				)}
			}
			if err := q.validateLegacyQueueV2TargetPartitions(ctx, queueType, state); err != nil {
				return fmt.Errorf("legacy queue v2 active bucket state is missing for queue type %d: %w", queueType, err)
			}
			q.cacheLegacyQueueV2ActiveBucket(queueType, state.activeBucket)
		}
		return nil
	case config.CassandraLegacyQueueMigrationModeTargetOnly:
		for _, queueType := range []persistence.QueueType{q.queueType, q.getDLQTypeFromQueueType()} {
			state, err := q.getLegacyQueueV2State(ctx, queueType)
			if err != nil {
				return fmt.Errorf("legacy queue v2 target state is missing for queue type %d: %w", queueType, err)
			}
			if err := q.validateLegacyQueueV2State(queueType, state); err != nil {
				return err
			}
			if state.authority != legacyQueueMigrationAuthorityTarget {
				return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
					"legacy queue type %d target-only mode requires target authority; persisted authority is %s",
					queueType,
					state.authority,
				)}
			}
			if err := q.validateLegacyQueueV2TargetPartitions(ctx, queueType, state); err != nil {
				return fmt.Errorf("legacy queue v2 active bucket state is missing for queue type %d: %w", queueType, err)
			}
			q.cacheLegacyQueueV2ActiveBucket(queueType, state.activeBucket)
		}
		return nil
	default:
		return unsupportedLegacyQueueMigrationMode(q.migrationMode)
	}
}

func (q *QueueStore) ensureLegacyQueueV2Layout(
	ctx context.Context,
	queueType persistence.QueueType,
	lastMessageID int64,
	record legacyQueueAuthorityRecord,
) error {
	desiredBucket := int64(0)
	if lastMessageID >= persistence.FirstQueueMessageID {
		var err error
		desiredBucket, err = legacyQueueV2MessageBucket(lastMessageID, q.messageBucketSize)
		if err != nil {
			return err
		}
	}
	applied, err := q.session.Query(
		templateInsertLegacyQueueV2State,
		queueType,
		desiredBucket,
		int64(persistence.FirstQueueMessageID),
		int64(persistence.FirstQueueMessageID),
		int64(0),
		int(record.authority),
		record.generation,
		q.messageBucketSize,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return cgocql.ConvertError("InitializeLegacyQueueV2State", err)
	}
	if !applied {
		if err := q.advanceLegacyQueueV2ActiveBucketWithAuthority(ctx, queueType, desiredBucket, record); err != nil {
			return err
		}
	}
	if err := initializeLegacyQueueTargetDeleteAuthority(ctx, q.session, queueType, record); err != nil {
		return err
	}
	if err := q.ensureLegacyQueueV2BucketStateWithAuthority(ctx, queueType, desiredBucket, lastMessageID, record); err != nil {
		return err
	}
	state, err := q.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return cgocql.ConvertError("GetLegacyQueueV2StateAfterInitialization", err)
	}
	if err := q.validateLegacyQueueV2State(queueType, state); err != nil {
		return err
	}
	if err := validateLegacyQueueAuthorityRecord(queueType, "target state", record, state.authorityRecord()); err != nil {
		return err
	}
	q.cacheLegacyQueueV2ActiveBucket(queueType, state.activeBucket)
	return nil
}

func (q *QueueStore) validateLegacyQueueV2State(
	queueType persistence.QueueType,
	state legacyQueueV2State,
) error {
	if state.messageBucketSize != q.messageBucketSize {
		return fmt.Errorf(
			"legacy queue type %d target state uses message bucket size %d; configured %d",
			queueType,
			state.messageBucketSize,
			q.messageBucketSize,
		)
	}
	if state.generation == (gocql.UUID{}) {
		return fmt.Errorf("legacy queue type %d target state has no migration generation", queueType)
	}
	switch state.authority {
	case legacyQueueMigrationAuthoritySource,
		legacyQueueMigrationAuthoritySealing,
		legacyQueueMigrationAuthorityTarget:
		return nil
	default:
		return fmt.Errorf(
			"legacy queue type %d target state has invalid migration authority %s",
			queueType,
			state.authority,
		)
	}
}

func (q *QueueStore) getLegacyQueueV2State(
	ctx context.Context,
	queueType persistence.QueueType,
) (legacyQueueV2State, error) {
	var (
		state             legacyQueueV2State
		authority         *int
		generation        *gocql.UUID
		messageBucketSize *int64
	)
	err := q.session.Query(templateGetLegacyQueueV2State, queueType).WithContext(ctx).Scan(
		&state.activeBucket,
		&state.minimumMessageID,
		&state.cleanupMessageID,
		&state.version,
		&authority,
		&generation,
		&messageBucketSize,
	)
	if err == nil && authority != nil && generation != nil && messageBucketSize != nil {
		state.authority = legacyQueueMigrationAuthority(*authority)
		state.generation = *generation
		state.messageBucketSize = *messageBucketSize
	}
	return state, err
}

func (q *QueueStore) validateLegacyQueueV2TargetPartitions(
	ctx context.Context,
	queueType persistence.QueueType,
	state legacyQueueV2State,
) error {
	expected := state.authorityRecord()
	buckets, err := scanLegacyQueueTargetBuckets(ctx, q.session, queueType)
	if err != nil {
		return err
	}
	activeFound := false
	for _, bucket := range buckets {
		if bucket == state.activeBucket {
			activeFound = true
		}
		record, err := readLegacyQueueTargetBucketAuthority(ctx, q.session, queueType, bucket)
		if err != nil {
			return err
		}
		if err := validateLegacyQueueAuthorityRecord(
			queueType,
			fmt.Sprintf("target bucket %d", bucket),
			expected,
			record,
		); err != nil {
			return err
		}
	}
	if !activeFound {
		return fmt.Errorf("legacy queue type %d active target bucket %d is missing", queueType, state.activeBucket)
	}
	if _, err := q.getLegacyQueueV2BucketState(ctx, queueType, state.activeBucket); err != nil {
		return err
	}
	deleteRecord, err := readLegacyQueueTargetDeleteAuthority(ctx, q.session, queueType)
	if err != nil {
		return err
	}
	return validateLegacyQueueAuthorityRecord(
		queueType,
		"target delete-range partition",
		expected,
		deleteRecord,
	)
}

func (q *QueueStore) legacyQueueV2TargetAuthorityRecord(
	ctx context.Context,
	queueType persistence.QueueType,
	authority legacyQueueMigrationAuthority,
) (legacyQueueAuthorityRecord, error) {
	state, err := q.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return legacyQueueAuthorityRecord{}, cgocql.ConvertError("GetLegacyQueueV2TargetAuthority", err)
	}
	if err := q.validateLegacyQueueV2State(queueType, state); err != nil {
		return legacyQueueAuthorityRecord{}, err
	}
	if state.authority != authority {
		return legacyQueueAuthorityRecord{}, &persistence.ConditionFailedError{Msg: fmt.Sprintf(
			"legacy queue type %d target state requires %s authority; persisted authority is %s",
			queueType,
			authority,
			state.authority,
		)}
	}
	return state.authorityRecord(), nil
}

func (q *QueueStore) getLegacyQueueV2BucketState(
	ctx context.Context,
	queueType persistence.QueueType,
	bucket int64,
) (legacyQueueV2BucketState, error) {
	var state legacyQueueV2BucketState
	err := q.session.Query(
		templateGetLegacyQueueV2BucketState,
		queueType,
		bucket,
		legacyQueueV2StateRowType,
		legacyQueueV2StateMessageID,
	).WithContext(ctx).Scan(&state.lastMessageID, &state.version)
	return state, err
}

func (q *QueueStore) ensureLegacyQueueV2BucketState(
	ctx context.Context,
	queueType persistence.QueueType,
	bucket int64,
	lastMessageID int64,
) error {
	state, err := q.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return cgocql.ConvertError("GetLegacyQueueV2StateForBucketInitialization", err)
	}
	return q.ensureLegacyQueueV2BucketStateWithAuthority(
		ctx,
		queueType,
		bucket,
		lastMessageID,
		state.authorityRecord(),
	)
}

func (q *QueueStore) ensureLegacyQueueV2BucketStateWithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	bucket int64,
	lastMessageID int64,
	record legacyQueueAuthorityRecord,
) error {
	if err := initializeLegacyQueueTargetBucketAuthority(ctx, q.session, queueType, bucket, record); err != nil {
		return err
	}
	firstMessageID, err := legacyQueueV2BucketFirstMessageID(bucket, q.messageBucketSize)
	if err != nil {
		return err
	}
	initialLastMessageID := firstMessageID - 1
	if lastMessageID >= firstMessageID {
		initialLastMessageID = lastMessageID
	}
	applied, err := q.session.Query(
		templateInsertLegacyQueueV2BucketState,
		queueType,
		bucket,
		legacyQueueV2StateRowType,
		legacyQueueV2StateMessageID,
		initialLastMessageID,
		int64(0),
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return cgocql.ConvertError("InitializeLegacyQueueV2BucketState", err)
	}
	if applied {
		return nil
	}
	return q.advanceLegacyQueueV2BucketTailWithAuthority(ctx, queueType, bucket, lastMessageID, record)
}

func (q *QueueStore) advanceLegacyQueueV2BucketTail(
	ctx context.Context,
	queueType persistence.QueueType,
	bucket int64,
	messageID int64,
) error {
	for range 8 {
		state, err := q.getLegacyQueueV2BucketState(ctx, queueType, bucket)
		if err != nil {
			return cgocql.ConvertError("GetLegacyQueueV2BucketStateForAdvance", err)
		}
		if state.lastMessageID >= messageID {
			return nil
		}
		applied, err := q.session.Query(
			templateAdvanceLegacyQueueV2BucketTail,
			messageID,
			state.version+1,
			queueType,
			bucket,
			legacyQueueV2StateRowType,
			legacyQueueV2StateMessageID,
			state.lastMessageID,
			state.version,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return cgocql.ConvertError("AdvanceLegacyQueueV2BucketTail", err)
		}
		if applied {
			return nil
		}
	}
	return ErrEnqueueMessageConflict
}

func (q *QueueStore) advanceLegacyQueueV2BucketTailWithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	bucket int64,
	messageID int64,
	record legacyQueueAuthorityRecord,
) error {
	for range 8 {
		state, err := q.getLegacyQueueV2BucketState(ctx, queueType, bucket)
		if err != nil {
			return cgocql.ConvertError("GetLegacyQueueV2BucketStateForGuardedAdvance", err)
		}
		if state.lastMessageID >= messageID {
			return q.requireLegacyQueueTargetBucketAuthority(ctx, queueType, bucket, record)
		}
		batch := q.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
		batch.Query(
			templateAdvanceLegacyQueueV2BucketTail,
			messageID,
			state.version+1,
			queueType,
			bucket,
			legacyQueueV2StateRowType,
			legacyQueueV2StateMessageID,
			state.lastMessageID,
			state.version,
		)
		q.addLegacyQueueTargetBucketAuthorityGuard(batch, queueType, bucket, record)
		applied, iter, err := q.session.MapExecuteBatchCAS(batch, make(map[string]any))
		if iter != nil {
			_ = iter.Close()
		}
		if err != nil {
			return cgocql.ConvertError("AdvanceLegacyQueueV2BucketTailGuarded", err)
		}
		if applied {
			return nil
		}
		if err := q.requireLegacyQueueTargetBucketAuthority(ctx, queueType, bucket, record); err != nil {
			return err
		}
	}
	return ErrEnqueueMessageConflict
}

func (q *QueueStore) advanceLegacyQueueV2ActiveBucket(
	ctx context.Context,
	queueType persistence.QueueType,
	bucket int64,
) error {
	for range 8 {
		state, err := q.getLegacyQueueV2State(ctx, queueType)
		if err != nil {
			return cgocql.ConvertError("GetLegacyQueueV2StateForActiveBucket", err)
		}
		if state.activeBucket >= bucket {
			q.cacheLegacyQueueV2ActiveBucket(queueType, state.activeBucket)
			return nil
		}
		applied, err := q.session.Query(
			templateAdvanceLegacyQueueV2ActiveBucket,
			bucket,
			state.version+1,
			queueType,
			state.activeBucket,
			state.version,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return cgocql.ConvertError("AdvanceLegacyQueueV2ActiveBucket", err)
		}
		if applied {
			q.cacheLegacyQueueV2ActiveBucket(queueType, bucket)
			return nil
		}
	}
	return ErrEnqueueMessageConflict
}

func (q *QueueStore) advanceLegacyQueueV2ActiveBucketWithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	bucket int64,
	record legacyQueueAuthorityRecord,
) error {
	for range 8 {
		state, err := q.getLegacyQueueV2State(ctx, queueType)
		if err != nil {
			return cgocql.ConvertError("GetLegacyQueueV2StateForGuardedActiveBucket", err)
		}
		if err := validateLegacyQueueAuthorityRecord(queueType, "target state", record, state.authorityRecord()); err != nil {
			return err
		}
		if state.activeBucket >= bucket {
			q.cacheLegacyQueueV2ActiveBucket(queueType, state.activeBucket)
			return nil
		}
		applied, err := q.session.Query(
			templateAdvanceLegacyQueueV2ActiveBucket+` AND migration_authority = ? AND migration_generation = ? AND message_bucket_size = ?`,
			bucket,
			state.version+1,
			queueType,
			state.activeBucket,
			state.version,
			int(record.authority),
			record.generation,
			record.messageBucketSize,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return cgocql.ConvertError("AdvanceLegacyQueueV2ActiveBucketGuarded", err)
		}
		if applied {
			q.cacheLegacyQueueV2ActiveBucket(queueType, bucket)
			return nil
		}
	}
	return ErrEnqueueMessageConflict
}

func (q *QueueStore) cacheLegacyQueueV2ActiveBucket(queueType persistence.QueueType, bucket int64) {
	q.activeBucketsMu.Lock()
	if q.activeBuckets == nil {
		q.activeBuckets = make(map[persistence.QueueType]int64, 2)
	}
	q.activeBuckets[queueType] = bucket
	q.activeBucketsMu.Unlock()
}

func (q *QueueStore) cachedLegacyQueueV2ActiveBucket(queueType persistence.QueueType) (int64, bool) {
	q.activeBucketsMu.RLock()
	bucket, ok := q.activeBuckets[queueType]
	q.activeBucketsMu.RUnlock()
	return bucket, ok
}

func (q *QueueStore) loadLegacyQueueV2ActiveBucket(
	ctx context.Context,
	queueType persistence.QueueType,
) (int64, error) {
	if bucket, ok := q.cachedLegacyQueueV2ActiveBucket(queueType); ok {
		return bucket, nil
	}
	state, err := q.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return 0, cgocql.ConvertError("GetLegacyQueueV2StateForActiveBucket", err)
	}
	if err := q.validateLegacyQueueV2State(queueType, state); err != nil {
		return 0, err
	}
	q.cacheLegacyQueueV2ActiveBucket(queueType, state.activeBucket)
	return state.activeBucket, nil
}

//nolint:revive // Bucket allocation is a bounded CAS state machine with explicit rollover handling.
func (q *QueueStore) enqueueMessageLegacyQueueV2(
	ctx context.Context,
	queueType persistence.QueueType,
	blob *commonpb.DataBlob,
) (int64, error) {
	for range 8 {
		bucket, err := q.loadLegacyQueueV2ActiveBucket(ctx, queueType)
		if err != nil {
			return persistence.EmptyQueueMessageID, err
		}
		bucketState, err := q.getLegacyQueueV2BucketState(ctx, queueType, bucket)
		if err != nil {
			if !cgocql.IsNotFoundError(err) {
				return persistence.EmptyQueueMessageID, cgocql.ConvertError("GetLegacyQueueV2BucketStateForEnqueue", err)
			}
			if err := q.ensureLegacyQueueV2BucketState(ctx, queueType, bucket, persistence.EmptyQueueMessageID); err != nil {
				return persistence.EmptyQueueMessageID, err
			}
			continue
		}
		bucketLastMessageID, err := legacyQueueV2BucketLastMessageID(bucket, q.messageBucketSize)
		if err != nil {
			return persistence.EmptyQueueMessageID, err
		}
		if bucketState.lastMessageID >= bucketLastMessageID {
			if bucket == math.MaxInt64 {
				return persistence.EmptyQueueMessageID, fmt.Errorf("legacy queue bucket ID exhausted for queue type %d", queueType)
			}
			nextBucket := bucket + 1
			nextFirstMessageID, err := legacyQueueV2BucketFirstMessageID(nextBucket, q.messageBucketSize)
			if err != nil {
				return persistence.EmptyQueueMessageID, err
			}
			if err := q.ensureLegacyQueueV2BucketState(ctx, queueType, nextBucket, nextFirstMessageID-1); err != nil {
				return persistence.EmptyQueueMessageID, err
			}
			if err := q.advanceLegacyQueueV2ActiveBucket(ctx, queueType, nextBucket); err != nil {
				return persistence.EmptyQueueMessageID, err
			}
			continue
		}
		messageID := bucketState.lastMessageID + 1
		if err := q.executeLegacyQueueV2EnqueueBatch(
			ctx,
			queueType,
			bucket,
			messageID,
			bucketState,
			blob,
		); err != nil {
			return persistence.EmptyQueueMessageID, err
		}
		return messageID, nil
	}
	return persistence.EmptyQueueMessageID, ErrEnqueueMessageConflict
}

func (q *QueueStore) executeLegacyQueueV2EnqueueBatch(
	ctx context.Context,
	queueType persistence.QueueType,
	bucket int64,
	messageID int64,
	bucketState legacyQueueV2BucketState,
	blob *commonpb.DataBlob,
) error {
	batch := q.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		templateInsertLegacyQueueV2Message,
		queueType,
		bucket,
		legacyQueueV2MessageRowType,
		messageID,
		blob.Data,
		blob.EncodingType.String(),
	)
	batch.Query(
		templateAdvanceLegacyQueueV2BucketTail,
		messageID,
		bucketState.version+1,
		queueType,
		bucket,
		legacyQueueV2StateRowType,
		legacyQueueV2StateMessageID,
		bucketState.lastMessageID,
		bucketState.version,
	)
	applied, iter, err := q.session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		if verifyErr := q.verifyLegacyQueueV2CommittedMessage(ctx, queueType, bucket, messageID, blob); verifyErr == nil {
			return nil
		}
		return cgocql.ConvertError("EnqueueLegacyQueueV2Message", err)
	}
	if !applied {
		return ErrEnqueueMessageConflict
	}
	return nil
}

func (q *QueueStore) verifyLegacyQueueV2CommittedMessage(
	ctx context.Context,
	queueType persistence.QueueType,
	bucket int64,
	messageID int64,
	blob *commonpb.DataBlob,
) error {
	if err := q.verifyLegacyQueueV2RawMessage(
		ctx,
		queueType,
		messageID,
		blob.Data,
		blob.EncodingType.String(),
	); err != nil {
		return err
	}
	state, err := q.getLegacyQueueV2BucketState(ctx, queueType, bucket)
	if err != nil {
		return err
	}
	if state.lastMessageID < messageID {
		return fmt.Errorf("legacy queue v2 bucket tail %d is below message %d", state.lastMessageID, messageID)
	}
	return nil
}

func (q *QueueStore) upsertLegacyQueueV2RawMessage(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
	data []byte,
	encoding string,
	record legacyQueueAuthorityRecord,
) error {
	bucket, err := legacyQueueV2MessageBucket(messageID, q.messageBucketSize)
	if err != nil {
		return err
	}
	if err := initializeLegacyQueueTargetBucketAuthority(ctx, q.session, queueType, bucket, record); err != nil {
		return err
	}
	applied, err := q.executeGuardedLegacyQueueTargetBucketMutation(
		ctx,
		queueType,
		bucket,
		record,
		"UpsertLegacyQueueV2MessageGuarded",
		templateInsertLegacyQueueV2Message,
		queueType,
		bucket,
		legacyQueueV2MessageRowType,
		messageID,
		data,
		encoding,
	)
	if err != nil {
		return err
	}
	if !applied {
		return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
			"legacy queue type %d target bucket %d changed during message upsert",
			queueType,
			bucket,
		)}
	}
	return nil
}

func (q *QueueStore) verifyLegacyQueueV2RawMessage(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
	expectedData []byte,
	expectedEncoding string,
) error {
	bucket, err := legacyQueueV2MessageBucket(messageID, q.messageBucketSize)
	if err != nil {
		return err
	}
	var data []byte
	var encoding string
	if err := q.session.Query(
		templateGetLegacyQueueV2Message,
		queueType,
		bucket,
		legacyQueueV2MessageRowType,
		messageID,
	).WithContext(ctx).Scan(&data, &encoding); err != nil {
		return cgocql.ConvertError("GetLegacyQueueV2Message", err)
	}
	if !bytes.Equal(data, expectedData) || encoding != expectedEncoding {
		return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
			"legacy queue v2 message %d for queue type %d already contains different data",
			messageID,
			queueType,
		)}
	}
	return nil
}

func (q *QueueStore) mirrorSourceMessageToLegacyQueueV2(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
	blob *commonpb.DataBlob,
	record legacyQueueAuthorityRecord,
) error {
	bucket, err := legacyQueueV2MessageBucket(messageID, q.messageBucketSize)
	if err != nil {
		return err
	}
	if err := q.ensureLegacyQueueV2Layout(ctx, queueType, messageID, record); err != nil {
		return err
	}
	if err := q.upsertLegacyQueueV2RawMessage(
		ctx,
		queueType,
		messageID,
		blob.Data,
		blob.EncodingType.String(),
		record,
	); err != nil {
		return err
	}
	if err := q.ensureLegacyQueueV2BucketStateWithAuthority(ctx, queueType, bucket, messageID, record); err != nil {
		return err
	}
	return q.advanceLegacyQueueV2ActiveBucketWithAuthority(ctx, queueType, bucket, record)
}

func (q *QueueStore) enqueueMessageSourceDual(
	ctx context.Context,
	queueType persistence.QueueType,
	blob *commonpb.DataBlob,
) (int64, error) {
	record, err := readLegacyQueueSourceAuthority(ctx, q.session, queueType)
	if err != nil {
		return persistence.EmptyQueueMessageID, cgocql.ConvertError("ReadLegacyQueueSourceAuthorityForDualEnqueue", err)
	}
	if err := validateLegacyQueueAuthority(
		queueType,
		q.messageBucketSize,
		legacyQueueMigrationAuthoritySource,
		record,
	); err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	sourceLastMessageID, err := q.getLastMessageID(ctx, queueType)
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	state, err := q.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return persistence.EmptyQueueMessageID, cgocql.ConvertError("GetLegacyQueueV2StateForSourceDualEnqueue", err)
	}
	if err := q.validateLegacyQueueV2State(queueType, state); err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	if state.authority != legacyQueueMigrationAuthoritySource {
		return persistence.EmptyQueueMessageID, &persistence.ConditionFailedError{Msg: fmt.Sprintf(
			"legacy queue type %d source-dual write found target state authority %s",
			queueType,
			state.authority,
		)}
	}
	targetLastMessageID, err := q.legacyQueueV2LastMessageID(ctx, queueType, state)
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	lastMessageID := max(sourceLastMessageID, targetLastMessageID)
	if lastMessageID == math.MaxInt64 {
		return persistence.EmptyQueueMessageID, fmt.Errorf("legacy queue message ID exhausted for queue type %d", queueType)
	}
	messageID := lastMessageID + 1
	applied, err := q.executeGuardedLegacyQueueSourceMutation(
		ctx,
		queueType,
		"EnqueueLegacyQueueSourceGuarded",
		templateEnqueueMessageQuery,
		queueType,
		messageID,
		blob.Data,
		blob.EncodingType.String(),
	)
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	if !applied {
		return persistence.EmptyQueueMessageID, ErrEnqueueMessageConflict
	}
	if err := q.mirrorSourceMessageToLegacyQueueV2(ctx, queueType, messageID, blob, record); err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	return messageID, nil
}

func (q *QueueStore) mirrorLegacyQueueV2MessageToSource(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
	blob *commonpb.DataBlob,
) error {
	applied, err := q.executeGuardedLegacyQueueSourceMutationWithAuthority(
		ctx,
		queueType,
		legacyQueueMigrationAuthorityTarget,
		"MirrorLegacyQueueV2MessageToSource",
		templateEnqueueMessageQuery,
		queueType,
		messageID,
		blob.Data,
		blob.EncodingType.String(),
	)
	if err != nil {
		return err
	}
	if applied {
		return nil
	}
	var data []byte
	var encoding string
	if err := q.session.Query(templateGetSourceQueueMessage, queueType, messageID).
		WithContext(ctx).Scan(&data, &encoding); err != nil {
		return cgocql.ConvertError("GetSourceQueueMessageAfterConflict", err)
	}
	if !bytes.Equal(data, blob.Data) || encoding != blob.EncodingType.String() {
		return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
			"source queue message %d for queue type %d already contains different data",
			messageID,
			queueType,
		)}
	}
	return nil
}

func (q *QueueStore) enqueueMessageLegacyQueueV2TargetDual(
	ctx context.Context,
	queueType persistence.QueueType,
	blob *commonpb.DataBlob,
) (int64, error) {
	messageID, err := q.enqueueMessageLegacyQueueV2(ctx, queueType, blob)
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	if err := q.mirrorLegacyQueueV2MessageToSource(ctx, queueType, messageID, blob); err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	return messageID, nil
}

func (q *QueueStore) legacyQueueV2LastMessageID(
	ctx context.Context,
	queueType persistence.QueueType,
	state legacyQueueV2State,
) (int64, error) {
	bucketState, err := q.getLegacyQueueV2BucketState(ctx, queueType, state.activeBucket)
	if err != nil {
		return persistence.EmptyQueueMessageID, cgocql.ConvertError("GetLegacyQueueV2ActiveBucketState", err)
	}
	return bucketState.lastMessageID, nil
}

func (q *QueueStore) readMessagesLegacyQueueV2(
	ctx context.Context,
	queueType persistence.QueueType,
	lastMessageID int64,
	maxCount int,
) ([]*persistence.QueueMessage, error) {
	if maxCount <= 0 || lastMessageID == math.MaxInt64 {
		return nil, nil
	}
	state, err := q.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		if cgocql.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, cgocql.ConvertError("GetLegacyQueueV2StateForRead", err)
	}
	lastTargetMessageID, err := q.legacyQueueV2LastMessageID(ctx, queueType, state)
	if err != nil {
		return nil, err
	}
	firstMessageID := max(lastMessageID+1, state.minimumMessageID)
	return q.readLegacyQueueV2Range(ctx, queueType, firstMessageID, lastTargetMessageID, maxCount, nil)
}

func (q *QueueStore) readMessagesFromDLQLegacyQueueV2(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {
	if pageSize <= 0 || firstMessageID >= lastMessageID {
		return nil, nil, nil
	}
	if len(pageToken) > 0 {
		continuationMessageID, err := decodeLegacyQueueV2PageToken(pageToken)
		if err != nil {
			return nil, nil, err
		}
		firstMessageID = max(firstMessageID, continuationMessageID)
	}
	queueType := q.getDLQTypeFromQueueType()
	state, err := q.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		if cgocql.IsNotFoundError(err) {
			return nil, nil, nil
		}
		return nil, nil, cgocql.ConvertError("GetLegacyQueueV2StateForDLQRead", err)
	}
	lastTargetMessageID, err := q.legacyQueueV2LastMessageID(ctx, queueType, state)
	if err != nil {
		return nil, nil, err
	}
	deleteRanges, err := q.getLegacyQueueV2DeleteRanges(ctx, queueType)
	if err != nil {
		return nil, nil, err
	}
	upperBound := min(lastMessageID, lastTargetMessageID)
	messages, err := q.readLegacyQueueV2Range(
		ctx,
		queueType,
		max(firstMessageID+1, state.minimumMessageID),
		upperBound,
		pageSize,
		deleteRanges,
	)
	if err != nil {
		return nil, nil, err
	}
	if len(messages) < pageSize || messages[len(messages)-1].ID >= upperBound {
		return messages, nil, nil
	}
	return messages, encodeLegacyQueueV2PageToken(messages[len(messages)-1].ID), nil
}

func (q *QueueStore) readMessagesFromDLQSourceMigrating(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {
	if len(pageToken) > 0 && bytes.HasPrefix(pageToken, []byte(legacyQueueV2PageTokenPrefix)) {
		continuationMessageID, err := decodeLegacyQueueV2PageToken(pageToken)
		if err != nil {
			return nil, nil, err
		}
		firstMessageID = max(firstMessageID, continuationMessageID)
		pageToken = nil
	}
	messages, sourcePageToken, err := q.readMessagesFromDLQSource(
		ctx,
		firstMessageID,
		lastMessageID,
		pageSize,
		pageToken,
	)
	if err != nil || len(messages) == 0 || len(sourcePageToken) == 0 {
		return messages, sourcePageToken, err
	}
	return messages, encodeLegacyQueueV2PageToken(messages[len(messages)-1].ID), nil
}

func (q *QueueStore) readLegacyQueueV2Range(
	ctx context.Context,
	queueType persistence.QueueType,
	firstMessageID int64,
	lastMessageID int64,
	limit int,
	deleteRanges []legacyQueueV2DeleteRange,
) ([]*persistence.QueueMessage, error) {
	if firstMessageID > lastMessageID || limit <= 0 {
		return nil, nil
	}
	firstBucket, err := legacyQueueV2MessageBucket(firstMessageID, q.messageBucketSize)
	if err != nil {
		return nil, err
	}
	lastBucket, err := legacyQueueV2MessageBucket(lastMessageID, q.messageBucketSize)
	if err != nil {
		return nil, err
	}
	messages := make([]*persistence.QueueMessage, 0, min(limit, 128))
	for bucket := firstBucket; bucket <= lastBucket && len(messages) < limit; bucket++ {
		bucketFirst, err := legacyQueueV2BucketFirstMessageID(bucket, q.messageBucketSize)
		if err != nil {
			return nil, err
		}
		bucketLast, err := legacyQueueV2BucketLastMessageID(bucket, q.messageBucketSize)
		if err != nil {
			return nil, err
		}
		bucketFirst = max(firstMessageID, bucketFirst)
		bucketLast = min(lastMessageID, bucketLast)
		iter := q.session.Query(
			templateGetLegacyQueueV2Messages,
			queueType,
			bucket,
			legacyQueueV2MessageRowType,
			bucketFirst,
			bucketLast,
		).WithContext(ctx).Iter()
		row := make(map[string]any)
		for len(messages) < limit && iter.MapScan(row) {
			message := convertQueueMessage(row)
			if !legacyQueueV2MessageDeleted(message.ID, deleteRanges) {
				messages = append(messages, message)
			}
			row = make(map[string]any)
		}
		if err := iter.Close(); err != nil {
			return nil, cgocql.ConvertError("ReadLegacyQueueV2Messages", err)
		}
		if bucket == math.MaxInt64 {
			break
		}
	}
	return messages, nil
}

func legacyQueueV2MessageDeleted(messageID int64, ranges []legacyQueueV2DeleteRange) bool {
	for _, deleteRange := range ranges {
		if messageID > deleteRange.firstMessageID && messageID <= deleteRange.lastMessageID {
			return true
		}
	}
	return false
}

func (q *QueueStore) deleteMessagesBeforeLegacyQueueV2(
	ctx context.Context,
	queueType persistence.QueueType,
	exclusiveMessageID int64,
) error {
	if exclusiveMessageID <= persistence.FirstQueueMessageID {
		return nil
	}
	for range 8 {
		state, err := q.getLegacyQueueV2State(ctx, queueType)
		if err != nil {
			if cgocql.IsNotFoundError(err) {
				return nil
			}
			return cgocql.ConvertError("GetLegacyQueueV2StateForDelete", err)
		}
		lastMessageID, err := q.legacyQueueV2LastMessageID(ctx, queueType, state)
		if err != nil {
			return err
		}
		newMinimum := min(exclusiveMessageID, lastMessageID+1)
		if newMinimum <= state.minimumMessageID {
			return q.cleanupLegacyQueueV2Prefix(ctx, queueType)
		}
		applied, err := q.session.Query(
			templateAdvanceLegacyQueueV2Minimum,
			newMinimum,
			state.version+1,
			queueType,
			state.version,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return cgocql.ConvertError("AdvanceLegacyQueueV2Minimum", err)
		}
		if applied {
			return q.cleanupLegacyQueueV2Prefix(ctx, queueType)
		}
	}
	return &persistence.ConditionFailedError{Msg: "legacy queue minimum message ID changed concurrently"}
}

func (q *QueueStore) deleteMessagesBeforeLegacyQueueV2WithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	exclusiveMessageID int64,
	record legacyQueueAuthorityRecord,
) error {
	if exclusiveMessageID <= persistence.FirstQueueMessageID {
		return nil
	}
	for range 8 {
		state, err := q.getLegacyQueueV2State(ctx, queueType)
		if err != nil {
			return cgocql.ConvertError("GetLegacyQueueV2StateForGuardedDelete", err)
		}
		if err := validateLegacyQueueAuthorityRecord(queueType, "target state", record, state.authorityRecord()); err != nil {
			return err
		}
		lastMessageID, err := q.legacyQueueV2LastMessageID(ctx, queueType, state)
		if err != nil {
			return err
		}
		newMinimum := min(exclusiveMessageID, lastMessageID+1)
		if newMinimum <= state.minimumMessageID {
			return q.cleanupLegacyQueueV2PrefixWithAuthority(ctx, queueType, record)
		}
		applied, err := q.session.Query(
			templateAdvanceLegacyQueueV2Minimum+` AND migration_authority = ? AND migration_generation = ? AND message_bucket_size = ?`,
			newMinimum,
			state.version+1,
			queueType,
			state.version,
			int(record.authority),
			record.generation,
			record.messageBucketSize,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return cgocql.ConvertError("AdvanceLegacyQueueV2MinimumGuarded", err)
		}
		if applied {
			return q.cleanupLegacyQueueV2PrefixWithAuthority(ctx, queueType, record)
		}
	}
	return &persistence.ConditionFailedError{Msg: "legacy queue minimum message ID changed concurrently"}
}

func (q *QueueStore) deleteMessagesBeforeLegacyQueueV2TargetDual(
	ctx context.Context,
	queueType persistence.QueueType,
	exclusiveMessageID int64,
) error {
	if err := q.deleteMessagesBeforeLegacyQueueV2(ctx, queueType, exclusiveMessageID); err != nil {
		return err
	}
	return q.deleteMessagesBeforeSourceWithAuthority(
		ctx,
		queueType,
		exclusiveMessageID,
		legacyQueueMigrationAuthorityTarget,
	)
}

func (q *QueueStore) cleanupLegacyQueueV2Prefix(
	ctx context.Context,
	queueType persistence.QueueType,
) error {
	for {
		state, err := q.getLegacyQueueV2State(ctx, queueType)
		if err != nil {
			if cgocql.IsNotFoundError(err) {
				return nil
			}
			return cgocql.ConvertError("GetLegacyQueueV2StateForCleanup", err)
		}
		if state.cleanupMessageID >= state.minimumMessageID {
			return nil
		}
		bucket, err := legacyQueueV2MessageBucket(state.cleanupMessageID, q.messageBucketSize)
		if err != nil {
			return err
		}
		bucketLast, err := legacyQueueV2BucketLastMessageID(bucket, q.messageBucketSize)
		if err != nil {
			return err
		}
		segmentEnd := min(state.minimumMessageID, bucketLast+1)
		if err := q.session.Query(
			templateDeleteLegacyQueueV2MessageRange,
			queueType,
			bucket,
			legacyQueueV2MessageRowType,
			state.cleanupMessageID,
			segmentEnd-1,
		).WithContext(ctx).Exec(); err != nil {
			return cgocql.ConvertError("CleanupLegacyQueueV2Prefix", err)
		}
		_, err = q.session.Query(
			templateAdvanceLegacyQueueV2Cleanup,
			segmentEnd,
			queueType,
			state.cleanupMessageID,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return cgocql.ConvertError("AdvanceLegacyQueueV2Cleanup", err)
		}
	}
}

func (q *QueueStore) cleanupLegacyQueueV2PrefixWithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	record legacyQueueAuthorityRecord,
) error {
	for {
		state, err := q.getLegacyQueueV2State(ctx, queueType)
		if err != nil {
			return cgocql.ConvertError("GetLegacyQueueV2StateForGuardedCleanup", err)
		}
		if err := validateLegacyQueueAuthorityRecord(queueType, "target state", record, state.authorityRecord()); err != nil {
			return err
		}
		if state.cleanupMessageID >= state.minimumMessageID {
			return nil
		}
		bucket, err := legacyQueueV2MessageBucket(state.cleanupMessageID, q.messageBucketSize)
		if err != nil {
			return err
		}
		bucketLast, err := legacyQueueV2BucketLastMessageID(bucket, q.messageBucketSize)
		if err != nil {
			return err
		}
		segmentEnd := min(state.minimumMessageID, bucketLast+1)
		if _, err := q.executeGuardedLegacyQueueTargetBucketMutation(
			ctx,
			queueType,
			bucket,
			record,
			"CleanupLegacyQueueV2PrefixGuarded",
			templateDeleteLegacyQueueV2MessageRange,
			queueType,
			bucket,
			legacyQueueV2MessageRowType,
			state.cleanupMessageID,
			segmentEnd-1,
		); err != nil {
			return err
		}
		_, err = q.session.Query(
			templateAdvanceLegacyQueueV2Cleanup+` AND migration_authority = ? AND migration_generation = ? AND message_bucket_size = ?`,
			segmentEnd,
			queueType,
			state.cleanupMessageID,
			int(record.authority),
			record.generation,
			record.messageBucketSize,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return cgocql.ConvertError("AdvanceLegacyQueueV2CleanupGuarded", err)
		}
	}
}

func (q *QueueStore) deleteMessageLegacyQueueV2(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
) error {
	bucket, err := legacyQueueV2MessageBucket(messageID, q.messageBucketSize)
	if err != nil {
		return err
	}
	if err := q.session.Query(
		templateDeleteLegacyQueueV2Message,
		queueType,
		bucket,
		legacyQueueV2MessageRowType,
		messageID,
	).WithContext(ctx).Exec(); err != nil {
		return cgocql.ConvertError("DeleteLegacyQueueV2Message", err)
	}
	return nil
}

func (q *QueueStore) deleteMessageLegacyQueueV2WithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
	record legacyQueueAuthorityRecord,
) error {
	bucket, err := legacyQueueV2MessageBucket(messageID, q.messageBucketSize)
	if err != nil {
		return err
	}
	_, err = q.executeGuardedLegacyQueueTargetBucketMutation(
		ctx,
		queueType,
		bucket,
		record,
		"DeleteLegacyQueueV2MessageGuarded",
		templateDeleteLegacyQueueV2Message,
		queueType,
		bucket,
		legacyQueueV2MessageRowType,
		messageID,
	)
	return err
}

func (q *QueueStore) deleteMessageLegacyQueueV2TargetDual(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
) error {
	if err := q.deleteMessageLegacyQueueV2(ctx, queueType, messageID); err != nil {
		return err
	}
	return q.deleteMessageSourceWithAuthority(
		ctx,
		queueType,
		messageID,
		legacyQueueMigrationAuthorityTarget,
	)
}

func (q *QueueStore) rangeDeleteMessagesLegacyQueueV2(
	ctx context.Context,
	queueType persistence.QueueType,
	firstMessageID int64,
	lastMessageID int64,
) error {
	if firstMessageID >= lastMessageID || lastMessageID < persistence.FirstQueueMessageID {
		return nil
	}
	deletionID := uuid.NewString()
	if err := q.session.Query(
		templateInsertLegacyQueueV2DeleteRange,
		queueType,
		deletionID,
		firstMessageID,
		lastMessageID,
	).WithContext(ctx).Exec(); err != nil {
		return cgocql.ConvertError("CreateLegacyQueueV2DeleteRange", err)
	}
	return q.cleanupLegacyQueueV2DeleteRange(ctx, queueType, legacyQueueV2DeleteRange{
		id:             deletionID,
		firstMessageID: firstMessageID,
		lastMessageID:  lastMessageID,
	})
}

func (q *QueueStore) rangeDeleteMessagesLegacyQueueV2WithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	firstMessageID int64,
	lastMessageID int64,
	record legacyQueueAuthorityRecord,
) error {
	if firstMessageID >= lastMessageID || lastMessageID < persistence.FirstQueueMessageID {
		return nil
	}
	deletionID := uuid.NewString()
	if _, err := q.executeGuardedLegacyQueueTargetDeleteMutation(
		ctx,
		queueType,
		record,
		"CreateLegacyQueueV2DeleteRangeGuarded",
		templateInsertLegacyQueueV2DeleteRange,
		queueType,
		deletionID,
		firstMessageID,
		lastMessageID,
	); err != nil {
		return err
	}
	return q.cleanupLegacyQueueV2DeleteRangeWithAuthority(ctx, queueType, legacyQueueV2DeleteRange{
		id:             deletionID,
		firstMessageID: firstMessageID,
		lastMessageID:  lastMessageID,
	}, record)
}

func (q *QueueStore) rangeDeleteMessagesLegacyQueueV2TargetDual(
	ctx context.Context,
	queueType persistence.QueueType,
	firstMessageID int64,
	lastMessageID int64,
) error {
	if err := q.rangeDeleteMessagesLegacyQueueV2(ctx, queueType, firstMessageID, lastMessageID); err != nil {
		return err
	}
	return q.rangeDeleteMessagesSourceWithAuthority(
		ctx,
		queueType,
		firstMessageID,
		lastMessageID,
		legacyQueueMigrationAuthorityTarget,
	)
}

func (q *QueueStore) getLegacyQueueV2DeleteRanges(
	ctx context.Context,
	queueType persistence.QueueType,
) ([]legacyQueueV2DeleteRange, error) {
	iter := q.session.Query(templateGetLegacyQueueV2DeleteRanges, queueType).WithContext(ctx).Iter()
	var ranges []legacyQueueV2DeleteRange
	for {
		var deleteRange legacyQueueV2DeleteRange
		if !iter.Scan(&deleteRange.id, &deleteRange.firstMessageID, &deleteRange.lastMessageID) {
			break
		}
		if deleteRange.id == "" {
			continue
		}
		ranges = append(ranges, deleteRange)
	}
	if err := iter.Close(); err != nil {
		return nil, cgocql.ConvertError("GetLegacyQueueV2DeleteRanges", err)
	}
	return ranges, nil
}

func (q *QueueStore) cleanupLegacyQueueV2DeleteRange(
	ctx context.Context,
	queueType persistence.QueueType,
	deleteRange legacyQueueV2DeleteRange,
) error {
	firstMessageID := max(deleteRange.firstMessageID+1, int64(persistence.FirstQueueMessageID))
	if firstMessageID <= deleteRange.lastMessageID {
		if err := q.deleteLegacyQueueV2PhysicalRange(ctx, queueType, firstMessageID, deleteRange.lastMessageID); err != nil {
			return err
		}
	}
	if err := q.session.Query(templateDeleteLegacyQueueV2DeleteRange, queueType, deleteRange.id).
		WithContext(ctx).Exec(); err != nil {
		return cgocql.ConvertError("FinishLegacyQueueV2DeleteRange", err)
	}
	return nil
}

func (q *QueueStore) cleanupLegacyQueueV2DeleteRangeWithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	deleteRange legacyQueueV2DeleteRange,
	record legacyQueueAuthorityRecord,
) error {
	firstMessageID := max(deleteRange.firstMessageID+1, int64(persistence.FirstQueueMessageID))
	if firstMessageID <= deleteRange.lastMessageID {
		if err := q.deleteLegacyQueueV2PhysicalRangeWithAuthority(
			ctx,
			queueType,
			firstMessageID,
			deleteRange.lastMessageID,
			record,
		); err != nil {
			return err
		}
	}
	_, err := q.executeGuardedLegacyQueueTargetDeleteMutation(
		ctx,
		queueType,
		record,
		"FinishLegacyQueueV2DeleteRangeGuarded",
		templateDeleteLegacyQueueV2DeleteRange,
		queueType,
		deleteRange.id,
	)
	return err
}

func (q *QueueStore) deleteLegacyQueueV2PhysicalRange(
	ctx context.Context,
	queueType persistence.QueueType,
	firstMessageID int64,
	lastMessageID int64,
) error {
	firstBucket, err := legacyQueueV2MessageBucket(firstMessageID, q.messageBucketSize)
	if err != nil {
		return err
	}
	lastBucket, err := legacyQueueV2MessageBucket(lastMessageID, q.messageBucketSize)
	if err != nil {
		return err
	}
	for bucket := firstBucket; bucket <= lastBucket; bucket++ {
		bucketFirst, err := legacyQueueV2BucketFirstMessageID(bucket, q.messageBucketSize)
		if err != nil {
			return err
		}
		bucketLast, err := legacyQueueV2BucketLastMessageID(bucket, q.messageBucketSize)
		if err != nil {
			return err
		}
		bucketFirst = max(firstMessageID, bucketFirst)
		bucketLast = min(lastMessageID, bucketLast)
		if err := q.session.Query(
			templateDeleteLegacyQueueV2MessageRange,
			queueType,
			bucket,
			legacyQueueV2MessageRowType,
			bucketFirst,
			bucketLast,
		).WithContext(ctx).Exec(); err != nil {
			return cgocql.ConvertError("DeleteLegacyQueueV2MessageRange", err)
		}
		if bucket == math.MaxInt64 {
			break
		}
	}
	return nil
}

func (q *QueueStore) deleteLegacyQueueV2PhysicalRangeWithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	firstMessageID int64,
	lastMessageID int64,
	record legacyQueueAuthorityRecord,
) error {
	firstBucket, err := legacyQueueV2MessageBucket(firstMessageID, q.messageBucketSize)
	if err != nil {
		return err
	}
	lastBucket, err := legacyQueueV2MessageBucket(lastMessageID, q.messageBucketSize)
	if err != nil {
		return err
	}
	for bucket := firstBucket; bucket <= lastBucket; bucket++ {
		bucketFirst, err := legacyQueueV2BucketFirstMessageID(bucket, q.messageBucketSize)
		if err != nil {
			return err
		}
		bucketLast, err := legacyQueueV2BucketLastMessageID(bucket, q.messageBucketSize)
		if err != nil {
			return err
		}
		bucketFirst = max(firstMessageID, bucketFirst)
		bucketLast = min(lastMessageID, bucketLast)
		if _, err := q.executeGuardedLegacyQueueTargetBucketMutation(
			ctx,
			queueType,
			bucket,
			record,
			"DeleteLegacyQueueV2MessageRangeGuarded",
			templateDeleteLegacyQueueV2MessageRange,
			queueType,
			bucket,
			legacyQueueV2MessageRowType,
			bucketFirst,
			bucketLast,
		); err != nil {
			return err
		}
		if bucket == math.MaxInt64 {
			break
		}
	}
	return nil
}

// CleanupLegacyQueueV2 resumes interrupted prefix and DLQ range cleanup.
func CleanupLegacyQueueV2(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	bucketSize int64,
) error {
	store := &QueueStore{
		session:           session,
		messageBucketSize: bucketSize,
		migrationMode:     config.CassandraLegacyQueueMigrationModeTargetOnly,
	}
	if err := store.cleanupLegacyQueueV2Prefix(ctx, queueType); err != nil {
		return err
	}
	ranges, err := store.getLegacyQueueV2DeleteRanges(ctx, queueType)
	if err != nil {
		return err
	}
	for _, deleteRange := range ranges {
		if err := store.cleanupLegacyQueueV2DeleteRange(ctx, queueType, deleteRange); err != nil {
			return err
		}
	}
	return nil
}

func encodeLegacyQueueV2PageToken(lastMessageID int64) []byte {
	token := make([]byte, len(legacyQueueV2PageTokenPrefix)+1+8)
	copy(token, legacyQueueV2PageTokenPrefix)
	token[len(legacyQueueV2PageTokenPrefix)] = legacyQueueV2PageTokenVersion
	binary.BigEndian.PutUint64(token[len(legacyQueueV2PageTokenPrefix)+1:], uint64(lastMessageID))
	return token
}

func decodeLegacyQueueV2PageToken(token []byte) (int64, error) {
	expectedLength := len(legacyQueueV2PageTokenPrefix) + 1 + 8
	if len(token) != expectedLength || string(token[:min(len(token), len(legacyQueueV2PageTokenPrefix))]) != legacyQueueV2PageTokenPrefix {
		return 0, errors.New("invalid legacy queue v2 page token")
	}
	if token[len(legacyQueueV2PageTokenPrefix)] != legacyQueueV2PageTokenVersion {
		return 0, fmt.Errorf("unsupported legacy queue v2 page token version %d", token[len(legacyQueueV2PageTokenPrefix)])
	}
	messageID := int64(binary.BigEndian.Uint64(token[len(legacyQueueV2PageTokenPrefix)+1:]))
	if messageID < persistence.EmptyQueueMessageID {
		return 0, fmt.Errorf("invalid legacy queue v2 page token message ID %d", messageID)
	}
	return messageID, nil
}
