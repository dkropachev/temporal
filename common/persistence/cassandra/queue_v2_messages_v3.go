package cassandra

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
)

const (
	DefaultQueueV2MessageBucketSpan = int64(4096)

	queueV2MessageDirectoryBucket = int64(-1)
	queueV2MessageStateRowType    = int16(0)
	queueV2MessageDataRowType     = int16(1)
	queueV2MessageStateMessageID  = int64(-1)

	queueV2MessagePageTokenPrefix  = "\xff\xffTEMPORAL-QUEUE-V2-MESSAGES"
	queueV2MessagePageTokenVersion = byte(1)
	queueV2MessageLayoutGeneration = byte(3)
	queueV2MessagePageTokenHeader  = len(queueV2MessagePageTokenPrefix) + 1 + 1 + 8 + 4 + 4 + 8

	templateInsertQueueV2MessageDirectory = `INSERT INTO queue_messages_v3
		(queue_type, queue_name, message_bucket, row_type, message_id, active_message_bucket, bucket_span, version,
		migration_authority, migration_generation, migration_epoch)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateGetQueueV2MessageDirectory = `SELECT active_message_bucket, bucket_span, version FROM queue_messages_v3
		WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ? AND message_id = ?`
	templateAdvanceQueueV2MessageDirectory = `UPDATE queue_messages_v3 SET active_message_bucket = ?, version = ?
		WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ? AND message_id = ?
		IF active_message_bucket = ? AND version = ?`

	templateInsertQueueV2MessageBucketState = `INSERT INTO queue_messages_v3
		(queue_type, queue_name, message_bucket, row_type, message_id, last_message_id, version, bucket_span,
		migration_authority, migration_generation, migration_epoch)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateGetQueueV2MessageBucketState = `SELECT last_message_id, version FROM queue_messages_v3
		WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ? AND message_id = ?`
	templateAdvanceQueueV2MessageBucketTail = `UPDATE queue_messages_v3 SET last_message_id = ?, version = ?
		WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ? AND message_id = ?
		IF last_message_id = ? AND version = ?`

	templateInsertQueueV2MessageV3 = `INSERT INTO queue_messages_v3
		(queue_type, queue_name, message_bucket, row_type, message_id, message_payload, message_encoding)
		VALUES (?, ?, ?, ?, ?, ?, ?)`
	templateInsertQueueV2MessageV3IfNotExists = `INSERT INTO queue_messages_v3
		(queue_type, queue_name, message_bucket, row_type, message_id, message_payload, message_encoding)
		VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateGetQueueV2MessageV3 = `SELECT message_payload, message_encoding FROM queue_messages_v3
		WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ? AND message_id = ?`
	templateGetQueueV2MessagesV3 = `SELECT message_id, message_payload, message_encoding FROM queue_messages_v3
		WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ?
		AND message_id >= ? AND message_id <= ? ORDER BY message_id ASC LIMIT ?`
	templateDeleteQueueV2MessagesV3 = `DELETE FROM queue_messages_v3
		WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ?
		AND message_id >= ? AND message_id <= ?`
)

type queueV2MessageLayout int

const (
	queueV2MessageLayoutSource queueV2MessageLayout = iota
	queueV2MessageLayoutTarget
)

type queueV2MessageDirectory struct {
	activeBucket int64
	bucketSpan   int64
	version      int64
}

type queueV2MessageBucketState struct {
	lastMessageID int64
	version       int64
}

func validateQueueV2MessageBucketSpan(span int64) error {
	if span <= 0 {
		return fmt.Errorf("cassandra QueueV2 message bucket span must be positive: %d", span)
	}
	return nil
}

func (s *queueV2Store) messageBucketSpan() int64 {
	if s.messageSpan == 0 {
		return DefaultQueueV2MessageBucketSpan
	}
	return s.messageSpan
}

func queueV2MessageBucketForID(messageID int64, span int64) (int64, error) {
	if messageID < persistence.FirstQueueMessageID {
		return 0, fmt.Errorf("QueueV2 message ID must be non-negative: %d", messageID)
	}
	if err := validateQueueV2MessageBucketSpan(span); err != nil {
		return 0, err
	}
	return messageID / span, nil
}

func queueV2MessageBucketFirstID(bucket int64, span int64) (int64, error) {
	if bucket < 0 || span <= 0 || bucket > math.MaxInt64/span {
		return 0, fmt.Errorf("invalid QueueV2 message bucket %d with span %d", bucket, span)
	}
	return bucket * span, nil
}

func queueV2MessageBucketLastID(bucket int64, span int64) (int64, error) {
	first, err := queueV2MessageBucketFirstID(bucket, span)
	if err != nil {
		return 0, err
	}
	if first > math.MaxInt64-(span-1) {
		return math.MaxInt64, nil
	}
	return first + span - 1, nil
}

func (s *queueV2Store) messageReadLayout() queueV2MessageLayout {
	switch s.migrationMode {
	case config.CassandraQueueV2MigrationModeTargetDual,
		config.CassandraQueueV2MigrationModeTargetOnly:
		return queueV2MessageLayoutTarget
	default:
		return queueV2MessageLayoutSource
	}
}

func (s *queueV2Store) messageWriteLayouts() (
	primary queueV2MessageLayout,
	mirror queueV2MessageLayout,
	hasMirror bool,
) {
	switch s.migrationMode {
	case config.CassandraQueueV2MigrationModeSourceDual,
		config.CassandraQueueV2MigrationModeTargetShadow:
		return queueV2MessageLayoutSource, queueV2MessageLayoutTarget, true
	case config.CassandraQueueV2MigrationModeTargetDual:
		return queueV2MessageLayoutTarget, queueV2MessageLayoutSource, true
	case config.CassandraQueueV2MigrationModeTargetOnly:
		return queueV2MessageLayoutTarget, 0, false
	default:
		return queueV2MessageLayoutSource, 0, false
	}
}

func (s *queueV2Store) messageUsesTarget() bool {
	primary, mirror, hasMirror := s.messageWriteLayouts()
	return primary == queueV2MessageLayoutTarget || hasMirror && mirror == queueV2MessageLayoutTarget
}

func (s *queueV2Store) messageHasMirror() bool {
	_, _, hasMirror := s.messageWriteLayouts()
	return hasMirror
}

func (s *queueV2Store) messageMirrorsToTarget() bool {
	_, mirror, hasMirror := s.messageWriteLayouts()
	return hasMirror && mirror == queueV2MessageLayoutTarget
}

func (s *queueV2Store) enqueueMessage(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	queue *Queue,
	blob *commonpb.DataBlob,
	route queueV2OperationRoute,
) (int64, error) {
	var (
		messageID int64
		err       error
	)
	if route.layout == queueV2MetadataLayoutTarget {
		messageID, err = s.enqueueQueueV2MessageTarget(ctx, queueType, queueName, blob, route.record)
	} else {
		messageID, err = s.enqueueQueueV2MessageSource(ctx, queueType, queueName, blob, route)
	}
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	if !route.mirror {
		return messageID, nil
	}
	if route.layout == queueV2MetadataLayoutSource {
		minimumMessageID, minimumErr := queueV2MinimumMessageID(queueType, queueName, queue)
		if minimumErr != nil {
			return persistence.EmptyQueueMessageID, minimumErr
		}
		err = s.mirrorQueueV2MessageToTarget(
			ctx,
			queueType,
			queueName,
			minimumMessageID,
			messageID,
			blob.Data,
			blob.EncodingType.String(),
			route.record,
		)
	} else {
		err = s.mirrorQueueV2MessageToSource(
			ctx,
			queueType,
			queueName,
			messageID,
			blob.Data,
			blob.EncodingType.String(),
			route.record,
		)
	}
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	return messageID, nil
}

func queueV2MinimumMessageID(
	queueType persistence.QueueV2Type,
	queueName string,
	queue *Queue,
) (int64, error) {
	if queue == nil {
		return 0, errors.New("QueueV2 metadata is required for mirrored message writes")
	}
	partition, err := persistence.GetPartitionForQueueV2(queueType, queueName, queue.Metadata)
	if err != nil {
		return 0, err
	}
	return partition.MinMessageId, nil
}

func (s *queueV2Store) enqueueQueueV2MessageSource(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	blob *commonpb.DataBlob,
	route queueV2OperationRoute,
) (int64, error) {
	lastMessageID, ok, err := s.getSourceMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	nextMessageID := int64(persistence.FirstQueueMessageID)
	if ok {
		if lastMessageID == math.MaxInt64 {
			return persistence.EmptyQueueMessageID, fmt.Errorf("QueueV2 message ID exhausted for queue type %v and name %v", queueType, queueName)
		}
		nextMessageID = lastMessageID + 1
	}
	if !route.guarded {
		if err := s.tryInsert(ctx, queueType, queueName, blob, nextMessageID); err != nil {
			return persistence.EmptyQueueMessageID, err
		}
		return nextMessageID, nil
	}
	batch := s.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		TemplateEnqueueMessageQuery,
		queueType,
		queueName,
		0,
		nextMessageID,
		blob.Data,
		blob.EncodingType.String(),
	)
	addQueueV2MessageAuthorityGuard(
		batch,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
		0,
		route.record,
	)
	applied, iter, err := s.session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		return persistence.EmptyQueueMessageID, cgocql.ConvertError("QueueV2EnqueueMessageGuarded", err)
	}
	if !applied {
		if err := s.requireQueueV2MessageAuthority(
			ctx,
			queueType,
			queueName,
			queueV2MetadataLayoutSource,
			0,
			route.record,
		); err != nil {
			return persistence.EmptyQueueMessageID, err
		}
		return persistence.EmptyQueueMessageID, ErrEnqueueMessageConflict
	}
	return nextMessageID, nil
}

func (s *queueV2Store) initializeQueueV2MessageTarget(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	record queueV2AuthorityRecord,
) error {
	return s.ensureQueueV2MessageTarget(ctx, queueType, queueName, persistence.FirstQueueMessageID, true, record)
}

func (s *queueV2Store) ensureQueueV2MessageTargetForExistingQueue(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	record queueV2AuthorityRecord,
) error {
	_, err := s.getQueueV2MessageDirectory(ctx, queueType, queueName)
	if err != nil {
		return fmt.Errorf("QueueV2 target message state is missing; backfill is required: %w", err)
	}
	return s.requireQueueV2MessageAuthority(
		ctx,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
		queueV2MessageDirectoryBucket,
		record,
	)
}

func (s *queueV2Store) ensureQueueV2MessageTarget(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	minimumMessageID int64,
	allowDeletedPrefix bool,
	record queueV2AuthorityRecord,
) error {
	if minimumMessageID < persistence.FirstQueueMessageID {
		return fmt.Errorf("invalid QueueV2 minimum message ID %d", minimumMessageID)
	}
	span := s.messageBucketSpan()
	initialBucket, err := queueV2MessageBucketForID(minimumMessageID, span)
	if err != nil {
		return err
	}
	initialTail := minimumMessageID - 1
	applied, err := s.session.Query(
		templateInsertQueueV2MessageDirectory,
		queueType,
		queueName,
		queueV2MessageDirectoryBucket,
		queueV2MessageStateRowType,
		queueV2MessageStateMessageID,
		initialBucket,
		span,
		int64(0),
		int(record.authority),
		record.generation,
		record.epoch,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return cgocql.ConvertError("InitializeQueueV2MessageDirectory", err)
	}
	if !applied {
		directory, err := s.getQueueV2MessageDirectory(ctx, queueType, queueName)
		if err != nil {
			return err
		}
		if directory.bucketSpan != span {
			return fmt.Errorf(
				"QueueV2 message bucket span mismatch for queue type %v and name %v: stored %d, configured %d",
				queueType,
				queueName,
				directory.bucketSpan,
				span,
			)
		}
		if err := s.initializeQueueV2TargetMessageAuthorityRecord(
			ctx,
			queueType,
			queueName,
			queueV2MessageDirectoryBucket,
			record,
		); err != nil {
			return err
		}
		if err := s.requireQueueV2MessageAuthority(
			ctx,
			queueType,
			queueName,
			queueV2MetadataLayoutTarget,
			queueV2MessageDirectoryBucket,
			record,
		); err != nil {
			return err
		}
	} else {
		s.cacheQueueV2ActiveMessageBucket(queueType, queueName, initialBucket)
	}
	return s.ensureQueueV2MessageBucketState(
		ctx,
		queueType,
		queueName,
		initialBucket,
		initialTail,
		allowDeletedPrefix,
		record,
	)
}

func (s *queueV2Store) getQueueV2MessageDirectory(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) (queueV2MessageDirectory, error) {
	var state queueV2MessageDirectory
	err := s.session.Query(
		templateGetQueueV2MessageDirectory,
		queueType,
		queueName,
		queueV2MessageDirectoryBucket,
		queueV2MessageStateRowType,
		queueV2MessageStateMessageID,
	).WithContext(ctx).Scan(&state.activeBucket, &state.bucketSpan, &state.version)
	if err != nil {
		return state, cgocql.ConvertError("GetQueueV2MessageDirectory", err)
	}
	if state.bucketSpan != s.messageBucketSpan() {
		return state, fmt.Errorf(
			"QueueV2 message bucket span mismatch for queue type %v and name %v: stored %d, configured %d",
			queueType,
			queueName,
			state.bucketSpan,
			s.messageBucketSpan(),
		)
	}
	s.cacheQueueV2ActiveMessageBucket(queueType, queueName, state.activeBucket)
	return state, nil
}

func (s *queueV2Store) cacheQueueV2ActiveMessageBucket(
	queueType persistence.QueueV2Type,
	queueName string,
	bucket int64,
) {
	key := queueV2Key{queueType: queueType, queueName: queueName}
	s.activeMessageBucketsMu.Lock()
	defer s.activeMessageBucketsMu.Unlock()
	if s.activeMessageBuckets == nil {
		s.activeMessageBuckets = make(map[queueV2Key]int64)
	}
	s.activeMessageBuckets[key] = bucket
}

func (s *queueV2Store) cachedQueueV2ActiveMessageBucket(
	queueType persistence.QueueV2Type,
	queueName string,
) (int64, bool) {
	key := queueV2Key{queueType: queueType, queueName: queueName}
	s.activeMessageBucketsMu.RLock()
	defer s.activeMessageBucketsMu.RUnlock()
	bucket, ok := s.activeMessageBuckets[key]
	return bucket, ok
}

func (s *queueV2Store) getQueueV2MessageBucketState(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	bucket int64,
) (queueV2MessageBucketState, error) {
	var state queueV2MessageBucketState
	err := s.session.Query(
		templateGetQueueV2MessageBucketState,
		queueType,
		queueName,
		bucket,
		queueV2MessageStateRowType,
		queueV2MessageStateMessageID,
	).WithContext(ctx).Scan(&state.lastMessageID, &state.version)
	if err != nil {
		return state, cgocql.ConvertError("GetQueueV2MessageBucketState", err)
	}
	return state, nil
}

func (s *queueV2Store) ensureQueueV2MessageBucketState(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	bucket int64,
	initialTail int64,
	allowAdvance bool,
	record queueV2AuthorityRecord,
) error {
	firstMessageID, err := queueV2MessageBucketFirstID(bucket, s.messageBucketSpan())
	if err != nil {
		return err
	}
	lastMessageID, err := queueV2MessageBucketLastID(bucket, s.messageBucketSpan())
	if err != nil {
		return err
	}
	if initialTail < firstMessageID-1 || initialTail > lastMessageID {
		return fmt.Errorf("invalid QueueV2 initial bucket tail %d for bucket %d", initialTail, bucket)
	}
	applied, err := s.session.Query(
		templateInsertQueueV2MessageBucketState,
		queueType,
		queueName,
		bucket,
		queueV2MessageStateRowType,
		queueV2MessageStateMessageID,
		initialTail,
		int64(0),
		record.messageSpan,
		int(record.authority),
		record.generation,
		record.epoch,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return cgocql.ConvertError("InitializeQueueV2MessageBucketState", err)
	}
	if !applied {
		if err := s.initializeQueueV2TargetMessageAuthorityRecord(
			ctx,
			queueType,
			queueName,
			bucket,
			record,
		); err != nil {
			return err
		}
		if err := s.requireQueueV2MessageAuthority(
			ctx,
			queueType,
			queueName,
			queueV2MetadataLayoutTarget,
			bucket,
			record,
		); err != nil {
			return err
		}
	}
	if applied || !allowAdvance {
		return nil
	}
	for range 8 {
		state, err := s.getQueueV2MessageBucketState(ctx, queueType, queueName, bucket)
		if err != nil {
			return err
		}
		if state.lastMessageID >= initialTail {
			return nil
		}
		applied, err := s.advanceQueueV2MessageBucketTail(ctx, queueType, queueName, bucket, state, initialTail, record)
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
	}
	return ErrEnqueueMessageConflict
}

func (s *queueV2Store) advanceQueueV2MessageBucketTail(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	bucket int64,
	state queueV2MessageBucketState,
	newTail int64,
	record queueV2AuthorityRecord,
) (bool, error) {
	batch := s.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		templateAdvanceQueueV2MessageBucketTail,
		newTail,
		state.version+1,
		queueType,
		queueName,
		bucket,
		queueV2MessageStateRowType,
		queueV2MessageStateMessageID,
		state.lastMessageID,
		state.version,
	)
	addQueueV2MessageAuthorityGuard(
		batch,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
		bucket,
		record,
	)
	applied, iter, err := s.session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		return false, cgocql.ConvertError("AdvanceQueueV2MessageBucketTail", err)
	}
	if !applied {
		if err := s.requireQueueV2MessageAuthority(
			ctx,
			queueType,
			queueName,
			queueV2MetadataLayoutTarget,
			bucket,
			record,
		); err != nil {
			return false, err
		}
	}
	return applied, nil
}

//nolint:revive // Enqueue advances bucket and directory state through bounded conditional retries.
func (s *queueV2Store) enqueueQueueV2MessageTarget(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	blob *commonpb.DataBlob,
	record queueV2AuthorityRecord,
) (int64, error) {
	for range 8 {
		activeBucket, ok := s.cachedQueueV2ActiveMessageBucket(queueType, queueName)
		if !ok {
			directory, err := s.getQueueV2MessageDirectory(ctx, queueType, queueName)
			if err != nil {
				return persistence.EmptyQueueMessageID, fmt.Errorf("QueueV2 target message state is unavailable: %w", err)
			}
			activeBucket = directory.activeBucket
			if err := s.initializeQueueV2TargetMessageAuthorityRecord(
				ctx,
				queueType,
				queueName,
				activeBucket,
				record,
			); err != nil {
				return persistence.EmptyQueueMessageID, err
			}
		}
		state, err := s.getQueueV2MessageBucketState(ctx, queueType, queueName, activeBucket)
		if err != nil {
			return persistence.EmptyQueueMessageID, err
		}
		bucketLastMessageID, err := queueV2MessageBucketLastID(activeBucket, s.messageBucketSpan())
		if err != nil {
			return persistence.EmptyQueueMessageID, err
		}
		if state.lastMessageID >= bucketLastMessageID {
			directory, err := s.getQueueV2MessageDirectory(ctx, queueType, queueName)
			if err != nil {
				return persistence.EmptyQueueMessageID, err
			}
			if directory.activeBucket != activeBucket {
				if err := s.initializeQueueV2TargetMessageAuthorityRecord(
					ctx,
					queueType,
					queueName,
					directory.activeBucket,
					record,
				); err != nil {
					return persistence.EmptyQueueMessageID, err
				}
				continue
			}
			if err := s.rollQueueV2MessageBucket(ctx, queueType, queueName, directory, record); err != nil {
				return persistence.EmptyQueueMessageID, err
			}
			continue
		}
		messageID := state.lastMessageID + 1
		if err := s.executeQueueV2TargetEnqueueBatch(
			ctx,
			queueType,
			queueName,
			activeBucket,
			messageID,
			state,
			blob,
			record,
		); err != nil {
			return persistence.EmptyQueueMessageID, err
		}
		return messageID, nil
	}
	return persistence.EmptyQueueMessageID, ErrEnqueueMessageConflict
}

func (s *queueV2Store) rollQueueV2MessageBucket(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	directory queueV2MessageDirectory,
	record queueV2AuthorityRecord,
) error {
	if directory.activeBucket == math.MaxInt64 {
		return fmt.Errorf("QueueV2 message bucket exhausted for queue type %v and name %v", queueType, queueName)
	}
	nextBucket := directory.activeBucket + 1
	firstMessageID, err := queueV2MessageBucketFirstID(nextBucket, directory.bucketSpan)
	if err != nil {
		return err
	}
	if err := s.ensureQueueV2MessageBucketState(
		ctx,
		queueType,
		queueName,
		nextBucket,
		firstMessageID-1,
		false,
		record,
	); err != nil {
		return err
	}
	batch := s.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		templateAdvanceQueueV2MessageDirectory,
		nextBucket,
		directory.version+1,
		queueType,
		queueName,
		queueV2MessageDirectoryBucket,
		queueV2MessageStateRowType,
		queueV2MessageStateMessageID,
		directory.activeBucket,
		directory.version,
	)
	addQueueV2MessageAuthorityGuard(
		batch,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
		queueV2MessageDirectoryBucket,
		record,
	)
	applied, iter, err := s.session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		return cgocql.ConvertError("RollQueueV2MessageBucket", err)
	}
	if !applied {
		if err := s.requireQueueV2MessageAuthority(
			ctx,
			queueType,
			queueName,
			queueV2MetadataLayoutTarget,
			queueV2MessageDirectoryBucket,
			record,
		); err != nil {
			return err
		}
		return ErrEnqueueMessageConflict
	}
	s.cacheQueueV2ActiveMessageBucket(queueType, queueName, nextBucket)
	return nil
}

func (s *queueV2Store) executeQueueV2TargetEnqueueBatch(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	bucket int64,
	messageID int64,
	state queueV2MessageBucketState,
	blob *commonpb.DataBlob,
	record queueV2AuthorityRecord,
) error {
	batch := s.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		templateInsertQueueV2MessageV3,
		queueType,
		queueName,
		bucket,
		queueV2MessageDataRowType,
		messageID,
		blob.Data,
		blob.EncodingType.String(),
	)
	batch.Query(
		templateAdvanceQueueV2MessageBucketTail,
		messageID,
		state.version+1,
		queueType,
		queueName,
		bucket,
		queueV2MessageStateRowType,
		queueV2MessageStateMessageID,
		state.lastMessageID,
		state.version,
	)
	addQueueV2MessageAuthorityGuard(
		batch,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
		bucket,
		record,
	)
	applied, iter, err := s.session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		if verifyErr := s.verifyQueueV2TargetCommittedMessage(
			ctx,
			queueType,
			queueName,
			bucket,
			messageID,
			blob.Data,
			blob.EncodingType.String(),
		); verifyErr == nil {
			return nil
		}
		return cgocql.ConvertError("QueueV2EnqueueMessageV3", err)
	}
	if !applied {
		if err := s.requireQueueV2MessageAuthority(
			ctx,
			queueType,
			queueName,
			queueV2MetadataLayoutTarget,
			bucket,
			record,
		); err != nil {
			return err
		}
		return ErrEnqueueMessageConflict
	}
	return nil
}

func (s *queueV2Store) verifyQueueV2TargetCommittedMessage(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	bucket int64,
	messageID int64,
	expectedData []byte,
	expectedEncoding string,
) error {
	if err := s.verifyQueueV2TargetRawMessage(
		ctx,
		queueType,
		queueName,
		messageID,
		expectedData,
		expectedEncoding,
	); err != nil {
		return err
	}
	state, err := s.getQueueV2MessageBucketState(ctx, queueType, queueName, bucket)
	if err != nil {
		return err
	}
	if state.lastMessageID < messageID {
		return fmt.Errorf("QueueV2 message bucket tail %d is below committed message %d", state.lastMessageID, messageID)
	}
	return nil
}

func (s *queueV2Store) mirrorQueueV2MessageToTarget(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	minimumMessageID int64,
	messageID int64,
	data []byte,
	encoding string,
	record queueV2AuthorityRecord,
) error {
	if err := s.ensureQueueV2MessageTarget(ctx, queueType, queueName, minimumMessageID, true, record); err != nil {
		return err
	}
	bucket, err := queueV2MessageBucketForID(messageID, s.messageBucketSpan())
	if err != nil {
		return err
	}
	firstMessageID, err := queueV2MessageBucketFirstID(bucket, s.messageBucketSpan())
	if err != nil {
		return err
	}
	if err := s.ensureQueueV2MessageBucketState(
		ctx,
		queueType,
		queueName,
		bucket,
		firstMessageID-1,
		false,
		record,
	); err != nil {
		return err
	}
	batch := s.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		templateInsertQueueV2MessageV3IfNotExists,
		queueType,
		queueName,
		bucket,
		queueV2MessageDataRowType,
		messageID,
		data,
		encoding,
	)
	addQueueV2MessageAuthorityGuard(
		batch,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
		bucket,
		record,
	)
	applied, iter, err := s.session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		return cgocql.ConvertError("MirrorQueueV2MessageToV3", err)
	}
	if !applied {
		if err := s.requireQueueV2MessageAuthority(
			ctx,
			queueType,
			queueName,
			queueV2MetadataLayoutTarget,
			bucket,
			record,
		); err != nil {
			return err
		}
		if err := s.verifyQueueV2TargetRawMessage(ctx, queueType, queueName, messageID, data, encoding); err != nil {
			return err
		}
	}
	if err := s.advanceQueueV2MirroredBucketTail(ctx, queueType, queueName, bucket, record); err != nil {
		return err
	}
	return s.advanceQueueV2MessageDirectoryOverFullBuckets(ctx, queueType, queueName, record)
}

func (s *queueV2Store) verifyQueueV2TargetRawMessage(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	messageID int64,
	expectedData []byte,
	expectedEncoding string,
) error {
	bucket, err := queueV2MessageBucketForID(messageID, s.messageBucketSpan())
	if err != nil {
		return err
	}
	var data []byte
	var encoding string
	err = s.session.Query(
		templateGetQueueV2MessageV3,
		queueType,
		queueName,
		bucket,
		queueV2MessageDataRowType,
		messageID,
	).WithContext(ctx).Scan(&data, &encoding)
	if err != nil {
		return cgocql.ConvertError("GetQueueV2MessageV3", err)
	}
	if !bytes.Equal(data, expectedData) || encoding != expectedEncoding {
		return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
			"QueueV2 message %d for queue type %v and name %v contains different data",
			messageID,
			queueType,
			queueName,
		)}
	}
	return nil
}

func (s *queueV2Store) advanceQueueV2MirroredBucketTail(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	bucket int64,
	record queueV2AuthorityRecord,
) error {
	bucketLastMessageID, err := queueV2MessageBucketLastID(bucket, s.messageBucketSpan())
	if err != nil {
		return err
	}
	for range s.messageBucketSpan() + 1 {
		state, err := s.getQueueV2MessageBucketState(ctx, queueType, queueName, bucket)
		if err != nil {
			return err
		}
		if state.lastMessageID >= bucketLastMessageID {
			return nil
		}
		nextMessageID := state.lastMessageID + 1
		var data []byte
		var encoding string
		err = s.session.Query(
			templateGetQueueV2MessageV3,
			queueType,
			queueName,
			bucket,
			queueV2MessageDataRowType,
			nextMessageID,
		).WithContext(ctx).Scan(&data, &encoding)
		if err != nil {
			if cgocql.IsNotFoundError(err) {
				return nil
			}
			return cgocql.ConvertError("GetNextQueueV2MessageV3ForTail", err)
		}
		_, err = s.advanceQueueV2MessageBucketTail(
			ctx,
			queueType,
			queueName,
			bucket,
			state,
			nextMessageID,
			record,
		)
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("QueueV2 message tail did not converge for bucket %d", bucket)
}

func (s *queueV2Store) advanceQueueV2MessageDirectoryOverFullBuckets(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	record queueV2AuthorityRecord,
) error {
	for range 8 {
		directory, err := s.getQueueV2MessageDirectory(ctx, queueType, queueName)
		if err != nil {
			return err
		}
		state, err := s.getQueueV2MessageBucketState(ctx, queueType, queueName, directory.activeBucket)
		if err != nil {
			return err
		}
		lastMessageID, err := queueV2MessageBucketLastID(directory.activeBucket, directory.bucketSpan)
		if err != nil {
			return err
		}
		if state.lastMessageID < lastMessageID {
			return nil
		}
		if err := s.rollQueueV2MessageBucket(ctx, queueType, queueName, directory, record); err != nil {
			if err == ErrEnqueueMessageConflict {
				continue
			}
			return err
		}
	}
	return ErrEnqueueMessageConflict
}

func (s *queueV2Store) mirrorQueueV2MessageToSource(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	messageID int64,
	data []byte,
	encoding string,
	record queueV2AuthorityRecord,
) error {
	batch := s.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		TemplateEnqueueMessageQuery,
		queueType,
		queueName,
		0,
		messageID,
		data,
		encoding,
	)
	addQueueV2MessageAuthorityGuard(
		batch,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
		0,
		record,
	)
	applied, iter, err := s.session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		if verifyErr := s.verifyQueueV2SourceRawMessage(
			ctx,
			queueType,
			queueName,
			messageID,
			data,
			encoding,
			record,
		); verifyErr == nil {
			return nil
		}
		return cgocql.ConvertError("MirrorQueueV2MessageToSource", err)
	}
	if applied {
		return nil
	}
	return s.verifyQueueV2SourceRawMessage(
		ctx,
		queueType,
		queueName,
		messageID,
		data,
		encoding,
		record,
	)
}

func (s *queueV2Store) verifyQueueV2SourceRawMessage(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	messageID int64,
	expectedData []byte,
	expectedEncoding string,
	record queueV2AuthorityRecord,
) error {
	if err := s.requireQueueV2MessageAuthority(
		ctx,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
		0,
		record,
	); err != nil {
		return err
	}
	var existingData []byte
	var existingEncoding string
	err := s.session.Query(
		`SELECT message_payload, message_encoding FROM queue_messages
			WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? AND message_id = ?`,
		queueType,
		queueName,
		0,
		messageID,
	).WithContext(ctx).Scan(&existingData, &existingEncoding)
	if err != nil {
		return cgocql.ConvertError("GetMirroredQueueV2SourceMessage", err)
	}
	if !bytes.Equal(existingData, expectedData) || existingEncoding != expectedEncoding {
		return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
			"source QueueV2 message %d for queue type %v and name %v contains different data",
			messageID,
			queueType,
			queueName,
		)}
	}
	return nil
}

func (s *queueV2Store) getQueueV2TargetMaxMessageID(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) (int64, bool, error) {
	directory, err := s.getQueueV2MessageDirectory(ctx, queueType, queueName)
	if err != nil {
		return 0, false, err
	}
	state, err := s.getQueueV2MessageBucketState(ctx, queueType, queueName, directory.activeBucket)
	if err != nil {
		return 0, false, err
	}
	if state.lastMessageID < persistence.FirstQueueMessageID {
		return 0, false, nil
	}
	return state.lastMessageID, true, nil
}

func (s *queueV2Store) readMessagesTarget(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
	route queueV2OperationRoute,
) (*persistence.InternalReadMessagesResponse, error) {
	queue, err := s.getQueueFromLayout(
		ctx,
		request.QueueName,
		request.QueueType,
		route.layout,
	)
	if err != nil {
		return nil, err
	}
	minimumMessageID, err := queueV2MinimumMessageID(request.QueueType, request.QueueName, queue)
	if err != nil {
		return nil, err
	}
	if len(request.NextPageToken) != 0 {
		lastMessageID, err := decodeQueueV2MessagePageToken(
			request.NextPageToken,
			request.QueueType,
			request.QueueName,
			s.messageBucketSpan(),
		)
		if err != nil {
			return nil, err
		}
		if lastMessageID == math.MaxInt64 {
			return &persistence.InternalReadMessagesResponse{}, nil
		}
		minimumMessageID = max(minimumMessageID, lastMessageID+1)
	}
	messages, err := s.readQueueV2TargetMessagesFrom(
		ctx,
		request.QueueType,
		request.QueueName,
		minimumMessageID,
		request.PageSize,
	)
	if err != nil {
		return nil, err
	}
	var nextPageToken []byte
	if len(messages) != 0 {
		nextPageToken = encodeQueueV2MessagePageToken(
			request.QueueType,
			request.QueueName,
			s.messageBucketSpan(),
			messages[len(messages)-1].MetaData.ID,
		)
	}
	return &persistence.InternalReadMessagesResponse{
		Messages:      messages,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *queueV2Store) readQueueV2TargetMessagesFrom(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	minimumMessageID int64,
	pageSize int,
) ([]persistence.QueueV2Message, error) {
	maxMessageID, ok, err := s.getQueueV2TargetMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	if !ok || minimumMessageID > maxMessageID {
		return nil, nil
	}
	firstBucket, err := queueV2MessageBucketForID(minimumMessageID, s.messageBucketSpan())
	if err != nil {
		return nil, err
	}
	lastBucket, err := queueV2MessageBucketForID(maxMessageID, s.messageBucketSpan())
	if err != nil {
		return nil, err
	}
	messages := make([]persistence.QueueV2Message, 0, preallocatedResultCapacity(pageSize))
	for bucket := firstBucket; bucket <= lastBucket && len(messages) < pageSize; bucket++ {
		bucketFirstMessageID, err := queueV2MessageBucketFirstID(bucket, s.messageBucketSpan())
		if err != nil {
			return nil, err
		}
		bucketLastMessageID, err := queueV2MessageBucketLastID(bucket, s.messageBucketSpan())
		if err != nil {
			return nil, err
		}
		iter := s.session.Query(
			templateGetQueueV2MessagesV3,
			queueType,
			queueName,
			bucket,
			queueV2MessageDataRowType,
			max(minimumMessageID, bucketFirstMessageID),
			min(maxMessageID, bucketLastMessageID),
			pageSize-len(messages),
		).WithContext(ctx).Iter()
		for {
			var (
				messageID       int64
				messageData     []byte
				messageEncoding string
			)
			if !iter.Scan(&messageID, &messageData, &messageEncoding) {
				break
			}
			encoding, err := enumspb.EncodingTypeFromString(messageEncoding)
			if err != nil {
				_ = iter.Close()
				return nil, serialization.NewUnknownEncodingTypeError(messageEncoding)
			}
			messages = append(messages, persistence.QueueV2Message{
				MetaData: persistence.MessageMetadata{ID: messageID},
				Data: &commonpb.DataBlob{
					EncodingType: enumspb.EncodingType(encoding),
					Data:         messageData,
				},
			})
		}
		if err := iter.Close(); err != nil {
			return nil, cgocql.ConvertError("QueueV2ReadMessagesV3", err)
		}
		if bucket == math.MaxInt64 {
			break
		}
	}
	return messages, nil
}

func (s *queueV2Store) shadowCompareTargetMessages(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
	minimumMessageID int64,
	sourceMessages []persistence.QueueV2Message,
) {
	targetMessages, err := s.readQueueV2TargetMessagesFrom(
		ctx,
		request.QueueType,
		request.QueueName,
		minimumMessageID,
		request.PageSize,
	)
	if err == nil && queueV2MessagesEqual(sourceMessages, targetMessages) {
		return
	}
	if s.logger != nil {
		s.logger.Warn(
			"Cassandra QueueV2 message shadow read mismatch",
			tag.NewStringTag("queue-name", request.QueueName),
			tag.NewInt("queue-type", int(request.QueueType)),
			tag.Error(err),
		)
	}
}

func queueV2MessagesEqual(left []persistence.QueueV2Message, right []persistence.QueueV2Message) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].MetaData.ID != right[i].MetaData.ID ||
			left[i].Data.EncodingType != right[i].Data.EncodingType ||
			!bytes.Equal(left[i].Data.Data, right[i].Data.Data) {
			return false
		}
	}
	return true
}

//nolint:revive // Range deletion spans physical buckets and optionally fences every mutation with authority LWTs.
func (s *queueV2Store) deleteMessageRangeFromLayout(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	minimumMessageID int64,
	maximumMessageID int64,
	layout queueV2MessageLayout,
	record queueV2AuthorityRecord,
	guarded bool,
) error {
	if layout == queueV2MessageLayoutSource {
		return s.deleteSourceMessageRange(
			ctx,
			queueType,
			queueName,
			minimumMessageID,
			maximumMessageID,
			record,
			guarded,
		)
	}
	firstBucket, err := queueV2MessageBucketForID(minimumMessageID, s.messageBucketSpan())
	if err != nil {
		return err
	}
	lastBucket, err := queueV2MessageBucketForID(maximumMessageID, s.messageBucketSpan())
	if err != nil {
		return err
	}
	for bucket := firstBucket; bucket <= lastBucket; bucket++ {
		bucketFirstMessageID, err := queueV2MessageBucketFirstID(bucket, s.messageBucketSpan())
		if err != nil {
			return err
		}
		bucketLastMessageID, err := queueV2MessageBucketLastID(bucket, s.messageBucketSpan())
		if err != nil {
			return err
		}
		args := []any{
			queueType,
			queueName,
			bucket,
			queueV2MessageDataRowType,
			max(minimumMessageID, bucketFirstMessageID),
			min(maximumMessageID, bucketLastMessageID),
		}
		if !guarded {
			if err := s.session.Query(templateDeleteQueueV2MessagesV3, args...).WithContext(ctx).Exec(); err != nil {
				return cgocql.ConvertError("QueueV2RangeDeleteMessagesV3", err)
			}
		} else {
			batch := s.session.NewBatch(cgocql.LoggedBatch).WithContext(ctx)
			batch.Query(templateDeleteQueueV2MessagesV3, args...)
			addQueueV2MessageAuthorityGuard(
				batch,
				queueType,
				queueName,
				queueV2MetadataLayoutTarget,
				bucket,
				record,
			)
			applied, iter, err := s.session.MapExecuteBatchCAS(batch, make(map[string]any))
			if iter != nil {
				_ = iter.Close()
			}
			if err != nil {
				return cgocql.ConvertError("QueueV2RangeDeleteMessagesV3Guarded", err)
			}
			if !applied {
				if err := s.requireQueueV2MessageAuthority(
					ctx,
					queueType,
					queueName,
					queueV2MetadataLayoutTarget,
					bucket,
					record,
				); err != nil {
					return err
				}
				return ErrUpdateQueueConflict
			}
		}
		if bucket == math.MaxInt64 {
			break
		}
	}
	return nil
}

func encodeQueueV2MessagePageToken(
	queueType persistence.QueueV2Type,
	queueName string,
	span int64,
	lastMessageID int64,
) []byte {
	nameBytes := []byte(queueName)
	token := make([]byte, queueV2MessagePageTokenHeader+len(nameBytes))
	offset := copy(token, queueV2MessagePageTokenPrefix)
	token[offset] = queueV2MessagePageTokenVersion
	offset++
	token[offset] = queueV2MessageLayoutGeneration
	offset++
	binary.BigEndian.PutUint64(token[offset:], uint64(span))
	offset += 8
	binary.BigEndian.PutUint32(token[offset:], uint32(queueType))
	offset += 4
	binary.BigEndian.PutUint32(token[offset:], uint32(len(nameBytes)))
	offset += 4
	binary.BigEndian.PutUint64(token[offset:], uint64(lastMessageID))
	offset += 8
	copy(token[offset:], nameBytes)
	return token
}

func decodeQueueV2MessagePageToken(
	token []byte,
	queueType persistence.QueueV2Type,
	queueName string,
	span int64,
) (int64, error) {
	prefix := []byte(queueV2MessagePageTokenPrefix)
	if !bytes.HasPrefix(token, prefix) || len(token) < queueV2MessagePageTokenHeader {
		return 0, invalidQueueV2MessagePageToken("target-layout token envelope is invalid")
	}
	offset := len(prefix)
	if token[offset] != queueV2MessagePageTokenVersion {
		return 0, invalidQueueV2MessagePageToken("target-layout token version is invalid")
	}
	offset++
	if token[offset] != queueV2MessageLayoutGeneration {
		return 0, invalidQueueV2MessagePageToken("target-layout generation is invalid")
	}
	offset++
	if int64(binary.BigEndian.Uint64(token[offset:])) != span {
		return 0, invalidQueueV2MessagePageToken("message bucket span changed; restart pagination")
	}
	offset += 8
	if persistence.QueueV2Type(int32(binary.BigEndian.Uint32(token[offset:]))) != queueType {
		return 0, invalidQueueV2MessagePageToken("queue type does not match request")
	}
	offset += 4
	nameLength := int(binary.BigEndian.Uint32(token[offset:]))
	offset += 4
	lastMessageID := int64(binary.BigEndian.Uint64(token[offset:]))
	offset += 8
	if nameLength != len(token)-offset || string(token[offset:]) != queueName {
		return 0, invalidQueueV2MessagePageToken("queue name does not match request")
	}
	if lastMessageID < persistence.FirstQueueMessageID {
		return 0, invalidQueueV2MessagePageToken("last message ID is invalid")
	}
	return lastMessageID, nil
}

func invalidQueueV2MessagePageToken(message string) error {
	return fmt.Errorf("%w: %s", persistence.ErrInvalidReadQueueMessagesNextPageToken, message)
}
