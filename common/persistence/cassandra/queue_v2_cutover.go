package cassandra

import (
	"context"
	"errors"
	"math"

	cgocql "github.com/gocql/gocql"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	templateScanQueueV2TargetMetadataBuckets = `SELECT metadata_bucket FROM queues_v2
		WHERE queue_type = ? AND queue_name = ? ALLOW FILTERING`
	templateDeleteQueueV2TargetMetadataForCutover = `DELETE FROM queues_v2
		WHERE queue_type = ? AND metadata_bucket = ? AND queue_name = ?
		IF migration_authority = ? AND migration_generation = ? AND migration_epoch = ? AND message_bucket_span = ?`
	templateWriteQueueV2TargetMetadataForCutover = `UPDATE queues_v2 SET metadata_payload = ?,
		metadata_encoding = ?, version = ? WHERE queue_type = ? AND metadata_bucket = ? AND queue_name = ?
		IF migration_authority = ? AND migration_generation = ? AND migration_epoch = ? AND message_bucket_span = ?`
	templateScanQueueV2TargetMessageBuckets = `SELECT message_bucket FROM queue_messages_v3
		WHERE queue_type = ? AND queue_name = ? ALLOW FILTERING`
	templateDeleteQueueV2TargetMessagePartitionForCutover = `DELETE FROM queue_messages_v3
		WHERE queue_type = ? AND queue_name = ? AND message_bucket = ?`
	templateDeleteQueueV2TargetMessageRowForCutover = `DELETE FROM queue_messages_v3
		WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? AND row_type = ? AND message_id = ?`
	templateWriteQueueV2TargetDirectoryForCutover = `INSERT INTO queue_messages_v3
		(queue_type, queue_name, message_bucket, row_type, message_id, active_message_bucket, bucket_span,
		version, migration_authority, migration_generation, migration_epoch)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	templateWriteQueueV2TargetBucketStateForCutover = `INSERT INTO queue_messages_v3
		(queue_type, queue_name, message_bucket, row_type, message_id, last_message_id, version, bucket_span,
		migration_authority, migration_generation, migration_epoch)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

var errQueueV2TargetActivatedDuringRepair = errors.New("QueueV2 target activated during sealed repair")

type QueueV2CutoverOptions struct {
	QueueType         persistence.QueueV2Type
	QueueName         string
	PageSize          int
	MessageBucketSpan int64
	Generation        cgocql.UUID
}

type QueueV2CutoverResult struct {
	Validation QueueV2MessageValidationResult
	Generation cgocql.UUID
	Epoch      int64
}

// ActivateQueueV2SourceFencing initializes source and target authority while the source remains authoritative.
func ActivateQueueV2SourceFencing(
	ctx context.Context,
	session gocql.Session,
	options QueueV2CutoverOptions,
) error {
	store, err := queueV2CutoverStore(session, options)
	if err != nil {
		return err
	}
	if _, err := store.getQueueFromLayout(
		ctx,
		options.QueueName,
		options.QueueType,
		queueV2MetadataLayoutSource,
	); err != nil {
		return err
	}
	record, err := store.initializeQueueV2SourceAuthority(ctx, options.QueueType, options.QueueName)
	if err != nil {
		return err
	}
	return store.ensureQueueV2TargetMirrorInitialized(ctx, options.QueueType, options.QueueName, record)
}

// CutoverQueueV2ToTarget seals one physical queue, repairs the target exactly, and publishes target authority.
// Every phase is idempotent, so an interrupted invocation can be rerun with the same options.
//
//nolint:revive // Cutover coordinates metadata and message authority through idempotent crash-resume states.
func CutoverQueueV2ToTarget(
	ctx context.Context,
	session gocql.Session,
	options QueueV2CutoverOptions,
) (QueueV2CutoverResult, error) {
	store, err := queueV2CutoverStore(session, options)
	if err != nil {
		return QueueV2CutoverResult{}, err
	}
	if err := ActivateQueueV2SourceFencing(ctx, session, options); err != nil {
		source, readErr := readQueueV2MetadataAuthority(
			ctx,
			session,
			options.QueueType,
			options.QueueName,
			queueV2MetadataLayoutSource,
		)
		if readErr != nil || source.authority == queueV2MigrationAuthoritySource || source.isUnspecified() {
			return QueueV2CutoverResult{}, err
		}
	}

	sourceMetadata, err := readQueueV2MetadataAuthority(
		ctx,
		session,
		options.QueueType,
		options.QueueName,
		queueV2MetadataLayoutSource,
	)
	if err != nil {
		return QueueV2CutoverResult{}, err
	}
	if err := store.validateQueueV2CutoverIdentity(options, sourceMetadata); err != nil {
		return QueueV2CutoverResult{}, err
	}
	if sourceMetadata.authority == queueV2MigrationAuthorityTarget {
		return store.finishQueueV2Cutover(ctx, options, sourceMetadata)
	}
	sealing := sourceMetadata
	sealing.authority = queueV2MigrationAuthoritySealing
	sealing.epoch = max(sourceMetadata.epoch+1, int64(queueV2MigrationAuthoritySealing))
	if sourceMetadata.authority == queueV2MigrationAuthoritySource {
		if err := transitionQueueV2SourceMetadataRecord(
			ctx,
			session,
			options.QueueType,
			options.QueueName,
			sourceMetadata,
			sealing,
		); err != nil {
			return QueueV2CutoverResult{}, err
		}
	} else if sourceMetadata.authority != queueV2MigrationAuthoritySealing {
		return QueueV2CutoverResult{}, serviceerror.NewDataLossf(
			"QueueV2 queue type %d and name %q has invalid source metadata authority %s",
			options.QueueType,
			options.QueueName,
			sourceMetadata.authority,
		)
	} else {
		sealing = sourceMetadata
	}

	metadataActivated, err := store.sealQueueV2TargetMetadata(ctx, options, sealing)
	if err != nil {
		return QueueV2CutoverResult{}, err
	}
	sourceMessages, err := readQueueV2MessageAuthority(
		ctx,
		session,
		options.QueueType,
		options.QueueName,
		queueV2MetadataLayoutSource,
		0,
	)
	if err != nil {
		return QueueV2CutoverResult{}, err
	}
	if sourceMessages.authority == queueV2MigrationAuthoritySource {
		if err := transitionQueueV2SourceMessageRecord(
			ctx,
			session,
			options.QueueType,
			options.QueueName,
			sourceMessages,
			sealing,
		); err != nil {
			return QueueV2CutoverResult{}, err
		}
	} else if sourceMessages.authority != queueV2MigrationAuthoritySealing &&
		sourceMessages.authority != queueV2MigrationAuthorityTarget {
		return QueueV2CutoverResult{}, serviceerror.NewDataLossf(
			"QueueV2 queue type %d and name %q has invalid source message authority %s",
			options.QueueType,
			options.QueueName,
			sourceMessages.authority,
		)
	}

	messageActivated, err := store.sealQueueV2TargetMessages(ctx, options, sealing)
	if err != nil {
		return QueueV2CutoverResult{}, err
	}
	if metadataActivated || messageActivated {
		validation, err := ValidateQueueV2Messages(
			ctx,
			session,
			options.QueueType,
			options.QueueName,
			options.PageSize,
			options.MessageBucketSpan,
		)
		if err != nil {
			return QueueV2CutoverResult{}, err
		}
		if !validation.Matches() {
			return QueueV2CutoverResult{}, serviceerror.NewDataLossf(
				"QueueV2 queue type %d and name %q was partially activated with mismatches: %v",
				options.QueueType,
				options.QueueName,
				validation.Mismatches,
			)
		}
	} else {
		if err := store.rebuildQueueV2TargetMetadata(ctx, options, sealing); err != nil {
			return store.finishQueueV2CutoverAfterRepairConflict(ctx, options, sealing, err)
		}
		if err := store.rebuildQueueV2TargetMessages(ctx, options, sealing); err != nil {
			return store.finishQueueV2CutoverAfterRepairConflict(ctx, options, sealing, err)
		}
	}

	validation, err := ValidateQueueV2Messages(
		ctx,
		session,
		options.QueueType,
		options.QueueName,
		options.PageSize,
		options.MessageBucketSpan,
	)
	if err != nil {
		return QueueV2CutoverResult{}, err
	}
	if !validation.Matches() {
		return QueueV2CutoverResult{}, serviceerror.NewDataLossf(
			"QueueV2 queue type %d and name %q failed sealed validation: %v",
			options.QueueType,
			options.QueueName,
			validation.Mismatches,
		)
	}

	target := sealing
	target.authority = queueV2MigrationAuthorityTarget
	target.epoch = sealing.epoch + 1
	if err := store.activateQueueV2TargetMessages(ctx, options, sealing, target); err != nil {
		return QueueV2CutoverResult{}, err
	}
	if err := store.activateQueueV2TargetMetadata(ctx, options, sealing, target); err != nil {
		return QueueV2CutoverResult{}, err
	}
	if sourceMessages.authority != queueV2MigrationAuthorityTarget {
		if err := transitionQueueV2SourceMessageRecord(
			ctx,
			session,
			options.QueueType,
			options.QueueName,
			sealing,
			target,
		); err != nil {
			return QueueV2CutoverResult{}, err
		}
	}
	if err := transitionQueueV2SourceMetadataRecord(
		ctx,
		session,
		options.QueueType,
		options.QueueName,
		sealing,
		target,
	); err != nil {
		return QueueV2CutoverResult{}, err
	}
	return store.finishQueueV2Cutover(ctx, options, target)
}

func queueV2CutoverStore(
	session gocql.Session,
	options QueueV2CutoverOptions,
) (*queueV2Store, error) {
	if options.QueueName == "" {
		return nil, errors.New("QueueV2 cutover queue name must not be empty")
	}
	if options.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveReadQueueMessagesPageSize
	}
	if err := validateQueueV2MessageBucketSpan(options.MessageBucketSpan); err != nil {
		return nil, err
	}
	return &queueV2Store{
		session:          session,
		logger:           log.NewNoopLogger(),
		migrationMode:    config.CassandraQueueV2MigrationModeTargetDual,
		messageSpan:      options.MessageBucketSpan,
		layoutGeneration: options.Generation,
	}, nil
}

func (s *queueV2Store) validateQueueV2CutoverIdentity(
	options QueueV2CutoverOptions,
	record queueV2AuthorityRecord,
) error {
	if record.messageSpan != options.MessageBucketSpan ||
		options.Generation != (cgocql.UUID{}) && record.generation != options.Generation {
		return serviceerror.NewUnavailablef(
			"QueueV2 queue type %d and name %q cutover identity mismatch: generation=%s span=%d; expected generation=%s span=%d",
			options.QueueType,
			options.QueueName,
			record.generation,
			record.messageSpan,
			options.Generation,
			options.MessageBucketSpan,
		)
	}
	return nil
}

func (s *queueV2Store) sealQueueV2TargetMetadata(
	ctx context.Context,
	options QueueV2CutoverOptions,
	sealing queueV2AuthorityRecord,
) (bool, error) {
	record, err := readQueueV2MetadataAuthority(
		ctx,
		s.session,
		options.QueueType,
		options.QueueName,
		queueV2MetadataLayoutTarget,
	)
	if err != nil {
		return false, err
	}
	if record.authority == queueV2MigrationAuthorityTarget {
		if record != queueV2TargetAuthorityRecord(sealing) {
			return false, serviceerror.NewDataLossf(
				"QueueV2 target metadata has unexpected target identity at epoch %d",
				record.epoch,
			)
		}
		return true, nil
	}
	if record == sealing {
		return false, nil
	}
	source := sealing
	source.authority = queueV2MigrationAuthoritySource
	source.epoch = sealing.epoch - 1
	if record != source {
		return false, serviceerror.NewDataLossf("QueueV2 target metadata has unexpected authority %s at epoch %d", record.authority, record.epoch)
	}
	return false, transitionQueueV2TargetMetadataRecord(
		ctx,
		s.session,
		options.QueueType,
		options.QueueName,
		source,
		sealing,
	)
}

func (s *queueV2Store) sealQueueV2TargetMessages(
	ctx context.Context,
	options QueueV2CutoverOptions,
	sealing queueV2AuthorityRecord,
) (bool, error) {
	buckets, err := scanQueueV2TargetMessageBuckets(ctx, s.session, options.QueueType, options.QueueName)
	if err != nil {
		return false, err
	}
	partialActivation := false
	source := sealing
	source.authority = queueV2MigrationAuthoritySource
	source.epoch = sealing.epoch - 1
	for _, bucket := range buckets {
		record, err := readQueueV2MessageAuthority(
			ctx,
			s.session,
			options.QueueType,
			options.QueueName,
			queueV2MetadataLayoutTarget,
			bucket,
		)
		if err != nil {
			return false, err
		}
		switch {
		case record.authority == queueV2MigrationAuthorityTarget:
			if record != queueV2TargetAuthorityRecord(sealing) {
				return false, serviceerror.NewDataLossf(
					"QueueV2 target message bucket %d has unexpected target identity at epoch %d",
					bucket,
					record.epoch,
				)
			}
			partialActivation = true
		case record == sealing:
		case record == source:
			if err := transitionQueueV2TargetMessageRecord(
				ctx,
				s.session,
				options.QueueType,
				options.QueueName,
				bucket,
				source,
				sealing,
			); err != nil {
				return false, err
			}
		default:
			return false, serviceerror.NewDataLossf(
				"QueueV2 target message bucket %d has unexpected authority %s at epoch %d",
				bucket,
				record.authority,
				record.epoch,
			)
		}
	}
	return partialActivation, nil
}

func (s *queueV2Store) rebuildQueueV2TargetMetadata(
	ctx context.Context,
	options QueueV2CutoverOptions,
	sealing queueV2AuthorityRecord,
) error {
	queue, err := s.getQueueFromLayout(ctx, options.QueueName, options.QueueType, queueV2MetadataLayoutSource)
	if err != nil {
		return err
	}
	row, err := queueV2MetadataRowFromQueue(options.QueueName, queue)
	if err != nil {
		return err
	}
	expectedBucket := queueV2MetadataBucket(options.QueueType, options.QueueName)
	applied, err := s.session.Query(
		templateWriteQueueV2TargetMetadataForCutover,
		row.metadataPayload,
		row.metadataEncoding,
		row.version,
		options.QueueType,
		expectedBucket,
		options.QueueName,
		int(sealing.authority),
		sealing.generation,
		sealing.epoch,
		sealing.messageSpan,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("WriteQueueV2TargetMetadataForCutover", err)
	}
	if !applied {
		return s.queueV2TargetMetadataRepairConflict(ctx, options, expectedBucket, sealing, false)
	}
	iter := s.session.Query(
		templateScanQueueV2TargetMetadataBuckets,
		options.QueueType,
		options.QueueName,
	).WithContext(ctx).Iter()
	var buckets []int
	for {
		var bucket int
		if !iter.Scan(&bucket) {
			break
		}
		buckets = append(buckets, bucket)
	}
	if err := iter.Close(); err != nil {
		return gocql.ConvertError("ScanQueueV2TargetMetadataForCutover", err)
	}
	for _, bucket := range buckets {
		if bucket == expectedBucket {
			continue
		}
		applied, err := s.session.Query(
			templateDeleteQueueV2TargetMetadataForCutover,
			options.QueueType,
			bucket,
			options.QueueName,
			int(sealing.authority),
			sealing.generation,
			sealing.epoch,
			sealing.messageSpan,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return gocql.ConvertError("DeleteQueueV2TargetMetadataForCutover", err)
		}
		if !applied {
			return s.queueV2TargetMetadataRepairConflict(ctx, options, bucket, sealing, true)
		}
	}
	return nil
}

//nolint:revive // Exact rebuilding converges directory state plus every source and target message bucket.
func (s *queueV2Store) rebuildQueueV2TargetMessages(
	ctx context.Context,
	options QueueV2CutoverOptions,
	sealing queueV2AuthorityRecord,
) error {
	queue, err := s.getQueueFromLayout(ctx, options.QueueName, options.QueueType, queueV2MetadataLayoutSource)
	if err != nil {
		return err
	}
	minimumMessageID, err := queueV2MinimumMessageID(options.QueueType, options.QueueName, queue)
	if err != nil {
		return err
	}
	maximumMessageID, exists, err := s.getSourceMaxMessageID(ctx, options.QueueType, options.QueueName)
	if err != nil {
		return err
	}
	minimumBucket, err := queueV2MessageBucketForID(minimumMessageID, options.MessageBucketSpan)
	if err != nil {
		return err
	}
	activeBucket, err := queueV2MessageBucketForID(minimumMessageID, options.MessageBucketSpan)
	if err != nil {
		return err
	}
	if exists && maximumMessageID >= minimumMessageID {
		activeBucket, err = queueV2MessageBucketForID(maximumMessageID, options.MessageBucketSpan)
		if err != nil {
			return err
		}
	}
	if err := s.copyQueueV2SourceMessagesUnderSeal(
		ctx,
		options,
		sealing,
		minimumMessageID,
		maximumMessageID,
		exists,
	); err != nil {
		return err
	}
	for bucket := minimumBucket; bucket <= activeBucket; bucket++ {
		if err := s.ensureQueueV2TargetMessageRepairPartition(ctx, options, bucket, sealing); err != nil {
			return err
		}
		lastMessageID, err := queueV2MessageBucketLastID(bucket, options.MessageBucketSpan)
		if err != nil {
			return err
		}
		if bucket == activeBucket {
			lastMessageID = minimumMessageID - 1
			if exists && maximumMessageID >= minimumMessageID {
				lastMessageID = maximumMessageID
			}
		}
		if err := s.writeQueueV2TargetBucketStateUnderSeal(ctx, options, bucket, lastMessageID, sealing); err != nil {
			return err
		}
		if bucket == math.MaxInt64 {
			break
		}
	}
	if err := s.writeQueueV2TargetDirectoryUnderSeal(ctx, options, activeBucket, sealing); err != nil {
		return err
	}
	return s.removeQueueV2TargetExtrasUnderSeal(
		ctx,
		options,
		sealing,
		minimumMessageID,
		maximumMessageID,
		exists,
		minimumBucket,
		activeBucket,
	)
}

//nolint:revive // Sealed copy validates every page and fences each physical bucket mutation.
func (s *queueV2Store) copyQueueV2SourceMessagesUnderSeal(
	ctx context.Context,
	options QueueV2CutoverOptions,
	sealing queueV2AuthorityRecord,
	minimumMessageID int64,
	maximumMessageID int64,
	exists bool,
) error {
	if !exists || maximumMessageID < minimumMessageID {
		return nil
	}
	var (
		cursor            = minimumMessageID
		initializedBucket = queueV2MessageDirectoryBucket
	)
	for cursor <= maximumMessageID {
		rows, err := readSourceQueueV2RawMessages(
			ctx,
			s.session,
			options.QueueType,
			options.QueueName,
			cursor,
			maximumMessageID,
			options.PageSize,
		)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return serviceerror.NewDataLossf("source QueueV2 messages are missing ID %d", cursor)
		}
		for index, row := range rows {
			expected := cursor + int64(index)
			if row.id != expected {
				return serviceerror.NewDataLossf("source QueueV2 messages are missing ID %d", expected)
			}
			bucket, err := queueV2MessageBucketForID(row.id, options.MessageBucketSpan)
			if err != nil {
				return err
			}
			if bucket != initializedBucket {
				if err := s.ensureQueueV2TargetMessageRepairPartition(ctx, options, bucket, sealing); err != nil {
					return err
				}
				initializedBucket = bucket
			}
			if err := s.writeQueueV2TargetMessageUnderSeal(ctx, options, bucket, row, sealing); err != nil {
				return err
			}
		}
		lastMessageID := rows[len(rows)-1].id
		if lastMessageID == math.MaxInt64 {
			break
		}
		cursor = lastMessageID + 1
	}
	return nil
}

func (s *queueV2Store) ensureQueueV2TargetMessageRepairPartition(
	ctx context.Context,
	options QueueV2CutoverOptions,
	bucket int64,
	sealing queueV2AuthorityRecord,
) error {
	record, err := readQueueV2MessageAuthority(
		ctx,
		s.session,
		options.QueueType,
		options.QueueName,
		queueV2MetadataLayoutTarget,
		bucket,
	)
	if err != nil {
		return err
	}
	if record == sealing {
		return nil
	}
	if !record.isUnspecified() {
		return s.queueV2TargetMessageRepairConflict(ctx, options, bucket, sealing, false)
	}
	applied, err := s.session.Query(
		templateInitializeQueueV2TargetMessageAuthority,
		int(sealing.authority),
		sealing.generation,
		sealing.epoch,
		sealing.messageSpan,
		options.QueueType,
		options.QueueName,
		bucket,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("InitializeQueueV2TargetMessageAuthorityForCutover", err)
	}
	if applied {
		return nil
	}
	return s.queueV2TargetMessageRepairConflict(ctx, options, bucket, sealing, false)
}

func (s *queueV2Store) writeQueueV2TargetMessageUnderSeal(
	ctx context.Context,
	options QueueV2CutoverOptions,
	bucket int64,
	row queueV2RawMessage,
	sealing queueV2AuthorityRecord,
) error {
	return s.executeQueueV2TargetMessageRepairBatch(
		ctx,
		options,
		bucket,
		sealing,
		false,
		"WriteQueueV2TargetMessageForCutover",
		func(batch *gocql.Batch) {
			batch.Query(
				templateInsertQueueV2MessageV3,
				options.QueueType,
				options.QueueName,
				bucket,
				queueV2MessageDataRowType,
				row.id,
				row.data,
				row.encoding,
			)
		},
	)
}

func (s *queueV2Store) writeQueueV2TargetBucketStateUnderSeal(
	ctx context.Context,
	options QueueV2CutoverOptions,
	bucket int64,
	lastMessageID int64,
	sealing queueV2AuthorityRecord,
) error {
	return s.executeQueueV2TargetMessageRepairBatch(
		ctx,
		options,
		bucket,
		sealing,
		false,
		"WriteQueueV2TargetBucketStateForCutover",
		func(batch *gocql.Batch) {
			batch.Query(
				templateWriteQueueV2TargetBucketStateForCutover,
				options.QueueType,
				options.QueueName,
				bucket,
				queueV2MessageStateRowType,
				queueV2MessageStateMessageID,
				lastMessageID,
				int64(0),
				options.MessageBucketSpan,
				int(sealing.authority),
				sealing.generation,
				sealing.epoch,
			)
		},
	)
}

func (s *queueV2Store) writeQueueV2TargetDirectoryUnderSeal(
	ctx context.Context,
	options QueueV2CutoverOptions,
	activeBucket int64,
	sealing queueV2AuthorityRecord,
) error {
	if err := s.ensureQueueV2TargetMessageRepairPartition(
		ctx,
		options,
		queueV2MessageDirectoryBucket,
		sealing,
	); err != nil {
		return err
	}
	return s.executeQueueV2TargetMessageRepairBatch(
		ctx,
		options,
		queueV2MessageDirectoryBucket,
		sealing,
		false,
		"WriteQueueV2TargetDirectoryForCutover",
		func(batch *gocql.Batch) {
			batch.Query(
				templateWriteQueueV2TargetDirectoryForCutover,
				options.QueueType,
				options.QueueName,
				queueV2MessageDirectoryBucket,
				queueV2MessageStateRowType,
				queueV2MessageStateMessageID,
				activeBucket,
				options.MessageBucketSpan,
				int64(0),
				int(sealing.authority),
				sealing.generation,
				sealing.epoch,
			)
		},
	)
}

type queueV2TargetMessageRowKey struct {
	bucket        int64
	rowType       int16
	messageID     int64
	hasClustering bool
}

//nolint:revive // Cleanup checks every physical key against the sealed logical source snapshot.
func (s *queueV2Store) removeQueueV2TargetExtrasUnderSeal(
	ctx context.Context,
	options QueueV2CutoverOptions,
	sealing queueV2AuthorityRecord,
	minimumMessageID int64,
	maximumMessageID int64,
	exists bool,
	minimumBucket int64,
	activeBucket int64,
) error {
	for range 8 {
		changed, err := s.removeQueueV2TargetExtrasPassUnderSeal(
			ctx,
			options,
			sealing,
			minimumMessageID,
			maximumMessageID,
			exists,
			minimumBucket,
			activeBucket,
		)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
	}
	return serviceerror.NewUnavailable("QueueV2 target cleanup did not converge under its sealing fence")
}

//nolint:revive // One bounded pass handles structural rows and data rows with the same partition fence.
func (s *queueV2Store) removeQueueV2TargetExtrasPassUnderSeal(
	ctx context.Context,
	options QueueV2CutoverOptions,
	sealing queueV2AuthorityRecord,
	minimumMessageID int64,
	maximumMessageID int64,
	exists bool,
	minimumBucket int64,
	activeBucket int64,
) (bool, error) {
	var (
		pageState []byte
		changed   bool
	)
	for {
		keys, nextPageState, err := scanQueueV2TargetMessageRowKeyPage(
			ctx,
			s.session,
			options,
			pageState,
		)
		if err != nil {
			return false, err
		}
		deletedBuckets := make(map[int64]struct{})
		for _, key := range keys {
			expectedPartition := key.bucket == queueV2MessageDirectoryBucket ||
				key.bucket >= minimumBucket && key.bucket <= activeBucket
			if !expectedPartition {
				if _, deleted := deletedBuckets[key.bucket]; deleted {
					continue
				}
				if err := s.deleteQueueV2TargetMessagePartitionUnderSeal(
					ctx,
					options,
					key.bucket,
					sealing,
				); err != nil {
					return false, err
				}
				deletedBuckets[key.bucket] = struct{}{}
				changed = true
				continue
			}
			if !key.hasClustering {
				continue
			}
			keep := key.rowType == queueV2MessageStateRowType &&
				key.messageID == queueV2MessageStateMessageID
			if key.rowType == queueV2MessageDataRowType && exists &&
				key.messageID >= minimumMessageID && key.messageID <= maximumMessageID {
				expectedBucket, bucketErr := queueV2MessageBucketForID(key.messageID, options.MessageBucketSpan)
				if bucketErr != nil {
					return false, bucketErr
				}
				keep = key.bucket == expectedBucket
			}
			if keep {
				continue
			}
			if err := s.deleteQueueV2TargetMessageRowUnderSeal(ctx, options, key, sealing); err != nil {
				return false, err
			}
			changed = true
		}
		if len(nextPageState) == 0 {
			return changed, nil
		}
		pageState = nextPageState
	}
}

func (s *queueV2Store) deleteQueueV2TargetMessageRowUnderSeal(
	ctx context.Context,
	options QueueV2CutoverOptions,
	key queueV2TargetMessageRowKey,
	sealing queueV2AuthorityRecord,
) error {
	return s.executeQueueV2TargetMessageRepairBatch(
		ctx,
		options,
		key.bucket,
		sealing,
		false,
		"DeleteQueueV2TargetMessageRowForCutover",
		func(batch *gocql.Batch) {
			batch.Query(
				templateDeleteQueueV2TargetMessageRowForCutover,
				options.QueueType,
				options.QueueName,
				key.bucket,
				key.rowType,
				key.messageID,
			)
		},
	)
}

func (s *queueV2Store) deleteQueueV2TargetMessagePartitionUnderSeal(
	ctx context.Context,
	options QueueV2CutoverOptions,
	bucket int64,
	sealing queueV2AuthorityRecord,
) error {
	return s.executeQueueV2TargetMessageRepairBatch(
		ctx,
		options,
		bucket,
		sealing,
		true,
		"DeleteQueueV2TargetMessagePartitionForCutover",
		func(batch *gocql.Batch) {
			batch.Query(
				templateDeleteQueueV2TargetMessagePartitionForCutover,
				options.QueueType,
				options.QueueName,
				bucket,
			)
		},
	)
}

func scanQueueV2TargetMessageRowKeyPage(
	ctx context.Context,
	session gocql.Session,
	options QueueV2CutoverOptions,
	pageState []byte,
) ([]queueV2TargetMessageRowKey, []byte, error) {
	pageSize := min(options.PageSize, 1024)
	iter := session.Query(
		templateScanTargetQueueV2MessagesForValidation,
		options.QueueType,
		options.QueueName,
	).WithContext(ctx).PageSize(pageSize).PageState(pageState).Iter()
	keys := make([]queueV2TargetMessageRowKey, 0, pageSize)
	for {
		var (
			bucket    int64
			rowType   *int16
			messageID *int64
		)
		if !iter.Scan(&bucket, &rowType, &messageID) {
			break
		}
		key := queueV2TargetMessageRowKey{bucket: bucket}
		if rowType != nil && messageID != nil {
			key.rowType = *rowType
			key.messageID = *messageID
			key.hasClustering = true
		}
		keys = append(keys, key)
	}
	nextPageState := append([]byte(nil), iter.PageState()...)
	if err := iter.Close(); err != nil {
		return nil, nil, gocql.ConvertError("ScanQueueV2TargetMessageRowsForCutover", err)
	}
	return keys, nextPageState, nil
}

func (s *queueV2Store) executeQueueV2TargetMessageRepairBatch(
	ctx context.Context,
	options QueueV2CutoverOptions,
	bucket int64,
	sealing queueV2AuthorityRecord,
	missingIsSuccess bool,
	operation string,
	addMutations func(*gocql.Batch),
) error {
	batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	addMutations(batch)
	addQueueV2MessageAuthorityGuard(
		batch,
		options.QueueType,
		options.QueueName,
		queueV2MetadataLayoutTarget,
		bucket,
		sealing,
	)
	applied, iter, err := s.session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		return gocql.ConvertError(operation, err)
	}
	if applied {
		return nil
	}
	return s.queueV2TargetMessageRepairConflict(ctx, options, bucket, sealing, missingIsSuccess)
}

func (s *queueV2Store) queueV2TargetMetadataRepairConflict(
	ctx context.Context,
	options QueueV2CutoverOptions,
	bucket int,
	sealing queueV2AuthorityRecord,
	missingIsSuccess bool,
) error {
	record, err := scanQueueV2Authority(
		ctx,
		s.session.Query(templateGetQueueV2TargetMetadataAuthority, options.QueueType, bucket, options.QueueName),
		"ReadQueueV2TargetMetadataAuthorityForCutover",
	)
	if err != nil {
		return err
	}
	if record == queueV2TargetAuthorityRecord(sealing) {
		return errQueueV2TargetActivatedDuringRepair
	}
	if missingIsSuccess && record.isUnspecified() {
		return nil
	}
	return serviceerror.NewUnavailablef(
		"QueueV2 target metadata repair lost its sealing fence; current authority is %s at epoch %d",
		record.authority,
		record.epoch,
	)
}

func (s *queueV2Store) queueV2TargetMessageRepairConflict(
	ctx context.Context,
	options QueueV2CutoverOptions,
	bucket int64,
	sealing queueV2AuthorityRecord,
	missingIsSuccess bool,
) error {
	record, err := readQueueV2MessageAuthority(
		ctx,
		s.session,
		options.QueueType,
		options.QueueName,
		queueV2MetadataLayoutTarget,
		bucket,
	)
	if err != nil {
		return err
	}
	if record == queueV2TargetAuthorityRecord(sealing) {
		return errQueueV2TargetActivatedDuringRepair
	}
	if missingIsSuccess && record.isUnspecified() {
		return nil
	}
	return serviceerror.NewUnavailablef(
		"QueueV2 target message bucket %d repair lost its sealing fence; current authority is %s at epoch %d",
		bucket,
		record.authority,
		record.epoch,
	)
}

func queueV2TargetAuthorityRecord(sealing queueV2AuthorityRecord) queueV2AuthorityRecord {
	target := sealing
	target.authority = queueV2MigrationAuthorityTarget
	target.epoch++
	return target
}

func (s *queueV2Store) finishQueueV2CutoverAfterRepairConflict(
	ctx context.Context,
	options QueueV2CutoverOptions,
	sealing queueV2AuthorityRecord,
	repairErr error,
) (QueueV2CutoverResult, error) {
	if !errors.Is(repairErr, errQueueV2TargetActivatedDuringRepair) {
		return QueueV2CutoverResult{}, repairErr
	}
	target := queueV2TargetAuthorityRecord(sealing)
	source, err := readQueueV2MetadataAuthority(
		ctx,
		s.session,
		options.QueueType,
		options.QueueName,
		queueV2MetadataLayoutSource,
	)
	if err != nil {
		return QueueV2CutoverResult{}, err
	}
	if source != target {
		return QueueV2CutoverResult{}, serviceerror.NewUnavailablef(
			"QueueV2 target activated while source authority remains %s at epoch %d; retry cutover",
			source.authority,
			source.epoch,
		)
	}
	return s.finishQueueV2Cutover(ctx, options, target)
}

func (s *queueV2Store) activateQueueV2TargetMessages(
	ctx context.Context,
	options QueueV2CutoverOptions,
	sealing queueV2AuthorityRecord,
	target queueV2AuthorityRecord,
) error {
	buckets, err := scanQueueV2TargetMessageBuckets(ctx, s.session, options.QueueType, options.QueueName)
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		if bucket == queueV2MessageDirectoryBucket {
			continue
		}
		if err := transitionQueueV2TargetMessageRecord(
			ctx,
			s.session,
			options.QueueType,
			options.QueueName,
			bucket,
			sealing,
			target,
		); err != nil {
			return err
		}
	}
	return transitionQueueV2TargetMessageRecord(
		ctx,
		s.session,
		options.QueueType,
		options.QueueName,
		queueV2MessageDirectoryBucket,
		sealing,
		target,
	)
}

func (s *queueV2Store) activateQueueV2TargetMetadata(
	ctx context.Context,
	options QueueV2CutoverOptions,
	sealing queueV2AuthorityRecord,
	target queueV2AuthorityRecord,
) error {
	return transitionQueueV2TargetMetadataRecord(
		ctx,
		s.session,
		options.QueueType,
		options.QueueName,
		sealing,
		target,
	)
}

func (s *queueV2Store) finishQueueV2Cutover(
	ctx context.Context,
	options QueueV2CutoverOptions,
	target queueV2AuthorityRecord,
) (QueueV2CutoverResult, error) {
	var (
		validation   QueueV2MessageValidationResult
		reconcileErr error
	)
	for range 8 {
		validation, reconcileErr = ReconcileQueueV2SourceMessages(
			ctx,
			s.session,
			options.QueueType,
			options.QueueName,
			options.PageSize,
			options.MessageBucketSpan,
		)
		if reconcileErr == nil {
			break
		}
		if !errors.Is(reconcileErr, errQueueV2ChangedDuringValidation) {
			return QueueV2CutoverResult{}, reconcileErr
		}
		if err := ctx.Err(); err != nil {
			return QueueV2CutoverResult{}, err
		}
	}
	if reconcileErr != nil {
		return QueueV2CutoverResult{}, reconcileErr
	}
	if !validation.Matches() {
		return QueueV2CutoverResult{}, serviceerror.NewDataLossf(
			"QueueV2 queue type %d and name %q target authority has mismatches: %v",
			options.QueueType,
			options.QueueName,
			validation.Mismatches,
		)
	}
	targetMetadata, err := readQueueV2MetadataAuthority(
		ctx,
		s.session,
		options.QueueType,
		options.QueueName,
		queueV2MetadataLayoutTarget,
	)
	if err != nil {
		return QueueV2CutoverResult{}, err
	}
	targetMessages, err := readQueueV2MessageAuthority(
		ctx,
		s.session,
		options.QueueType,
		options.QueueName,
		queueV2MetadataLayoutTarget,
		queueV2MessageDirectoryBucket,
	)
	if err != nil {
		return QueueV2CutoverResult{}, err
	}
	if err := validateQueueV2AuthorityPair(options.QueueType, options.QueueName, target, targetMetadata); err != nil {
		return QueueV2CutoverResult{}, err
	}
	if err := validateQueueV2AuthorityPair(options.QueueType, options.QueueName, target, targetMessages); err != nil {
		return QueueV2CutoverResult{}, err
	}
	return QueueV2CutoverResult{
		Validation: validation,
		Generation: target.generation,
		Epoch:      target.epoch,
	}, nil
}

func scanQueueV2TargetMessageBuckets(
	ctx context.Context,
	session gocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
) ([]int64, error) {
	iter := session.Query(
		templateScanQueueV2TargetMessageBuckets,
		queueType,
		queueName,
	).WithContext(ctx).Iter()
	seen := make(map[int64]struct{})
	for {
		var bucket int64
		if !iter.Scan(&bucket) {
			break
		}
		seen[bucket] = struct{}{}
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ScanQueueV2TargetMessageBuckets", err)
	}
	buckets := make([]int64, 0, len(seen))
	for bucket := range seen {
		buckets = append(buckets, bucket)
	}
	return buckets, nil
}

func transitionQueueV2TargetMetadataRecord(
	ctx context.Context,
	session gocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
	from queueV2AuthorityRecord,
	to queueV2AuthorityRecord,
) error {
	applied, err := transitionQueueV2Authority(
		ctx,
		session,
		templateTransitionQueueV2TargetMetadataAuthority,
		[]any{
			int(to.authority),
			to.epoch,
			queueType,
			queueV2MetadataBucket(queueType, queueName),
			queueName,
			int(from.authority),
			from.generation,
			from.epoch,
			from.messageSpan,
		},
		"TransitionQueueV2TargetMetadataAuthority",
	)
	if err != nil || applied {
		return err
	}
	record, err := readQueueV2MetadataAuthority(ctx, session, queueType, queueName, queueV2MetadataLayoutTarget)
	if err != nil {
		return err
	}
	if record == to {
		return nil
	}
	return serviceerror.NewUnavailablef("QueueV2 target metadata authority transition conflicted; current authority is %s at epoch %d", record.authority, record.epoch)
}

func transitionQueueV2TargetMessageRecord(
	ctx context.Context,
	session gocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
	bucket int64,
	from queueV2AuthorityRecord,
	to queueV2AuthorityRecord,
) error {
	applied, err := transitionQueueV2Authority(
		ctx,
		session,
		templateTransitionQueueV2TargetMessageAuthority,
		[]any{
			int(to.authority),
			to.epoch,
			queueType,
			queueName,
			bucket,
			int(from.authority),
			from.generation,
			from.epoch,
			from.messageSpan,
		},
		"TransitionQueueV2TargetMessageAuthority",
	)
	if err != nil || applied {
		return err
	}
	record, err := readQueueV2MessageAuthority(ctx, session, queueType, queueName, queueV2MetadataLayoutTarget, bucket)
	if err != nil {
		return err
	}
	if record == to {
		return nil
	}
	return serviceerror.NewUnavailablef("QueueV2 target message bucket %d authority transition conflicted; current authority is %s at epoch %d", bucket, record.authority, record.epoch)
}
