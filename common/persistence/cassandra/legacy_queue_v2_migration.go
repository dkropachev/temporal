package cassandra

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	templateScanSourceLegacyQueueForV2 = `SELECT message_id, message_payload, message_encoding
		FROM queue WHERE queue_type = ? AND message_id >= ? ORDER BY message_id ASC`
	templateUpsertSourceLegacyQueueMessage = `INSERT INTO queue
		(queue_type, message_id, message_payload, message_encoding) VALUES (?, ?, ?, ?)`
	templateScanLegacyQueueV2BucketForValidation = `SELECT message_id, message_payload, message_encoding
		FROM legacy_queue_v2_messages WHERE queue_type = ? AND bucket_id = ? AND row_type = ? ORDER BY message_id ASC`
)

type LegacyQueueV2MigrationOptions struct {
	PageSize                  int
	MessageBucketSize         int64
	MaxMismatches             int
	ConfirmQueueTrafficFenced bool
}

type LegacyQueueV2ValidationResult struct {
	SourceRows int64
	TargetRows int64
	Mismatches []string
}

type LegacyQueueV2CutoverResult struct {
	Validation    LegacyQueueV2ValidationResult
	AlreadyTarget bool
}

func (r LegacyQueueV2ValidationResult) Matches() bool {
	return len(r.Mismatches) == 0
}

// BackfillLegacyQueueV2 copies one physical queue type. Call it for both the queue type and its negative DLQ type.
func BackfillLegacyQueueV2(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	options LegacyQueueV2MigrationOptions,
) (int64, error) {
	if err := validateLegacyQueueV2MigrationOptions(options); err != nil {
		return 0, err
	}
	record, err := requireLegacyQueueMigrationAuthorityRecord(
		ctx,
		session,
		queueType,
		options.MessageBucketSize,
		legacyQueueMigrationAuthoritySource,
	)
	if err != nil {
		return 0, err
	}
	store := newLegacyQueueV2MigrationStore(session, options.MessageBucketSize)
	rows, lastMessageID, bucketTails, err := copySourceLegacyQueueToV2(
		ctx,
		store,
		queueType,
		options.PageSize,
		record,
	)
	if err != nil {
		return rows, err
	}
	if err := initializeLegacyQueueV2MigrationState(
		ctx,
		store,
		queueType,
		lastMessageID,
		bucketTails,
		record,
	); err != nil {
		return rows, err
	}
	return rows, nil
}

// ReconcileLegacyQueueV2 repairs target rows while the source table remains authoritative.
func ReconcileLegacyQueueV2(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	options LegacyQueueV2MigrationOptions,
) (LegacyQueueV2ValidationResult, error) {
	if err := validateLegacyQueueV2MigrationOptions(options); err != nil {
		return LegacyQueueV2ValidationResult{}, err
	}
	record, err := requireLegacyQueueMigrationAuthorityRecord(
		ctx,
		session,
		queueType,
		options.MessageBucketSize,
		legacyQueueMigrationAuthoritySource,
	)
	if err != nil {
		return LegacyQueueV2ValidationResult{}, err
	}
	return reconcileLegacyQueueV2(ctx, session, queueType, options, record)
}

func reconcileLegacyQueueV2(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	options LegacyQueueV2MigrationOptions,
	record legacyQueueAuthorityRecord,
) (LegacyQueueV2ValidationResult, error) {
	store := newLegacyQueueV2MigrationStore(session, options.MessageBucketSize)
	if err := resetLegacyQueueV2ForSourceReconcile(ctx, store, queueType, record); err != nil {
		return LegacyQueueV2ValidationResult{}, err
	}
	rows, lastMessageID, bucketTails, err := copySourceLegacyQueueToV2(
		ctx,
		store,
		queueType,
		options.PageSize,
		record,
	)
	if err != nil {
		return LegacyQueueV2ValidationResult{SourceRows: rows}, err
	}
	if err := initializeLegacyQueueV2MigrationState(
		ctx,
		store,
		queueType,
		lastMessageID,
		bucketTails,
		record,
	); err != nil {
		return LegacyQueueV2ValidationResult{SourceRows: rows}, err
	}
	buckets, err := scanLegacyQueueTargetBuckets(ctx, store.session, queueType)
	if err != nil {
		return LegacyQueueV2ValidationResult{SourceRows: rows}, err
	}
	for _, bucket := range buckets {
		targetIter := session.Query(
			templateScanLegacyQueueV2BucketForValidation,
			queueType,
			bucket,
			legacyQueueV2MessageRowType,
		).WithContext(ctx).PageSize(options.PageSize).Iter()
		var (
			messageID int64
			data      []byte
			encoding  string
		)
		for targetIter.Scan(&messageID, &data, &encoding) {
			var sourceData []byte
			var sourceEncoding string
			err := session.Query(templateGetSourceQueueMessage, queueType, messageID).
				WithContext(ctx).
				Scan(&sourceData, &sourceEncoding)
			if cgocql.IsNotFoundError(err) {
				if err := store.deleteMessageLegacyQueueV2WithAuthority(ctx, queueType, messageID, record); err != nil {
					_ = targetIter.Close()
					return LegacyQueueV2ValidationResult{SourceRows: rows}, err
				}
			} else if err != nil {
				_ = targetIter.Close()
				return LegacyQueueV2ValidationResult{SourceRows: rows}, cgocql.ConvertError("GetSourceLegacyQueueMessageForReconcile", err)
			}
			data = nil
		}
		if err := targetIter.Close(); err != nil {
			return LegacyQueueV2ValidationResult{SourceRows: rows}, cgocql.ConvertError("ScanLegacyQueueV2BucketForReconcile", err)
		}
	}
	result, err := ValidateLegacyQueueV2(ctx, session, queueType, options)
	if err != nil {
		return result, err
	}
	state, err := store.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return result, cgocql.ConvertError("ReadLegacyQueueV2TargetAuthorityAfterReconcile", err)
	}
	if err := validateLegacyQueueAuthorityRecord(queueType, "target state", record, state.authorityRecord()); err != nil {
		return result, err
	}
	return result, nil
}

// CutoverLegacyQueueV2 seals one physical source queue, reconciles it exactly, activates the target, and publishes it.
//
//nolint:revive // Cutover is an idempotent authority state machine with explicit crash-resume transitions.
func CutoverLegacyQueueV2(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	options LegacyQueueV2MigrationOptions,
) (LegacyQueueV2CutoverResult, error) {
	if err := validateLegacyQueueV2MigrationOptions(options); err != nil {
		return LegacyQueueV2CutoverResult{}, err
	}
	var sealing legacyQueueAuthorityRecord
	for {
		record, err := readLegacyQueueSourceAuthority(ctx, session, queueType)
		if err != nil {
			return LegacyQueueV2CutoverResult{}, cgocql.ConvertError("ReadLegacyQueueSourceAuthorityForCutover", err)
		}
		if err := validateLegacyQueueAuthority(
			queueType,
			options.MessageBucketSize,
			record.authority,
			record,
		); err != nil {
			return LegacyQueueV2CutoverResult{}, err
		}
		switch record.authority {
		case legacyQueueMigrationAuthoritySource:
			applied, err := transitionLegacyQueueSourceAuthority(
				ctx,
				session,
				queueType,
				record,
				legacyQueueMigrationAuthoritySealing,
			)
			if err != nil {
				return LegacyQueueV2CutoverResult{}, err
			}
			if !applied {
				continue
			}
			sealing = record
			sealing.authority = legacyQueueMigrationAuthoritySealing
		case legacyQueueMigrationAuthoritySealing:
			sealing = record
		case legacyQueueMigrationAuthorityTarget:
			store := newLegacyQueueV2MigrationStore(session, options.MessageBucketSize)
			state, err := store.getLegacyQueueV2State(ctx, queueType)
			if err != nil {
				return LegacyQueueV2CutoverResult{}, cgocql.ConvertError("ReadLegacyQueueV2TargetForCompletedCutover", err)
			}
			if err := store.validateLegacyQueueV2State(queueType, state); err != nil {
				return LegacyQueueV2CutoverResult{}, err
			}
			if err := validateLegacyQueueAuthorityRecord(queueType, "target state", record, state.authorityRecord()); err != nil {
				return LegacyQueueV2CutoverResult{}, err
			}
			if err := store.validateLegacyQueueV2TargetPartitions(ctx, queueType, state); err != nil {
				return LegacyQueueV2CutoverResult{}, err
			}
			return LegacyQueueV2CutoverResult{AlreadyTarget: true}, nil
		default:
			return LegacyQueueV2CutoverResult{}, fmt.Errorf(
				"legacy queue type %d has invalid migration authority %s",
				queueType,
				record.authority,
			)
		}
		break
	}

	store := newLegacyQueueV2MigrationStore(session, options.MessageBucketSize)
	partialActivation, err := sealLegacyQueueV2Target(ctx, store, queueType, sealing)
	if err != nil {
		return LegacyQueueV2CutoverResult{}, err
	}
	var validation LegacyQueueV2ValidationResult
	if partialActivation {
		validation, err = ValidateLegacyQueueV2(ctx, session, queueType, options)
		if err != nil {
			return LegacyQueueV2CutoverResult{Validation: validation}, err
		}
	} else {
		validation, err = reconcileLegacyQueueV2(ctx, session, queueType, options, sealing)
		if err != nil {
			return LegacyQueueV2CutoverResult{Validation: validation}, err
		}
	}
	if !validation.Matches() {
		return LegacyQueueV2CutoverResult{Validation: validation}, fmt.Errorf(
			"legacy queue type %d cannot activate target with mismatches: %v",
			queueType,
			validation.Mismatches,
		)
	}
	target := sealing
	target.authority = legacyQueueMigrationAuthorityTarget
	if err := activateLegacyQueueV2Target(ctx, store, queueType, sealing); err != nil {
		return LegacyQueueV2CutoverResult{Validation: validation}, err
	}

	applied, err := transitionLegacyQueueSourceAuthority(
		ctx,
		session,
		queueType,
		sealing,
		legacyQueueMigrationAuthorityTarget,
	)
	if err != nil {
		return LegacyQueueV2CutoverResult{Validation: validation}, err
	}
	if !applied {
		record, readErr := readLegacyQueueSourceAuthority(ctx, session, queueType)
		if readErr != nil {
			return LegacyQueueV2CutoverResult{Validation: validation}, readErr
		}
		if err := validateLegacyQueueAuthorityRecord(queueType, "source", target, record); err != nil {
			return LegacyQueueV2CutoverResult{Validation: validation}, err
		}
	}
	return LegacyQueueV2CutoverResult{Validation: validation}, nil
}

func sealLegacyQueueV2Target(
	ctx context.Context,
	store *QueueStore,
	queueType persistence.QueueType,
	sealing legacyQueueAuthorityRecord,
) (bool, error) {
	state, err := store.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return false, cgocql.ConvertError("ReadLegacyQueueV2TargetForSealing", err)
	}
	if err := store.validateLegacyQueueV2State(queueType, state); err != nil {
		return false, err
	}
	source := sealing
	source.authority = legacyQueueMigrationAuthoritySource
	target := sealing
	target.authority = legacyQueueMigrationAuthorityTarget
	partialActivation := false
	switch state.authorityRecord() {
	case source:
		applied, err := transitionLegacyQueueTargetAuthority(
			ctx,
			store.session,
			queueType,
			source,
			legacyQueueMigrationAuthoritySealing,
		)
		if err != nil {
			return false, err
		}
		if !applied {
			return false, &persistence.ConditionFailedError{Msg: fmt.Sprintf(
				"legacy queue type %d target state changed while sealing",
				queueType,
			)}
		}
		state.authority = legacyQueueMigrationAuthoritySealing
	case sealing:
	case target:
		partialActivation = true
	default:
		return false, validateLegacyQueueAuthorityRecord(queueType, "target state", sealing, state.authorityRecord())
	}

	buckets, err := scanLegacyQueueTargetBuckets(ctx, store.session, queueType)
	if err != nil {
		return false, err
	}
	for _, bucket := range buckets {
		partial, err := sealLegacyQueueV2TargetBucket(
			ctx,
			store,
			queueType,
			bucket,
			source,
			sealing,
			target,
		)
		if err != nil {
			return false, err
		}
		partialActivation = partialActivation || partial
	}

	partial, err := sealLegacyQueueV2TargetDeletePartition(
		ctx,
		store,
		queueType,
		source,
		sealing,
		target,
	)
	if err != nil {
		return false, err
	}
	return partialActivation || partial, nil
}

func sealLegacyQueueV2TargetBucket(
	ctx context.Context,
	store *QueueStore,
	queueType persistence.QueueType,
	bucket int64,
	source legacyQueueAuthorityRecord,
	sealing legacyQueueAuthorityRecord,
	target legacyQueueAuthorityRecord,
) (bool, error) {
	record, err := readLegacyQueueTargetBucketAuthority(ctx, store.session, queueType, bucket)
	if err != nil {
		return false, err
	}
	switch record {
	case legacyQueueAuthorityRecord{}:
		return false, initializeLegacyQueueTargetBucketAuthority(
			ctx,
			store.session,
			queueType,
			bucket,
			sealing,
		)
	case source:
		return false, transitionLegacyQueueTargetBucketAuthority(
			ctx,
			store.session,
			queueType,
			bucket,
			source,
			legacyQueueMigrationAuthoritySealing,
		)
	case sealing:
		return false, nil
	case target:
		return true, nil
	default:
		return false, validateLegacyQueueAuthorityRecord(
			queueType,
			fmt.Sprintf("target bucket %d", bucket),
			sealing,
			record,
		)
	}
}

func sealLegacyQueueV2TargetDeletePartition(
	ctx context.Context,
	store *QueueStore,
	queueType persistence.QueueType,
	source legacyQueueAuthorityRecord,
	sealing legacyQueueAuthorityRecord,
	target legacyQueueAuthorityRecord,
) (bool, error) {
	record, err := readLegacyQueueTargetDeleteAuthority(ctx, store.session, queueType)
	if err != nil {
		return false, err
	}
	switch record {
	case legacyQueueAuthorityRecord{}:
		return false, initializeLegacyQueueTargetDeleteAuthority(ctx, store.session, queueType, sealing)
	case source:
		return false, transitionLegacyQueueTargetDeleteAuthority(
			ctx,
			store.session,
			queueType,
			source,
			legacyQueueMigrationAuthoritySealing,
		)
	case sealing:
		return false, nil
	case target:
		return true, nil
	default:
		return false, validateLegacyQueueAuthorityRecord(
			queueType,
			"target delete-range partition",
			sealing,
			record,
		)
	}
}

func activateLegacyQueueV2Target(
	ctx context.Context,
	store *QueueStore,
	queueType persistence.QueueType,
	sealing legacyQueueAuthorityRecord,
) error {
	state, err := store.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return cgocql.ConvertError("ReadLegacyQueueV2TargetForActivation", err)
	}
	target := sealing
	target.authority = legacyQueueMigrationAuthorityTarget
	source := sealing
	source.authority = legacyQueueMigrationAuthoritySource
	buckets, err := scanLegacyQueueTargetBuckets(ctx, store.session, queueType)
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		record, err := readLegacyQueueTargetBucketAuthority(ctx, store.session, queueType, bucket)
		if err != nil {
			return err
		}
		switch record {
		case source:
			if err := transitionLegacyQueueTargetBucketAuthority(
				ctx,
				store.session,
				queueType,
				bucket,
				source,
				legacyQueueMigrationAuthoritySealing,
			); err != nil {
				return err
			}
			fallthrough
		case sealing:
			if err := transitionLegacyQueueTargetBucketAuthority(
				ctx,
				store.session,
				queueType,
				bucket,
				sealing,
				legacyQueueMigrationAuthorityTarget,
			); err != nil {
				return err
			}
		case target:
		default:
			return validateLegacyQueueAuthorityRecord(
				queueType,
				fmt.Sprintf("target bucket %d", bucket),
				target,
				record,
			)
		}
	}
	deleteRecord, err := readLegacyQueueTargetDeleteAuthority(ctx, store.session, queueType)
	if err != nil {
		return err
	}
	switch deleteRecord {
	case sealing:
		if err := transitionLegacyQueueTargetDeleteAuthority(
			ctx,
			store.session,
			queueType,
			sealing,
			legacyQueueMigrationAuthorityTarget,
		); err != nil {
			return err
		}
	case target:
	default:
		return validateLegacyQueueAuthorityRecord(
			queueType,
			"target delete-range partition",
			target,
			deleteRecord,
		)
	}

	state, err = store.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return cgocql.ConvertError("ReadLegacyQueueV2TargetStateBeforeActivation", err)
	}
	switch state.authorityRecord() {
	case sealing:
		applied, err := transitionLegacyQueueTargetAuthority(
			ctx,
			store.session,
			queueType,
			sealing,
			legacyQueueMigrationAuthorityTarget,
		)
		if err != nil {
			return err
		}
		if !applied {
			return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
				"legacy queue type %d target state changed during activation",
				queueType,
			)}
		}
	case target:
	default:
		return validateLegacyQueueAuthorityRecord(queueType, "target state", target, state.authorityRecord())
	}
	state.authority = legacyQueueMigrationAuthorityTarget
	return store.validateLegacyQueueV2TargetPartitions(ctx, queueType, state)
}

// ReconcileSourceLegacyQueueFromV2 repairs the source copy while externally fenced queue traffic is quiescent.
//
//nolint:revive // Reverse reconciliation compares and repairs metadata and every message bucket.
func ReconcileSourceLegacyQueueFromV2(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	options LegacyQueueV2MigrationOptions,
) (LegacyQueueV2ValidationResult, error) {
	if err := validateLegacyQueueV2MigrationOptions(options); err != nil {
		return LegacyQueueV2ValidationResult{}, err
	}
	if !options.ConfirmQueueTrafficFenced {
		return LegacyQueueV2ValidationResult{}, errors.New(
			"legacy queue reverse reconciliation requires externally fenced, quiescent per-queue traffic",
		)
	}
	sourceRecord, err := requireLegacyQueueMigrationAuthorityRecord(
		ctx,
		session,
		queueType,
		options.MessageBucketSize,
		legacyQueueMigrationAuthorityTarget,
	)
	if err != nil {
		return LegacyQueueV2ValidationResult{}, err
	}
	store := newLegacyQueueV2MigrationStore(session, options.MessageBucketSize)
	if err := CleanupLegacyQueueV2(ctx, session, queueType, options.MessageBucketSize); err != nil {
		return LegacyQueueV2ValidationResult{}, err
	}
	state, err := store.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return LegacyQueueV2ValidationResult{}, cgocql.ConvertError("GetLegacyQueueV2StateForSourceReconcile", err)
	}
	if err := validateLegacyQueueAuthorityRecord(
		queueType,
		"target state before reverse reconcile",
		sourceRecord,
		state.authorityRecord(),
	); err != nil {
		return LegacyQueueV2ValidationResult{}, err
	}
	if err := store.validateLegacyQueueV2TargetPartitions(ctx, queueType, state); err != nil {
		return LegacyQueueV2ValidationResult{}, err
	}
	for bucket := int64(0); bucket <= state.activeBucket; bucket++ {
		targetIter := session.Query(
			templateScanLegacyQueueV2BucketForValidation,
			queueType,
			bucket,
			legacyQueueV2MessageRowType,
		).WithContext(ctx).PageSize(options.PageSize).Iter()
		var (
			messageID int64
			data      []byte
			encoding  string
		)
		for targetIter.Scan(&messageID, &data, &encoding) {
			if messageID >= state.minimumMessageID {
				if _, err := store.executeGuardedLegacyQueueSourceMutationWithAuthority(
					ctx,
					queueType,
					legacyQueueMigrationAuthorityTarget,
					"UpsertSourceLegacyQueueMessage",
					templateUpsertSourceLegacyQueueMessage,
					queueType,
					messageID,
					bytes.Clone(data),
					encoding,
				); err != nil {
					_ = targetIter.Close()
					return LegacyQueueV2ValidationResult{}, err
				}
			}
			data = nil
		}
		if err := targetIter.Close(); err != nil {
			return LegacyQueueV2ValidationResult{}, cgocql.ConvertError("ScanLegacyQueueV2ForSourceReconcile", err)
		}
	}

	sourceIter := session.Query(
		templateScanSourceLegacyQueueForV2,
		queueType,
		int64(persistence.FirstQueueMessageID),
	).
		WithContext(ctx).
		PageSize(options.PageSize).
		Iter()
	var (
		messageID int64
		data      []byte
		encoding  string
	)
	for sourceIter.Scan(&messageID, &data, &encoding) {
		if messageID < state.minimumMessageID {
			if err := store.deleteMessageSourceWithAuthority(
				ctx,
				queueType,
				messageID,
				legacyQueueMigrationAuthorityTarget,
			); err != nil {
				_ = sourceIter.Close()
				return LegacyQueueV2ValidationResult{}, err
			}
			continue
		}
		bucket, err := legacyQueueV2MessageBucket(messageID, store.messageBucketSize)
		if err != nil {
			_ = sourceIter.Close()
			return LegacyQueueV2ValidationResult{}, err
		}
		var targetData []byte
		var targetEncoding string
		err = session.Query(
			templateGetLegacyQueueV2Message,
			queueType,
			bucket,
			legacyQueueV2MessageRowType,
			messageID,
		).WithContext(ctx).Scan(&targetData, &targetEncoding)
		if cgocql.IsNotFoundError(err) {
			if err := store.deleteMessageSourceWithAuthority(
				ctx,
				queueType,
				messageID,
				legacyQueueMigrationAuthorityTarget,
			); err != nil {
				_ = sourceIter.Close()
				return LegacyQueueV2ValidationResult{}, err
			}
		} else if err != nil {
			_ = sourceIter.Close()
			return LegacyQueueV2ValidationResult{}, cgocql.ConvertError("GetLegacyQueueV2MessageForSourceReconcile", err)
		}
		data = nil
	}
	if err := sourceIter.Close(); err != nil {
		return LegacyQueueV2ValidationResult{}, cgocql.ConvertError("ScanSourceLegacyQueueForReconcile", err)
	}
	result, err := ValidateLegacyQueueV2(ctx, session, queueType, options)
	if err != nil {
		return result, err
	}
	currentSource, err := readLegacyQueueSourceAuthority(ctx, session, queueType)
	if err != nil {
		return result, cgocql.ConvertError("ReadLegacyQueueSourceAuthorityAfterReverseReconcile", err)
	}
	if err := validateLegacyQueueAuthorityRecord(
		queueType,
		"source after reverse reconcile",
		sourceRecord,
		currentSource,
	); err != nil {
		return result, err
	}
	currentTarget, err := store.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return result, cgocql.ConvertError("ReadLegacyQueueV2StateAfterReverseReconcile", err)
	}
	if err := validateLegacyQueueAuthorityRecord(
		queueType,
		"target state after reverse reconcile",
		sourceRecord,
		currentTarget.authorityRecord(),
	); err != nil {
		return result, err
	}
	if err := store.validateLegacyQueueV2TargetPartitions(ctx, queueType, currentTarget); err != nil {
		return result, err
	}
	return result, nil
}

// ValidateLegacyQueueV2 compares all source and target messages and verifies target cleanup state.
func ValidateLegacyQueueV2(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	options LegacyQueueV2MigrationOptions,
) (LegacyQueueV2ValidationResult, error) {
	if err := validateLegacyQueueV2MigrationOptions(options); err != nil {
		return LegacyQueueV2ValidationResult{}, err
	}
	store := newLegacyQueueV2MigrationStore(session, options.MessageBucketSize)
	state, err := store.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return LegacyQueueV2ValidationResult{}, cgocql.ConvertError("GetLegacyQueueV2StateForValidation", err)
	}
	result := LegacyQueueV2ValidationResult{}
	if err := store.validateLegacyQueueV2State(queueType, state); err != nil {
		result.addMismatch(options.MaxMismatches, err.Error())
	}
	deleteRanges, err := store.getLegacyQueueV2DeleteRanges(ctx, queueType)
	if err != nil {
		return result, err
	}
	if len(deleteRanges) != 0 {
		result.addMismatch(options.MaxMismatches, fmt.Sprintf("target has %d unfinished delete ranges", len(deleteRanges)))
	}
	if state.cleanupMessageID < state.minimumMessageID {
		result.addMismatch(
			options.MaxMismatches,
			fmt.Sprintf("target prefix cleanup stopped at %d below logical minimum %d", state.cleanupMessageID, state.minimumMessageID),
		)
	}
	activeBucketState, err := store.getLegacyQueueV2BucketState(ctx, queueType, state.activeBucket)
	if err != nil {
		return result, cgocql.ConvertError("GetLegacyQueueV2ActiveBucketForValidation", err)
	}

	sourceIter := session.Query(
		templateScanSourceLegacyQueueForV2,
		queueType,
		int64(persistence.FirstQueueMessageID),
	).
		WithContext(ctx).
		PageSize(options.PageSize).
		Iter()
	var (
		sourceMessageID int64
		sourceData      []byte
		sourceEncoding  string
		sourceLastID    = int64(persistence.EmptyQueueMessageID)
		sourceFirstID   = int64(-1)
	)
	for sourceIter.Scan(&sourceMessageID, &sourceData, &sourceEncoding) {
		if sourceFirstID == -1 {
			sourceFirstID = sourceMessageID
		}
		result.SourceRows++
		sourceLastID = sourceMessageID
		if err := store.compareLegacyQueueV2TargetMessage(
			ctx,
			queueType,
			sourceMessageID,
			sourceData,
			sourceEncoding,
		); err != nil {
			result.addMismatch(options.MaxMismatches, err.Error())
		}
		sourceData = nil
	}
	if err := sourceIter.Close(); err != nil {
		return result, cgocql.ConvertError("ScanSourceLegacyQueueForV2Validation", err)
	}
	if activeBucketState.lastMessageID < sourceLastID {
		result.addMismatch(
			options.MaxMismatches,
			fmt.Sprintf("target high watermark %d is below source high watermark %d", activeBucketState.lastMessageID, sourceLastID),
		)
	}
	if sourceFirstID >= persistence.FirstQueueMessageID && state.minimumMessageID > sourceFirstID {
		result.addMismatch(
			options.MaxMismatches,
			fmt.Sprintf("target logical minimum %d hides source message %d", state.minimumMessageID, sourceFirstID),
		)
	}

	for bucket := int64(0); bucket <= state.activeBucket; bucket++ {
		targetIter := session.Query(
			templateScanLegacyQueueV2BucketForValidation,
			queueType,
			bucket,
			legacyQueueV2MessageRowType,
		).WithContext(ctx).PageSize(options.PageSize).Iter()
		var (
			targetMessageID int64
			targetData      []byte
			targetEncoding  string
		)
		for targetIter.Scan(&targetMessageID, &targetData, &targetEncoding) {
			result.TargetRows++
			if err := store.compareLegacyQueueV2SourceMessage(
				ctx,
				queueType,
				targetMessageID,
				targetData,
				targetEncoding,
			); err != nil {
				result.addMismatch(options.MaxMismatches, err.Error())
			}
			targetData = nil
		}
		if err := targetIter.Close(); err != nil {
			return result, cgocql.ConvertError("ScanLegacyQueueV2BucketForValidation", err)
		}
	}
	if result.SourceRows != result.TargetRows {
		result.addMismatch(
			options.MaxMismatches,
			fmt.Sprintf("source row count %d differs from target row count %d", result.SourceRows, result.TargetRows),
		)
	}
	return result, nil
}

func requireLegacyQueueMigrationAuthority(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	messageBucketSize int64,
	allowed ...legacyQueueMigrationAuthority,
) error {
	_, err := requireLegacyQueueMigrationAuthorityRecord(
		ctx,
		session,
		queueType,
		messageBucketSize,
		allowed...,
	)
	return err
}

func requireLegacyQueueMigrationAuthorityRecord(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueType,
	messageBucketSize int64,
	allowed ...legacyQueueMigrationAuthority,
) (legacyQueueAuthorityRecord, error) {
	record, err := readLegacyQueueSourceAuthority(ctx, session, queueType)
	if err != nil {
		if cgocql.IsNotFoundError(err) {
			return legacyQueueAuthorityRecord{}, &persistence.ConditionFailedError{Msg: fmt.Sprintf(
				"legacy queue type %d source authority is not initialized",
				queueType,
			)}
		}
		return legacyQueueAuthorityRecord{}, cgocql.ConvertError("ReadLegacyQueueMigrationAuthority", err)
	}
	if record.messageBucketSize != messageBucketSize {
		return legacyQueueAuthorityRecord{}, validateLegacyQueueAuthority(
			queueType,
			messageBucketSize,
			record.authority,
			record,
		)
	}
	for _, authority := range allowed {
		if record.authority == authority {
			if err := validateLegacyQueueAuthority(queueType, messageBucketSize, authority, record); err != nil {
				return legacyQueueAuthorityRecord{}, err
			}
			return record, nil
		}
	}
	return legacyQueueAuthorityRecord{}, &persistence.ConditionFailedError{Msg: fmt.Sprintf(
		"legacy queue type %d has %s authority; expected one of %v",
		queueType,
		record.authority,
		allowed,
	)}
}

func resetLegacyQueueV2ForSourceReconcile(
	ctx context.Context,
	store *QueueStore,
	queueType persistence.QueueType,
	record legacyQueueAuthorityRecord,
) error {
	deleteRanges, err := store.getLegacyQueueV2DeleteRanges(ctx, queueType)
	if err != nil {
		return err
	}
	for _, deleteRange := range deleteRanges {
		if _, err := store.executeGuardedLegacyQueueTargetDeleteMutation(
			ctx,
			queueType,
			record,
			"ClearLegacyQueueV2DeleteRangeForReconcile",
			templateDeleteLegacyQueueV2DeleteRange,
			queueType,
			deleteRange.id,
		); err != nil {
			return err
		}
	}
	for range 8 {
		state, err := store.getLegacyQueueV2State(ctx, queueType)
		if err != nil {
			return cgocql.ConvertError("GetLegacyQueueV2StateForReconcile", err)
		}
		if err := store.validateLegacyQueueV2State(queueType, state); err != nil {
			return err
		}
		if err := validateLegacyQueueAuthorityRecord(queueType, "target state", record, state.authorityRecord()); err != nil {
			return err
		}
		if state.minimumMessageID == persistence.FirstQueueMessageID &&
			state.cleanupMessageID == persistence.FirstQueueMessageID {
			return nil
		}
		applied, err := store.session.Query(
			templateResetLegacyQueueV2LogicalState,
			int64(persistence.FirstQueueMessageID),
			int64(persistence.FirstQueueMessageID),
			state.version+1,
			queueType,
			state.version,
			int(record.authority),
			record.generation,
			record.messageBucketSize,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return cgocql.ConvertError("ResetLegacyQueueV2LogicalStateForReconcile", err)
		}
		if applied {
			return nil
		}
	}
	return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
		"legacy queue type %d target state changed repeatedly during reconcile",
		queueType,
	)}
}

func copySourceLegacyQueueToV2(
	ctx context.Context,
	store *QueueStore,
	queueType persistence.QueueType,
	pageSize int,
	record legacyQueueAuthorityRecord,
) (copied int64, lastMessageID int64, bucketTails map[int64]int64, err error) {
	iter := store.session.Query(
		templateScanSourceLegacyQueueForV2,
		queueType,
		int64(persistence.FirstQueueMessageID),
	).
		WithContext(ctx).
		PageSize(pageSize).
		Iter()
	lastMessageID = int64(persistence.EmptyQueueMessageID)
	var (
		messageID int64
		data      []byte
		encoding  string
	)
	bucketTails = make(map[int64]int64)
	for iter.Scan(&messageID, &data, &encoding) {
		if err := store.upsertLegacyQueueV2RawMessage(
			ctx,
			queueType,
			messageID,
			bytes.Clone(data),
			encoding,
			record,
		); err != nil {
			_ = iter.Close()
			return copied, lastMessageID, bucketTails, fmt.Errorf("backfill legacy queue message %d for queue type %d: %w", messageID, queueType, err)
		}
		bucket, err := legacyQueueV2MessageBucket(messageID, store.messageBucketSize)
		if err != nil {
			_ = iter.Close()
			return copied, lastMessageID, bucketTails, err
		}
		bucketTails[bucket] = messageID
		copied++
		lastMessageID = messageID
		data = nil
	}
	if err := iter.Close(); err != nil {
		return copied, lastMessageID, bucketTails, cgocql.ConvertError("ScanSourceLegacyQueueForV2Backfill", err)
	}
	return copied, lastMessageID, bucketTails, nil
}

func initializeLegacyQueueV2MigrationState(
	ctx context.Context,
	store *QueueStore,
	queueType persistence.QueueType,
	lastMessageID int64,
	bucketTails map[int64]int64,
	record legacyQueueAuthorityRecord,
) error {
	if err := store.ensureLegacyQueueV2Layout(ctx, queueType, lastMessageID, record); err != nil {
		return err
	}
	for bucket, tail := range bucketTails {
		if err := store.ensureLegacyQueueV2BucketStateWithAuthority(ctx, queueType, bucket, tail, record); err != nil {
			return err
		}
	}
	return nil
}

func newLegacyQueueV2MigrationStore(session cgocql.Session, bucketSize int64) *QueueStore {
	return &QueueStore{
		session:           session,
		migrationMode:     config.CassandraLegacyQueueMigrationModeTargetOnly,
		messageBucketSize: bucketSize,
		activeBuckets:     make(map[persistence.QueueType]int64, 2),
	}
}

func validateLegacyQueueV2MigrationOptions(options LegacyQueueV2MigrationOptions) error {
	if options.PageSize <= 0 {
		return fmt.Errorf("legacy queue v2 migration page size must be positive: %d", options.PageSize)
	}
	if options.MessageBucketSize <= 0 {
		return fmt.Errorf("legacy queue v2 message bucket size must be positive: %d", options.MessageBucketSize)
	}
	if options.MaxMismatches < 0 {
		return fmt.Errorf("legacy queue v2 maximum mismatches must not be negative: %d", options.MaxMismatches)
	}
	return nil
}

func (r *LegacyQueueV2ValidationResult) addMismatch(limit int, mismatch string) {
	if limit == 0 || len(r.Mismatches) < limit {
		r.Mismatches = append(r.Mismatches, mismatch)
	}
}

func (q *QueueStore) compareLegacyQueueV2TargetMessage(
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
	err = q.session.Query(
		templateGetLegacyQueueV2Message,
		queueType,
		bucket,
		legacyQueueV2MessageRowType,
		messageID,
	).WithContext(ctx).Scan(&data, &encoding)
	if err != nil {
		if cgocql.IsNotFoundError(err) {
			return fmt.Errorf("target is missing source message %d", messageID)
		}
		return cgocql.ConvertError("GetLegacyQueueV2TargetMessageForValidation", err)
	}
	if !bytes.Equal(data, expectedData) || encoding != expectedEncoding {
		return fmt.Errorf("target message %d differs from source", messageID)
	}
	return nil
}

func (q *QueueStore) compareLegacyQueueV2SourceMessage(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
	expectedData []byte,
	expectedEncoding string,
) error {
	var data []byte
	var encoding string
	err := q.session.Query(templateGetSourceQueueMessage, queueType, messageID).
		WithContext(ctx).
		Scan(&data, &encoding)
	if err != nil {
		if cgocql.IsNotFoundError(err) {
			return fmt.Errorf("target has extra message %d", messageID)
		}
		return cgocql.ConvertError("GetSourceLegacyQueueMessageForValidation", err)
	}
	if !bytes.Equal(data, expectedData) || encoding != expectedEncoding {
		return fmt.Errorf("source message %d differs from target", messageID)
	}
	return nil
}
