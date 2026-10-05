package cassandra

import (
	"context"
	"errors"
	"fmt"

	cgocql "github.com/gocql/gocql"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type queueV2MigrationAuthority int

const queueV2MigrationAuthorityCreating queueV2MigrationAuthority = -1

const (
	queueV2MigrationAuthorityUnspecified queueV2MigrationAuthority = iota
	queueV2MigrationAuthoritySource
	queueV2MigrationAuthoritySealing
	queueV2MigrationAuthorityTarget
)

const (
	templateGetQueueV2SourceMetadataAuthority = `SELECT migration_authority, migration_generation, migration_epoch, message_bucket_span
		FROM queues WHERE queue_type = ? AND queue_name = ?`
	templateGetQueueV2TargetMetadataAuthority = `SELECT migration_authority, migration_generation, migration_epoch, message_bucket_span
		FROM queues_v2 WHERE queue_type = ? AND metadata_bucket = ? AND queue_name = ?`
	templateInitializeQueueV2SourceMetadataAuthority = `UPDATE queues SET migration_authority = ?, migration_generation = ?,
		migration_epoch = ?, message_bucket_span = ? WHERE queue_type = ? AND queue_name = ?
		IF migration_authority = null AND migration_generation = null AND migration_epoch = null AND message_bucket_span = null`
	templateTransitionQueueV2SourceMetadataAuthority = `UPDATE queues SET migration_authority = ?, migration_epoch = ?
		WHERE queue_type = ? AND queue_name = ?
		IF migration_authority = ? AND migration_generation = ? AND migration_epoch = ? AND message_bucket_span = ?`
	templateTransitionQueueV2TargetMetadataAuthority = `UPDATE queues_v2 SET migration_authority = ?, migration_epoch = ?
		WHERE queue_type = ? AND metadata_bucket = ? AND queue_name = ?
		IF migration_authority = ? AND migration_generation = ? AND migration_epoch = ? AND message_bucket_span = ?`
	templateInitializeQueueV2TargetMetadataAuthority = `UPDATE queues_v2 SET migration_authority = ?, migration_generation = ?,
		migration_epoch = ?, message_bucket_span = ? WHERE queue_type = ? AND metadata_bucket = ? AND queue_name = ?
		IF migration_authority = null AND migration_generation = null AND migration_epoch = null AND message_bucket_span = null`
	templateCreateQueueV2SourceWithAuthority = `INSERT INTO queues
		(queue_type, queue_name, metadata_payload, metadata_encoding, version, migration_authority,
		migration_generation, migration_epoch, message_bucket_span) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateCreateQueueV2TargetWithAuthority = `INSERT INTO queues_v2
		(queue_type, metadata_bucket, queue_name, metadata_payload, metadata_encoding, version, migration_authority,
		migration_generation, migration_epoch, message_bucket_span) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateUpdateQueueV2SourceWithAuthority = `UPDATE queues SET metadata_payload = ?, metadata_encoding = ?, version = ?
		WHERE queue_type = ? AND queue_name = ? IF version = ? AND migration_authority = ?
		AND migration_generation = ? AND migration_epoch = ? AND message_bucket_span = ?`
	templateUpdateQueueV2TargetWithAuthority = `UPDATE queues_v2 SET metadata_payload = ?, metadata_encoding = ?, version = ?
		WHERE queue_type = ? AND metadata_bucket = ? AND queue_name = ? IF version = ? AND migration_authority = ?
		AND migration_generation = ? AND migration_epoch = ? AND message_bucket_span = ?`

	templateGetQueueV2SourceMessageAuthority = `SELECT migration_authority, migration_generation, migration_epoch, bucket_span
		FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? LIMIT 1`
	templateGetQueueV2TargetMessageAuthority = `SELECT migration_authority, migration_generation, migration_epoch, bucket_span
		FROM queue_messages_v3 WHERE queue_type = ? AND queue_name = ? AND message_bucket = ? LIMIT 1`
	templateInitializeQueueV2SourceMessageAuthority = `UPDATE queue_messages SET migration_authority = ?, migration_generation = ?,
		migration_epoch = ?, bucket_span = ? WHERE queue_type = ? AND queue_name = ? AND queue_partition = ?
		IF migration_authority = null AND migration_generation = null AND migration_epoch = null AND bucket_span = null`
	templateTransitionQueueV2SourceMessageAuthority = `UPDATE queue_messages SET migration_authority = ?, migration_epoch = ?
		WHERE queue_type = ? AND queue_name = ? AND queue_partition = ?
		IF migration_authority = ? AND migration_generation = ? AND migration_epoch = ? AND bucket_span = ?`
	templateTransitionQueueV2TargetMessageAuthority = `UPDATE queue_messages_v3 SET migration_authority = ?, migration_epoch = ?
		WHERE queue_type = ? AND queue_name = ? AND message_bucket = ?
		IF migration_authority = ? AND migration_generation = ? AND migration_epoch = ? AND bucket_span = ?`
	templateInitializeQueueV2TargetMessageAuthority = `UPDATE queue_messages_v3 SET migration_authority = ?, migration_generation = ?,
		migration_epoch = ?, bucket_span = ? WHERE queue_type = ? AND queue_name = ? AND message_bucket = ?
		IF migration_authority = null AND migration_generation = null AND migration_epoch = null AND bucket_span = null`
	templateGuardQueueV2SourceMessageAuthority = `UPDATE queue_messages SET migration_authority = ?
		WHERE queue_type = ? AND queue_name = ? AND queue_partition = ?
		IF migration_authority = ? AND migration_generation = ? AND migration_epoch = ? AND bucket_span = ?`
	templateGuardQueueV2TargetMessageAuthority = `UPDATE queue_messages_v3 SET migration_authority = ?
		WHERE queue_type = ? AND queue_name = ? AND message_bucket = ?
		IF migration_authority = ? AND migration_generation = ? AND migration_epoch = ? AND bucket_span = ?`
)

type queueV2AuthorityRecord struct {
	authority   queueV2MigrationAuthority
	generation  cgocql.UUID
	epoch       int64
	messageSpan int64
}

type queueV2OperationRoute struct {
	record  queueV2AuthorityRecord
	layout  queueV2MetadataLayout
	mirror  bool
	shadow  bool
	guarded bool
}

func (a queueV2MigrationAuthority) String() string {
	switch a {
	case queueV2MigrationAuthorityCreating:
		return "creating"
	case queueV2MigrationAuthoritySource:
		return "source"
	case queueV2MigrationAuthoritySealing:
		return "sealing"
	case queueV2MigrationAuthorityTarget:
		return "target"
	default:
		return "unspecified"
	}
}

func (r queueV2AuthorityRecord) isUnspecified() bool {
	return r.authority == queueV2MigrationAuthorityUnspecified &&
		r.generation == (cgocql.UUID{}) && r.epoch == 0 && r.messageSpan == 0
}

func readQueueV2MetadataAuthority(
	ctx context.Context,
	session gocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
	layout queueV2MetadataLayout,
) (queueV2AuthorityRecord, error) {
	query := templateGetQueueV2SourceMetadataAuthority
	args := []any{queueType, queueName}
	operation := "ReadQueueV2SourceMetadataAuthority"
	if layout == queueV2MetadataLayoutTarget {
		query = templateGetQueueV2TargetMetadataAuthority
		args = []any{queueType, queueV2MetadataBucket(queueType, queueName), queueName}
		operation = "ReadQueueV2TargetMetadataAuthority"
	}
	return scanQueueV2Authority(
		ctx,
		session.Query(query, args...),
		operation,
	)
}

func readQueueV2MessageAuthority(
	ctx context.Context,
	session gocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
	layout queueV2MetadataLayout,
	bucket int64,
) (queueV2AuthorityRecord, error) {
	query := templateGetQueueV2SourceMessageAuthority
	args := []any{queueType, queueName, 0}
	operation := "ReadQueueV2SourceMessageAuthority"
	if layout == queueV2MetadataLayoutTarget {
		query = templateGetQueueV2TargetMessageAuthority
		args = []any{queueType, queueName, bucket}
		operation = "ReadQueueV2TargetMessageAuthority"
	}
	return scanQueueV2Authority(
		ctx,
		session.Query(query, args...),
		operation,
	)
}

func scanQueueV2Authority(
	ctx context.Context,
	query gocql.Query,
	operation string,
) (queueV2AuthorityRecord, error) {
	var (
		authority  *int
		generation *cgocql.UUID
		epoch      *int64
		span       *int64
	)
	err := query.WithContext(ctx).Scan(&authority, &generation, &epoch, &span)
	if gocql.IsNotFoundError(err) {
		return queueV2AuthorityRecord{}, nil
	}
	if err != nil {
		return queueV2AuthorityRecord{}, gocql.ConvertError(operation, err)
	}
	if authority == nil && generation == nil && epoch == nil && span == nil {
		return queueV2AuthorityRecord{}, nil
	}
	if authority == nil || generation == nil || epoch == nil || span == nil {
		return queueV2AuthorityRecord{}, serviceerror.NewDataLoss(operation + ": authority is partially initialized")
	}
	record := queueV2AuthorityRecord{
		authority:   queueV2MigrationAuthority(*authority),
		generation:  *generation,
		epoch:       *epoch,
		messageSpan: *span,
	}
	if record.authority != queueV2MigrationAuthorityCreating &&
		(record.authority < queueV2MigrationAuthoritySource || record.authority > queueV2MigrationAuthorityTarget) {
		return queueV2AuthorityRecord{}, serviceerror.NewDataLossf(
			"%s: invalid authority %d",
			operation,
			*authority,
		)
	}
	if record.generation == (cgocql.UUID{}) || record.epoch <= 0 || record.messageSpan <= 0 {
		return queueV2AuthorityRecord{}, serviceerror.NewDataLoss(operation + ": authority identity is invalid")
	}
	return record, nil
}

func (s *queueV2Store) newQueueV2AuthorityRecord(authority queueV2MigrationAuthority) (queueV2AuthorityRecord, error) {
	generation := s.layoutGeneration
	if generation == (cgocql.UUID{}) {
		var err error
		generation, err = cgocql.RandomUUID()
		if err != nil {
			return queueV2AuthorityRecord{}, fmt.Errorf("generate QueueV2 migration identity: %w", err)
		}
	}
	epoch := int64(authority)
	if authority == queueV2MigrationAuthorityCreating {
		epoch = 1
	}
	return queueV2AuthorityRecord{
		authority:   authority,
		generation:  generation,
		epoch:       epoch,
		messageSpan: s.messageBucketSpan(),
	}, nil
}

func (s *queueV2Store) validateQueueV2Authority(
	queueType persistence.QueueV2Type,
	queueName string,
	record queueV2AuthorityRecord,
	expected queueV2MigrationAuthority,
) error {
	if record.authority != expected || record.messageSpan != s.messageBucketSpan() ||
		s.layoutGeneration != (cgocql.UUID{}) && record.generation != s.layoutGeneration {
		return serviceerror.NewUnavailablef(
			"QueueV2 queue type %d and name %q requires authority %s, generation %s, message span %d; got authority %s, generation %s, message span %d",
			queueType,
			queueName,
			expected,
			s.layoutGeneration,
			s.messageBucketSpan(),
			record.authority,
			record.generation,
			record.messageSpan,
		)
	}
	return nil
}

func validateQueueV2AuthorityPair(
	queueType persistence.QueueV2Type,
	queueName string,
	metadata queueV2AuthorityRecord,
	messages queueV2AuthorityRecord,
) error {
	if metadata != messages {
		return serviceerror.NewDataLossf(
			"QueueV2 queue type %d and name %q has inconsistent metadata and message authority identity: "+
				"metadata=(authority=%s,generation=%s,epoch=%d,span=%d), "+
				"messages=(authority=%s,generation=%s,epoch=%d,span=%d)",
			queueType,
			queueName,
			metadata.authority,
			metadata.generation,
			metadata.epoch,
			metadata.messageSpan,
			messages.authority,
			messages.generation,
			messages.epoch,
			messages.messageSpan,
		)
	}
	return nil
}

func (s *queueV2Store) initializeQueueV2SourceAuthority(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) (queueV2AuthorityRecord, error) {
	record, err := readQueueV2MetadataAuthority(
		ctx,
		s.session,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
	)
	if err != nil {
		return record, err
	}
	if record.isUnspecified() {
		record, err = s.newQueueV2AuthorityRecord(queueV2MigrationAuthoritySource)
		if err != nil {
			return record, err
		}
		applied, err := s.session.Query(
			templateInitializeQueueV2SourceMetadataAuthority,
			int(record.authority),
			record.generation,
			record.epoch,
			record.messageSpan,
			queueType,
			queueName,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return record, gocql.ConvertError("InitializeQueueV2SourceMetadataAuthority", err)
		}
		if !applied {
			record, err = readQueueV2MetadataAuthority(
				ctx,
				s.session,
				queueType,
				queueName,
				queueV2MetadataLayoutSource,
			)
			if err != nil {
				return record, err
			}
		}
	}
	if err := s.validateQueueV2Authority(queueType, queueName, record, queueV2MigrationAuthoritySource); err != nil {
		return record, err
	}
	messageRecord, err := readQueueV2MessageAuthority(
		ctx,
		s.session,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
		0,
	)
	if err != nil {
		return record, err
	}
	if messageRecord.isUnspecified() {
		applied, err := s.session.Query(
			templateInitializeQueueV2SourceMessageAuthority,
			int(record.authority),
			record.generation,
			record.epoch,
			record.messageSpan,
			queueType,
			queueName,
			0,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return record, gocql.ConvertError("InitializeQueueV2SourceMessageAuthority", err)
		}
		if applied {
			messageRecord = record
		} else {
			messageRecord, err = readQueueV2MessageAuthority(
				ctx,
				s.session,
				queueType,
				queueName,
				queueV2MetadataLayoutSource,
				0,
			)
			if err != nil {
				return record, err
			}
		}
	}
	if err := validateQueueV2AuthorityPair(queueType, queueName, record, messageRecord); err != nil {
		return record, err
	}
	s.cacheQueueV2Authority(queueType, queueName, record)
	return record, nil
}

func (s *queueV2Store) initializeQueueV2SourceMessageAuthorityRecord(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	record queueV2AuthorityRecord,
) error {
	existing, err := readQueueV2MessageAuthority(
		ctx,
		s.session,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
		0,
	)
	if err != nil {
		return err
	}
	if !existing.isUnspecified() {
		return validateQueueV2AuthorityPair(queueType, queueName, record, existing)
	}
	applied, err := s.session.Query(
		templateInitializeQueueV2SourceMessageAuthority,
		int(record.authority),
		record.generation,
		record.epoch,
		record.messageSpan,
		queueType,
		queueName,
		0,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("InitializeQueueV2SourceMessageAuthority", err)
	}
	if applied {
		return nil
	}
	existing, err = readQueueV2MessageAuthority(
		ctx,
		s.session,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
		0,
	)
	if err != nil {
		return err
	}
	return validateQueueV2AuthorityPair(queueType, queueName, record, existing)
}

func (s *queueV2Store) repairExistingQueueV2SourceMirror(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) error {
	record, err := s.initializeQueueV2SourceAuthority(ctx, queueType, queueName)
	if err != nil {
		return err
	}
	return s.ensureQueueV2TargetMirrorInitialized(ctx, queueType, queueName, record)
}

func (s *queueV2Store) ensureQueueV2TargetMirrorInitialized(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	record queueV2AuthorityRecord,
) error {
	queue, err := s.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutSource)
	if err != nil {
		return err
	}
	row, err := queueV2MetadataRowFromQueue(queueName, queue)
	if err != nil {
		return err
	}
	row.authority = record
	if _, targetErr := s.getQueueFromLayout(
		ctx,
		queueName,
		queueType,
		queueV2MetadataLayoutTarget,
	); targetErr == nil {
		if err := s.initializeQueueV2TargetMetadataAuthorityRecord(ctx, queueType, queueName, record); err != nil {
			return err
		}
	} else {
		var notFound *serviceerror.NotFound
		if !errors.As(targetErr, &notFound) {
			return targetErr
		}
	}
	if err := s.mirrorQueueMetadata(ctx, queueType, row, queueV2MetadataLayoutTarget); err != nil {
		return err
	}
	minimumMessageID, err := queueV2MinimumMessageID(queueType, queueName, queue)
	if err != nil {
		return err
	}
	return s.ensureQueueV2MessageTarget(ctx, queueType, queueName, minimumMessageID, true, record)
}

func (s *queueV2Store) initializeQueueV2TargetMetadataAuthorityRecord(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	record queueV2AuthorityRecord,
) error {
	existing, err := readQueueV2MetadataAuthority(
		ctx,
		s.session,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
	)
	if err != nil {
		return err
	}
	if !existing.isUnspecified() {
		return validateQueueV2AuthorityPair(queueType, queueName, record, existing)
	}
	applied, err := s.session.Query(
		templateInitializeQueueV2TargetMetadataAuthority,
		int(record.authority),
		record.generation,
		record.epoch,
		record.messageSpan,
		queueType,
		queueV2MetadataBucket(queueType, queueName),
		queueName,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("InitializeQueueV2TargetMetadataAuthority", err)
	}
	if applied {
		return nil
	}
	existing, err = readQueueV2MetadataAuthority(
		ctx,
		s.session,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
	)
	if err != nil {
		return err
	}
	return validateQueueV2AuthorityPair(queueType, queueName, record, existing)
}

func (s *queueV2Store) initializeQueueV2TargetMessageAuthorityRecord(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	bucket int64,
	record queueV2AuthorityRecord,
) error {
	existing, err := readQueueV2MessageAuthority(
		ctx,
		s.session,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
		bucket,
	)
	if err != nil {
		return err
	}
	if !existing.isUnspecified() {
		return validateQueueV2AuthorityPair(queueType, queueName, record, existing)
	}
	applied, err := s.session.Query(
		templateInitializeQueueV2TargetMessageAuthority,
		int(record.authority),
		record.generation,
		record.epoch,
		record.messageSpan,
		queueType,
		queueName,
		bucket,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("InitializeQueueV2TargetMessageAuthority", err)
	}
	if applied {
		return nil
	}
	existing, err = readQueueV2MessageAuthority(
		ctx,
		s.session,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
		bucket,
	)
	if err != nil {
		return err
	}
	return validateQueueV2AuthorityPair(queueType, queueName, record, existing)
}

//nolint:revive // Target-dual creation is an idempotent, crash-resumable authority transition.
func (s *queueV2Store) createQueueV2TargetDual(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	row queueV2MetadataRow,
) (bool, queueV2AuthorityRecord, error) {
	creating, err := s.newQueueV2AuthorityRecord(queueV2MigrationAuthorityCreating)
	if err != nil {
		return false, queueV2AuthorityRecord{}, err
	}
	row.authority = creating
	applied, err := s.createQueueMetadata(ctx, queueType, row, queueV2MetadataLayoutSource)
	if err != nil {
		return false, queueV2AuthorityRecord{}, err
	}
	if !applied {
		existing, readErr := readQueueV2MetadataAuthority(
			ctx,
			s.session,
			queueType,
			row.queueName,
			queueV2MetadataLayoutSource,
		)
		if readErr != nil {
			return false, queueV2AuthorityRecord{}, readErr
		}
		switch existing.authority {
		case queueV2MigrationAuthorityUnspecified, queueV2MigrationAuthoritySource:
			if repairErr := s.repairExistingQueueV2SourceMirror(ctx, queueType, row.queueName); repairErr != nil {
				return false, existing, repairErr
			}
			return false, existing, nil
		case queueV2MigrationAuthorityCreating:
			creating = existing
			row.authority = creating
		case queueV2MigrationAuthorityTarget:
			if err := s.validateQueueV2Authority(
				queueType,
				row.queueName,
				existing,
				queueV2MigrationAuthorityTarget,
			); err != nil {
				return false, existing, err
			}
			return false, existing, nil
		default:
			return false, existing, serviceerror.NewUnavailablef(
				"QueueV2 queue type %d and name %q already exists with %s authority",
				queueType,
				row.queueName,
				existing.authority,
			)
		}
	}
	sourceMessage, err := readQueueV2MessageAuthority(
		ctx,
		s.session,
		queueType,
		row.queueName,
		queueV2MetadataLayoutSource,
		0,
	)
	if err != nil {
		return false, creating, err
	}
	if sourceMessage.isUnspecified() {
		if err := s.initializeQueueV2SourceMessageAuthorityRecord(ctx, queueType, row.queueName, creating); err != nil {
			return false, creating, err
		}
	} else if sourceMessage != creating && sourceMessage.authority != queueV2MigrationAuthorityTarget {
		return false, creating, serviceerror.NewUnavailablef(
			"QueueV2 fresh queue source message state has %s authority",
			sourceMessage.authority,
		)
	}
	target := creating
	target.authority = queueV2MigrationAuthorityTarget
	target.epoch = creating.epoch + 1
	row.authority = target
	if targetApplied, err := s.createQueueMetadata(ctx, queueType, row, queueV2MetadataLayoutTarget); err != nil {
		return false, creating, err
	} else if !targetApplied {
		existingTarget, err := readQueueV2MetadataAuthority(
			ctx,
			s.session,
			queueType,
			row.queueName,
			queueV2MetadataLayoutTarget,
		)
		if err != nil {
			return false, creating, err
		}
		if err := validateQueueV2AuthorityPair(queueType, row.queueName, target, existingTarget); err != nil {
			return false, creating, err
		}
	}
	if err := s.initializeQueueV2MessageTarget(ctx, queueType, row.queueName, target); err != nil {
		return false, creating, err
	}
	if err := transitionQueueV2SourceMessageRecord(
		ctx,
		s.session,
		queueType,
		row.queueName,
		creating,
		target,
	); err != nil {
		return false, creating, err
	}
	if err := transitionQueueV2SourceMetadataRecord(
		ctx,
		s.session,
		queueType,
		row.queueName,
		creating,
		target,
	); err != nil {
		return false, creating, err
	}
	return applied, target, nil
}

func transitionQueueV2SourceMetadataRecord(
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
		templateTransitionQueueV2SourceMetadataAuthority,
		[]any{
			int(to.authority),
			to.epoch,
			queueType,
			queueName,
			int(from.authority),
			from.generation,
			from.epoch,
			from.messageSpan,
		},
		"TransitionQueueV2SourceMetadataAuthority",
	)
	if err != nil || applied {
		return err
	}
	record, err := readQueueV2MetadataAuthority(
		ctx,
		session,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
	)
	if err != nil {
		return err
	}
	if record == to {
		return nil
	}
	return serviceerror.NewUnavailablef(
		"QueueV2 source metadata authority transition %s -> %s conflicted; current authority is %s at epoch %d",
		from.authority,
		to.authority,
		record.authority,
		record.epoch,
	)
}

func transitionQueueV2SourceMessageRecord(
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
		templateTransitionQueueV2SourceMessageAuthority,
		[]any{
			int(to.authority),
			to.epoch,
			queueType,
			queueName,
			0,
			int(from.authority),
			from.generation,
			from.epoch,
			from.messageSpan,
		},
		"TransitionQueueV2SourceMessageAuthority",
	)
	if err != nil || applied {
		return err
	}
	record, err := readQueueV2MessageAuthority(
		ctx,
		session,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
		0,
	)
	if err != nil {
		return err
	}
	if record == to {
		return nil
	}
	return serviceerror.NewUnavailablef(
		"QueueV2 source message authority transition %s -> %s conflicted; current authority is %s at epoch %d",
		from.authority,
		to.authority,
		record.authority,
		record.epoch,
	)
}

//nolint:revive // Routing validates all source and target authority combinations before selecting a layout.
func (s *queueV2Store) resolveQueueV2Route(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) (queueV2OperationRoute, error) {
	mode := normalizeQueueV2MigrationMode(s.migrationMode)
	if mode == config.CassandraQueueV2MigrationModeSourceOnly {
		return queueV2OperationRoute{layout: queueV2MetadataLayoutSource}, nil
	}
	if mode != config.CassandraQueueV2MigrationModeTargetDual {
		if record, ok := s.cachedQueueV2Authority(queueType, queueName); ok {
			switch mode {
			case config.CassandraQueueV2MigrationModeSourceDual:
				return queueV2OperationRoute{record: record, layout: queueV2MetadataLayoutSource, mirror: true, guarded: true}, nil
			case config.CassandraQueueV2MigrationModeTargetShadow:
				return queueV2OperationRoute{record: record, layout: queueV2MetadataLayoutSource, mirror: true, shadow: true, guarded: true}, nil
			case config.CassandraQueueV2MigrationModeTargetOnly:
				return queueV2OperationRoute{record: record, layout: queueV2MetadataLayoutTarget, guarded: true}, nil
			default:
			}
		}
	}
	if mode == config.CassandraQueueV2MigrationModeTargetOnly {
		metadataRecord, err := readQueueV2MetadataAuthority(
			ctx,
			s.session,
			queueType,
			queueName,
			queueV2MetadataLayoutTarget,
		)
		if err != nil {
			return queueV2OperationRoute{}, err
		}
		if metadataRecord.isUnspecified() {
			if _, err := s.getQueueFromLayout(
				ctx,
				queueName,
				queueType,
				queueV2MetadataLayoutTarget,
			); err != nil {
				return queueV2OperationRoute{}, err
			}
			return queueV2OperationRoute{}, serviceerror.NewDataLossf(
				"QueueV2 queue type %d and name %q has target metadata without migration authority",
				queueType,
				queueName,
			)
		}
		messageRecord, err := readQueueV2MessageAuthority(
			ctx,
			s.session,
			queueType,
			queueName,
			queueV2MetadataLayoutTarget,
			queueV2MessageDirectoryBucket,
		)
		if err != nil {
			return queueV2OperationRoute{}, err
		}
		if err := validateQueueV2AuthorityPair(queueType, queueName, metadataRecord, messageRecord); err != nil {
			return queueV2OperationRoute{}, err
		}
		if err := s.validateQueueV2Authority(queueType, queueName, metadataRecord, queueV2MigrationAuthorityTarget); err != nil {
			return queueV2OperationRoute{}, err
		}
		s.cacheQueueV2Authority(queueType, queueName, metadataRecord)
		return queueV2OperationRoute{record: metadataRecord, layout: queueV2MetadataLayoutTarget, guarded: true}, nil
	}
	if _, err := s.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutSource); err != nil {
		return queueV2OperationRoute{}, err
	}

	sourceRecord, err := readQueueV2MetadataAuthority(
		ctx,
		s.session,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
	)
	if err != nil {
		return queueV2OperationRoute{}, err
	}
	if sourceRecord.isUnspecified() {
		sourceRecord, err = s.initializeQueueV2SourceAuthority(ctx, queueType, queueName)
		if err != nil {
			return queueV2OperationRoute{}, err
		}
	}
	if sourceRecord.messageSpan != s.messageBucketSpan() ||
		s.layoutGeneration != (cgocql.UUID{}) && sourceRecord.generation != s.layoutGeneration {
		return queueV2OperationRoute{}, s.validateQueueV2Authority(
			queueType,
			queueName,
			sourceRecord,
			sourceRecord.authority,
		)
	}
	switch sourceRecord.authority {
	case queueV2MigrationAuthoritySource:
		messageRecord, err := readQueueV2MessageAuthority(
			ctx,
			s.session,
			queueType,
			queueName,
			queueV2MetadataLayoutSource,
			0,
		)
		if err != nil {
			return queueV2OperationRoute{}, err
		}
		if err := validateQueueV2AuthorityPair(queueType, queueName, sourceRecord, messageRecord); err != nil {
			return queueV2OperationRoute{}, err
		}
		if err := s.ensureQueueV2TargetMirrorInitialized(ctx, queueType, queueName, sourceRecord); err != nil {
			return queueV2OperationRoute{}, err
		}
		s.cacheQueueV2Authority(queueType, queueName, sourceRecord)
		return queueV2OperationRoute{
			record:  sourceRecord,
			layout:  queueV2MetadataLayoutSource,
			mirror:  true,
			shadow:  mode == config.CassandraQueueV2MigrationModeTargetShadow,
			guarded: true,
		}, nil
	case queueV2MigrationAuthorityCreating, queueV2MigrationAuthoritySealing:
		return queueV2OperationRoute{}, serviceerror.NewUnavailablef(
			"QueueV2 queue type %d and name %q cutover is in progress",
			queueType,
			queueName,
		)
	case queueV2MigrationAuthorityTarget:
		if mode != config.CassandraQueueV2MigrationModeTargetDual {
			return queueV2OperationRoute{}, serviceerror.NewUnavailablef(
				"QueueV2 queue type %d and name %q is target-authoritative; deploy target-dual or target-only mode",
				queueType,
				queueName,
			)
		}
		targetRecord, err := readQueueV2MetadataAuthority(
			ctx,
			s.session,
			queueType,
			queueName,
			queueV2MetadataLayoutTarget,
		)
		if err != nil {
			return queueV2OperationRoute{}, err
		}
		sourceMessageRecord, err := readQueueV2MessageAuthority(
			ctx,
			s.session,
			queueType,
			queueName,
			queueV2MetadataLayoutSource,
			0,
		)
		if err != nil {
			return queueV2OperationRoute{}, err
		}
		targetMessageRecord, err := readQueueV2MessageAuthority(
			ctx,
			s.session,
			queueType,
			queueName,
			queueV2MetadataLayoutTarget,
			queueV2MessageDirectoryBucket,
		)
		if err != nil {
			return queueV2OperationRoute{}, err
		}
		if err := validateQueueV2AuthorityPair(queueType, queueName, sourceRecord, targetRecord); err != nil {
			return queueV2OperationRoute{}, err
		}
		if err := validateQueueV2AuthorityPair(queueType, queueName, sourceRecord, sourceMessageRecord); err != nil {
			return queueV2OperationRoute{}, err
		}
		if err := validateQueueV2AuthorityPair(queueType, queueName, sourceRecord, targetMessageRecord); err != nil {
			return queueV2OperationRoute{}, err
		}
		s.cacheQueueV2Authority(queueType, queueName, sourceRecord)
		return queueV2OperationRoute{
			record:  sourceRecord,
			layout:  queueV2MetadataLayoutTarget,
			mirror:  true,
			guarded: true,
		}, nil
	default:
		return queueV2OperationRoute{}, serviceerror.NewDataLossf(
			"QueueV2 queue type %d and name %q has invalid authority %d",
			queueType,
			queueName,
			sourceRecord.authority,
		)
	}
}

func (s *queueV2Store) cacheQueueV2Authority(
	queueType persistence.QueueV2Type,
	queueName string,
	record queueV2AuthorityRecord,
) {
	s.knownQueuesMu.Lock()
	defer s.knownQueuesMu.Unlock()
	if s.queueAuthorities == nil {
		s.queueAuthorities = make(map[queueV2Key]queueV2AuthorityRecord)
	}
	s.queueAuthorities[queueV2Key{queueType: queueType, queueName: queueName}] = record
}

func (s *queueV2Store) cachedQueueV2Authority(
	queueType persistence.QueueV2Type,
	queueName string,
) (queueV2AuthorityRecord, bool) {
	s.knownQueuesMu.RLock()
	defer s.knownQueuesMu.RUnlock()
	record, ok := s.queueAuthorities[queueV2Key{queueType: queueType, queueName: queueName}]
	return record, ok
}

func transitionQueueV2Authority(
	ctx context.Context,
	session gocql.Session,
	query string,
	args []any,
	operation string,
) (bool, error) {
	applied, err := session.Query(query, args...).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return false, gocql.ConvertError(operation, err)
	}
	return applied, nil
}

func addQueueV2MessageAuthorityGuard(
	batch *gocql.Batch,
	queueType persistence.QueueV2Type,
	queueName string,
	layout queueV2MetadataLayout,
	bucket int64,
	record queueV2AuthorityRecord,
) {
	query := templateGuardQueueV2SourceMessageAuthority
	args := []any{
		int(record.authority),
		queueType,
		queueName,
		0,
		int(record.authority),
		record.generation,
		record.epoch,
		record.messageSpan,
	}
	if layout == queueV2MetadataLayoutTarget {
		query = templateGuardQueueV2TargetMessageAuthority
		args = []any{
			int(record.authority),
			queueType,
			queueName,
			bucket,
			int(record.authority),
			record.generation,
			record.epoch,
			record.messageSpan,
		}
	}
	batch.Query(query, args...)
}

func (s *queueV2Store) requireQueueV2MessageAuthority(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	layout queueV2MetadataLayout,
	bucket int64,
	expected queueV2AuthorityRecord,
) error {
	record, err := readQueueV2MessageAuthority(ctx, s.session, queueType, queueName, layout, bucket)
	if err != nil {
		return err
	}
	if record != expected {
		return serviceerror.NewUnavailablef(
			"QueueV2 queue type %d and name %q message authority changed from (%s,%s,%d,%d) to (%s,%s,%d,%d)",
			queueType,
			queueName,
			expected.authority,
			expected.generation,
			expected.epoch,
			expected.messageSpan,
			record.authority,
			record.generation,
			record.epoch,
			record.messageSpan,
		)
	}
	return nil
}
