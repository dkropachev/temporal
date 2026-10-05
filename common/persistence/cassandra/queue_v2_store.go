package cassandra

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	cgocql "github.com/gocql/gocql"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"golang.org/x/sync/errgroup"
)

type (
	// queueV2Store contains the SQL queries and serialization/deserialization functions to interact with the queues and
	// queue_messages tables that implement the QueueV2 interface. The schema is located at:
	//	schema/cassandra/temporal/versioned/v1.9/queues.cql and
	//	schema/cassandra/temporal/versioned/v1.16/queues_v2_bucketed.cql
	queueV2Store struct {
		session                gocql.Session
		logger                 log.Logger
		migrationMode          config.CassandraQueueV2MigrationMode
		messageSpan            int64
		layoutGeneration       cgocql.UUID
		allowTargetOnlyCreate  bool
		knownQueuesMu          sync.RWMutex
		knownQueues            map[queueV2Key]struct{}
		queueAuthorities       map[queueV2Key]queueV2AuthorityRecord
		activeMessageBucketsMu sync.RWMutex
		activeMessageBuckets   map[queueV2Key]int64
		queueLocks             [queueV2LockStripes]queueV2Lock
	}

	Queue struct {
		Metadata *persistencespb.Queue
		Version  int64
	}

	queueV2Key struct {
		queueType persistence.QueueV2Type
		queueName string
	}

	queueV2Lock struct {
		initialize sync.Once
		semaphore  chan struct{}
	}

	queueV2MetadataLayout int

	queueV2MetadataRow struct {
		bucket           int
		queueName        string
		metadataPayload  []byte
		metadataEncoding string
		version          int64
		authority        queueV2AuthorityRecord
	}
)

const (
	queueV2LockStripes             = 256
	queueV2KnownQueuesSize         = 4096
	queueV2MetadataBucketCount     = 64
	queueV2ListTokenEnvelopePrefix = "\xff\xffTEMPORAL-QUEUE-V2-LIST"
	queueV2ListTokenVersion        = byte(1)
	queueV2ListTokenHeaderLength   = len(queueV2ListTokenEnvelopePrefix) + 1 + 2 + 4 + 4

	queueV2MetadataLayoutSource queueV2MetadataLayout = 0
	queueV2MetadataLayoutTarget queueV2MetadataLayout = 1

	TemplateEnqueueMessageQuery      = `INSERT INTO queue_messages (queue_type, queue_name, queue_partition, message_id, message_payload, message_encoding) VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	TemplateGetMessagesQuery         = `SELECT message_id, message_payload, message_encoding FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? AND message_id >= ? ORDER BY message_id ASC LIMIT ?`
	TemplateGetMaxMessageIDQuery     = `SELECT message_id FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? AND message_id >= 0 ORDER BY message_id DESC LIMIT 1`
	TemplateCreateQueueQuery         = `INSERT INTO queues (queue_type, queue_name, metadata_payload, metadata_encoding, version) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS`
	TemplateGetQueueQuery            = `SELECT metadata_payload, metadata_encoding, version FROM queues WHERE queue_type = ? AND queue_name = ?`
	TemplateRangeDeleteMessagesQuery = `DELETE FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? AND message_id >= ? AND message_id <= ?`
	TemplateUpdateQueueMetadataQuery = `UPDATE queues SET metadata_payload = ?, metadata_encoding = ?, version = ? WHERE queue_type = ? AND queue_name = ? IF version = ?`
	templateGetQueueNamesQuery       = `SELECT queue_name, metadata_payload, metadata_encoding, version FROM queues WHERE queue_type = ? ALLOW FILTERING`

	TemplateCreateQueueV2Query               = `INSERT INTO queues_v2 (queue_type, metadata_bucket, queue_name, metadata_payload, metadata_encoding, version) VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	TemplateGetQueueV2Query                  = `SELECT metadata_payload, metadata_encoding, version FROM queues_v2 WHERE queue_type = ? AND metadata_bucket = ? AND queue_name = ?`
	TemplateUpdateQueueMetadataV2Query       = `UPDATE queues_v2 SET metadata_payload = ?, metadata_encoding = ?, version = ? WHERE queue_type = ? AND metadata_bucket = ? AND queue_name = ? IF version = ?`
	templateGetQueueNamesV2FirstPageQuery    = `SELECT queue_name, metadata_payload, metadata_encoding, version FROM queues_v2 WHERE queue_type = ? AND metadata_bucket = ? ORDER BY queue_name ASC LIMIT ?`
	templateGetQueueNamesV2ContinuationQuery = `SELECT queue_name, metadata_payload, metadata_encoding, version FROM queues_v2 WHERE queue_type = ? AND metadata_bucket = ? AND queue_name > ? ORDER BY queue_name ASC LIMIT ?`
)

var (
	// ErrEnqueueMessageConflict is returned by queue implementations that use conditional inserts to allocate message
	// IDs and lose a race with another writer.
	ErrEnqueueMessageConflict = &persistence.ConditionFailedError{
		Msg: "conflict inserting queue message, likely due to concurrent writes",
	}
	// ErrUpdateQueueConflict is returned when a queue is updated with the wrong version. This happens when there are
	// concurrent writes to the queue because we update a queue using two queries, similar to the enqueue message query.
	//
	// 	1. SELECT (queue, version) FROM queues
	// 	2. UPDATE queue, version IF version = version from step 1
	//
	// See the following example:
	//
	//  Client A           Client B                           Cassandra DB
	//  |                  |                                            |
	//  |--1. SELECT (queue, version) FROM queues---------------------->|
	//  |                  |                                            |
	//  |<-2. Return (queue, v1)----------------------------------------|
	//  |                  |                                            |
	//  |                  |--3. SELECT (queue, version) FROM queues--->|
	//  |                  |                                            |
	//  |                  |<-4. Return (queue, v1)---------------------|
	//  |                  |                                            |
	//  |--5. UPDATE queue, version IF version = v1-------------------->|
	//  |                  |                                            |
	//  |<-6. Acknowledge-----------------------------------------------|
	//  |                  |                                            |
	//  |                  |--7. UPDATE queue, version IF version = v1->|
	//  |                  |                                            |
	//  |                  |<-8. Conflict/Error-------------------------|
	//  |                  |                                            |
	ErrUpdateQueueConflict = &persistence.ConditionFailedError{
		Msg: "conflict updating queue, likely due to concurrent writes",
	}
)

func NewQueueV2Store(session gocql.Session, logger log.Logger) persistence.QueueV2 {
	return &queueV2Store{
		session:       session,
		logger:        logger,
		migrationMode: config.CassandraQueueV2MigrationModeSourceOnly,
		messageSpan:   DefaultQueueV2MessageBucketSpan,
	}
}

func NewQueueV2StoreWithMigrationMode(
	session gocql.Session,
	logger log.Logger,
	migrationMode config.CassandraQueueV2MigrationMode,
) (persistence.QueueV2, error) {
	return NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
		session,
		logger,
		migrationMode,
		DefaultQueueV2MessageBucketSpan,
	)
}

// NewQueueV2StoreWithMigrationModeAndMessageBucketSpan constructs a QueueV2 store for a fixed layout generation.
func NewQueueV2StoreWithMigrationModeAndMessageBucketSpan(
	session gocql.Session,
	logger log.Logger,
	migrationMode config.CassandraQueueV2MigrationMode,
	messageBucketSpan int64,
) (persistence.QueueV2, error) {
	return newQueueV2StoreWithMigrationIdentity(
		session,
		logger,
		migrationMode,
		messageBucketSpan,
		cgocql.UUID{},
		false,
	)
}

func newQueueV2StoreWithMigrationIdentity(
	session gocql.Session,
	logger log.Logger,
	migrationMode config.CassandraQueueV2MigrationMode,
	messageBucketSpan int64,
	layoutGeneration cgocql.UUID,
	allowTargetOnlyCreate bool,
) (persistence.QueueV2, error) {
	migrationMode = normalizeQueueV2MigrationMode(migrationMode)
	if err := ValidateQueueV2MigrationMode(migrationMode); err != nil {
		return nil, err
	}
	if err := validateQueueV2MessageBucketSpan(messageBucketSpan); err != nil {
		return nil, err
	}
	return &queueV2Store{
		session:               session,
		logger:                logger,
		migrationMode:         migrationMode,
		messageSpan:           messageBucketSpan,
		layoutGeneration:      layoutGeneration,
		allowTargetOnlyCreate: allowTargetOnlyCreate,
	}, nil
}

func normalizeQueueV2MigrationMode(
	mode config.CassandraQueueV2MigrationMode,
) config.CassandraQueueV2MigrationMode {
	if mode == "" {
		return config.CassandraQueueV2MigrationModeSourceOnly
	}
	return mode
}

// ValidateQueueV2MigrationMode checks whether a QueueV2 metadata migration mode is supported.
func ValidateQueueV2MigrationMode(mode config.CassandraQueueV2MigrationMode) error {
	switch normalizeQueueV2MigrationMode(mode) {
	case config.CassandraQueueV2MigrationModeSourceOnly,
		config.CassandraQueueV2MigrationModeSourceDual,
		config.CassandraQueueV2MigrationModeTargetShadow,
		config.CassandraQueueV2MigrationModeTargetDual,
		config.CassandraQueueV2MigrationModeTargetOnly:
		return nil
	default:
		return fmt.Errorf("unsupported Cassandra QueueV2 migration mode %q", mode)
	}
}

func (s *queueV2Store) metadataReadLayout() queueV2MetadataLayout {
	switch s.migrationMode {
	case config.CassandraQueueV2MigrationModeTargetDual,
		config.CassandraQueueV2MigrationModeTargetOnly:
		return queueV2MetadataLayoutTarget
	default:
		return queueV2MetadataLayoutSource
	}
}

func (s *queueV2Store) metadataWriteLayouts() (
	primary queueV2MetadataLayout,
	mirror queueV2MetadataLayout,
	hasMirror bool,
) {
	switch s.migrationMode {
	case config.CassandraQueueV2MigrationModeSourceDual,
		config.CassandraQueueV2MigrationModeTargetShadow:
		return queueV2MetadataLayoutSource, queueV2MetadataLayoutTarget, true
	case config.CassandraQueueV2MigrationModeTargetDual:
		return queueV2MetadataLayoutTarget, queueV2MetadataLayoutSource, true
	case config.CassandraQueueV2MigrationModeTargetOnly:
		return queueV2MetadataLayoutTarget, 0, false
	default:
		return queueV2MetadataLayoutSource, 0, false
	}
}

func (s *queueV2Store) EnqueueMessage(
	ctx context.Context,
	request *persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	queueType := request.QueueType
	queueName := request.QueueName
	unlock, err := s.lockQueue(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	defer unlock()

	route, err := s.resolveQueueV2Route(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	var queue *Queue
	if !s.isKnownQueue(queueType, queueName) {
		queue, err = s.getQueueFromLayout(ctx, queueName, queueType, route.layout)
		if err != nil {
			return nil, err
		}
		s.markKnownQueue(queueType, queueName)
	} else if route.mirror && route.layout == queueV2MetadataLayoutSource {
		queue, err = s.getQueueFromLayout(ctx, queueName, queueType, route.layout)
		if err != nil {
			return nil, err
		}
	}
	nextMessageID, err := s.enqueueMessage(ctx, queueType, queueName, queue, request.Blob, route)
	if err != nil {
		s.forgetKnownQueue(queueType, queueName)
		return nil, err
	}
	return &persistence.InternalEnqueueMessageResponse{
		Metadata: persistence.MessageMetadata{ID: nextMessageID},
	}, nil
}

func (s *queueV2Store) ReadMessages(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	if request.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveReadQueueMessagesPageSize
	}
	route, err := s.resolveQueueV2Route(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	if route.layout == queueV2MetadataLayoutTarget {
		return s.readMessagesTarget(ctx, request, route)
	}
	if bytes.HasPrefix(request.NextPageToken, []byte(queueV2MessagePageTokenPrefix)) {
		return nil, fmt.Errorf(
			"%w: target-layout token cannot continue a source-layout query",
			persistence.ErrInvalidReadQueueMessagesNextPageToken,
		)
	}
	q, err := s.getQueueFromLayout(
		ctx,
		request.QueueName,
		request.QueueType,
		queueV2MetadataLayoutSource,
	)
	if err != nil {
		return nil, err
	}
	minMessageID, err := persistence.GetMinMessageIDToReadForQueueV2(
		request.QueueType,
		request.QueueName,
		request.NextPageToken,
		q.Metadata,
	)
	if err != nil {
		return nil, err
	}

	iter := s.session.Query(
		TemplateGetMessagesQuery,
		request.QueueType,
		request.QueueName,
		0,
		minMessageID,
		request.PageSize,
	).WithContext(ctx).Iter()

	var (
		messages []persistence.QueueV2Message
		// messageID is the ID of the last message returned by the query.
		messageID int64
	)

	for {
		var (
			messagePayload  []byte
			messageEncoding string
		)
		if !iter.Scan(&messageID, &messagePayload, &messageEncoding) {
			break
		}
		encoding, err := enumspb.EncodingTypeFromString(messageEncoding)
		if err != nil {
			_ = iter.Close()
			return nil, serialization.NewUnknownEncodingTypeError(messageEncoding)
		}

		encodingType := enumspb.EncodingType(encoding)

		message := persistence.QueueV2Message{
			MetaData: persistence.MessageMetadata{ID: messageID},
			Data: &commonpb.DataBlob{
				EncodingType: encodingType,
				Data:         messagePayload,
			},
		}
		messages = append(messages, message)
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("QueueV2ReadMessages", err)
	}

	response := &persistence.InternalReadMessagesResponse{
		Messages:      messages,
		NextPageToken: persistence.GetNextPageTokenForReadMessages(messages),
	}
	if route.shadow {
		s.shadowCompareTargetMessages(ctx, request, minMessageID, response.Messages)
	}
	return response, nil
}

func (s *queueV2Store) CreateQueue(
	ctx context.Context,
	request *persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	queueType := request.QueueType
	queueName := request.QueueName
	q := persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: persistence.FirstQueueMessageID,
			},
		},
	}
	metadataPayload, err := q.Marshal()
	if err != nil {
		return nil, serialization.NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	row := queueV2MetadataRow{
		queueName:        queueName,
		metadataPayload:  metadataPayload,
		metadataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
		version:          0,
	}
	mode := normalizeQueueV2MigrationMode(s.migrationMode)
	switch mode {
	case config.CassandraQueueV2MigrationModeSourceOnly:
		applied, err := s.createQueueMetadata(ctx, queueType, row, queueV2MetadataLayoutSource)
		if err != nil {
			return nil, err
		}
		if !applied {
			return nil, queueV2AlreadyExistsError(queueType, queueName)
		}
	case config.CassandraQueueV2MigrationModeSourceDual,
		config.CassandraQueueV2MigrationModeTargetShadow:
		record, err := s.newQueueV2AuthorityRecord(queueV2MigrationAuthoritySource)
		if err != nil {
			return nil, err
		}
		row.authority = record
		applied, err := s.createQueueMetadata(ctx, queueType, row, queueV2MetadataLayoutSource)
		if err != nil {
			return nil, err
		}
		if !applied {
			if err := s.repairExistingQueueV2SourceMirror(ctx, queueType, queueName); err != nil {
				return nil, fmt.Errorf("%w; repair migration mirror: %v", queueV2AlreadyExistsError(queueType, queueName), err)
			}
			return nil, queueV2AlreadyExistsError(queueType, queueName)
		}
		if err := s.initializeQueueV2SourceMessageAuthorityRecord(ctx, queueType, queueName, record); err != nil {
			return nil, err
		}
		if err := s.mirrorQueueMetadata(ctx, queueType, row, queueV2MetadataLayoutTarget); err != nil {
			return nil, err
		}
		if err := s.initializeQueueV2MessageTarget(ctx, queueType, queueName, record); err != nil {
			return nil, err
		}
		s.cacheQueueV2Authority(queueType, queueName, record)
	case config.CassandraQueueV2MigrationModeTargetDual:
		created, record, err := s.createQueueV2TargetDual(ctx, queueType, row)
		if err != nil {
			return nil, err
		}
		if !created {
			return nil, queueV2AlreadyExistsError(queueType, queueName)
		}
		s.cacheQueueV2Authority(queueType, queueName, record)
	case config.CassandraQueueV2MigrationModeTargetOnly:
		if !s.allowTargetOnlyCreate || s.layoutGeneration == (cgocql.UUID{}) {
			return nil, serviceerror.NewUnavailable(
				"creating a fresh target-only QueueV2 queue requires a factory validated against the global target-only layout generation",
			)
		}
		creating, err := s.newQueueV2AuthorityRecord(queueV2MigrationAuthorityCreating)
		if err != nil {
			return nil, err
		}
		row.authority = creating
		applied, err := s.createQueueMetadata(ctx, queueType, row, queueV2MetadataLayoutTarget)
		if err != nil {
			return nil, err
		}
		if !applied {
			existingRecord, err := readQueueV2MetadataAuthority(
				ctx,
				s.session,
				queueType,
				queueName,
				queueV2MetadataLayoutTarget,
			)
			if err != nil {
				return nil, fmt.Errorf("%w; read target authority: %v", queueV2AlreadyExistsError(queueType, queueName), err)
			}
			switch existingRecord.authority {
			case queueV2MigrationAuthorityCreating:
				if err := s.validateQueueV2Authority(
					queueType,
					queueName,
					existingRecord,
					queueV2MigrationAuthorityCreating,
				); err != nil {
					return nil, fmt.Errorf("%w; validate interrupted target creation: %v", queueV2AlreadyExistsError(queueType, queueName), err)
				}
				creating = existingRecord
			case queueV2MigrationAuthorityTarget:
				if _, err := s.resolveQueueV2Route(ctx, queueType, queueName); err != nil {
					return nil, fmt.Errorf("%w; validate target authority: %v", queueV2AlreadyExistsError(queueType, queueName), err)
				}
				return nil, queueV2AlreadyExistsError(queueType, queueName)
			default:
				return nil, fmt.Errorf(
					"%w; target metadata has non-resumable %s authority",
					queueV2AlreadyExistsError(queueType, queueName),
					existingRecord.authority,
				)
			}
		}
		target := creating
		target.authority = queueV2MigrationAuthorityTarget
		target.epoch = creating.epoch + 1
		if err := s.initializeQueueV2MessageTarget(ctx, queueType, queueName, target); err != nil {
			return nil, err
		}
		if err := transitionQueueV2TargetMetadataRecord(
			ctx,
			s.session,
			queueType,
			queueName,
			creating,
			target,
		); err != nil {
			return nil, err
		}
		s.cacheQueueV2Authority(queueType, queueName, target)
		if !applied {
			return nil, queueV2AlreadyExistsError(queueType, queueName)
		}
	default:
		return nil, fmt.Errorf("unsupported Cassandra QueueV2 migration mode %q", mode)
	}
	s.markKnownQueue(queueType, queueName)
	return &persistence.InternalCreateQueueResponse{}, nil
}

func queueV2AlreadyExistsError(queueType persistence.QueueV2Type, queueName string) error {
	return fmt.Errorf(
		"%w: queue type %v and name %v",
		persistence.ErrQueueAlreadyExists,
		queueType,
		queueName,
	)
}

func (s *queueV2Store) RangeDeleteMessages(
	ctx context.Context,
	request *persistence.InternalRangeDeleteMessagesRequest,
) (*persistence.InternalRangeDeleteMessagesResponse, error) {
	if request.InclusiveMaxMessageMetadata.ID < persistence.FirstQueueMessageID {
		return nil, fmt.Errorf(
			"%w: id is %d but must be >= %d",
			persistence.ErrInvalidQueueRangeDeleteMaxMessageID,
			request.InclusiveMaxMessageMetadata.ID,
			persistence.FirstQueueMessageID,
		)
	}
	queueType := request.QueueType
	queueName := request.QueueName
	route, err := s.resolveQueueV2Route(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	q, err := s.getQueueFromLayout(ctx, queueName, queueType, route.layout)
	if err != nil {
		return nil, err
	}
	if route.mirror && route.layout == queueV2MetadataLayoutTarget {
		if err := s.mirrorQueueV2MetadataForRoute(ctx, queueType, queueName, q, route); err != nil {
			return nil, err
		}
	}
	partition, err := persistence.GetPartitionForQueueV2(queueType, queueName, q.Metadata)
	if err != nil {
		return nil, err
	}
	maxMessageID, ok, err := s.getMaxMessageIDFromLayout(ctx, queueType, queueName, route.layout)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Nothing in the queue to delete.
		return &persistence.InternalRangeDeleteMessagesResponse{}, nil
	}
	// A retry after metadata was committed but physical deletion failed must finish the
	// idempotent cleanup without moving the queue's logical minimum again.
	if request.InclusiveMaxMessageMetadata.ID < partition.MinMessageId ||
		partition.MinMessageId > maxMessageID {
		err := s.deleteMessageRange(
			ctx,
			queueType,
			queueName,
			persistence.FirstQueueMessageID,
			min(request.InclusiveMaxMessageMetadata.ID, maxMessageID-1),
			route,
		)
		if err != nil {
			return nil, err
		}
		return &persistence.InternalRangeDeleteMessagesResponse{}, nil
	}
	deleteRange, ok := persistence.GetDeleteRange(persistence.DeleteRequest{
		LastIDToDeleteInclusive: request.InclusiveMaxMessageMetadata.ID,
		ExistingMessageRange: persistence.InclusiveMessageRange{
			MinMessageID: partition.MinMessageId,
			MaxMessageID: maxMessageID,
		},
	})
	if !ok {
		return &persistence.InternalRangeDeleteMessagesResponse{}, nil
	}
	partition.MinMessageId = deleteRange.NewMinMessageID
	err = s.updateQueue(ctx, q, queueType, queueName, route)
	if err != nil {
		return nil, err
	}
	err = s.deleteMessageRange(
		ctx,
		queueType,
		queueName,
		deleteRange.MinMessageID,
		deleteRange.MaxMessageID,
		route,
	)
	if err != nil {
		return nil, err
	}
	return &persistence.InternalRangeDeleteMessagesResponse{
		MessagesDeleted: deleteRange.MessagesToDelete,
	}, nil
}

func (s *queueV2Store) deleteMessageRange(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	minMessageID int64,
	maxMessageID int64,
	route queueV2OperationRoute,
) error {
	if maxMessageID < minMessageID {
		return nil
	}
	primary := queueV2MessageLayoutSource
	if route.layout == queueV2MetadataLayoutTarget {
		primary = queueV2MessageLayoutTarget
	}
	if err := s.deleteMessageRangeFromLayout(
		ctx,
		queueType,
		queueName,
		minMessageID,
		maxMessageID,
		primary,
		route.record,
		route.guarded,
	); err != nil {
		return err
	}
	if route.mirror {
		mirror := queueV2MessageLayoutTarget
		if primary == queueV2MessageLayoutTarget {
			mirror = queueV2MessageLayoutSource
		}
		return s.deleteMessageRangeFromLayout(
			ctx,
			queueType,
			queueName,
			minMessageID,
			maxMessageID,
			mirror,
			route.record,
			true,
		)
	}
	return nil
}

func (s *queueV2Store) deleteSourceMessageRange(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	minMessageID int64,
	maxMessageID int64,
	record queueV2AuthorityRecord,
	guarded bool,
) error {
	if guarded {
		batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
		batch.Query(
			TemplateRangeDeleteMessagesQuery,
			queueType,
			queueName,
			0,
			minMessageID,
			maxMessageID,
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
			return gocql.ConvertError("QueueV2RangeDeleteMessagesGuarded", err)
		}
		if !applied {
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
			return ErrUpdateQueueConflict
		}
		return nil
	}
	err := s.session.Query(
		TemplateRangeDeleteMessagesQuery,
		queueType,
		queueName,
		0, // partition
		minMessageID,
		maxMessageID,
	).WithContext(ctx).Exec()
	if err != nil {
		return gocql.ConvertError("QueueV2RangeDeleteMessages", err)
	}
	return nil
}

func (s *queueV2Store) updateQueue(
	ctx context.Context,
	q *Queue,
	queueType persistence.QueueV2Type,
	queueName string,
	route queueV2OperationRoute,
) error {
	metadataPayload, err := q.Metadata.Marshal()
	if err != nil {
		return serialization.NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	version := q.Version
	nextVersion := version + 1
	row := queueV2MetadataRow{
		queueName:        queueName,
		metadataPayload:  metadataPayload,
		metadataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
		version:          nextVersion,
		authority:        route.record,
	}
	applied, err := s.updateQueueMetadata(ctx, queueType, row, version, route.layout)
	if err != nil {
		return err
	}
	if !applied {
		s.forgetKnownQueue(queueType, queueName)
		if route.guarded {
			record, authorityErr := readQueueV2MetadataAuthority(
				ctx,
				s.session,
				queueType,
				queueName,
				route.layout,
			)
			if authorityErr != nil {
				return authorityErr
			}
			if record != route.record {
				return serviceerror.NewUnavailablef(
					"QueueV2 queue type %d and name %q authority changed during metadata update",
					queueType,
					queueName,
				)
			}
		}
		return fmt.Errorf(
			"%w: queue type %v and name %v",
			ErrUpdateQueueConflict,
			queueType,
			queueName,
		)
	}
	q.Version = nextVersion
	if route.mirror {
		if err := s.mirrorQueueV2MetadataRowForRoute(ctx, queueType, row, route); err != nil {
			s.forgetKnownQueue(queueType, queueName)
			return err
		}
	}
	s.markKnownQueue(queueType, queueName)
	return nil
}

func (s *queueV2Store) tryInsert(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	blob *commonpb.DataBlob,
	messageID int64,
) error {
	applied, err := s.session.Query(
		TemplateEnqueueMessageQuery,
		queueType,
		queueName,
		0,
		messageID,
		blob.Data,
		blob.EncodingType.String(),
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("QueueV2EnqueueMessage", err)
	}
	if !applied {
		return ErrEnqueueMessageConflict
	}

	return nil
}

func (s *queueV2Store) getQueue(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	name string,
) (*Queue, error) {
	route, err := s.resolveQueueV2Route(ctx, queueType, name)
	if err != nil {
		return nil, err
	}
	q, err := s.getQueueFromLayout(ctx, name, queueType, route.layout)
	if err != nil {
		return nil, err
	}
	if route.mirror && !route.shadow {
		if err := s.mirrorQueueV2MetadataForRoute(ctx, queueType, name, q, route); err != nil {
			return nil, err
		}
	} else if route.shadow {
		target, targetErr := s.getQueueFromLayout(ctx, name, queueType, queueV2MetadataLayoutTarget)
		if targetErr != nil || !queueV2MetadataEqual(q, target) {
			s.logger.Warn(
				"Cassandra QueueV2 metadata shadow read mismatch",
				tag.NewStringTag("queue-name", name),
				tag.NewInt("queue-type", int(queueType)),
				tag.Error(targetErr),
			)
		}
	}
	s.markKnownQueue(queueType, name)
	return q, nil
}

func (s *queueV2Store) mirrorQueueV2MetadataForRoute(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	queue *Queue,
	route queueV2OperationRoute,
) error {
	row, err := queueV2MetadataRowFromQueue(queueName, queue)
	if err != nil {
		return err
	}
	row.authority = route.record
	return s.mirrorQueueV2MetadataRowForRoute(ctx, queueType, row, route)
}

func (s *queueV2Store) mirrorQueueV2MetadataRowForRoute(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	row queueV2MetadataRow,
	route queueV2OperationRoute,
) error {
	mirrorLayout := queueV2MetadataLayoutTarget
	if route.layout == queueV2MetadataLayoutTarget {
		mirrorLayout = queueV2MetadataLayoutSource
	}
	return s.mirrorQueueMetadata(ctx, queueType, row, mirrorLayout)
}

func (s *queueV2Store) isKnownQueue(queueType persistence.QueueV2Type, queueName string) bool {
	s.knownQueuesMu.RLock()
	defer s.knownQueuesMu.RUnlock()
	_, ok := s.knownQueues[queueV2Key{
		queueType: queueType,
		queueName: queueName,
	}]
	return ok
}

func (s *queueV2Store) markKnownQueue(queueType persistence.QueueV2Type, queueName string) {
	key := queueV2Key{
		queueType: queueType,
		queueName: queueName,
	}
	s.knownQueuesMu.Lock()
	defer s.knownQueuesMu.Unlock()
	if _, ok := s.knownQueues[key]; ok {
		return
	}
	if len(s.knownQueues) >= queueV2KnownQueuesSize {
		clear(s.knownQueues)
		clear(s.queueAuthorities)
	}
	if s.knownQueues == nil {
		s.knownQueues = make(map[queueV2Key]struct{})
	}
	s.knownQueues[key] = struct{}{}
}

func (s *queueV2Store) forgetKnownQueue(queueType persistence.QueueV2Type, queueName string) {
	s.knownQueuesMu.Lock()
	defer s.knownQueuesMu.Unlock()
	key := queueV2Key{
		queueType: queueType,
		queueName: queueName,
	}
	delete(s.knownQueues, key)
	delete(s.queueAuthorities, key)
}

func (s *queueV2Store) lockQueue(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) (func(), error) {
	lock := &s.queueLocks[queueV2LockIndex(queueType, queueName)]
	lock.initialize.Do(func() {
		lock.semaphore = make(chan struct{}, 1)
	})
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case lock.semaphore <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-lock.semaphore
			return nil, err
		}
		return func() {
			<-lock.semaphore
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func queueV2LockIndex(queueType persistence.QueueV2Type, queueName string) uint32 {
	return queueV2Hash(queueType, queueName) % queueV2LockStripes
}

func queueV2MetadataBucket(queueType persistence.QueueV2Type, queueName string) int {
	return int(queueV2Hash(queueType, queueName) % queueV2MetadataBucketCount)
}

func queueV2Hash(queueType persistence.QueueV2Type, queueName string) uint32 {
	const (
		fnvOffset32 = uint32(2166136261)
		fnvPrime32  = uint32(16777619)
	)
	hash := fnvOffset32
	hash ^= uint32(queueType)
	hash *= fnvPrime32
	for i := range len(queueName) {
		hash ^= uint32(queueName[i])
		hash *= fnvPrime32
	}
	return hash
}

func GetQueue(
	ctx context.Context,
	session gocql.Session,
	queueName string,
	queueType persistence.QueueV2Type,
) (*Queue, error) {
	store := queueV2Store{session: session}
	return store.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutSource)
}

func (s *queueV2Store) getQueueFromLayout(
	ctx context.Context,
	queueName string,
	queueType persistence.QueueV2Type,
	layout queueV2MetadataLayout,
) (*Queue, error) {
	var (
		queueBytes       []byte
		queueEncodingStr string
		version          int64
	)

	query, args := queueV2GetQueueQuery(layout, queueType, queueName)
	err := s.session.Query(query, args...).WithContext(ctx).Scan(
		&queueBytes,
		&queueEncodingStr,
		&version,
	)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return nil, persistence.NewQueueNotFoundError(queueType, queueName)
		}
		return nil, gocql.ConvertError(queueV2LayoutOperation("QueueV2GetQueue", layout), err)
	}
	return getQueueFromMetadata(queueType, queueName, queueBytes, queueEncodingStr, version)
}

func queueV2GetQueueQuery(
	layout queueV2MetadataLayout,
	queueType persistence.QueueV2Type,
	queueName string,
) (string, []any) {
	if layout == queueV2MetadataLayoutTarget {
		return TemplateGetQueueV2Query, []any{
			queueType,
			queueV2MetadataBucket(queueType, queueName),
			queueName,
		}
	}
	return TemplateGetQueueQuery, []any{queueType, queueName}
}

func queueV2MetadataEqual(left *Queue, right *Queue) bool {
	if left == nil || right == nil || left.Version != right.Version {
		return false
	}
	return left.Metadata.Equal(right.Metadata)
}

func queueV2MetadataRowFromQueue(queueName string, queue *Queue) (queueV2MetadataRow, error) {
	metadataPayload, err := queue.Metadata.Marshal()
	if err != nil {
		return queueV2MetadataRow{}, serialization.NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return queueV2MetadataRow{
		queueName:        queueName,
		metadataPayload:  metadataPayload,
		metadataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
		version:          queue.Version,
	}, nil
}

func (s *queueV2Store) createQueueMetadata(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	row queueV2MetadataRow,
	layout queueV2MetadataLayout,
) (bool, error) {
	query := TemplateCreateQueueQuery
	args := []any{
		queueType,
		row.queueName,
		row.metadataPayload,
		row.metadataEncoding,
		row.version,
	}
	if !row.authority.isUnspecified() {
		query = templateCreateQueueV2SourceWithAuthority
		args = append(args,
			int(row.authority.authority),
			row.authority.generation,
			row.authority.epoch,
			row.authority.messageSpan,
		)
	}
	if layout == queueV2MetadataLayoutTarget {
		query = TemplateCreateQueueV2Query
		args = []any{
			queueType,
			queueV2MetadataBucket(queueType, row.queueName),
			row.queueName,
			row.metadataPayload,
			row.metadataEncoding,
			row.version,
		}
		if !row.authority.isUnspecified() {
			query = templateCreateQueueV2TargetWithAuthority
			args = append(args,
				int(row.authority.authority),
				row.authority.generation,
				row.authority.epoch,
				row.authority.messageSpan,
			)
		}
	}
	applied, err := s.session.Query(query, args...).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return false, gocql.ConvertError(queueV2LayoutOperation("QueueV2CreateQueue", layout), err)
	}
	return applied, nil
}

func (s *queueV2Store) updateQueueMetadata(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	row queueV2MetadataRow,
	expectedVersion int64,
	layout queueV2MetadataLayout,
) (bool, error) {
	query := TemplateUpdateQueueMetadataQuery
	args := []any{
		row.metadataPayload,
		row.metadataEncoding,
		row.version,
		queueType,
		row.queueName,
		expectedVersion,
	}
	if !row.authority.isUnspecified() {
		query = templateUpdateQueueV2SourceWithAuthority
		args = append(args,
			int(row.authority.authority),
			row.authority.generation,
			row.authority.epoch,
			row.authority.messageSpan,
		)
	}
	if layout == queueV2MetadataLayoutTarget {
		query = TemplateUpdateQueueMetadataV2Query
		args = []any{
			row.metadataPayload,
			row.metadataEncoding,
			row.version,
			queueType,
			queueV2MetadataBucket(queueType, row.queueName),
			row.queueName,
			expectedVersion,
		}
		if !row.authority.isUnspecified() {
			query = templateUpdateQueueV2TargetWithAuthority
			args = append(args,
				int(row.authority.authority),
				row.authority.generation,
				row.authority.epoch,
				row.authority.messageSpan,
			)
		}
	}
	applied, err := s.session.Query(query, args...).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return false, gocql.ConvertError(queueV2LayoutOperation("QueueV2UpdateQueueMetadata", layout), err)
	}
	return applied, nil
}

//nolint:revive // Mirroring handles create, update, and idempotent conflict repair for either layout.
func (s *queueV2Store) mirrorQueueMetadata(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	row queueV2MetadataRow,
	layout queueV2MetadataLayout,
) error {
	expected, err := getQueueFromMetadata(
		queueType,
		row.queueName,
		row.metadataPayload,
		row.metadataEncoding,
		row.version,
	)
	if err != nil {
		return err
	}
	for range 2 {
		existing, err := s.getQueueFromLayout(ctx, row.queueName, queueType, layout)
		if err == nil {
			if !row.authority.isUnspecified() {
				existingAuthority, authorityErr := readQueueV2MetadataAuthority(
					ctx,
					s.session,
					queueType,
					row.queueName,
					layout,
				)
				if authorityErr != nil {
					return authorityErr
				}
				if existingAuthority != row.authority {
					return serviceerror.NewUnavailablef(
						"QueueV2 metadata mirror for queue type %d and name %q expected authority %s at epoch %d; got %s at epoch %d",
						queueType,
						row.queueName,
						row.authority.authority,
						row.authority.epoch,
						existingAuthority.authority,
						existingAuthority.epoch,
					)
				}
			}
			switch {
			case existing.Version > row.version:
				return nil
			case queueV2MetadataEqual(existing, expected):
				return nil
			case existing.Version == row.version:
				return queueV2MetadataMirrorConflictError(queueType, row)
			}
			applied, err := s.updateQueueMetadata(ctx, queueType, row, existing.Version, layout)
			if err != nil {
				return err
			}
			if applied {
				return nil
			}
			continue
		}
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return fmt.Errorf("read mirrored QueueV2 metadata: %w", err)
		}
		applied, err := s.createQueueMetadata(ctx, queueType, row, layout)
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
	}
	return queueV2MetadataMirrorConflictError(queueType, row)
}

func queueV2MetadataMirrorConflictError(
	queueType persistence.QueueV2Type,
	row queueV2MetadataRow,
) error {
	return fmt.Errorf(
		"queueV2 metadata mirror conflict for queue type %v and name %v at version %d",
		queueType,
		row.queueName,
		row.version,
	)
}

func queueV2LayoutOperation(operation string, layout queueV2MetadataLayout) string {
	if layout == queueV2MetadataLayoutTarget {
		return operation + "V2"
	}
	return operation
}

func getQueueFromMetadata(
	queueType persistence.QueueV2Type,
	queueName string,
	queueBytes []byte,
	queueEncodingStr string,
	version int64,
) (*Queue, error) {
	if queueEncodingStr != enumspb.ENCODING_TYPE_PROTO3.String() {
		return nil, fmt.Errorf(
			"%w: invalid queue encoding type: queue with type %v and name %v has invalid encoding",
			serialization.NewUnknownEncodingTypeError(queueEncodingStr, enumspb.ENCODING_TYPE_PROTO3),
			queueType,
			queueName,
		)
	}

	q := &persistencespb.Queue{}
	err := q.Unmarshal(queueBytes)
	if err != nil {
		return nil, serialization.NewDeserializationError(
			enumspb.ENCODING_TYPE_PROTO3,
			fmt.Errorf("%w: unmarshal queue payload: failed for queue with type %v and name %v",
				err, queueType, queueName),
		)
	}

	return &Queue{
		Metadata: q,
		Version:  version,
	}, nil
}

func (s *queueV2Store) getMessageCountAndLastID(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	partition *persistencespb.QueuePartition,
) (messageCount int64, maxMessageID int64, err error) {
	var ok bool
	maxMessageID, ok, err = s.getMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, -1, nil // No messages
	}
	messageCount = maxMessageID - partition.MinMessageId + 1
	return messageCount, maxMessageID, nil
}

func (s *queueV2Store) getMessageCountAndLastIDFromLayout(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	partition *persistencespb.QueuePartition,
	layout queueV2MetadataLayout,
) (messageCount int64, maxMessageID int64, err error) {
	maxMessageID, ok, err := s.getMaxMessageIDFromLayout(ctx, queueType, queueName, layout)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, -1, nil
	}
	return maxMessageID - partition.MinMessageId + 1, maxMessageID, nil
}

func (s *queueV2Store) getMaxMessageID(ctx context.Context, queueType persistence.QueueV2Type, queueName string) (int64, bool, error) {
	if s.messageReadLayout() == queueV2MessageLayoutTarget {
		return s.getQueueV2TargetMaxMessageID(ctx, queueType, queueName)
	}
	return s.getSourceMaxMessageID(ctx, queueType, queueName)
}

func (s *queueV2Store) getMaxMessageIDFromLayout(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	layout queueV2MetadataLayout,
) (int64, bool, error) {
	if layout == queueV2MetadataLayoutTarget {
		return s.getQueueV2TargetMaxMessageID(ctx, queueType, queueName)
	}
	return s.getSourceMaxMessageID(ctx, queueType, queueName)
}

func (s *queueV2Store) getSourceMaxMessageID(ctx context.Context, queueType persistence.QueueV2Type, queueName string) (int64, bool, error) {
	var maxMessageID int64

	err := s.session.Query(TemplateGetMaxMessageIDQuery, queueType, queueName, 0).WithContext(ctx).Scan(&maxMessageID)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return 0, false, nil
		}
		return 0, false, gocql.ConvertError("QueueV2GetMaxMessageID", err)
	}
	return maxMessageID, true, nil
}

func (s *queueV2Store) ListQueues(
	ctx context.Context,
	request *persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	if request.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveListQueuesPageSize
	}
	if normalizeQueueV2MigrationMode(s.migrationMode) == config.CassandraQueueV2MigrationModeTargetOnly {
		return s.listQueuesTarget(ctx, request)
	}
	return s.listQueuesSource(ctx, request)
}

//nolint:revive // Source listing filters migration sentinels while preserving paginated queue ordering.
func (s *queueV2Store) listQueuesSource(
	ctx context.Context,
	request *persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	if bytes.HasPrefix(request.NextPageToken, []byte(queueV2ListTokenEnvelopePrefix)) {
		return nil, fmt.Errorf(
			"%w: target-layout token cannot continue a source-layout query",
			persistence.ErrInvalidListQueuesNextPageToken,
		)
	}
	queues := make([]persistence.QueueInfo, 0, preallocatedResultCapacity(request.PageSize))
	nextPageToken := request.NextPageToken
	for len(queues) < request.PageSize {
		initialQueueCount := len(queues)
		iter := s.session.Query(
			templateGetQueueNamesQuery,
			request.QueueType,
		).PageSize(request.PageSize - len(queues)).PageState(nextPageToken).WithContext(ctx).Iter()
		for len(queues) < request.PageSize {
			var (
				queueName        string
				metadataBytes    []byte
				metadataEncoding string
				version          int64
			)
			if !iter.Scan(&queueName, &metadataBytes, &metadataEncoding, &version) {
				break
			}
			q, err := getQueueFromMetadata(request.QueueType, queueName, metadataBytes, metadataEncoding, version)
			if err != nil {
				_ = iter.Close()
				return nil, err
			}
			route, err := s.resolveQueueV2Route(ctx, request.QueueType, queueName)
			if err != nil {
				_ = iter.Close()
				return nil, err
			}
			if route.layout == queueV2MetadataLayoutTarget {
				q, err = s.getQueueFromLayout(ctx, queueName, request.QueueType, queueV2MetadataLayoutTarget)
				if err != nil {
					_ = iter.Close()
					return nil, err
				}
			}
			switch {
			case route.mirror && !route.shadow:
				if err := s.mirrorQueueV2MetadataForRoute(
					ctx,
					request.QueueType,
					queueName,
					q,
					route,
				); err != nil {
					_ = iter.Close()
					return nil, err
				}
			case route.shadow:
				target, targetErr := s.getQueueFromLayout(
					ctx,
					queueName,
					request.QueueType,
					queueV2MetadataLayoutTarget,
				)
				if targetErr != nil || !queueV2MetadataEqual(q, target) {
					s.logger.Warn(
						"Cassandra QueueV2 metadata shadow list mismatch",
						tag.NewStringTag("queue-name", queueName),
						tag.NewInt("queue-type", int(request.QueueType)),
						tag.Error(targetErr),
					)
				}
			default:
			}
			partition, err := persistence.GetPartitionForQueueV2(request.QueueType, queueName, q.Metadata)
			if err != nil {
				_ = iter.Close()
				return nil, err
			}
			messageCount, lastMessageID, err := s.getMessageCountAndLastIDFromLayout(
				ctx,
				request.QueueType,
				queueName,
				partition,
				route.layout,
			)
			if err != nil {
				_ = iter.Close()
				return nil, err
			}
			queues = append(queues, persistence.QueueInfo{
				QueueName:     queueName,
				MessageCount:  messageCount,
				LastMessageID: lastMessageID,
			})
		}
		iterPageToken := append([]byte(nil), iter.PageState()...)
		if err := iter.Close(); err != nil {
			return nil, gocql.ConvertError("QueueV2ListQueues", err)
		}
		if len(iterPageToken) == 0 {
			nextPageToken = nil
			break
		}
		if bytes.Equal(iterPageToken, nextPageToken) && len(queues) == initialQueueCount {
			nextPageToken = nil
			break
		}
		nextPageToken = iterPageToken
	}
	return &persistence.InternalListQueuesResponse{
		Queues:        queues,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *queueV2Store) listQueuesTarget(
	ctx context.Context,
	request *persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	rows, nextPageToken, err := s.readQueueV2MetadataPage(
		ctx,
		request.QueueType,
		request.PageSize,
		request.NextPageToken,
	)
	if err != nil {
		return nil, err
	}

	queues := make([]persistence.QueueInfo, 0, len(rows))
	for _, row := range rows {
		queueInfo, err := s.queueInfoFromMetadataRow(ctx, request.QueueType, row)
		if err != nil {
			return nil, err
		}
		queues = append(queues, queueInfo)
	}

	return &persistence.InternalListQueuesResponse{
		Queues:        queues,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *queueV2Store) readQueueV2MetadataPage(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	pageSize int,
	nextPageToken []byte,
) ([]queueV2MetadataRow, []byte, error) {
	cursor, hasCursor, err := decodeQueueV2ListPageToken(nextPageToken, queueType)
	if err != nil {
		return nil, nil, err
	}

	rowsByBucket := make([][]queueV2MetadataRow, queueV2MetadataBucketCount)
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(16)
	for bucket := range queueV2MetadataBucketCount {
		group.Go(func() error {
			rows, err := s.readQueueV2MetadataBucket(
				groupCtx,
				queueType,
				bucket,
				cursor,
				hasCursor,
				pageSize,
			)
			if err != nil {
				return err
			}
			rowsByBucket[bucket] = rows
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, nil, err
	}

	var rows []queueV2MetadataRow
	seenNames := make(map[string]int)
	for _, bucketRows := range rowsByBucket {
		for _, row := range bucketRows {
			expectedBucket := queueV2MetadataBucket(queueType, row.queueName)
			if row.bucket != expectedBucket {
				return nil, nil, fmt.Errorf(
					"QueueV2 metadata %q is in bucket %d, expected %d",
					row.queueName,
					row.bucket,
					expectedBucket,
				)
			}
			if previousBucket, ok := seenNames[row.queueName]; ok {
				return nil, nil, fmt.Errorf(
					"QueueV2 metadata %q is duplicated in buckets %d and %d",
					row.queueName,
					previousBucket,
					row.bucket,
				)
			}
			seenNames[row.queueName] = row.bucket
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i int, j int) bool {
		return rows[i].queueName < rows[j].queueName
	})
	if len(rows) > pageSize {
		rows = rows[:pageSize]
	}
	if len(rows) == pageSize {
		return rows, encodeQueueV2ListPageToken(queueType, rows[len(rows)-1].queueName), nil
	}
	return rows, nil, nil
}

func (s *queueV2Store) readQueueV2MetadataBucket(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	bucket int,
	cursor string,
	hasCursor bool,
	pageSize int,
) ([]queueV2MetadataRow, error) {
	query := templateGetQueueNamesV2FirstPageQuery
	args := []any{queueType, bucket, pageSize}
	if hasCursor {
		query = templateGetQueueNamesV2ContinuationQuery
		args = []any{queueType, bucket, cursor, pageSize}
	}
	iter := s.session.Query(query, args...).WithContext(ctx).Iter()
	rows := make([]queueV2MetadataRow, 0, preallocatedResultCapacity(pageSize))
	for {
		row := queueV2MetadataRow{bucket: bucket}
		if !iter.Scan(
			&row.queueName,
			&row.metadataPayload,
			&row.metadataEncoding,
			&row.version,
		) {
			break
		}
		rows = append(rows, row)
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("QueueV2ListQueuesV2", err)
	}
	return rows, nil
}

func (s *queueV2Store) queueInfoFromMetadataRow(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	row queueV2MetadataRow,
) (persistence.QueueInfo, error) {
	route, err := s.resolveQueueV2Route(ctx, queueType, row.queueName)
	if err != nil {
		return persistence.QueueInfo{}, err
	}
	if route.layout != queueV2MetadataLayoutTarget {
		return persistence.QueueInfo{}, serviceerror.NewUnavailablef(
			"QueueV2 target list encountered source-authoritative queue type %d and name %q",
			queueType,
			row.queueName,
		)
	}
	q, err := getQueueFromMetadata(
		queueType,
		row.queueName,
		row.metadataPayload,
		row.metadataEncoding,
		row.version,
	)
	if err != nil {
		return persistence.QueueInfo{}, err
	}
	partition, err := persistence.GetPartitionForQueueV2(queueType, row.queueName, q.Metadata)
	if err != nil {
		return persistence.QueueInfo{}, err
	}
	messageCount, lastMessageID, err := s.getMessageCountAndLastIDFromLayout(
		ctx,
		queueType,
		row.queueName,
		partition,
		route.layout,
	)
	if err != nil {
		return persistence.QueueInfo{}, err
	}
	return persistence.QueueInfo{
		QueueName:     row.queueName,
		MessageCount:  messageCount,
		LastMessageID: lastMessageID,
	}, nil
}

func encodeQueueV2ListPageToken(queueType persistence.QueueV2Type, cursor string) []byte {
	token := make([]byte, queueV2ListTokenHeaderLength+len(cursor))
	offset := copy(token, queueV2ListTokenEnvelopePrefix)
	token[offset] = queueV2ListTokenVersion
	offset++
	binary.BigEndian.PutUint16(token[offset:], uint16(queueV2MetadataBucketCount))
	offset += 2
	binary.BigEndian.PutUint32(token[offset:], uint32(queueType))
	offset += 4
	binary.BigEndian.PutUint32(token[offset:], uint32(len(cursor)))
	offset += 4
	copy(token[offset:], cursor)
	return token
}

func decodeQueueV2ListPageToken(
	token []byte,
	queueType persistence.QueueV2Type,
) (string, bool, error) {
	if len(token) == 0 {
		return "", false, nil
	}
	prefix := []byte(queueV2ListTokenEnvelopePrefix)
	if !bytes.HasPrefix(token, prefix) || len(token) < queueV2ListTokenHeaderLength {
		return "", false, fmt.Errorf(
			"%w: target-layout token envelope is invalid",
			persistence.ErrInvalidListQueuesNextPageToken,
		)
	}
	offset := len(prefix)
	if token[offset] != queueV2ListTokenVersion {
		return "", false, fmt.Errorf(
			"%w: target-layout token version %d is invalid",
			persistence.ErrInvalidListQueuesNextPageToken,
			token[offset],
		)
	}
	offset++
	if int(binary.BigEndian.Uint16(token[offset:])) != queueV2MetadataBucketCount {
		return "", false, fmt.Errorf(
			"%w: target-layout bucket count changed; restart pagination",
			persistence.ErrInvalidListQueuesNextPageToken,
		)
	}
	offset += 2
	if persistence.QueueV2Type(int32(binary.BigEndian.Uint32(token[offset:]))) != queueType {
		return "", false, fmt.Errorf(
			"%w: target-layout queue type changed; restart pagination",
			persistence.ErrInvalidListQueuesNextPageToken,
		)
	}
	offset += 4
	cursorLength := int(binary.BigEndian.Uint32(token[offset:]))
	offset += 4
	if cursorLength != len(token)-offset {
		return "", false, fmt.Errorf(
			"%w: target-layout token length is invalid",
			persistence.ErrInvalidListQueuesNextPageToken,
		)
	}
	return string(token[offset:]), true, nil
}
