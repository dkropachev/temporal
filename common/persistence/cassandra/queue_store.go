package cassandra

import (
	"context"
	"fmt"
	"math"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
)

const (
	templateEnqueueMessageQuery       = `INSERT INTO queue (queue_type, message_id, message_payload, message_encoding) VALUES(?, ?, ?, ?) IF NOT EXISTS`
	templateGetLastMessageIDQuery     = `SELECT message_id FROM queue WHERE queue_type = ? AND message_id >= ? ORDER BY message_id DESC LIMIT 1`
	templateGetMessagesQuery          = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = ? AND message_id >= ? LIMIT ?`
	templateGetMessagesFromDLQQuery   = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = ? AND message_id >= ? AND message_id <= ?`
	templateDeleteMessagesBeforeQuery = `DELETE FROM queue WHERE queue_type = ? AND message_id >= ? AND message_id < ?`
	templateDeleteMessagesQuery       = `DELETE FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ?`
	templateDeleteMessageQuery        = `DELETE FROM queue WHERE queue_type = ? and message_id = ?`

	templateGetQueueMetadataQuery    = `SELECT cluster_ack_level, data, data_encoding, version FROM queue_metadata WHERE queue_type = ?`
	templateInsertQueueMetadataQuery = `INSERT INTO queue_metadata (queue_type, cluster_ack_level, data, data_encoding, version) VALUES(?, ?, ?, ?, ?) IF NOT EXISTS`
	templateUpdateQueueMetadataQuery = `UPDATE queue_metadata SET cluster_ack_level = ?, data = ?, data_encoding = ?, version = ? WHERE queue_type = ? IF version = ?`
)

type (
	QueueStore struct {
		queueType         persistence.QueueType
		session           gocql.Session
		logger            log.Logger
		serializer        serialization.Serializer
		migrationMode     config.CassandraLegacyQueueMigrationMode
		messageBucketSize int64
		activeBucketsMu   sync.RWMutex
		activeBuckets     map[persistence.QueueType]int64
	}
)

func NewQueueStore(
	queueType persistence.QueueType,
	session gocql.Session,
	logger log.Logger,
) (persistence.Queue, error) {
	return NewQueueStoreWithMigrationMode(
		queueType,
		session,
		logger,
		config.CassandraLegacyQueueMigrationModeSourceOnly,
		DefaultLegacyQueueV2MessageBucketSize,
	)
}

func NewQueueStoreWithMigrationMode(
	queueType persistence.QueueType,
	session gocql.Session,
	logger log.Logger,
	migrationMode config.CassandraLegacyQueueMigrationMode,
	messageBucketSize int64,
) (persistence.Queue, error) {
	migrationMode = normalizeLegacyQueueMigrationMode(migrationMode)
	if err := ValidateLegacyQueueMigrationMode(migrationMode); err != nil {
		return nil, err
	}
	if messageBucketSize <= 0 {
		return nil, fmt.Errorf("cassandra legacy queue message bucket size must be positive: %d", messageBucketSize)
	}
	return &QueueStore{
		queueType:         queueType,
		session:           session,
		logger:            logger,
		serializer:        serialization.NewSerializer(),
		migrationMode:     migrationMode,
		messageBucketSize: messageBucketSize,
		activeBuckets:     make(map[persistence.QueueType]int64, 2),
	}, nil
}

func (q *QueueStore) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	if err := q.initializeQueueMetadata(ctx, blob); err != nil {
		return err
	}
	if err := q.initializeDLQMetadata(ctx, blob); err != nil {
		return err
	}
	return q.initializeLegacyQueueV2StateForMode(ctx)
}

func (q *QueueStore) EnqueueMessage(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	_, err := q.enqueueMessage(ctx, q.queueType, blob)
	return err
}

func (q *QueueStore) EnqueueMessageToDLQ(
	ctx context.Context,
	blob *commonpb.DataBlob,
) (int64, error) {
	// Use negative queue type as the dlq type
	return q.enqueueMessage(ctx, q.getDLQTypeFromQueueType(), blob)
}

func (q *QueueStore) enqueueMessage(
	ctx context.Context,
	queueType persistence.QueueType,
	blob *commonpb.DataBlob,
) (int64, error) {
	switch q.migrationMode {
	case config.CassandraLegacyQueueMigrationModeSourceOnly:
		return q.enqueueMessageSource(ctx, queueType, blob)
	case config.CassandraLegacyQueueMigrationModeSourceDual:
		return q.enqueueMessageSourceDual(ctx, queueType, blob)
	case config.CassandraLegacyQueueMigrationModeTargetDual:
		authority, err := q.resolveLegacyQueueTargetDualAuthority(ctx, queueType)
		if err != nil {
			return persistence.EmptyQueueMessageID, err
		}
		switch authority {
		case legacyQueueMigrationAuthoritySource:
			return q.enqueueMessageSourceDual(ctx, queueType, blob)
		case legacyQueueMigrationAuthoritySealing:
			return persistence.EmptyQueueMessageID, q.legacyQueueSealingError(queueType)
		case legacyQueueMigrationAuthorityTarget:
			return q.enqueueMessageLegacyQueueV2TargetDual(ctx, queueType, blob)
		default:
			return persistence.EmptyQueueMessageID, fmt.Errorf("invalid legacy queue authority %s", authority)
		}
	case config.CassandraLegacyQueueMigrationModeTargetOnly:
		return q.enqueueMessageLegacyQueueV2(ctx, queueType, blob)
	default:
		return persistence.EmptyQueueMessageID, unsupportedLegacyQueueMigrationMode(q.migrationMode)
	}
}

func (q *QueueStore) enqueueMessageSource(
	ctx context.Context,
	queueType persistence.QueueType,
	blob *commonpb.DataBlob,
) (int64, error) {
	lastMessageID, err := q.getLastMessageID(ctx, queueType)
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	messageID := lastMessageID + 1
	err = q.tryEnqueue(ctx, queueType, messageID, blob)
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	return messageID, nil
}

func (q *QueueStore) tryEnqueue(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
	blob *commonpb.DataBlob,
) error {
	applied, err := q.session.Query(templateEnqueueMessageQuery, queueType, messageID, blob.Data, blob.EncodingType.String()).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("tryEnqueue", err)
	}
	if !applied {
		return ErrEnqueueMessageConflict
	}

	return nil
}

func (q *QueueStore) getLastMessageID(
	ctx context.Context,
	queueType persistence.QueueType,
) (int64, error) {

	query := q.session.Query(
		templateGetLastMessageIDQuery,
		queueType,
		int64(persistence.FirstQueueMessageID),
	).WithContext(ctx)
	result := make(map[string]any)
	err := query.MapScan(result)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return persistence.EmptyQueueMessageID, nil
		}
		return persistence.EmptyQueueMessageID, gocql.ConvertError("getLastMessageID", err)
	}
	return result["message_id"].(int64), nil
}

func (q *QueueStore) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*persistence.QueueMessage, error) {
	switch q.migrationMode {
	case config.CassandraLegacyQueueMigrationModeSourceOnly,
		config.CassandraLegacyQueueMigrationModeSourceDual:
		return q.readMessagesSource(ctx, lastMessageID, maxCount)
	case config.CassandraLegacyQueueMigrationModeTargetDual:
		authority, err := q.resolveLegacyQueueTargetDualAuthority(ctx, q.queueType)
		if err != nil {
			return nil, err
		}
		if authority != legacyQueueMigrationAuthorityTarget {
			return q.readMessagesSource(ctx, lastMessageID, maxCount)
		}
		return q.readMessagesLegacyQueueV2(ctx, q.queueType, lastMessageID, maxCount)
	case config.CassandraLegacyQueueMigrationModeTargetOnly:
		return q.readMessagesLegacyQueueV2(ctx, q.queueType, lastMessageID, maxCount)
	default:
		return nil, unsupportedLegacyQueueMigrationMode(q.migrationMode)
	}
}

func (q *QueueStore) readMessagesSource(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*persistence.QueueMessage, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could lose tasks
	if maxCount <= 0 || lastMessageID == math.MaxInt64 {
		return nil, nil
	}
	query := q.session.Query(templateGetMessagesQuery,
		q.queueType,
		max(lastMessageID+1, int64(persistence.FirstQueueMessageID)),
		maxCount,
	).WithContext(ctx)

	iter := query.Iter()

	var result []*persistence.QueueMessage
	message := make(map[string]any)
	for iter.MapScan(message) {
		queueMessage := convertQueueMessage(message)
		result = append(result, queueMessage)
		message = make(map[string]any)
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ReadMessages", err)
	}

	return result, nil
}

func (q *QueueStore) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {
	switch q.migrationMode {
	case config.CassandraLegacyQueueMigrationModeSourceOnly:
		return q.readMessagesFromDLQSource(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	case config.CassandraLegacyQueueMigrationModeSourceDual:
		return q.readMessagesFromDLQSourceMigrating(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	case config.CassandraLegacyQueueMigrationModeTargetDual:
		queueType := q.getDLQTypeFromQueueType()
		authority, err := q.resolveLegacyQueueTargetDualAuthority(ctx, queueType)
		if err != nil {
			return nil, nil, err
		}
		if authority != legacyQueueMigrationAuthorityTarget {
			return q.readMessagesFromDLQSourceMigrating(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
		}
		return q.readMessagesFromDLQLegacyQueueV2(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	case config.CassandraLegacyQueueMigrationModeTargetOnly:
		return q.readMessagesFromDLQLegacyQueueV2(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	default:
		return nil, nil, unsupportedLegacyQueueMigrationMode(q.migrationMode)
	}
}

func (q *QueueStore) readMessagesFromDLQSource(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could lose tasks
	// Use negative queue type as the dlq type
	query := q.session.Query(templateGetMessagesFromDLQQuery,
		q.getDLQTypeFromQueueType(),
		max(firstMessageID+1, int64(persistence.FirstQueueMessageID)),
		lastMessageID,
	).WithContext(ctx)
	iter := query.PageSize(pageSize).PageState(pageToken).Iter()

	var result []*persistence.QueueMessage
	message := make(map[string]any)
	for iter.MapScan(message) {
		queueMessage := convertQueueMessage(message)
		result = append(result, queueMessage)
		message = make(map[string]any)
	}

	var nextPageToken []byte
	if len(iter.PageState()) > 0 {
		nextPageToken = iter.PageState()
	}
	if err := iter.Close(); err != nil {
		return nil, nil, gocql.ConvertError("ReadMessagesFromDLQ", err)
	}

	return result, nextPageToken, nil
}

func (q *QueueStore) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	switch q.migrationMode {
	case config.CassandraLegacyQueueMigrationModeSourceOnly:
		return q.deleteMessagesBeforeSource(ctx, q.queueType, messageID)
	case config.CassandraLegacyQueueMigrationModeSourceDual:
		if err := q.deleteMessagesBeforeSourceGuarded(ctx, q.queueType, messageID); err != nil {
			return err
		}
		record, err := q.legacyQueueV2TargetAuthorityRecord(
			ctx,
			q.queueType,
			legacyQueueMigrationAuthoritySource,
		)
		if err != nil {
			return err
		}
		return q.deleteMessagesBeforeLegacyQueueV2WithAuthority(ctx, q.queueType, messageID, record)
	case config.CassandraLegacyQueueMigrationModeTargetDual:
		authority, err := q.resolveLegacyQueueTargetDualAuthority(ctx, q.queueType)
		if err != nil {
			return err
		}
		switch authority {
		case legacyQueueMigrationAuthoritySource:
			if err := q.deleteMessagesBeforeSourceGuarded(ctx, q.queueType, messageID); err != nil {
				return err
			}
			record, err := q.legacyQueueV2TargetAuthorityRecord(
				ctx,
				q.queueType,
				legacyQueueMigrationAuthoritySource,
			)
			if err != nil {
				return err
			}
			return q.deleteMessagesBeforeLegacyQueueV2WithAuthority(ctx, q.queueType, messageID, record)
		case legacyQueueMigrationAuthoritySealing:
			return q.legacyQueueSealingError(q.queueType)
		case legacyQueueMigrationAuthorityTarget:
			return q.deleteMessagesBeforeLegacyQueueV2TargetDual(ctx, q.queueType, messageID)
		default:
			return fmt.Errorf("invalid legacy queue authority %s", authority)
		}
	case config.CassandraLegacyQueueMigrationModeTargetOnly:
		return q.deleteMessagesBeforeLegacyQueueV2(ctx, q.queueType, messageID)
	default:
		return unsupportedLegacyQueueMigrationMode(q.migrationMode)
	}
}

func (q *QueueStore) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	queueType := q.getDLQTypeFromQueueType()
	switch q.migrationMode {
	case config.CassandraLegacyQueueMigrationModeSourceOnly:
		return q.deleteMessageSource(ctx, queueType, messageID)
	case config.CassandraLegacyQueueMigrationModeSourceDual:
		if err := q.deleteMessageSourceGuarded(ctx, queueType, messageID); err != nil {
			return err
		}
		record, err := q.legacyQueueV2TargetAuthorityRecord(
			ctx,
			queueType,
			legacyQueueMigrationAuthoritySource,
		)
		if err != nil {
			return err
		}
		return q.deleteMessageLegacyQueueV2WithAuthority(ctx, queueType, messageID, record)
	case config.CassandraLegacyQueueMigrationModeTargetDual:
		authority, err := q.resolveLegacyQueueTargetDualAuthority(ctx, queueType)
		if err != nil {
			return err
		}
		switch authority {
		case legacyQueueMigrationAuthoritySource:
			if err := q.deleteMessageSourceGuarded(ctx, queueType, messageID); err != nil {
				return err
			}
			record, err := q.legacyQueueV2TargetAuthorityRecord(
				ctx,
				queueType,
				legacyQueueMigrationAuthoritySource,
			)
			if err != nil {
				return err
			}
			return q.deleteMessageLegacyQueueV2WithAuthority(ctx, queueType, messageID, record)
		case legacyQueueMigrationAuthoritySealing:
			return q.legacyQueueSealingError(queueType)
		case legacyQueueMigrationAuthorityTarget:
			return q.deleteMessageLegacyQueueV2TargetDual(ctx, queueType, messageID)
		default:
			return fmt.Errorf("invalid legacy queue authority %s", authority)
		}
	case config.CassandraLegacyQueueMigrationModeTargetOnly:
		return q.deleteMessageLegacyQueueV2(ctx, queueType, messageID)
	default:
		return unsupportedLegacyQueueMigrationMode(q.migrationMode)
	}
}

func (q *QueueStore) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	queueType := q.getDLQTypeFromQueueType()
	switch q.migrationMode {
	case config.CassandraLegacyQueueMigrationModeSourceOnly:
		return q.rangeDeleteMessagesSource(ctx, queueType, firstMessageID, lastMessageID)
	case config.CassandraLegacyQueueMigrationModeSourceDual:
		if err := q.rangeDeleteMessagesSourceGuarded(ctx, queueType, firstMessageID, lastMessageID); err != nil {
			return err
		}
		record, err := q.legacyQueueV2TargetAuthorityRecord(
			ctx,
			queueType,
			legacyQueueMigrationAuthoritySource,
		)
		if err != nil {
			return err
		}
		return q.rangeDeleteMessagesLegacyQueueV2WithAuthority(
			ctx,
			queueType,
			firstMessageID,
			lastMessageID,
			record,
		)
	case config.CassandraLegacyQueueMigrationModeTargetDual:
		authority, err := q.resolveLegacyQueueTargetDualAuthority(ctx, queueType)
		if err != nil {
			return err
		}
		switch authority {
		case legacyQueueMigrationAuthoritySource:
			if err := q.rangeDeleteMessagesSourceGuarded(ctx, queueType, firstMessageID, lastMessageID); err != nil {
				return err
			}
			record, err := q.legacyQueueV2TargetAuthorityRecord(
				ctx,
				queueType,
				legacyQueueMigrationAuthoritySource,
			)
			if err != nil {
				return err
			}
			return q.rangeDeleteMessagesLegacyQueueV2WithAuthority(
				ctx,
				queueType,
				firstMessageID,
				lastMessageID,
				record,
			)
		case legacyQueueMigrationAuthoritySealing:
			return q.legacyQueueSealingError(queueType)
		case legacyQueueMigrationAuthorityTarget:
			return q.rangeDeleteMessagesLegacyQueueV2TargetDual(
				ctx,
				queueType,
				firstMessageID,
				lastMessageID,
			)
		default:
			return fmt.Errorf("invalid legacy queue authority %s", authority)
		}
	case config.CassandraLegacyQueueMigrationModeTargetOnly:
		return q.rangeDeleteMessagesLegacyQueueV2(ctx, queueType, firstMessageID, lastMessageID)
	default:
		return unsupportedLegacyQueueMigrationMode(q.migrationMode)
	}
}

func (q *QueueStore) deleteMessagesBeforeSource(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
) error {
	if err := q.session.Query(
		templateDeleteMessagesBeforeQuery,
		queueType,
		int64(persistence.FirstQueueMessageID),
		messageID,
	).WithContext(ctx).Exec(); err != nil {
		return gocql.ConvertError("DeleteMessagesBefore", err)
	}
	return nil
}

func (q *QueueStore) deleteMessagesBeforeSourceGuarded(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
) error {
	_, err := q.executeGuardedLegacyQueueSourceMutation(
		ctx,
		queueType,
		"DeleteMessagesBeforeGuarded",
		templateDeleteMessagesBeforeQuery,
		queueType,
		int64(persistence.FirstQueueMessageID),
		messageID,
	)
	return err
}

func (q *QueueStore) deleteMessagesBeforeSourceWithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
	authority legacyQueueMigrationAuthority,
) error {
	_, err := q.executeGuardedLegacyQueueSourceMutationWithAuthority(
		ctx,
		queueType,
		authority,
		"DeleteLegacyQueueSourceMessagesBeforeGuarded",
		templateDeleteMessagesBeforeQuery,
		queueType,
		int64(persistence.FirstQueueMessageID),
		messageID,
	)
	return err
}

func (q *QueueStore) deleteMessageSource(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
) error {
	if err := q.session.Query(templateDeleteMessageQuery, queueType, messageID).WithContext(ctx).Exec(); err != nil {
		return gocql.ConvertError("DeleteMessageFromDLQ", err)
	}
	return nil
}

func (q *QueueStore) deleteMessageSourceGuarded(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
) error {
	if messageID < persistence.FirstQueueMessageID {
		return nil
	}
	_, err := q.executeGuardedLegacyQueueSourceMutation(
		ctx,
		queueType,
		"DeleteMessageFromDLQGuarded",
		templateDeleteMessageQuery,
		queueType,
		messageID,
	)
	return err
}

func (q *QueueStore) deleteMessageSourceWithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
	authority legacyQueueMigrationAuthority,
) error {
	if messageID < persistence.FirstQueueMessageID {
		return nil
	}
	_, err := q.executeGuardedLegacyQueueSourceMutationWithAuthority(
		ctx,
		queueType,
		authority,
		"DeleteLegacyQueueSourceMessageGuarded",
		templateDeleteMessageQuery,
		queueType,
		messageID,
	)
	return err
}

func (q *QueueStore) rangeDeleteMessagesSource(
	ctx context.Context,
	queueType persistence.QueueType,
	firstMessageID int64,
	lastMessageID int64,
) error {
	if err := q.session.Query(templateDeleteMessagesQuery, queueType, firstMessageID, lastMessageID).WithContext(ctx).Exec(); err != nil {
		return gocql.ConvertError("RangeDeleteMessagesFromDLQ", err)
	}
	return nil
}

func (q *QueueStore) rangeDeleteMessagesSourceGuarded(
	ctx context.Context,
	queueType persistence.QueueType,
	firstMessageID int64,
	lastMessageID int64,
) error {
	_, err := q.executeGuardedLegacyQueueSourceMutation(
		ctx,
		queueType,
		"RangeDeleteMessagesFromDLQGuarded",
		templateDeleteMessagesQuery,
		queueType,
		max(firstMessageID, int64(persistence.EmptyQueueMessageID)),
		lastMessageID,
	)
	return err
}

func (q *QueueStore) rangeDeleteMessagesSourceWithAuthority(
	ctx context.Context,
	queueType persistence.QueueType,
	firstMessageID int64,
	lastMessageID int64,
	authority legacyQueueMigrationAuthority,
) error {
	_, err := q.executeGuardedLegacyQueueSourceMutationWithAuthority(
		ctx,
		queueType,
		authority,
		"RangeDeleteLegacyQueueSourceMessagesGuarded",
		templateDeleteMessagesQuery,
		queueType,
		max(firstMessageID, int64(persistence.EmptyQueueMessageID)),
		lastMessageID,
	)
	return err
}

func (q *QueueStore) legacyQueueSealingError(queueType persistence.QueueType) error {
	return &persistence.ConditionFailedError{Msg: fmt.Sprintf(
		"legacy queue type %d is sealed for target cutover",
		queueType,
	)}
}

func (q *QueueStore) UpdateAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) error {
	return q.updateAckLevel(ctx, metadata, q.queueType)
}

func (q *QueueStore) GetAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	queueMetadata, err := q.getQueueMetadata(ctx, q.queueType)
	if err != nil {
		return nil, gocql.ConvertError("GetAckLevels", err)
	}

	return queueMetadata, nil
}

func (q *QueueStore) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) error {
	return q.updateAckLevel(ctx, metadata, q.getDLQTypeFromQueueType())
}

func (q *QueueStore) GetDLQAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	// Use negative queue type as the dlq type
	queueMetadata, err := q.getQueueMetadata(ctx, q.getDLQTypeFromQueueType())
	if err != nil {
		return nil, gocql.ConvertError("GetDLQAckLevels", err)
	}

	return queueMetadata, nil
}

func (q *QueueStore) insertInitialQueueMetadataRecord(
	ctx context.Context,
	queueType persistence.QueueType,
	blob *commonpb.DataBlob,
) error {

	version := 0
	// TODO: remove once cluster_ack_level is removed from DB
	clusterAckLevels := map[string]int64{}
	query := q.session.Query(templateInsertQueueMetadataQuery,
		queueType,
		clusterAckLevels,
		blob.Data,
		blob.EncodingType.String(),
		version,
	).WithContext(ctx)
	_, err := query.MapScanCAS(make(map[string]any))
	if err != nil {
		return fmt.Errorf("failed to insert initial queue metadata record: %v, Type: %v", err, queueType)
	}
	// it's ok if the query is not applied, which means that the record exists already.
	return nil
}

func (q *QueueStore) getQueueMetadata(
	ctx context.Context,
	queueType persistence.QueueType,
) (*persistence.InternalQueueMetadata, error) {

	query := q.session.Query(templateGetQueueMetadataQuery, queueType).WithContext(ctx)
	message := make(map[string]any)
	err := query.MapScan(message)
	if err != nil {
		return nil, err
	}

	return convertQueueMetadata(message, q.serializer)
}

func (q *QueueStore) updateAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
	queueType persistence.QueueType,
) error {

	// TODO: remove this once cluster_ack_level is removed from DB
	metadataStruct, err := q.serializer.QueueMetadataFromBlob(metadata.Blob)
	if err != nil {
		return gocql.ConvertError("updateAckLevel", err)
	}

	query := q.session.Query(templateUpdateQueueMetadataQuery,
		metadataStruct.ClusterAckLevels,
		metadata.Blob.Data,
		metadata.Blob.EncodingType.String(),
		metadata.Version+1, // always increase version number on update
		queueType,
		metadata.Version, // condition update
	).WithContext(ctx)
	applied, err := query.MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("updateAckLevel", err)
	}
	if !applied {
		return &persistence.ConditionFailedError{Msg: "UpdateAckLevel operation encountered concurrent write."}
	}

	return nil
}

func (q *QueueStore) Close() {
	if q.session != nil {
		q.session.Close()
	}
}

func (q *QueueStore) getDLQTypeFromQueueType() persistence.QueueType {
	return -q.queueType
}

func (q *QueueStore) initializeQueueMetadata(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	_, err := q.getQueueMetadata(ctx, q.queueType)
	if gocql.IsNotFoundError(err) {
		return q.insertInitialQueueMetadataRecord(ctx, q.queueType, blob)
	}
	return err
}

func (q *QueueStore) initializeDLQMetadata(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	_, err := q.getQueueMetadata(ctx, q.getDLQTypeFromQueueType())
	if gocql.IsNotFoundError(err) {
		return q.insertInitialQueueMetadataRecord(ctx, q.getDLQTypeFromQueueType(), blob)
	}
	return err
}

func convertQueueMessage(
	message map[string]any,
) *persistence.QueueMessage {

	id := message["message_id"].(int64)
	data := message["message_payload"].([]byte)
	encoding := message["message_encoding"].(string)
	if encoding == "" {
		encoding = enumspb.ENCODING_TYPE_PROTO3.String()
	}
	return &persistence.QueueMessage{
		ID:       id,
		Data:     data,
		Encoding: encoding,
	}
}

func convertQueueMetadata(
	message map[string]any,
	serializer serialization.Serializer,
) (*persistence.InternalQueueMetadata, error) {

	metadata := &persistence.InternalQueueMetadata{
		Version: message["version"].(int64),
	}
	_, ok := message["cluster_ack_level"]
	if ok {
		clusterAckLevel := message["cluster_ack_level"].(map[string]int64)
		// TODO: remove this once we remove cluster_ack_level from DB.
		blob, err := serializer.QueueMetadataToBlob(&persistencespb.QueueMetadata{ClusterAckLevels: clusterAckLevel})
		if err != nil {
			return nil, err
		}
		metadata.Blob = blob
	} else {
		data := message["data"].([]byte)
		encoding := message["data_encoding"].(string)

		metadata.Blob = persistence.NewDataBlob(data, encoding)
	}

	return metadata, nil
}
