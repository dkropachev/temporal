package cassandra

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const templateGetSourceQueueV2MessagesForMigration = `SELECT message_id, message_payload, message_encoding
	FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ?
	AND message_id >= ? AND message_id <= ? ORDER BY message_id ASC LIMIT ?`

const templateScanTargetQueueV2MessagesForValidation = `SELECT message_bucket, row_type, message_id
	FROM queue_messages_v3 WHERE queue_type = ? AND queue_name = ? ALLOW FILTERING`

var errQueueV2ChangedDuringValidation = errors.New("QueueV2 changed during validation; retry validation")

type QueueV2MessageMigrationPage struct {
	RowsProcessed int
	NextPageToken []byte
}

type QueueV2MessageValidationResult struct {
	SourceRows int64
	TargetRows int64
	Mismatches []string
}

func (r QueueV2MessageValidationResult) Matches() bool {
	return len(r.Mismatches) == 0 && r.SourceRows == r.TargetRows
}

type queueV2RawMessage struct {
	id       int64
	data     []byte
	encoding string
}

// BackfillQueueV2MessagesPage copies one queue page from queue_messages into queue_messages_v3.
//
//nolint:revive // Page backfill validates queue authority and maintains resumable target directory state.
func BackfillQueueV2MessagesPage(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
	pageSize int,
	nextPageToken []byte,
	messageBucketSpan int64,
) (*QueueV2MessageMigrationPage, error) {
	if pageSize <= 0 {
		return nil, persistence.ErrNonPositiveReadQueueMessagesPageSize
	}
	if err := validateQueueV2MessageBucketSpan(messageBucketSpan); err != nil {
		return nil, err
	}
	store := &queueV2Store{
		session:     session,
		messageSpan: messageBucketSpan,
	}
	queue, err := store.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutSource)
	if err != nil {
		return nil, err
	}
	minimumMessageID, err := queueV2MinimumMessageID(queueType, queueName, queue)
	if err != nil {
		return nil, err
	}
	record, err := store.initializeQueueV2SourceAuthority(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	row, err := queueV2MetadataRowFromQueue(queueName, queue)
	if err != nil {
		return nil, err
	}
	row.authority = record
	if _, targetErr := store.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutTarget); targetErr == nil {
		if err := store.initializeQueueV2TargetMetadataAuthorityRecord(ctx, queueType, queueName, record); err != nil {
			return nil, err
		}
	} else {
		var notFound *serviceerror.NotFound
		if !errors.As(targetErr, &notFound) {
			return nil, targetErr
		}
	}
	if err := store.mirrorQueueMetadata(ctx, queueType, row, queueV2MetadataLayoutTarget); err != nil {
		return nil, err
	}
	if err := store.ensureQueueV2MessageTarget(ctx, queueType, queueName, minimumMessageID, true, record); err != nil {
		return nil, err
	}
	maximumMessageID, ok, err := store.getSourceMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &QueueV2MessageMigrationPage{}, nil
	}
	startMessageID := minimumMessageID
	if len(nextPageToken) != 0 {
		lastMessageID, err := decodeQueueV2MessagePageToken(
			nextPageToken,
			queueType,
			queueName,
			messageBucketSpan,
		)
		if err != nil {
			return nil, err
		}
		if lastMessageID == math.MaxInt64 {
			return &QueueV2MessageMigrationPage{}, nil
		}
		startMessageID = max(startMessageID, lastMessageID+1)
	}
	if startMessageID > maximumMessageID {
		return &QueueV2MessageMigrationPage{}, nil
	}
	rows, err := readSourceQueueV2RawMessages(
		ctx,
		session,
		queueType,
		queueName,
		startMessageID,
		maximumMessageID,
		pageSize,
	)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf(
			"source QueueV2 message range [%d,%d] is not contiguous for queue type %v and name %v",
			startMessageID,
			maximumMessageID,
			queueType,
			queueName,
		)
	}
	for index, row := range rows {
		expectedID := startMessageID + int64(index)
		if row.id != expectedID {
			return nil, fmt.Errorf(
				"source QueueV2 message range is missing ID %d for queue type %v and name %v",
				expectedID,
				queueType,
				queueName,
			)
		}
		if err := store.mirrorQueueV2MessageToTarget(
			ctx,
			queueType,
			queueName,
			minimumMessageID,
			row.id,
			row.data,
			row.encoding,
			record,
		); err != nil {
			return nil, err
		}
	}
	page := &QueueV2MessageMigrationPage{RowsProcessed: len(rows)}
	lastMessageID := rows[len(rows)-1].id
	if lastMessageID < maximumMessageID {
		page.NextPageToken = encodeQueueV2MessagePageToken(
			queueType,
			queueName,
			messageBucketSpan,
			lastMessageID,
		)
	}
	return page, nil
}

// ReconcileQueueV2Messages copies source-authoritative rows until target validation succeeds.
func ReconcileQueueV2Messages(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
	pageSize int,
	messageBucketSpan int64,
) (QueueV2MessageValidationResult, error) {
	var token []byte
	for {
		page, err := BackfillQueueV2MessagesPage(
			ctx,
			session,
			queueType,
			queueName,
			pageSize,
			token,
			messageBucketSpan,
		)
		if err != nil {
			return QueueV2MessageValidationResult{}, err
		}
		if len(page.NextPageToken) == 0 {
			break
		}
		token = page.NextPageToken
	}
	return ValidateQueueV2Messages(ctx, session, queueType, queueName, pageSize, messageBucketSpan)
}

// ReconcileQueueV2SourceMessages repairs the rollback copy while queue_messages_v3 is authoritative.
//
//nolint:revive // Reverse reconciliation repairs metadata and every message bucket while preserving IDs.
func ReconcileQueueV2SourceMessages(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
	pageSize int,
	messageBucketSpan int64,
) (QueueV2MessageValidationResult, error) {
	if pageSize <= 0 {
		return QueueV2MessageValidationResult{}, persistence.ErrNonPositiveReadQueueMessagesPageSize
	}
	if err := validateQueueV2MessageBucketSpan(messageBucketSpan); err != nil {
		return QueueV2MessageValidationResult{}, err
	}
	store := &queueV2Store{session: session, messageSpan: messageBucketSpan}
	targetRecord, err := readQueueV2MetadataAuthority(
		ctx,
		session,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
	)
	if err != nil {
		return QueueV2MessageValidationResult{}, err
	}
	sourceRecord, err := readQueueV2MetadataAuthority(
		ctx,
		session,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
	)
	if err != nil {
		return QueueV2MessageValidationResult{}, err
	}
	sourceMessageRecord, err := readQueueV2MessageAuthority(
		ctx,
		session,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
		0,
	)
	if err != nil {
		return QueueV2MessageValidationResult{}, err
	}
	targetMessageRecord, err := readQueueV2MessageAuthority(
		ctx,
		session,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
		queueV2MessageDirectoryBucket,
	)
	if err != nil {
		return QueueV2MessageValidationResult{}, err
	}
	for _, record := range []queueV2AuthorityRecord{sourceRecord, sourceMessageRecord, targetMessageRecord} {
		if err := validateQueueV2AuthorityPair(queueType, queueName, targetRecord, record); err != nil {
			return QueueV2MessageValidationResult{}, err
		}
	}
	if err := store.validateQueueV2Authority(
		queueType,
		queueName,
		targetRecord,
		queueV2MigrationAuthorityTarget,
	); err != nil {
		return QueueV2MessageValidationResult{}, err
	}
	queue, err := store.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutTarget)
	if err != nil {
		return QueueV2MessageValidationResult{}, err
	}
	row, err := queueV2MetadataRowFromQueue(queueName, queue)
	if err != nil {
		return QueueV2MessageValidationResult{}, err
	}
	row.authority = targetRecord
	if err := store.mirrorQueueMetadata(ctx, queueType, row, queueV2MetadataLayoutSource); err != nil {
		return QueueV2MessageValidationResult{}, err
	}
	minimumMessageID, err := queueV2MinimumMessageID(queueType, queueName, queue)
	if err != nil {
		return QueueV2MessageValidationResult{}, err
	}
	maximumMessageID, ok, err := store.getQueueV2TargetMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return QueueV2MessageValidationResult{}, err
	}
	if ok && minimumMessageID <= maximumMessageID {
		cursor := minimumMessageID
		for cursor <= maximumMessageID {
			rows, err := store.readQueueV2TargetRawMessages(
				ctx,
				queueType,
				queueName,
				cursor,
				maximumMessageID,
				pageSize,
			)
			if err != nil {
				return QueueV2MessageValidationResult{}, err
			}
			if len(rows) == 0 {
				changed, err := store.queueV2TargetSnapshotChanged(
					ctx,
					queueType,
					queueName,
					queue,
					maximumMessageID,
					ok,
				)
				if err != nil {
					return QueueV2MessageValidationResult{}, err
				}
				if changed {
					return QueueV2MessageValidationResult{}, errQueueV2ChangedDuringValidation
				}
				return QueueV2MessageValidationResult{}, fmt.Errorf("target QueueV2 message range is missing ID %d", cursor)
			}
			for index, row := range rows {
				if row.id != cursor+int64(index) {
					changed, err := store.queueV2TargetSnapshotChanged(
						ctx,
						queueType,
						queueName,
						queue,
						maximumMessageID,
						ok,
					)
					if err != nil {
						return QueueV2MessageValidationResult{}, err
					}
					if changed {
						return QueueV2MessageValidationResult{}, errQueueV2ChangedDuringValidation
					}
					return QueueV2MessageValidationResult{}, fmt.Errorf("target QueueV2 message range is missing ID %d", cursor+int64(index))
				}
				if err := store.mirrorQueueV2MessageToSource(
					ctx,
					queueType,
					queueName,
					row.id,
					row.data,
					row.encoding,
					targetRecord,
				); err != nil {
					return QueueV2MessageValidationResult{}, err
				}
			}
			lastMessageID := rows[len(rows)-1].id
			if lastMessageID == math.MaxInt64 {
				break
			}
			cursor = lastMessageID + 1
		}
	}
	return ValidateQueueV2Messages(ctx, session, queueType, queueName, pageSize, messageBucketSpan)
}

func (s *queueV2Store) queueV2TargetSnapshotChanged(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	queue *Queue,
	maximumMessageID int64,
	hasMessages bool,
) (bool, error) {
	currentQueue, err := s.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutTarget)
	if err != nil {
		return false, err
	}
	currentMaximumMessageID, currentHasMessages, err := s.getQueueV2TargetMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return false, err
	}
	return !queueV2MetadataEqual(queue, currentQueue) ||
		maximumMessageID != currentMaximumMessageID || hasMessages != currentHasMessages, nil
}

// ValidateQueueV2Messages checks stable logical source and target snapshots in both directions.
//
//nolint:revive // Exact validation compares source and target messages in both directions across buckets.
func ValidateQueueV2Messages(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
	pageSize int,
	messageBucketSpan int64,
) (QueueV2MessageValidationResult, error) {
	result := QueueV2MessageValidationResult{}
	if pageSize <= 0 {
		return result, persistence.ErrNonPositiveReadQueueMessagesPageSize
	}
	if err := validateQueueV2MessageBucketSpan(messageBucketSpan); err != nil {
		return result, err
	}
	store := &queueV2Store{session: session, messageSpan: messageBucketSpan}
	sourceQueueBefore, err := store.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutSource)
	if err != nil {
		return result, err
	}
	targetQueueBefore, err := store.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutTarget)
	if err != nil {
		return result, err
	}
	if !queueV2MetadataEqual(sourceQueueBefore, targetQueueBefore) {
		result.addMismatch(100, "queue metadata differs")
	}
	minimumMessageID, err := queueV2MinimumMessageID(queueType, queueName, sourceQueueBefore)
	if err != nil {
		return result, err
	}
	sourceMaximumBefore, sourceExists, err := store.getSourceMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return result, err
	}
	targetMaximumBefore, targetExists, err := store.getQueueV2TargetMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return result, err
	}
	if sourceExists != targetExists || sourceExists && sourceMaximumBefore != targetMaximumBefore {
		result.addMismatch(100, fmt.Sprintf(
			"maximum message ID differs: source=(%d,%t), target=(%d,%t)",
			sourceMaximumBefore,
			sourceExists,
			targetMaximumBefore,
			targetExists,
		))
	}
	result.TargetRows, err = scanTargetQueueV2MessagesForValidation(
		ctx,
		session,
		queueType,
		queueName,
		pageSize,
		minimumMessageID,
		sourceMaximumBefore,
		sourceExists,
		messageBucketSpan,
		&result,
	)
	if err != nil {
		return result, err
	}
	if sourceExists && minimumMessageID <= sourceMaximumBefore {
		cursor := minimumMessageID
		for cursor <= sourceMaximumBefore {
			sourceRows, err := readSourceQueueV2RawMessages(
				ctx,
				session,
				queueType,
				queueName,
				cursor,
				sourceMaximumBefore,
				pageSize,
			)
			if err != nil {
				return result, err
			}
			targetRows, err := store.readQueueV2TargetRawMessages(
				ctx,
				queueType,
				queueName,
				cursor,
				sourceMaximumBefore,
				pageSize,
			)
			if err != nil {
				return result, err
			}
			result.SourceRows += int64(len(sourceRows))
			compareQueueV2RawMessagePages(&result, cursor, sourceRows, targetRows)
			if len(sourceRows) == 0 {
				break
			}
			lastMessageID := sourceRows[len(sourceRows)-1].id
			if lastMessageID == math.MaxInt64 {
				break
			}
			cursor = lastMessageID + 1
		}
	}
	sourceQueueAfter, err := store.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutSource)
	if err != nil {
		return result, err
	}
	targetQueueAfter, err := store.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutTarget)
	if err != nil {
		return result, err
	}
	sourceMaximumAfter, sourceExistsAfter, err := store.getSourceMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return result, err
	}
	targetMaximumAfter, targetExistsAfter, err := store.getQueueV2TargetMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return result, err
	}
	if !queueV2MetadataEqual(sourceQueueBefore, sourceQueueAfter) ||
		!queueV2MetadataEqual(targetQueueBefore, targetQueueAfter) ||
		sourceMaximumBefore != sourceMaximumAfter || sourceExists != sourceExistsAfter ||
		targetMaximumBefore != targetMaximumAfter || targetExists != targetExistsAfter {
		return result, errQueueV2ChangedDuringValidation
	}
	return result, nil
}

func scanTargetQueueV2MessagesForValidation(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
	pageSize int,
	minimumMessageID int64,
	maximumMessageID int64,
	sourceExists bool,
	messageBucketSpan int64,
	result *QueueV2MessageValidationResult,
) (int64, error) {
	iter := session.Query(
		templateScanTargetQueueV2MessagesForValidation,
		queueType,
		queueName,
	).WithContext(ctx).PageSize(pageSize).Iter()
	var rows int64
	for {
		var (
			bucket    int64
			rowType   int
			messageID int64
		)
		if !iter.Scan(&bucket, &rowType, &messageID) {
			break
		}
		if rowType == int(queueV2MessageStateRowType) {
			continue
		}
		if rowType != int(queueV2MessageDataRowType) {
			result.addMismatch(100, fmt.Sprintf(
				"target row has unsupported type %d in bucket %d at message ID %d",
				rowType,
				bucket,
				messageID,
			))
			continue
		}
		rows++
		expectedBucket, err := queueV2MessageBucketForID(messageID, messageBucketSpan)
		if err != nil {
			result.addMismatch(100, fmt.Sprintf("target row has invalid message ID %d", messageID))
			continue
		}
		if bucket != expectedBucket {
			result.addMismatch(100, fmt.Sprintf(
				"target message %d is in bucket %d, expected %d",
				messageID,
				bucket,
				expectedBucket,
			))
		}
		if !sourceExists || messageID < minimumMessageID || messageID > maximumMessageID {
			result.addMismatch(100, fmt.Sprintf(
				"target contains message %d outside source range [%d,%d]",
				messageID,
				minimumMessageID,
				maximumMessageID,
			))
		}
	}
	if err := iter.Close(); err != nil {
		return 0, cgocql.ConvertError("ScanQueueV2MessagesV3ForValidation", err)
	}
	return rows, nil
}

func readSourceQueueV2RawMessages(
	ctx context.Context,
	session cgocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
	minimumMessageID int64,
	maximumMessageID int64,
	limit int,
) ([]queueV2RawMessage, error) {
	iter := session.Query(
		templateGetSourceQueueV2MessagesForMigration,
		queueType,
		queueName,
		0,
		minimumMessageID,
		maximumMessageID,
		limit,
	).WithContext(ctx).Iter()
	rows := make([]queueV2RawMessage, 0, preallocatedResultCapacity(limit))
	for {
		var row queueV2RawMessage
		if !iter.Scan(&row.id, &row.data, &row.encoding) {
			break
		}
		rows = append(rows, row)
	}
	if err := iter.Close(); err != nil {
		return nil, cgocql.ConvertError("ReadSourceQueueV2MessagesForMigration", err)
	}
	return rows, nil
}

func (s *queueV2Store) readQueueV2TargetRawMessages(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	minimumMessageID int64,
	maximumMessageID int64,
	limit int,
) ([]queueV2RawMessage, error) {
	if minimumMessageID > maximumMessageID {
		return nil, nil
	}
	firstBucket, err := queueV2MessageBucketForID(minimumMessageID, s.messageBucketSpan())
	if err != nil {
		return nil, err
	}
	lastBucket, err := queueV2MessageBucketForID(maximumMessageID, s.messageBucketSpan())
	if err != nil {
		return nil, err
	}
	rows := make([]queueV2RawMessage, 0, preallocatedResultCapacity(limit))
	for bucket := firstBucket; bucket <= lastBucket && len(rows) < limit; bucket++ {
		bucketFirst, err := queueV2MessageBucketFirstID(bucket, s.messageBucketSpan())
		if err != nil {
			return nil, err
		}
		bucketLast, err := queueV2MessageBucketLastID(bucket, s.messageBucketSpan())
		if err != nil {
			return nil, err
		}
		iter := s.session.Query(
			templateGetQueueV2MessagesV3,
			queueType,
			queueName,
			bucket,
			queueV2MessageDataRowType,
			max(minimumMessageID, bucketFirst),
			min(maximumMessageID, bucketLast),
			limit-len(rows),
		).WithContext(ctx).Iter()
		for {
			var row queueV2RawMessage
			if !iter.Scan(&row.id, &row.data, &row.encoding) {
				break
			}
			rows = append(rows, row)
		}
		if err := iter.Close(); err != nil {
			return nil, cgocql.ConvertError("ReadQueueV2MessagesV3ForMigration", err)
		}
		if bucket == math.MaxInt64 {
			break
		}
	}
	return rows, nil
}

func compareQueueV2RawMessagePages(
	result *QueueV2MessageValidationResult,
	startMessageID int64,
	sourceRows []queueV2RawMessage,
	targetRows []queueV2RawMessage,
) {
	if len(sourceRows) != len(targetRows) {
		result.addMismatch(100, fmt.Sprintf(
			"row count differs at message %d: source=%d target=%d",
			startMessageID,
			len(sourceRows),
			len(targetRows),
		))
	}
	for index := 0; index < min(len(sourceRows), len(targetRows)); index++ {
		source := sourceRows[index]
		target := targetRows[index]
		if source.id != target.id || source.encoding != target.encoding || !bytes.Equal(source.data, target.data) {
			result.addMismatch(100, fmt.Sprintf("message differs near source ID %d and target ID %d", source.id, target.id))
		}
	}
}

func (r *QueueV2MessageValidationResult) addMismatch(limit int, mismatch string) {
	if len(r.Mismatches) < limit {
		r.Mismatches = append(r.Mismatches, mismatch)
	}
}
