package cassandra

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type QueueV2MetadataMigrationPage struct {
	RowsProcessed int
	NextPageToken []byte
}

// BackfillQueueV2MetadataPage copies one source-table page into the bucketed table.
func BackfillQueueV2MetadataPage(
	ctx context.Context,
	session gocql.Session,
	queueType persistence.QueueV2Type,
	pageSize int,
	nextPageToken []byte,
) (*QueueV2MetadataMigrationPage, error) {
	if pageSize <= 0 {
		return nil, persistence.ErrNonPositiveListQueuesPageSize
	}
	rows, nextPageToken, err := readSourceQueueV2MetadataPage(
		ctx,
		session,
		queueType,
		pageSize,
		nextPageToken,
	)
	if err != nil {
		return nil, err
	}
	store := &queueV2Store{session: session}
	for _, row := range rows {
		if _, err := getQueueFromMetadata(
			queueType,
			row.queueName,
			row.metadataPayload,
			row.metadataEncoding,
			row.version,
		); err != nil {
			return nil, err
		}
		if err := store.mirrorQueueMetadata(
			ctx,
			queueType,
			row,
			queueV2MetadataLayoutTarget,
		); err != nil {
			return nil, err
		}
	}
	return &QueueV2MetadataMigrationPage{
		RowsProcessed: len(rows),
		NextPageToken: nextPageToken,
	}, nil
}

// ValidateQueueV2MetadataPage compares one source-table page with the bucketed table.
func ValidateQueueV2MetadataPage(
	ctx context.Context,
	session gocql.Session,
	queueType persistence.QueueV2Type,
	pageSize int,
	nextPageToken []byte,
) (*QueueV2MetadataMigrationPage, error) {
	if pageSize <= 0 {
		return nil, persistence.ErrNonPositiveListQueuesPageSize
	}
	rows, nextPageToken, err := readSourceQueueV2MetadataPage(
		ctx,
		session,
		queueType,
		pageSize,
		nextPageToken,
	)
	if err != nil {
		return nil, err
	}
	store := &queueV2Store{session: session}
	for _, row := range rows {
		source, err := getQueueFromMetadata(
			queueType,
			row.queueName,
			row.metadataPayload,
			row.metadataEncoding,
			row.version,
		)
		if err != nil {
			return nil, err
		}
		target, err := store.getQueueFromLayout(
			ctx,
			row.queueName,
			queueType,
			queueV2MetadataLayoutTarget,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"validate QueueV2 metadata for queue type %v and name %v: %w",
				queueType,
				row.queueName,
				err,
			)
		}
		if !queueV2MetadataEqual(source, target) {
			return nil, fmt.Errorf(
				"queueV2 metadata mismatch for queue type %v and name %v",
				queueType,
				row.queueName,
			)
		}
	}
	return &QueueV2MetadataMigrationPage{
		RowsProcessed: len(rows),
		NextPageToken: nextPageToken,
	}, nil
}

// ValidateQueueV2TargetMetadataPage checks one bucketed-table page against the source table.
func ValidateQueueV2TargetMetadataPage(
	ctx context.Context,
	session gocql.Session,
	queueType persistence.QueueV2Type,
	pageSize int,
	nextPageToken []byte,
) (*QueueV2MetadataMigrationPage, error) {
	if pageSize <= 0 {
		return nil, persistence.ErrNonPositiveListQueuesPageSize
	}
	store := &queueV2Store{session: session}
	rows, nextPageToken, err := store.readQueueV2MetadataPage(
		ctx,
		queueType,
		pageSize,
		nextPageToken,
	)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		target, err := getQueueFromMetadata(
			queueType,
			row.queueName,
			row.metadataPayload,
			row.metadataEncoding,
			row.version,
		)
		if err != nil {
			return nil, err
		}
		source, err := store.getQueueFromLayout(
			ctx,
			row.queueName,
			queueType,
			queueV2MetadataLayoutSource,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"validate QueueV2 source metadata for queue type %v and name %v: %w",
				queueType,
				row.queueName,
				err,
			)
		}
		if !queueV2MetadataEqual(source, target) {
			return nil, fmt.Errorf(
				"queueV2 source metadata mismatch for queue type %v and name %v",
				queueType,
				row.queueName,
			)
		}
	}
	return &QueueV2MetadataMigrationPage{
		RowsProcessed: len(rows),
		NextPageToken: nextPageToken,
	}, nil
}

func readSourceQueueV2MetadataPage(
	ctx context.Context,
	session gocql.Session,
	queueType persistence.QueueV2Type,
	pageSize int,
	nextPageToken []byte,
) ([]queueV2MetadataRow, []byte, error) {
	iter := session.Query(
		templateGetQueueNamesQuery,
		queueType,
	).PageSize(pageSize).PageState(nextPageToken).WithContext(ctx).Iter()
	rows := make([]queueV2MetadataRow, 0, preallocatedResultCapacity(pageSize))
	for range min(iter.NumRows(), pageSize) {
		var row queueV2MetadataRow
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
	pageToken := iter.PageState()
	if err := iter.Close(); err != nil {
		return nil, nil, gocql.ConvertError("QueueV2ReadMetadataMigrationPage", err)
	}
	return rows, pageToken, nil
}
