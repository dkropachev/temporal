package cassandra

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"golang.org/x/sync/errgroup"
)

const (
	templateScanHistoryNodeForV2Backfill = `SELECT tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding, ` +
		`writetime(data) FROM history_node`
	templateScanHistoryNodeV2ForV1Backfill = `SELECT tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding, ` +
		`writetime(data) FROM history_node_v2`
	templateBackfillHistoryNodeV2 = `INSERT INTO history_node_v2 (` +
		`tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`
	templateBackfillHistoryNodeV1 = `INSERT INTO history_node (` +
		`tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`
)

// HistoryNodeBackfillOptions controls the read page size and bounded write parallelism.
type HistoryNodeBackfillOptions struct {
	PageSize    int
	Concurrency int
}

// HistoryNodeV2BackfillOptions is kept as an alias for callers of the original forward backfill.
type HistoryNodeV2BackfillOptions = HistoryNodeBackfillOptions

type historyNodeBackfillSpec struct {
	sourceTable string
	targetTable string
	scanQuery   string
	insertQuery string
}

// BackfillHistoryNodeV2 idempotently copies history_node rows while preserving their write timestamps.
func BackfillHistoryNodeV2(
	ctx context.Context,
	session gocql.Session,
	options HistoryNodeBackfillOptions,
) (int64, error) {
	return backfillHistoryNodes(ctx, session, options, historyNodeBackfillSpec{
		sourceTable: historyNodeTableName,
		targetTable: historyNodeV2TableName,
		scanQuery:   templateScanHistoryNodeForV2Backfill,
		insertQuery: templateBackfillHistoryNodeV2,
	})
}

// BackfillHistoryNodeV1 idempotently restores the legacy table while preserving source write timestamps.
func BackfillHistoryNodeV1(
	ctx context.Context,
	session gocql.Session,
	options HistoryNodeBackfillOptions,
) (int64, error) {
	return backfillHistoryNodes(ctx, session, options, historyNodeBackfillSpec{
		sourceTable: historyNodeV2TableName,
		targetTable: historyNodeTableName,
		scanQuery:   templateScanHistoryNodeV2ForV1Backfill,
		insertQuery: templateBackfillHistoryNodeV1,
	})
}

func backfillHistoryNodes(
	ctx context.Context,
	session gocql.Session,
	options HistoryNodeBackfillOptions,
	spec historyNodeBackfillSpec,
) (int64, error) {
	if options.PageSize <= 0 {
		return 0, errors.New("history node backfill page size must be positive")
	}
	if options.Concurrency <= 0 {
		return 0, errors.New("history node backfill concurrency must be positive")
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(options.Concurrency)

	iter := session.Query(spec.scanQuery).
		WithContext(groupCtx).
		PageSize(options.PageSize).
		Iter()

	var copied atomic.Int64

	for groupCtx.Err() == nil {
		var (
			treeID       string
			branchID     string
			nodeID       int64
			prevTxnID    int64
			txnID        int64
			data         []byte
			dataEncoding string
			writeTime    int64
		)
		if !iter.Scan(&treeID, &branchID, &nodeID, &prevTxnID, &txnID, &data, &dataEncoding, &writeTime) {
			break
		}
		data = bytes.Clone(data)
		group.Go(func() error {
			err := session.Query(
				spec.insertQuery,
				treeID,
				branchID,
				nodeID,
				prevTxnID,
				txnID,
				data,
				dataEncoding,
				writeTime,
			).WithContext(groupCtx).Idempotent(true).Exec()
			if err != nil {
				return fmt.Errorf(
					"backfill %s to %s row tree_id=%s branch_id=%s node_id=%d txn_id=%d: %w",
					spec.sourceTable,
					spec.targetTable,
					treeID,
					branchID,
					nodeID,
					txnID,
					err,
				)
			}
			copied.Add(1)
			return nil
		})
	}

	iterErr := iter.Close()
	groupErr := group.Wait()
	if groupErr != nil {
		return copied.Load(), groupErr
	}
	if iterErr != nil {
		return copied.Load(), fmt.Errorf(
			"scan %s for %s backfill: %w",
			spec.sourceTable,
			spec.targetTable,
			iterErr,
		)
	}
	if err := ctx.Err(); err != nil {
		return copied.Load(), err
	}
	return copied.Load(), nil
}
