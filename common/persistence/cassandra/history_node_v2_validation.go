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
	templateValidateScanHistoryNodeV1 = `SELECT tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding, ` +
		`writetime(data) FROM history_node`
	templateValidateScanHistoryNodeV2 = `SELECT tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding, ` +
		`writetime(data) FROM history_node_v2`
	templateValidateHistoryNodeV1Row = `SELECT prev_txn_id, data, data_encoding, writetime(data) FROM history_node ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ?`
	templateValidateHistoryNodeV2Row = `SELECT prev_txn_id, data, data_encoding, writetime(data) FROM history_node_v2 ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ?`
)

// HistoryNodeValidationResult reports differences between legacy and canonical history node tables.
type HistoryNodeValidationResult struct {
	LegacyRows       int64
	V2Rows           int64
	MissingInV2      int64
	MismatchedInV2   int64
	MissingInLegacy  int64
	MismatchedLegacy int64
}

// Matches reports whether both history node tables contain identical rows and write timestamps.
func (r HistoryNodeValidationResult) Matches() bool {
	return r.LegacyRows == r.V2Rows &&
		r.MissingInV2 == 0 &&
		r.MismatchedInV2 == 0 &&
		r.MissingInLegacy == 0 &&
		r.MismatchedLegacy == 0
}

type historyNodeValidationRow struct {
	treeID       string
	branchID     string
	nodeID       int64
	prevTxnID    int64
	txnID        int64
	data         []byte
	dataEncoding string
	writeTime    int64
}

// ValidateHistoryNodeV2 compares both node tables in both directions.
func ValidateHistoryNodeV2(
	ctx context.Context,
	session gocql.Session,
	pageSize int,
	concurrency int,
) (HistoryNodeValidationResult, error) {
	if pageSize <= 0 {
		return HistoryNodeValidationResult{}, errors.New("history node validation page size must be positive")
	}
	if concurrency <= 0 {
		return HistoryNodeValidationResult{}, errors.New("history node validation concurrency must be positive")
	}
	var result HistoryNodeValidationResult
	if err := validateHistoryNodeDirection(
		ctx,
		session,
		templateValidateScanHistoryNodeV1,
		templateValidateHistoryNodeV2Row,
		pageSize,
		concurrency,
		&result.LegacyRows,
		&result.MissingInV2,
		&result.MismatchedInV2,
	); err != nil {
		return result, fmt.Errorf("validate history_node against history_node_v2: %w", err)
	}
	if err := validateHistoryNodeDirection(
		ctx,
		session,
		templateValidateScanHistoryNodeV2,
		templateValidateHistoryNodeV1Row,
		pageSize,
		concurrency,
		&result.V2Rows,
		&result.MissingInLegacy,
		&result.MismatchedLegacy,
	); err != nil {
		return result, fmt.Errorf("validate history_node_v2 against history_node: %w", err)
	}
	return result, nil
}

func validateHistoryNodeDirection(
	ctx context.Context,
	session gocql.Session,
	scanQuery string,
	pointQuery string,
	pageSize int,
	concurrency int,
	rowCount *int64,
	missingCount *int64,
	mismatchCount *int64,
) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	iter := session.Query(scanQuery).WithContext(groupCtx).PageSize(pageSize).Iter()
	var rows, missing, mismatched atomic.Int64
	for groupCtx.Err() == nil {
		var row historyNodeValidationRow
		if !iter.Scan(
			&row.treeID,
			&row.branchID,
			&row.nodeID,
			&row.prevTxnID,
			&row.txnID,
			&row.data,
			&row.dataEncoding,
			&row.writeTime,
		) {
			break
		}
		row.data = bytes.Clone(row.data)
		rows.Add(1)
		group.Go(func() error {
			match, found, err := readHistoryNodeValidationRow(groupCtx, session, pointQuery, row)
			if err != nil {
				return err
			}
			if !found {
				missing.Add(1)
			} else if !match {
				mismatched.Add(1)
			}
			return nil
		})
	}
	iterErr := iter.Close()
	groupErr := group.Wait()
	*rowCount = rows.Load()
	*missingCount = missing.Load()
	*mismatchCount = mismatched.Load()
	if groupErr != nil {
		return groupErr
	}
	if iterErr != nil {
		return iterErr
	}
	return ctx.Err()
}

func readHistoryNodeValidationRow(
	ctx context.Context,
	session gocql.Session,
	query string,
	expected historyNodeValidationRow,
) (matches bool, found bool, err error) {
	var prevTxnID, writeTime int64
	var data []byte
	var dataEncoding string
	err = session.Query(
		query,
		expected.treeID,
		expected.branchID,
		expected.nodeID,
		expected.txnID,
	).WithContext(ctx).Scan(&prevTxnID, &data, &dataEncoding, &writeTime)
	if gocql.IsNotFoundError(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return prevTxnID == expected.prevTxnID &&
		bytes.Equal(data, expected.data) &&
		dataEncoding == expected.dataEncoding &&
		writeTime == expected.writeTime, true, nil
}
