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
	templateScanHistoryTreeForV2Backfill = `SELECT tree_id, branch_id, branch, branch_encoding, writetime(branch) ` +
		`FROM history_tree WHERE token(tree_id) >= ? AND token(tree_id) <= ?`
	templateScanHistoryTreeV2ForV1Backfill = `SELECT tree_id, branch_bucket, branch_id, branch, branch_encoding, writetime(branch) ` +
		`FROM history_tree_v2 WHERE token(tree_id, branch_bucket) >= ? AND token(tree_id, branch_bucket) <= ?`
	templateBackfillHistoryTreeV2 = `INSERT INTO history_tree_v2 (` +
		`tree_id, branch_bucket, branch_id, branch, branch_encoding) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?`
	templateBackfillHistoryTreeV1 = `INSERT INTO history_tree (` +
		`tree_id, branch_id, branch, branch_encoding) VALUES (?, ?, ?, ?) USING TIMESTAMP ?`
	templateValidateHistoryTreeV2Row = `SELECT branch, branch_encoding, writetime(branch) FROM history_tree_v2 ` +
		`WHERE tree_id = ? AND branch_bucket = ? AND branch_id = ?`
	templateValidateHistoryTreeV1Row = `SELECT branch, branch_encoding, writetime(branch) FROM history_tree ` +
		`WHERE tree_id = ? AND branch_id = ?`
)

// HistoryTreeBackfillOptions controls scan granularity and bounded write parallelism.
type HistoryTreeBackfillOptions struct {
	PageSize        int
	Concurrency     int
	TokenRangeCount int
	Partitioner     string
}

// HistoryTreeValidationResult reports differences between both history tree layouts.
type HistoryTreeValidationResult struct {
	LegacyRows       int64
	V2Rows           int64
	MissingInV2      int64
	MismatchedInV2   int64
	MissingInLegacy  int64
	MismatchedLegacy int64
}

// Matches reports whether both layouts contain identical rows and write timestamps.
func (r HistoryTreeValidationResult) Matches() bool {
	return r.LegacyRows == r.V2Rows &&
		r.MissingInV2 == 0 &&
		r.MismatchedInV2 == 0 &&
		r.MissingInLegacy == 0 &&
		r.MismatchedLegacy == 0
}

// ValidateHistoryTreeV2BackfillSchema checks layouts for a forward backfill.
func ValidateHistoryTreeV2BackfillSchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
) error {
	if err := validateHistoryTreeTableLayout(
		ctx,
		session,
		keyspace,
		historyTreeTableName,
		HistoryTreeTableLayoutLegacyV1,
	); err != nil {
		return err
	}
	return validateHistoryTreeTableLayout(
		ctx,
		session,
		keyspace,
		historyTreeV2TableName,
		HistoryTreeTableLayoutBucketV2,
	)
}

// ValidateHistoryTreeV1BackfillSchema checks layouts for a reverse backfill.
func ValidateHistoryTreeV1BackfillSchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
) error {
	return ValidateHistoryTreeV2BackfillSchema(ctx, session, keyspace)
}

// ValidateHistoryTreeReconcileSchema checks the layouts and source authority columns used by exact repair.
func ValidateHistoryTreeReconcileSchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
) error {
	if err := ValidateHistoryTreeV2BackfillSchema(ctx, session, keyspace); err != nil {
		return err
	}
	return validateHistoryTreeSourceAuthorityColumns(ctx, session, keyspace)
}

func validateHistoryTreeTableLayout(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	table string,
	expected HistoryTreeTableLayout,
) error {
	layout, err := GetHistoryTreeTableLayout(ctx, session, keyspace, table)
	if err != nil {
		return err
	}
	if layout != expected {
		return fmt.Errorf("%s.%s has %s layout, expected %s", keyspace, table, layout, expected)
	}
	return nil
}

// BackfillHistoryTreeV2 idempotently copies legacy rows while preserving write timestamps.
func BackfillHistoryTreeV2(
	ctx context.Context,
	session gocql.Session,
	options HistoryTreeBackfillOptions,
) (int64, error) {
	return backfillHistoryTreeTokenRanges(ctx, session, options, true)
}

// BackfillHistoryTreeV1 idempotently restores legacy rows while preserving write timestamps.
func BackfillHistoryTreeV1(
	ctx context.Context,
	session gocql.Session,
	options HistoryTreeBackfillOptions,
) (int64, error) {
	return backfillHistoryTreeTokenRanges(ctx, session, options, false)
}

// BackfillHistoryTreeV2Range copies one inclusive legacy token range.
func BackfillHistoryTreeV2Range(
	ctx context.Context,
	session gocql.Session,
	options HistoryTreeBackfillOptions,
	tokenRange HistoryNodeBackfillTokenRange,
) (int64, error) {
	return backfillHistoryTreeTokenRange(ctx, session, options, tokenRange, true)
}

// BackfillHistoryTreeV1Range copies one inclusive V2 token range.
func BackfillHistoryTreeV1Range(
	ctx context.Context,
	session gocql.Session,
	options HistoryTreeBackfillOptions,
	tokenRange HistoryNodeBackfillTokenRange,
) (int64, error) {
	return backfillHistoryTreeTokenRange(ctx, session, options, tokenRange, false)
}

func backfillHistoryTreeTokenRanges(
	ctx context.Context,
	session gocql.Session,
	options HistoryTreeBackfillOptions,
	forward bool,
) (int64, error) {
	if err := validateHistoryTreeBackfillOptions(options); err != nil {
		return 0, err
	}
	rangeCount := options.TokenRangeCount
	if rangeCount == 0 {
		rangeCount = DefaultHistoryNodeBackfillTokenRangeCount
	}
	ranges, err := HistoryNodeBackfillTokenRanges(rangeCount)
	if err != nil {
		return 0, err
	}
	if err := validateHistoryNodeBackfillPartitioner(ctx, session, options.Partitioner); err != nil {
		return 0, err
	}
	var copied int64
	for _, tokenRange := range ranges {
		rangeCopied, err := backfillHistoryTreeTokenRange(ctx, session, options, tokenRange, forward)
		copied += rangeCopied
		if err != nil {
			return copied, fmt.Errorf(
				"backfill history tree token range %d [%d, %d]: %w",
				tokenRange.Index,
				tokenRange.StartToken,
				tokenRange.EndToken,
				err,
			)
		}
	}
	return copied, nil
}

func validateHistoryTreeBackfillOptions(options HistoryTreeBackfillOptions) error {
	if options.PageSize <= 0 {
		return errors.New("history tree backfill page size must be positive")
	}
	if options.Concurrency <= 0 {
		return errors.New("history tree backfill concurrency must be positive")
	}
	return nil
}

//nolint:revive // The bounded concurrent scanner handles both migration directions and iterator shutdown paths.
func backfillHistoryTreeTokenRange(
	ctx context.Context,
	session gocql.Session,
	options HistoryTreeBackfillOptions,
	tokenRange HistoryNodeBackfillTokenRange,
	forward bool,
) (int64, error) {
	if err := validateHistoryTreeBackfillOptions(options); err != nil {
		return 0, err
	}
	if tokenRange.Index < 0 || tokenRange.StartToken > tokenRange.EndToken {
		return 0, fmt.Errorf("invalid history tree backfill token range: %+v", tokenRange)
	}
	if err := validateHistoryNodeBackfillPartitioner(ctx, session, options.Partitioner); err != nil {
		return 0, err
	}
	scanQuery := templateScanHistoryTreeForV2Backfill
	if !forward {
		scanQuery = templateScanHistoryTreeV2ForV1Backfill
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(options.Concurrency)
	iter := session.Query(scanQuery, tokenRange.StartToken, tokenRange.EndToken).
		WithContext(groupCtx).
		PageSize(options.PageSize).
		Iter()
	var copied atomic.Int64
	for groupCtx.Err() == nil {
		var treeID, branchID, encoding string
		var branch []byte
		var writeTime nullableInt64
		var scanned bool
		if forward {
			scanned = iter.Scan(&treeID, &branchID, &branch, &encoding, &writeTime)
		} else {
			var bucket int
			scanned = iter.Scan(&treeID, &bucket, &branchID, &branch, &encoding, &writeTime)
		}
		if !scanned {
			break
		}
		if forward && branchID == historyTreeAuthorityBranchID {
			continue
		}
		if !writeTime.valid {
			_ = iter.Close()
			return copied.Load(), fmt.Errorf(
				"history tree row tree_id=%s branch_id=%s has no branch write timestamp",
				treeID,
				branchID,
			)
		}
		branch = bytes.Clone(branch)
		rowWriteTime := writeTime.value
		group.Go(func() error {
			var err error
			if forward {
				bucket, bucketErr := historyTreeBranchBucket(branchID)
				if bucketErr != nil {
					return bucketErr
				}
				err = session.Query(
					templateBackfillHistoryTreeV2,
					treeID,
					bucket,
					branchID,
					branch,
					encoding,
					rowWriteTime,
				).WithContext(groupCtx).Idempotent(true).Exec()
			} else {
				err = session.Query(
					templateBackfillHistoryTreeV1,
					treeID,
					branchID,
					branch,
					encoding,
					rowWriteTime,
				).WithContext(groupCtx).Idempotent(true).Exec()
			}
			if err != nil {
				return fmt.Errorf("backfill history tree row tree_id=%s branch_id=%s: %w", treeID, branchID, err)
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
		return copied.Load(), fmt.Errorf("scan history tree backfill source: %w", iterErr)
	}
	if err := ctx.Err(); err != nil {
		return copied.Load(), err
	}
	return copied.Load(), nil
}

// ValidateHistoryTreeV2 compares both layouts, including write timestamps, in both directions.
//
//nolint:revive // Exact validation intentionally scans both layouts and accounts for authority sentinel rows.
func ValidateHistoryTreeV2(
	ctx context.Context,
	session gocql.Session,
	pageSize int,
) (HistoryTreeValidationResult, error) {
	if pageSize <= 0 {
		return HistoryTreeValidationResult{}, errors.New("history tree validation page size must be positive")
	}
	var result HistoryTreeValidationResult
	legacyIter := session.Query(
		`SELECT tree_id, branch_id, branch, branch_encoding, writetime(branch) FROM history_tree`,
	).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var treeID, branchID, encoding string
		var branch []byte
		var writeTime nullableInt64
		if !legacyIter.Scan(&treeID, &branchID, &branch, &encoding, &writeTime) {
			break
		}
		if branchID == historyTreeAuthorityBranchID {
			continue
		}
		if !writeTime.valid {
			_ = legacyIter.Close()
			return result, fmt.Errorf("history_tree row %s/%s has no branch write timestamp", treeID, branchID)
		}
		result.LegacyRows++
		bucket, err := historyTreeBranchBucket(branchID)
		if err != nil {
			_ = legacyIter.Close()
			return result, err
		}
		match, found, err := readHistoryTreeValidationRow(
			ctx,
			session,
			templateValidateHistoryTreeV2Row,
			[]any{treeID, bucket, branchID},
			branch,
			encoding,
			writeTime.value,
		)
		if err != nil {
			_ = legacyIter.Close()
			return result, err
		}
		if !found {
			result.MissingInV2++
		} else if !match {
			result.MismatchedInV2++
		}
	}
	if err := legacyIter.Close(); err != nil {
		return result, fmt.Errorf("scan history_tree for validation: %w", err)
	}

	v2Iter := session.Query(
		`SELECT tree_id, branch_bucket, branch_id, branch, branch_encoding, writetime(branch) FROM history_tree_v2`,
	).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var treeID, branchID, encoding string
		var branch []byte
		var bucket int
		var writeTime int64
		if !v2Iter.Scan(&treeID, &bucket, &branchID, &branch, &encoding, &writeTime) {
			break
		}
		result.V2Rows++
		expectedBucket, err := historyTreeBranchBucket(branchID)
		if err != nil {
			_ = v2Iter.Close()
			return result, err
		}
		if bucket != expectedBucket {
			result.MismatchedLegacy++
			continue
		}
		match, found, err := readHistoryTreeValidationRow(
			ctx,
			session,
			templateValidateHistoryTreeV1Row,
			[]any{treeID, branchID},
			branch,
			encoding,
			writeTime,
		)
		if err != nil {
			_ = v2Iter.Close()
			return result, err
		}
		if !found {
			result.MissingInLegacy++
		} else if !match {
			result.MismatchedLegacy++
		}
	}
	if err := v2Iter.Close(); err != nil {
		return result, fmt.Errorf("scan history_tree_v2 for validation: %w", err)
	}
	return result, nil
}

func readHistoryTreeValidationRow(
	ctx context.Context,
	session gocql.Session,
	query string,
	args []any,
	expectedBranch []byte,
	expectedEncoding string,
	expectedWriteTime int64,
) (matches bool, found bool, err error) {
	var branch []byte
	var encoding string
	var writeTime int64
	err = session.Query(query, args...).WithContext(ctx).Scan(&branch, &encoding, &writeTime)
	if gocql.IsNotFoundError(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return bytes.Equal(branch, expectedBranch) &&
		encoding == expectedEncoding &&
		writeTime == expectedWriteTime, true, nil
}
