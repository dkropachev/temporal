package cassandra

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"golang.org/x/sync/errgroup"
)

const (
	templateScanLegacyHistoryTreeIDs = `SELECT DISTINCT tree_id FROM history_tree ` +
		`WHERE token(tree_id) >= ? AND token(tree_id) <= ?`
	templateReadLegacyHistoryTreeForRepair = `SELECT branch_id, branch, branch_encoding, writetime(branch) ` +
		`FROM history_tree WHERE tree_id = ?`
	templateReadV2HistoryTreeBucketForRepair = `SELECT branch_id, branch, branch_encoding, writetime(branch) ` +
		`FROM history_tree_v2 WHERE tree_id = ? AND branch_bucket = ?`
	templateDeleteLegacyHistoryTreeAtTimestamp = `DELETE FROM history_tree USING TIMESTAMP ? ` +
		`WHERE tree_id = ? AND branch_id = ?`
	templateDeleteV2HistoryTreePartitionAtTimestamp = `DELETE FROM history_tree_v2 USING TIMESTAMP ? ` +
		`WHERE tree_id = ? AND branch_bucket = ?`
)

// HistoryTreeReconcileResult reports the work performed by an exact, fenced repair.
type HistoryTreeReconcileResult struct {
	Trees         int64
	RowsRewritten int64
	RowsDeleted   int64
}

func (r HistoryTreeReconcileResult) totalMutations() int64 {
	return r.RowsRewritten + r.RowsDeleted
}

// ReconcileHistoryTreeV2Range makes V2 exactly match source-authoritative legacy trees in one token range.
func ReconcileHistoryTreeV2Range(
	ctx context.Context,
	session gocql.Session,
	options HistoryTreeBackfillOptions,
	tokenRange HistoryNodeBackfillTokenRange,
) (int64, error) {
	result, err := reconcileHistoryTreeTokenRange(ctx, session, options, tokenRange, true)
	return result.totalMutations(), err
}

// ReconcileHistoryTreeV1Range makes legacy rows exactly match target-authoritative V2 trees in one token range.
func ReconcileHistoryTreeV1Range(
	ctx context.Context,
	session gocql.Session,
	options HistoryTreeBackfillOptions,
	tokenRange HistoryNodeBackfillTokenRange,
) (int64, error) {
	result, err := reconcileHistoryTreeTokenRange(ctx, session, options, tokenRange, false)
	return result.totalMutations(), err
}

func reconcileHistoryTreeTokenRange(
	ctx context.Context,
	session gocql.Session,
	options HistoryTreeBackfillOptions,
	tokenRange HistoryNodeBackfillTokenRange,
	forward bool,
) (HistoryTreeReconcileResult, error) {
	if err := validateHistoryTreeBackfillOptions(options); err != nil {
		return HistoryTreeReconcileResult{}, err
	}
	if tokenRange.Index < 0 || tokenRange.StartToken > tokenRange.EndToken {
		return HistoryTreeReconcileResult{}, fmt.Errorf("invalid history tree reconcile token range: %+v", tokenRange)
	}
	if err := validateHistoryNodeBackfillPartitioner(ctx, session, options.Partitioner); err != nil {
		return HistoryTreeReconcileResult{}, err
	}
	iter := session.Query(templateScanLegacyHistoryTreeIDs, tokenRange.StartToken, tokenRange.EndToken).
		WithContext(ctx).
		PageSize(options.PageSize).
		Iter()
	treeIDs := make(map[string]struct{})
	for {
		var treeID string
		if !iter.Scan(&treeID) {
			break
		}
		treeIDs[treeID] = struct{}{}
	}
	if err := iter.Close(); err != nil {
		return HistoryTreeReconcileResult{}, fmt.Errorf("scan history tree IDs for reconciliation: %w", err)
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(options.Concurrency)
	var trees atomic.Int64
	var rewritten atomic.Int64
	var deleted atomic.Int64
	for treeID := range treeIDs {
		group.Go(func() error {
			var result HistoryTreeReconcileResult
			var err error
			if forward {
				result, err = ReconcileHistoryTreeV2Tree(groupCtx, session, treeID, options.PageSize)
			} else {
				result, err = ReconcileHistoryTreeV1Tree(groupCtx, session, treeID, options.PageSize)
			}
			if err != nil {
				return fmt.Errorf("reconcile history tree %s: %w", treeID, err)
			}
			trees.Add(result.Trees)
			rewritten.Add(result.RowsRewritten)
			deleted.Add(result.RowsDeleted)
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return HistoryTreeReconcileResult{
			Trees:         trees.Load(),
			RowsRewritten: rewritten.Load(),
			RowsDeleted:   deleted.Load(),
		}, err
	}
	return HistoryTreeReconcileResult{
		Trees:         trees.Load(),
		RowsRewritten: rewritten.Load(),
		RowsDeleted:   deleted.Load(),
	}, nil
}

// ReconcileHistoryTreeV2Tree seals one legacy partition, repairs V2, validates it, and publishes target authority.
func ReconcileHistoryTreeV2Tree(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	pageSize int,
) (HistoryTreeReconcileResult, error) {
	return reconcileHistoryTree(ctx, session, treeID, pageSize, true)
}

// ReconcileHistoryTreeV1Tree seals one V2 tree, repairs legacy, validates it, and restores source authority.
func ReconcileHistoryTreeV1Tree(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	pageSize int,
) (HistoryTreeReconcileResult, error) {
	return reconcileHistoryTree(ctx, session, treeID, pageSize, false)
}

func reconcileHistoryTree(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	pageSize int,
	forward bool,
) (HistoryTreeReconcileResult, error) {
	if pageSize <= 0 {
		return HistoryTreeReconcileResult{}, errors.New("history tree reconciliation page size must be positive")
	}
	from := historyTreeMigrationAuthoritySource
	to := historyTreeMigrationAuthorityTarget
	if !forward {
		from, to = to, from
	}
	record, alreadyPublished, err := sealHistoryTree(ctx, session, treeID, from, to, pageSize)
	if err != nil {
		return HistoryTreeReconcileResult{}, err
	}
	if alreadyPublished {
		return HistoryTreeReconcileResult{}, nil
	}
	result := HistoryTreeReconcileResult{Trees: 1}
	if forward {
		err = repairHistoryTreeV2(ctx, session, treeID, pageSize, record.timestamp, &result)
	} else {
		err = repairHistoryTreeV1(ctx, session, treeID, pageSize, record.timestamp, &result)
	}
	if err != nil {
		return result, err
	}
	validation, err := validateOneHistoryTree(ctx, session, treeID, pageSize)
	if err != nil {
		return result, err
	}
	if !validation.Matches() {
		return result, fmt.Errorf("history tree %s differs after fenced repair: %+v", treeID, validation)
	}
	applied, err := session.Query(
		templateTransitionHistoryTreeAuthority,
		int(to),
		record.timestamp,
		treeID,
		historyTreeAuthorityBranchID,
		int(historyTreeMigrationAuthoritySealing),
		record.timestamp,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return result, gocql.ConvertError("PublishHistoryTreeAuthority", err)
	}
	if applied {
		return result, nil
	}
	current, err := readHistoryTreeAuthority(ctx, session, treeID)
	if err != nil {
		return result, gocql.ConvertError("ReadHistoryTreeAuthorityAfterPublish", err)
	}
	if current.authority == to && current.timestamp == record.timestamp {
		return result, nil
	}
	return result, historyTreeAuthorityConflict(treeID, []historyTreeMigrationAuthority{to}, current)
}

//nolint:revive // Sealing is an idempotent authority transition with retry and resume paths.
func sealHistoryTree(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	from historyTreeMigrationAuthority,
	to historyTreeMigrationAuthority,
	pageSize int,
) (historyTreeAuthorityRecord, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return historyTreeAuthorityRecord{}, false, err
		}
		record, err := readHistoryTreeAuthority(ctx, session, treeID)
		if gocql.IsNotFoundError(err) || (err == nil && record.authority == historyTreeMigrationAuthorityUnspecified) {
			record, err = initializeHistoryTreeAuthority(ctx, session, treeID, from)
		}
		if err != nil {
			return historyTreeAuthorityRecord{}, false, gocql.ConvertError("ReadHistoryTreeAuthorityForSeal", err)
		}
		if record.authority == to {
			return record, true, nil
		}
		if record.authority == historyTreeMigrationAuthoritySealing {
			return record, false, nil
		}
		if record.authority != from {
			return historyTreeAuthorityRecord{}, false, historyTreeAuthorityConflict(
				treeID,
				[]historyTreeMigrationAuthority{from},
				record,
			)
		}
		maximumWriteTime, err := maximumHistoryTreeWriteTime(ctx, session, treeID, pageSize)
		if err != nil {
			return historyTreeAuthorityRecord{}, false, err
		}
		if record.timestamp >= math.MaxInt64-1 || maximumWriteTime >= math.MaxInt64-1 {
			return historyTreeAuthorityRecord{}, false, errors.New("history tree repair timestamp overflow")
		}
		now := time.Now().UTC().UnixMicro()
		if now >= math.MaxInt64-1 {
			return historyTreeAuthorityRecord{}, false, errors.New("history tree repair timestamp overflow")
		}
		repairTimestamp := max(now+1, record.timestamp+2, maximumWriteTime+2)
		applied, err := session.Query(
			templateTransitionHistoryTreeAuthority,
			int(historyTreeMigrationAuthoritySealing),
			repairTimestamp,
			treeID,
			historyTreeAuthorityBranchID,
			int(from),
			record.timestamp,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return historyTreeAuthorityRecord{}, false, gocql.ConvertError("SealHistoryTreeAuthority", err)
		}
		if applied {
			return historyTreeAuthorityRecord{
				authority: historyTreeMigrationAuthoritySealing,
				timestamp: repairTimestamp,
			}, false, nil
		}
	}
}

func maximumHistoryTreeWriteTime(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	pageSize int,
) (int64, error) {
	var maximum int64
	legacyIter := session.Query(templateReadLegacyHistoryTreeForRepair, treeID).
		WithContext(ctx).
		PageSize(pageSize).
		Iter()
	for {
		var branchID, encoding string
		var branch []byte
		var writeTime nullableInt64
		if !legacyIter.Scan(&branchID, &branch, &encoding, &writeTime) {
			break
		}
		if branchID != historyTreeAuthorityBranchID && writeTime.valid {
			maximum = max(maximum, writeTime.value)
		}
	}
	if err := legacyIter.Close(); err != nil {
		return 0, fmt.Errorf("scan legacy history tree timestamps: %w", err)
	}
	for bucket := range historyTreeV2BucketCount {
		iter := session.Query(templateReadV2HistoryTreeBucketForRepair, treeID, bucket).
			WithContext(ctx).
			PageSize(pageSize).
			Iter()
		for {
			var branchID, encoding string
			var branch []byte
			var writeTime nullableInt64
			if !iter.Scan(&branchID, &branch, &encoding, &writeTime) {
				break
			}
			if writeTime.valid {
				maximum = max(maximum, writeTime.value)
			}
		}
		if err := iter.Close(); err != nil {
			return 0, fmt.Errorf("scan V2 history tree bucket %d timestamps: %w", bucket, err)
		}
	}
	return maximum, nil
}

func repairHistoryTreeV2(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	pageSize int,
	repairTimestamp int64,
	result *HistoryTreeReconcileResult,
) error {
	for bucket := range historyTreeV2BucketCount {
		if err := session.Query(
			templateDeleteV2HistoryTreePartitionAtTimestamp,
			repairTimestamp-1,
			treeID,
			bucket,
		).WithContext(ctx).Idempotent(true).Exec(); err != nil {
			return fmt.Errorf("reset V2 history tree bucket %d: %w", bucket, err)
		}
	}
	iter := session.Query(templateReadLegacyHistoryTreeForRepair, treeID).
		WithContext(ctx).
		PageSize(pageSize).
		Iter()
	for {
		var branchID, encoding string
		var branch []byte
		var writeTime nullableInt64
		if !iter.Scan(&branchID, &branch, &encoding, &writeTime) {
			break
		}
		if branchID == historyTreeAuthorityBranchID {
			continue
		}
		bucket, err := historyTreeBranchBucket(branchID)
		if err != nil {
			_ = iter.Close()
			return err
		}
		if err := session.Query(
			templateBackfillHistoryTreeV1,
			treeID,
			branchID,
			branch,
			encoding,
			repairTimestamp,
		).WithContext(ctx).Idempotent(true).Exec(); err != nil {
			_ = iter.Close()
			return fmt.Errorf("rewrite legacy history tree row %s: %w", branchID, err)
		}
		if err := session.Query(
			templateBackfillHistoryTreeV2,
			treeID,
			bucket,
			branchID,
			branch,
			encoding,
			repairTimestamp,
		).WithContext(ctx).Idempotent(true).Exec(); err != nil {
			_ = iter.Close()
			return fmt.Errorf("rewrite V2 history tree row %s: %w", branchID, err)
		}
		result.RowsRewritten += 2
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("scan legacy history tree for repair: %w", err)
	}
	return nil
}

//nolint:revive // Reverse repair compares and rewrites every bucket while preserving write timestamps.
func repairHistoryTreeV1(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	pageSize int,
	repairTimestamp int64,
	result *HistoryTreeReconcileResult,
) error {
	type historyTreeRepairRow struct {
		branchID string
		branch   []byte
		encoding string
	}
	for bucket := range historyTreeV2BucketCount {
		iter := session.Query(templateReadV2HistoryTreeBucketForRepair, treeID, bucket).
			WithContext(ctx).
			PageSize(pageSize).
			Iter()
		rows := make([]historyTreeRepairRow, 0, pageSize)
		for {
			var branchID, encoding string
			var branch []byte
			var writeTime nullableInt64
			if !iter.Scan(&branchID, &branch, &encoding, &writeTime) {
				break
			}
			rows = append(rows, historyTreeRepairRow{
				branchID: branchID,
				branch:   bytes.Clone(branch),
				encoding: encoding,
			})
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("scan V2 history tree bucket %d for reverse repair: %w", bucket, err)
		}
		if err := session.Query(
			templateDeleteV2HistoryTreePartitionAtTimestamp,
			repairTimestamp-1,
			treeID,
			bucket,
		).WithContext(ctx).Idempotent(true).Exec(); err != nil {
			return fmt.Errorf("reset V2 history tree bucket %d for reverse repair: %w", bucket, err)
		}
		for _, row := range rows {
			branchID := row.branchID
			branch := row.branch
			encoding := row.encoding
			expectedBucket, err := historyTreeBranchBucket(branchID)
			if err != nil {
				return err
			}
			if expectedBucket != bucket {
				canonicalFound, canonicalErr := v2HistoryTreeBranchExists(
					ctx,
					session,
					treeID,
					expectedBucket,
					branchID,
				)
				if canonicalErr != nil {
					return canonicalErr
				}
				if !canonicalFound {
					if err := session.Query(
						templateBackfillHistoryTreeV2,
						treeID,
						expectedBucket,
						branchID,
						branch,
						encoding,
						repairTimestamp,
					).WithContext(ctx).Idempotent(true).Exec(); err != nil {
						return fmt.Errorf("move V2 history tree row %s to canonical bucket: %w", branchID, err)
					}
					if err := session.Query(
						templateBackfillHistoryTreeV1,
						treeID,
						branchID,
						branch,
						encoding,
						repairTimestamp,
					).WithContext(ctx).Idempotent(true).Exec(); err != nil {
						return fmt.Errorf("restore moved history tree row %s in legacy: %w", branchID, err)
					}
					result.RowsRewritten += 2
				}
				result.RowsDeleted++
				continue
			}
			if err := session.Query(
				templateBackfillHistoryTreeV2,
				treeID,
				bucket,
				branchID,
				branch,
				encoding,
				repairTimestamp,
			).WithContext(ctx).Idempotent(true).Exec(); err != nil {
				return fmt.Errorf("rewrite V2 history tree row %s: %w", branchID, err)
			}
			if err := session.Query(
				templateBackfillHistoryTreeV1,
				treeID,
				branchID,
				branch,
				encoding,
				repairTimestamp,
			).WithContext(ctx).Idempotent(true).Exec(); err != nil {
				return fmt.Errorf("rewrite legacy history tree row %s: %w", branchID, err)
			}
			result.RowsRewritten += 2
		}
	}

	iter := session.Query(templateReadLegacyHistoryTreeForRepair, treeID).
		WithContext(ctx).
		PageSize(pageSize).
		Iter()
	for {
		var branchID, encoding string
		var branch []byte
		var writeTime nullableInt64
		if !iter.Scan(&branchID, &branch, &encoding, &writeTime) {
			break
		}
		if branchID == historyTreeAuthorityBranchID {
			continue
		}
		bucket, err := historyTreeBranchBucket(branchID)
		if err != nil {
			_ = iter.Close()
			return err
		}
		found, err := v2HistoryTreeBranchExists(ctx, session, treeID, bucket, branchID)
		if err != nil {
			_ = iter.Close()
			return err
		}
		if found {
			continue
		}
		if err := session.Query(
			templateDeleteLegacyHistoryTreeAtTimestamp,
			repairTimestamp,
			treeID,
			branchID,
		).WithContext(ctx).Idempotent(true).Exec(); err != nil {
			_ = iter.Close()
			return fmt.Errorf("delete extra legacy history tree row %s: %w", branchID, err)
		}
		result.RowsDeleted++
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("scan legacy history tree for reverse repair: %w", err)
	}
	return nil
}

func v2HistoryTreeBranchExists(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	bucket int,
	branchID string,
) (bool, error) {
	var branch []byte
	err := session.Query(
		templateValidateHistoryTreeV2Row,
		treeID,
		bucket,
		branchID,
	).WithContext(ctx).Scan(&branch, new(string), new(int64))
	if gocql.IsNotFoundError(err) {
		return false, nil
	}
	return err == nil, err
}

//nolint:revive // Exact per-tree validation compares both layouts in both directions.
func validateOneHistoryTree(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	pageSize int,
) (HistoryTreeValidationResult, error) {
	var result HistoryTreeValidationResult
	legacyIter := session.Query(templateReadLegacyHistoryTreeForRepair, treeID).
		WithContext(ctx).
		PageSize(pageSize).
		Iter()
	for {
		var branchID, encoding string
		var branch []byte
		var writeTime nullableInt64
		if !legacyIter.Scan(&branchID, &branch, &encoding, &writeTime) {
			break
		}
		if branchID == historyTreeAuthorityBranchID {
			continue
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
		return result, err
	}
	for bucket := range historyTreeV2BucketCount {
		iter := session.Query(templateReadV2HistoryTreeBucketForRepair, treeID, bucket).
			WithContext(ctx).
			PageSize(pageSize).
			Iter()
		for {
			var branchID, encoding string
			var branch []byte
			var writeTime nullableInt64
			if !iter.Scan(&branchID, &branch, &encoding, &writeTime) {
				break
			}
			result.V2Rows++
			expectedBucket, err := historyTreeBranchBucket(branchID)
			if err != nil {
				_ = iter.Close()
				return result, err
			}
			if expectedBucket != bucket {
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
				writeTime.value,
			)
			if err != nil {
				_ = iter.Close()
				return result, err
			}
			if !found {
				result.MissingInLegacy++
			} else if !match {
				result.MismatchedLegacy++
			}
		}
		if err := iter.Close(); err != nil {
			return result, err
		}
	}
	return result, nil
}
