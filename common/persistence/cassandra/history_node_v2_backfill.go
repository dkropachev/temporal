package cassandra

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sync/atomic"

	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultHistoryNodeBackfillTokenRangeCount bounds replay work without excessive scan-query overhead.
	DefaultHistoryNodeBackfillTokenRangeCount = 4096
	// HistoryNodeBackfillMurmur3Partitioner is the only partitioner supported by signed 64-bit token ranges.
	HistoryNodeBackfillMurmur3Partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"

	maxHistoryNodeBackfillTokenRangeCount = 1 << 20

	templateGetHistoryNodeBackfillPartitioner = `SELECT partitioner FROM system.local WHERE key = 'local'`
	templateScanHistoryNodeForV2Backfill      = `SELECT tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding, ` +
		`writetime(data) FROM history_node WHERE token(tree_id) >= ? AND token(tree_id) <= ?`
	templateScanBranchHistoryNodeForV2Backfill = `SELECT tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding, ` +
		`writetime(data) FROM history_node WHERE token(tree_id, branch_id) >= ? AND token(tree_id, branch_id) <= ?`
	templateScanHistoryNodeV2ForV1Backfill = `SELECT tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding, ` +
		`writetime(data) FROM history_node_v2 WHERE token(tree_id, branch_id) >= ? AND token(tree_id, branch_id) <= ?`
	templateBackfillHistoryNodeV2 = `INSERT INTO history_node_v2 (` +
		`tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`
	templateBackfillHistoryNodeV1 = `INSERT INTO history_node (` +
		`tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`
)

// HistoryNodeBackfillOptions controls source layout, scan granularity, and bounded write parallelism.
type HistoryNodeBackfillOptions struct {
	PageSize        int
	Concurrency     int
	TokenRangeCount int
	SourceLayout    HistoryNodeTableLayout
	Partitioner     string
}

// HistoryNodeV2BackfillOptions is kept as an alias for callers of the original forward backfill.
type HistoryNodeV2BackfillOptions = HistoryNodeBackfillOptions

// HistoryNodeBackfillTokenRange is an inclusive Murmur3 token interval.
type HistoryNodeBackfillTokenRange struct {
	Index      int
	StartToken int64
	EndToken   int64
}

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
	spec, err := historyNodeV2BackfillSpec(options.SourceLayout)
	if err != nil {
		return 0, err
	}
	return backfillHistoryNodeTokenRanges(ctx, session, options, spec)
}

// BackfillHistoryNodeV1 idempotently restores the legacy table while preserving source write timestamps.
func BackfillHistoryNodeV1(
	ctx context.Context,
	session gocql.Session,
	options HistoryNodeBackfillOptions,
) (int64, error) {
	if options.SourceLayout != HistoryNodeTableLayoutMissing &&
		options.SourceLayout != HistoryNodeTableLayoutBranchV2 {
		return 0, fmt.Errorf(
			"history_node_v2 backfill source must use %s layout, got %s",
			HistoryNodeTableLayoutBranchV2,
			options.SourceLayout,
		)
	}
	return backfillHistoryNodeTokenRanges(ctx, session, options, historyNodeBackfillSpec{
		sourceTable: historyNodeV2TableName,
		targetTable: historyNodeTableName,
		scanQuery:   templateScanHistoryNodeV2ForV1Backfill,
		insertQuery: templateBackfillHistoryNodeV1,
	})
}

// BackfillHistoryNodeV2Range copies one inclusive source token range.
func BackfillHistoryNodeV2Range(
	ctx context.Context,
	session gocql.Session,
	options HistoryNodeBackfillOptions,
	tokenRange HistoryNodeBackfillTokenRange,
) (int64, error) {
	if err := validateHistoryNodeBackfillExecutionOptions(options); err != nil {
		return 0, err
	}
	if err := validateHistoryNodeBackfillPartitioner(ctx, session, options.Partitioner); err != nil {
		return 0, err
	}
	spec, err := historyNodeV2BackfillSpec(options.SourceLayout)
	if err != nil {
		return 0, err
	}
	return backfillHistoryNodeTokenRange(ctx, session, options, tokenRange, spec)
}

// BackfillHistoryNodeV1Range copies one inclusive source token range.
func BackfillHistoryNodeV1Range(
	ctx context.Context,
	session gocql.Session,
	options HistoryNodeBackfillOptions,
	tokenRange HistoryNodeBackfillTokenRange,
) (int64, error) {
	if err := validateHistoryNodeBackfillExecutionOptions(options); err != nil {
		return 0, err
	}
	if err := validateHistoryNodeBackfillPartitioner(ctx, session, options.Partitioner); err != nil {
		return 0, err
	}
	if options.SourceLayout != HistoryNodeTableLayoutMissing &&
		options.SourceLayout != HistoryNodeTableLayoutBranchV2 {
		return 0, fmt.Errorf(
			"history_node_v2 backfill source must use %s layout, got %s",
			HistoryNodeTableLayoutBranchV2,
			options.SourceLayout,
		)
	}
	return backfillHistoryNodeTokenRange(ctx, session, options, tokenRange, historyNodeBackfillSpec{
		sourceTable: historyNodeV2TableName,
		targetTable: historyNodeTableName,
		scanQuery:   templateScanHistoryNodeV2ForV1Backfill,
		insertQuery: templateBackfillHistoryNodeV1,
	})
}

// HistoryNodeBackfillTokenRanges splits the Murmur3 ring into contiguous inclusive intervals.
func HistoryNodeBackfillTokenRanges(count int) ([]HistoryNodeBackfillTokenRange, error) {
	if count <= 0 {
		return nil, errors.New("history node backfill token range count must be positive")
	}
	if count > maxHistoryNodeBackfillTokenRangeCount {
		return nil, fmt.Errorf(
			"history node backfill token range count must not exceed %d",
			maxHistoryNodeBackfillTokenRangeCount,
		)
	}

	ranges := make([]HistoryNodeBackfillTokenRange, count)
	for index := range count {
		// Divide the full 2^64 token space as a 128-bit integer to avoid overflow.
		startOffset, _ := bits.Div64(uint64(index), 0, uint64(count))
		startToken := int64(startOffset ^ (uint64(1) << 63))
		endToken := int64(^uint64(0) >> 1)
		if index+1 < count {
			nextOffset, _ := bits.Div64(uint64(index+1), 0, uint64(count))
			endToken = int64((nextOffset - 1) ^ (uint64(1) << 63))
		}
		ranges[index] = HistoryNodeBackfillTokenRange{
			Index:      index,
			StartToken: startToken,
			EndToken:   endToken,
		}
	}
	return ranges, nil
}

// GetHistoryNodeBackfillPartitioner reads the cluster partitioner used by token() queries.
func GetHistoryNodeBackfillPartitioner(
	ctx context.Context,
	session gocql.Session,
) (string, error) {
	var partitioner string
	if err := session.Query(
		templateGetHistoryNodeBackfillPartitioner,
	).WithContext(ctx).Scan(&partitioner); err != nil {
		return "", fmt.Errorf("read Cassandra partitioner for history node backfill: %w", err)
	}
	return partitioner, nil
}

func validateHistoryNodeBackfillPartitioner(
	ctx context.Context,
	session gocql.Session,
	partitioner string,
) error {
	if partitioner == "" {
		var err error
		partitioner, err = GetHistoryNodeBackfillPartitioner(ctx, session)
		if err != nil {
			return err
		}
	}
	if partitioner != HistoryNodeBackfillMurmur3Partitioner {
		return fmt.Errorf(
			"history node backfill requires Cassandra partitioner %q, got %q",
			HistoryNodeBackfillMurmur3Partitioner,
			partitioner,
		)
	}
	return nil
}

func historyNodeV2BackfillSpec(sourceLayout HistoryNodeTableLayout) (historyNodeBackfillSpec, error) {
	if sourceLayout == HistoryNodeTableLayoutMissing {
		sourceLayout = HistoryNodeTableLayoutLegacyV1
	}
	scanQuery := templateScanHistoryNodeForV2Backfill
	if sourceLayout == HistoryNodeTableLayoutBranchV2 {
		scanQuery = templateScanBranchHistoryNodeForV2Backfill
	} else if sourceLayout != HistoryNodeTableLayoutLegacyV1 {
		return historyNodeBackfillSpec{}, fmt.Errorf(
			"history_node backfill source must use %s or %s layout, got %s",
			HistoryNodeTableLayoutLegacyV1,
			HistoryNodeTableLayoutBranchV2,
			sourceLayout,
		)
	}
	return historyNodeBackfillSpec{
		sourceTable: historyNodeTableName,
		targetTable: historyNodeV2TableName,
		scanQuery:   scanQuery,
		insertQuery: templateBackfillHistoryNodeV2,
	}, nil
}

func backfillHistoryNodeTokenRanges(
	ctx context.Context,
	session gocql.Session,
	options HistoryNodeBackfillOptions,
	spec historyNodeBackfillSpec,
) (int64, error) {
	if err := validateHistoryNodeBackfillExecutionOptions(options); err != nil {
		return 0, err
	}
	tokenRangeCount := options.TokenRangeCount
	if tokenRangeCount == 0 {
		tokenRangeCount = DefaultHistoryNodeBackfillTokenRangeCount
	}
	tokenRanges, err := HistoryNodeBackfillTokenRanges(tokenRangeCount)
	if err != nil {
		return 0, err
	}
	if err := validateHistoryNodeBackfillPartitioner(ctx, session, options.Partitioner); err != nil {
		return 0, err
	}

	var copied int64
	for _, tokenRange := range tokenRanges {
		rangeCopied, err := backfillHistoryNodeTokenRange(ctx, session, options, tokenRange, spec)
		copied += rangeCopied
		if err != nil {
			return copied, fmt.Errorf(
				"backfill token range %d [%d, %d]: %w",
				tokenRange.Index,
				tokenRange.StartToken,
				tokenRange.EndToken,
				err,
			)
		}
	}
	return copied, nil
}

func validateHistoryNodeBackfillExecutionOptions(options HistoryNodeBackfillOptions) error {
	if options.PageSize <= 0 {
		return errors.New("history node backfill page size must be positive")
	}
	if options.Concurrency <= 0 {
		return errors.New("history node backfill concurrency must be positive")
	}
	return nil
}

func backfillHistoryNodeTokenRange(
	ctx context.Context,
	session gocql.Session,
	options HistoryNodeBackfillOptions,
	tokenRange HistoryNodeBackfillTokenRange,
	spec historyNodeBackfillSpec,
) (int64, error) {
	if options.PageSize <= 0 {
		return 0, errors.New("history node backfill page size must be positive")
	}
	if options.Concurrency <= 0 {
		return 0, errors.New("history node backfill concurrency must be positive")
	}
	if tokenRange.Index < 0 || tokenRange.StartToken > tokenRange.EndToken {
		return 0, fmt.Errorf("invalid history node backfill token range: %+v", tokenRange)
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(options.Concurrency)

	iter := session.Query(spec.scanQuery, tokenRange.StartToken, tokenRange.EndToken).
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
