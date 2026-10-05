package cassandra

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultTaskQueueUserDataBackfillTokenRangeCount = 4096
	taskQueueUserDataMurmur3Partitioner             = "org.apache.cassandra.dht.Murmur3Partitioner"
	maxTaskQueueUserDataBackfillTokenRangeCount     = 1 << 20

	templateGetTaskQueueUserDataBackfillPartitioner = `SELECT partitioner FROM system.local WHERE key = 'local'`
	templateScanTaskQueueUserDataForV2Backfill      = `SELECT namespace_id, build_id, task_queue_name, data, data_encoding, version
		, writetime(data), writetime(data_encoding), writetime(version)
		FROM task_queue_user_data WHERE token(namespace_id) >= ? AND token(namespace_id) <= ?`
	templateBackfillTaskQueueUserDataV2 = `INSERT INTO task_queue_user_data_v2
		(namespace_id, bucket_id, build_id, task_queue_name, data, data_encoding, version, present)
		VALUES (?, ?, ?, ?, ?, ?, ?, true) USING TIMESTAMP ?`
	templateScanTaskQueueUserDataNamespaceForValidation = `SELECT build_id, task_queue_name, data, data_encoding, version
		, writetime(data), writetime(data_encoding), writetime(version)
		FROM task_queue_user_data WHERE namespace_id = ?`
	templateScanTaskQueueUserDataV2BucketForValidation = `SELECT build_id, task_queue_name, data, data_encoding, version, present,
		pending_txn_id, pending_data, pending_data_encoding, pending_version, pending_present,
		writetime(data) AS migration_wt_data, writetime(data_encoding) AS migration_wt_encoding,
		writetime(version) AS migration_wt_version, writetime(present) AS migration_wt_present,
		writetime(pending_data) AS migration_wt_pending_data,
		writetime(pending_data_encoding) AS migration_wt_pending_encoding,
		writetime(pending_version) AS migration_wt_pending_version,
		writetime(pending_present) AS migration_wt_pending_present
		FROM task_queue_user_data_v2 WHERE namespace_id = ? AND bucket_id = ?`
	templateScanPendingTaskQueueUserDataV2ForRecovery = `SELECT namespace_id, bucket_id, build_id, task_queue_name,
		data, data_encoding, version, present, pending_txn_id, pending_data, pending_data_encoding,
		pending_version, pending_present FROM task_queue_user_data_v2
		WHERE token(namespace_id, bucket_id) >= ? AND token(namespace_id, bucket_id) <= ?`
	templateScanTaskQueueUserDataV2TransactionsForRecovery = `SELECT namespace_id, txn_id, state, expires_at
		FROM task_queue_user_data_v2_txn WHERE token(namespace_id) >= ? AND token(namespace_id) <= ?`
	templateScanTaskQueueUserDataV2PendingTransactions = `SELECT pending_txn_id FROM task_queue_user_data_v2
		WHERE namespace_id = ? AND bucket_id = ?`
	templateScanTaskQueueUserDataV1Namespaces = `SELECT DISTINCT namespace_id FROM task_queue_user_data`
	templateScanTaskQueueUserDataV2Namespaces = `SELECT DISTINCT namespace_id, bucket_id FROM task_queue_user_data_v2`
	templateBackfillTaskQueueUserDataV1       = `INSERT INTO task_queue_user_data
		(namespace_id, build_id, task_queue_name, data, data_encoding, version)
		VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`
	templateDeleteTaskQueueUserDataV1 = `DELETE FROM task_queue_user_data USING TIMESTAMP ?
		WHERE namespace_id = ? AND build_id = ? AND task_queue_name = ?`
	templateDeleteTaskQueueUserDataV2 = `DELETE FROM task_queue_user_data_v2 USING TIMESTAMP ?
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ? AND task_queue_name = ?`
	templateReconcileTaskQueueUserDataV2 = `UPDATE task_queue_user_data_v2 USING TIMESTAMP ? SET
		data = ?, data_encoding = ?, version = ?, present = true,
		pending_txn_id = null, pending_data = null, pending_data_encoding = null,
		pending_version = null, pending_present = null
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ? AND task_queue_name = ?`
)

var taskQueueUserDataMigrationClock atomic.Int64

type TaskQueueUserDataBackfillOptions struct {
	PageSize        int
	Concurrency     int
	TokenRangeCount int
	BucketCount     int
	Partitioner     string
}

type TaskQueueUserDataBackfillTokenRange struct {
	Index      int
	StartToken int64
	EndToken   int64
}

type TaskQueueUserDataValidationResult struct {
	SourceRows int
	TargetRows int
	Mismatches []string
}

type TaskQueueUserDataReconciliationResult struct {
	Copied  int
	Deleted int
}

type taskQueueUserDataReconciliationOperation struct {
	copied bool
	run    func(context.Context) error
}

type TaskQueueUserDataRecoveryOptions struct {
	PageSize        int
	Concurrency     int
	TokenRangeCount int
	BucketCount     int
	Partitioner     string
}

type taskQueueUserDataV2TransactionKey struct {
	namespaceID string
	txnID       string
}

//nolint:revive // Recovery coordinates transaction, namespace, and row state across crash-resume paths.
func RecoverTaskQueueUserDataV2NamespaceTransactions(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	bucketCount int,
	concurrency int,
) (int64, error) {
	if namespaceID == "" {
		return 0, errors.New("task queue user data recovery requires namespace ID")
	}
	if concurrency <= 0 {
		return 0, errors.New("task queue user data recovery concurrency must be positive")
	}
	if _, err := taskQueueUserDataBucket("", bucketCount); err != nil {
		return 0, err
	}
	store := newUserDataStore(
		session,
		nil,
		TaskQueueUserDataMigrationModeTargetOnly,
		bucketCount,
	)
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	var recovered atomic.Int64
	var unresolved atomic.Int64
	for bucket := range bucketCount {
		iter := session.Query(
			templateScanTaskQueueUserDataV2BucketForValidation,
			namespaceID,
			int16(bucket),
		).WithContext(groupCtx).Iter()
		rowMap := make(map[string]any)
		for groupCtx.Err() == nil && iter.MapScan(rowMap) {
			buildID, ok, err := optionalTaskQueueUserDataV2Field[string](rowMap, "build_id")
			if err != nil {
				_ = iter.Close()
				return recovered.Load(), err
			}
			if !ok {
				_ = iter.Close()
				return recovered.Load(), newFieldNotFoundError("build_id", rowMap)
			}
			row, err := decodeTaskQueueUserDataV2Row(rowMap, int16(bucket), buildID)
			if err != nil {
				_ = iter.Close()
				return recovered.Load(), err
			}
			rowMap = make(map[string]any)
			if row.pendingTxnID == "" {
				continue
			}
			participant := row
			group.Go(func() error {
				resolved, err := store.recoverTaskQueueUserDataV2Row(groupCtx, namespaceID, participant)
				if err != nil {
					return err
				}
				if resolved {
					recovered.Add(1)
				} else {
					unresolved.Add(1)
				}
				return nil
			})
		}
		if err := iter.Close(); err != nil {
			return recovered.Load(), gocql.ConvertError("ScanTaskQueueUserDataV2NamespaceForRecovery", err)
		}
	}
	if err := group.Wait(); err != nil {
		return recovered.Load(), err
	}
	if unresolved.Load() != 0 {
		return recovered.Load(), serviceerror.NewUnavailablef(
			"task queue user data namespace %s has %d active transaction participants; retry cutover",
			namespaceID,
			unresolved.Load(),
		)
	}
	return recovered.Load(), nil
}

func (r TaskQueueUserDataValidationResult) Matches() bool {
	return len(r.Mismatches) == 0
}

func RecoverTaskQueueUserDataV2Transactions(
	ctx context.Context,
	session gocql.Session,
	options TaskQueueUserDataRecoveryOptions,
) (int64, error) {
	if err := validateTaskQueueUserDataRecoveryOptions(options); err != nil {
		return 0, err
	}
	tokenRangeCount := options.TokenRangeCount
	if tokenRangeCount == 0 {
		tokenRangeCount = DefaultTaskQueueUserDataBackfillTokenRangeCount
	}
	ranges, err := TaskQueueUserDataBackfillTokenRanges(tokenRangeCount)
	if err != nil {
		return 0, err
	}
	if err := validateTaskQueueUserDataBackfillPartitioner(ctx, session, options.Partitioner); err != nil {
		return 0, err
	}
	var recovered int64
	for _, tokenRange := range ranges {
		rangeRecovered, err := RecoverTaskQueueUserDataV2TransactionsRange(ctx, session, options, tokenRange)
		recovered += rangeRecovered
		if err != nil {
			return recovered, fmt.Errorf(
				"recover task queue user data token range %d [%d, %d]: %w",
				tokenRange.Index,
				tokenRange.StartToken,
				tokenRange.EndToken,
				err,
			)
		}
	}
	return recovered, nil
}

//nolint:revive // Range recovery validates leases and repairs multiple transaction outcomes concurrently.
func RecoverTaskQueueUserDataV2TransactionsRange(
	ctx context.Context,
	session gocql.Session,
	options TaskQueueUserDataRecoveryOptions,
	tokenRange TaskQueueUserDataBackfillTokenRange,
) (int64, error) {
	if err := validateTaskQueueUserDataRecoveryOptions(options); err != nil {
		return 0, err
	}
	if tokenRange.Index < 0 || tokenRange.StartToken > tokenRange.EndToken {
		return 0, fmt.Errorf("invalid task queue user data recovery token range: %+v", tokenRange)
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(options.Concurrency)
	iter := session.Query(
		templateScanPendingTaskQueueUserDataV2ForRecovery,
		tokenRange.StartToken,
		tokenRange.EndToken,
	).WithContext(groupCtx).PageSize(options.PageSize).Iter()
	store := newUserDataStore(
		session,
		nil,
		TaskQueueUserDataMigrationModeTargetOnly,
		effectiveTaskQueueUserDataRecoveryBucketCount(options),
	)
	var recovered atomic.Int64
	candidates := make(map[taskQueueUserDataV2TransactionKey]struct{})
	rowMap := make(map[string]any)
	for groupCtx.Err() == nil && iter.MapScan(rowMap) {
		namespaceID, ok, err := optionalTaskQueueUserDataV2Field[string](rowMap, "namespace_id")
		if err != nil {
			_ = iter.Close()
			return recovered.Load(), err
		}
		if !ok {
			_ = iter.Close()
			return recovered.Load(), newFieldNotFoundError("namespace_id", rowMap)
		}
		bucket, ok, err := optionalTaskQueueUserDataV2Field[int16](rowMap, "bucket_id")
		if err != nil {
			_ = iter.Close()
			return recovered.Load(), err
		}
		if !ok {
			_ = iter.Close()
			return recovered.Load(), newFieldNotFoundError("bucket_id", rowMap)
		}
		buildID, ok, err := optionalTaskQueueUserDataV2Field[string](rowMap, "build_id")
		if err != nil {
			_ = iter.Close()
			return recovered.Load(), err
		}
		if !ok {
			_ = iter.Close()
			return recovered.Load(), newFieldNotFoundError("build_id", rowMap)
		}
		row, err := decodeTaskQueueUserDataV2Row(rowMap, bucket, buildID)
		if err != nil {
			_ = iter.Close()
			return recovered.Load(), err
		}
		rowMap = make(map[string]any)
		if row.pendingTxnID == "" {
			continue
		}
		candidates[taskQueueUserDataV2TransactionKey{
			namespaceID: namespaceID,
			txnID:       row.pendingTxnID,
		}] = struct{}{}
		participantNamespaceID := namespaceID
		participant := row
		group.Go(func() error {
			resolved, err := store.recoverTaskQueueUserDataV2Row(groupCtx, participantNamespaceID, participant)
			if err != nil {
				return err
			}
			if resolved {
				recovered.Add(1)
			}
			return nil
		})
	}
	closeErr := iter.Close()
	groupErr := group.Wait()
	if groupErr != nil {
		return recovered.Load(), groupErr
	}
	if closeErr != nil {
		return recovered.Load(), gocql.ConvertError("ScanTaskQueueUserDataV2ForRecovery", closeErr)
	}
	terminalCandidates, err := scanTaskQueueUserDataV2TerminalTransactionsRange(ctx, session, options, tokenRange)
	if err != nil {
		return recovered.Load(), err
	}
	for candidate := range terminalCandidates {
		candidates[candidate] = struct{}{}
	}
	cleanupGroup, cleanupCtx := errgroup.WithContext(ctx)
	cleanupGroup.SetLimit(options.Concurrency)
	for candidate := range candidates {
		candidate := candidate
		cleanupGroup.Go(func() error {
			return store.expireUnreferencedTaskQueueUserDataV2Transaction(cleanupCtx, candidate)
		})
	}
	if err := cleanupGroup.Wait(); err != nil {
		return recovered.Load(), err
	}
	return recovered.Load(), nil
}

func scanTaskQueueUserDataV2TerminalTransactionsRange(
	ctx context.Context,
	session gocql.Session,
	options TaskQueueUserDataRecoveryOptions,
	tokenRange TaskQueueUserDataBackfillTokenRange,
) (map[taskQueueUserDataV2TransactionKey]struct{}, error) {
	iter := session.Query(
		templateScanTaskQueueUserDataV2TransactionsForRecovery,
		tokenRange.StartToken,
		tokenRange.EndToken,
	).WithContext(ctx).PageSize(options.PageSize).Iter()
	transactions := make(map[taskQueueUserDataV2TransactionKey]struct{})
	for {
		var namespaceID, txnID, state string
		var expiresAt time.Time
		if !iter.Scan(&namespaceID, &txnID, &state, &expiresAt) {
			break
		}
		if txnID == "" {
			continue
		}
		if state == taskQueueUserDataV2TransactionPreparing && time.Now().UTC().Before(expiresAt) {
			continue
		}
		if state != taskQueueUserDataV2TransactionPreparing &&
			state != taskQueueUserDataV2TransactionCommitted &&
			state != taskQueueUserDataV2TransactionAborted {
			continue
		}
		transactions[taskQueueUserDataV2TransactionKey{namespaceID: namespaceID, txnID: txnID}] = struct{}{}
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ScanTaskQueueUserDataV2TransactionsForRecovery", err)
	}
	return transactions, nil
}

//nolint:revive // Expiration must distinguish every transaction and namespace ownership state.
func (d *userDataStore) expireUnreferencedTaskQueueUserDataV2Transaction(
	ctx context.Context,
	key taskQueueUserDataV2TransactionKey,
) error {
	txn, err := d.readTaskQueueUserDataV2Transaction(ctx, key.namespaceID, key.txnID)
	if gocql.IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return gocql.ConvertError("ReadTaskQueueUserDataV2TransactionForCleanup", err)
	}
	if txn.state == taskQueueUserDataV2TransactionPreparing {
		if time.Now().UTC().Before(txn.expiresAt) {
			return nil
		}
		namespaceState, err := d.readTaskQueueUserDataV2Namespace(ctx, key.namespaceID)
		if err != nil {
			return err
		}
		var aborted bool
		if namespaceState.activeTxnID == key.txnID {
			aborted, err = d.abortTaskQueueUserDataV2Transaction(
				ctx,
				key.namespaceID,
				key.txnID,
				namespaceState.epoch,
			)
		} else {
			aborted, err = d.Session.Query(
				templateForceAbortTaskQueueUserDataV2TransactionQuery,
				taskQueueUserDataV2TransactionAborted,
				time.Now().UTC().Add(taskQueueUserDataV2TransactionRetention),
				key.namespaceID,
				key.txnID,
				taskQueueUserDataV2TransactionPreparing,
			).WithContext(ctx).MapScanCAS(make(map[string]any))
		}
		if err != nil {
			return gocql.ConvertError("AbortExpiredTaskQueueUserDataV2Transaction", err)
		}
		if !aborted {
			txn, err = d.readTaskQueueUserDataV2Transaction(ctx, key.namespaceID, key.txnID)
			if gocql.IsNotFoundError(err) {
				return nil
			}
			if err != nil {
				return gocql.ConvertError("ResolveExpiredTaskQueueUserDataV2Transaction", err)
			}
			if txn.state == taskQueueUserDataV2TransactionPreparing {
				return nil
			}
		} else {
			txn.state = taskQueueUserDataV2TransactionAborted
		}
	}
	if txn.state != taskQueueUserDataV2TransactionCommitted && txn.state != taskQueueUserDataV2TransactionAborted {
		return serviceerror.NewDataLossf("unknown task queue user data transaction state %q", txn.state)
	}
	for bucket := range d.effectiveTaskQueueUserDataBucketCount() {
		iter := d.Session.Query(
			templateScanTaskQueueUserDataV2PendingTransactions,
			key.namespaceID,
			int16(bucket),
		).WithContext(ctx).Iter()
		row := make(map[string]any)
		for iter.MapScan(row) {
			pendingTxnID, ok, fieldErr := optionalTaskQueueUserDataV2Field[string](row, "pending_txn_id")
			if fieldErr != nil {
				_ = iter.Close()
				return fieldErr
			}
			if ok && pendingTxnID == key.txnID {
				if closeErr := iter.Close(); closeErr != nil {
					return gocql.ConvertError("ScanTaskQueueUserDataV2ParticipantsForCleanup", closeErr)
				}
				return nil
			}
			row = make(map[string]any)
		}
		if err := iter.Close(); err != nil {
			return gocql.ConvertError("ScanTaskQueueUserDataV2ParticipantsForCleanup", err)
		}
	}
	return d.expireTerminalTaskQueueUserDataV2Transaction(ctx, key.namespaceID, key.txnID, txn.state)
}

func BackfillTaskQueueUserDataV2(
	ctx context.Context,
	session gocql.Session,
	options TaskQueueUserDataBackfillOptions,
) (int64, error) {
	if err := validateTaskQueueUserDataBackfillOptions(options); err != nil {
		return 0, err
	}
	tokenRangeCount := options.TokenRangeCount
	if tokenRangeCount == 0 {
		tokenRangeCount = DefaultTaskQueueUserDataBackfillTokenRangeCount
	}
	ranges, err := TaskQueueUserDataBackfillTokenRanges(tokenRangeCount)
	if err != nil {
		return 0, err
	}
	if err := validateTaskQueueUserDataBackfillPartitioner(ctx, session, options.Partitioner); err != nil {
		return 0, err
	}

	var copied int64
	for _, tokenRange := range ranges {
		rangeCopied, err := BackfillTaskQueueUserDataV2Range(ctx, session, options, tokenRange)
		copied += rangeCopied
		if err != nil {
			return copied, fmt.Errorf(
				"backfill task queue user data token range %d [%d, %d]: %w",
				tokenRange.Index,
				tokenRange.StartToken,
				tokenRange.EndToken,
				err,
			)
		}
	}
	return copied, nil
}

func BackfillTaskQueueUserDataV2Range(
	ctx context.Context,
	session gocql.Session,
	options TaskQueueUserDataBackfillOptions,
	tokenRange TaskQueueUserDataBackfillTokenRange,
) (int64, error) {
	if err := validateTaskQueueUserDataBackfillOptions(options); err != nil {
		return 0, err
	}
	if tokenRange.Index < 0 || tokenRange.StartToken > tokenRange.EndToken {
		return 0, fmt.Errorf("invalid task queue user data backfill token range: %+v", tokenRange)
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(options.Concurrency)
	iter := session.Query(
		templateScanTaskQueueUserDataForV2Backfill,
		tokenRange.StartToken,
		tokenRange.EndToken,
	).WithContext(groupCtx).PageSize(options.PageSize).Iter()
	var copied atomic.Int64
	for groupCtx.Err() == nil {
		var (
			namespaceID  string
			buildID      string
			taskQueue    string
			data         []byte
			encoding     string
			version      int64
			dataTime     nullableInt64
			encodingTime nullableInt64
			versionTime  nullableInt64
		)
		if !iter.Scan(
			&namespaceID,
			&buildID,
			&taskQueue,
			&data,
			&encoding,
			&version,
			&dataTime,
			&encodingTime,
			&versionTime,
		) {
			break
		}
		bucket, err := taskQueueUserDataBucket(taskQueue, effectiveTaskQueueUserDataBackfillBucketCount(options))
		if err != nil {
			_ = iter.Close()
			return copied.Load(), err
		}
		data = bytes.Clone(data)
		writeTime := taskQueueUserDataMigrationWriteTime(dataTime, encodingTime, versionTime)
		group.Go(func() error {
			if err := session.Query(
				templateBackfillTaskQueueUserDataV2,
				namespaceID,
				bucket,
				buildID,
				taskQueue,
				data,
				encoding,
				version,
				writeTime,
			).WithContext(groupCtx).Idempotent(true).Exec(); err != nil {
				return fmt.Errorf("backfill task queue user data namespace=%s task_queue=%q build_id=%q: %w", namespaceID, taskQueue, buildID, err)
			}
			copied.Add(1)
			return nil
		})
	}
	closeErr := iter.Close()
	groupErr := group.Wait()
	if groupErr != nil {
		return copied.Load(), groupErr
	}
	if closeErr != nil {
		return copied.Load(), gocql.ConvertError("ScanTaskQueueUserDataForV2Backfill", closeErr)
	}
	return copied.Load(), nil
}

func taskQueueUserDataMigrationWriteTime(writeTimes ...nullableInt64) int64 {
	writeTime := taskQueueUserDataMaxWriteTime(writeTimes...)
	if writeTime == 0 {
		return nextTaskQueueUserDataMigrationWriteTime(0)
	}
	return writeTime
}

func ValidateTaskQueueUserDataV2Namespace(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	bucketCount int,
) (TaskQueueUserDataValidationResult, error) {
	if _, err := taskQueueUserDataBucket("", bucketCount); err != nil {
		return TaskQueueUserDataValidationResult{}, err
	}
	source, err := readTaskQueueUserDataV1NamespaceForValidation(ctx, session, namespaceID)
	if err != nil {
		return TaskQueueUserDataValidationResult{}, err
	}
	store := newUserDataStore(session, nil, TaskQueueUserDataMigrationModeTargetOnly, bucketCount)
	target, targetRows, targetMismatches, err := store.readTaskQueueUserDataV2NamespaceForValidation(ctx, namespaceID)
	if err != nil {
		return TaskQueueUserDataValidationResult{}, err
	}

	result := TaskQueueUserDataValidationResult{
		SourceRows: len(source),
		TargetRows: targetRows,
		Mismatches: targetMismatches,
	}
	for key, sourceRow := range source {
		targetRow, ok := target[key]
		if !ok {
			result.Mismatches = append(result.Mismatches, "missing target row "+key)
			continue
		}
		if sourceRow.version != targetRow.version ||
			sourceRow.dataEncoding != targetRow.dataEncoding ||
			!bytes.Equal(sourceRow.data, targetRow.data) {
			result.Mismatches = append(result.Mismatches, "different target row "+key)
		}
	}
	for key := range target {
		if _, ok := source[key]; !ok {
			result.Mismatches = append(result.Mismatches, "extra target row "+key)
		}
	}
	sort.Strings(result.Mismatches)
	return result, nil
}

func ValidateTaskQueueUserDataV2AllNamespaces(
	ctx context.Context,
	session gocql.Session,
	bucketCount int,
) (map[string]TaskQueueUserDataValidationResult, error) {
	if _, err := taskQueueUserDataBucket("", bucketCount); err != nil {
		return nil, err
	}
	namespaces := make(map[string]struct{})
	invalidTargetBuckets := make(map[string][]int16)
	sourceIter := session.Query(templateScanTaskQueueUserDataV1Namespaces).WithContext(ctx).Iter()
	for {
		var namespaceID string
		if !sourceIter.Scan(&namespaceID) {
			break
		}
		namespaces[namespaceID] = struct{}{}
	}
	if err := sourceIter.Close(); err != nil {
		return nil, gocql.ConvertError("ScanTaskQueueUserDataV1Namespaces", err)
	}
	targetIter := session.Query(templateScanTaskQueueUserDataV2Namespaces).WithContext(ctx).Iter()
	for {
		var namespaceID string
		var bucket int16
		if !targetIter.Scan(&namespaceID, &bucket) {
			break
		}
		namespaces[namespaceID] = struct{}{}
		if bucket < 0 || int(bucket) >= bucketCount {
			invalidTargetBuckets[namespaceID] = append(invalidTargetBuckets[namespaceID], bucket)
		}
	}
	if err := targetIter.Close(); err != nil {
		return nil, gocql.ConvertError("ScanTaskQueueUserDataV2Namespaces", err)
	}

	namespaceIDs := make([]string, 0, len(namespaces))
	for namespaceID := range namespaces {
		namespaceIDs = append(namespaceIDs, namespaceID)
	}
	sort.Strings(namespaceIDs)
	results := make(map[string]TaskQueueUserDataValidationResult, len(namespaceIDs))
	for _, namespaceID := range namespaceIDs {
		result, err := ValidateTaskQueueUserDataV2Namespace(ctx, session, namespaceID, bucketCount)
		if err != nil {
			return nil, fmt.Errorf("validate task queue user data namespace %s: %w", namespaceID, err)
		}
		for _, bucket := range invalidTargetBuckets[namespaceID] {
			result.Mismatches = append(result.Mismatches, fmt.Sprintf(
				"target namespace has bucket %d outside [0,%d)",
				bucket,
				bucketCount,
			))
		}
		sort.Strings(result.Mismatches)
		results[namespaceID] = result
	}
	return results, nil
}

//nolint:revive // Reconciliation computes exact inserts and deletes across all target buckets.
func ReconcileTaskQueueUserDataV2Namespace(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	bucketCount int,
	concurrency int,
) (TaskQueueUserDataReconciliationResult, error) {
	if concurrency <= 0 {
		return TaskQueueUserDataReconciliationResult{}, errors.New("task queue user data reconciliation concurrency must be positive")
	}
	if _, err := taskQueueUserDataBucket("", bucketCount); err != nil {
		return TaskQueueUserDataReconciliationResult{}, err
	}
	source, err := readTaskQueueUserDataV1NamespaceForValidation(ctx, session, namespaceID)
	if err != nil {
		return TaskQueueUserDataReconciliationResult{}, err
	}
	store := newUserDataStore(session, nil, TaskQueueUserDataMigrationModeTargetOnly, bucketCount)
	target, err := store.readTaskQueueUserDataV2NamespaceCopies(ctx, namespaceID, true)
	if err != nil {
		return TaskQueueUserDataReconciliationResult{}, err
	}

	operations := make([]taskQueueUserDataReconciliationOperation, 0)
	for key, sourceRow := range source {
		expectedBucket, err := taskQueueUserDataBucket(sourceRow.taskQueue, bucketCount)
		if err != nil {
			return TaskQueueUserDataReconciliationResult{}, err
		}
		copies := target[key]
		var expectedCopy *taskQueueUserDataV2Row
		for index := range copies {
			rowCopy := copies[index]
			if rowCopy.bucket == expectedBucket {
				expectedCopy = &copies[index]
				continue
			}
			operations = append(operations, taskQueueUserDataReconciliationOperation{run: func(operationCtx context.Context) error {
				writeTime, err := taskQueueUserDataReconciliationWriteTime(0, rowCopy.writeTime)
				if err != nil {
					return err
				}
				return session.Query(
					templateDeleteTaskQueueUserDataV2,
					writeTime,
					namespaceID,
					rowCopy.bucket,
					rowCopy.buildID,
					rowCopy.taskQueue,
				).WithContext(operationCtx).Idempotent(true).Exec()
			}})
		}
		if expectedCopy != nil && taskQueueUserDataRowsEqual(sourceRow, *expectedCopy) {
			continue
		}
		staleWriteTime := int64(0)
		if expectedCopy != nil {
			staleWriteTime = expectedCopy.writeTime
		}
		sourceRow := sourceRow
		operations = append(operations, taskQueueUserDataReconciliationOperation{copied: true, run: func(operationCtx context.Context) error {
			writeTime, err := taskQueueUserDataReconciliationWriteTime(sourceRow.writeTime, staleWriteTime)
			if err != nil {
				return err
			}
			return session.Query(
				templateReconcileTaskQueueUserDataV2,
				writeTime,
				sourceRow.data,
				sourceRow.dataEncoding,
				sourceRow.version,
				namespaceID,
				expectedBucket,
				sourceRow.buildID,
				sourceRow.taskQueue,
			).WithContext(operationCtx).Idempotent(true).Exec()
		}})
	}
	for key, copies := range target {
		if _, exists := source[key]; exists {
			continue
		}
		for _, rowCopy := range copies {
			rowCopy := rowCopy
			operations = append(operations, taskQueueUserDataReconciliationOperation{run: func(operationCtx context.Context) error {
				writeTime, err := taskQueueUserDataReconciliationWriteTime(0, rowCopy.writeTime)
				if err != nil {
					return err
				}
				return session.Query(
					templateDeleteTaskQueueUserDataV2,
					writeTime,
					namespaceID,
					rowCopy.bucket,
					rowCopy.buildID,
					rowCopy.taskQueue,
				).WithContext(operationCtx).Idempotent(true).Exec()
			}})
		}
	}
	return runTaskQueueUserDataReconciliation(ctx, concurrency, operations)
}

//nolint:revive // Reverse reconciliation computes exact inserts and deletes while preserving timestamps.
func ReconcileTaskQueueUserDataV1Namespace(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	bucketCount int,
	concurrency int,
) (TaskQueueUserDataReconciliationResult, error) {
	if concurrency <= 0 {
		return TaskQueueUserDataReconciliationResult{}, errors.New("task queue user data reconciliation concurrency must be positive")
	}
	store := newUserDataStore(session, nil, TaskQueueUserDataMigrationModeTargetOnly, bucketCount)
	targetCopies, err := store.readTaskQueueUserDataV2NamespaceCopies(ctx, namespaceID, true)
	if err != nil {
		return TaskQueueUserDataReconciliationResult{}, err
	}
	target := make(map[string]taskQueueUserDataV2Row, len(targetCopies))
	for key, copies := range targetCopies {
		for _, row := range copies {
			if !row.present {
				continue
			}
			if err := validateTaskQueueUserDataV2RowBucket(row, bucketCount); err != nil {
				return TaskQueueUserDataReconciliationResult{}, err
			}
			if _, duplicate := target[key]; duplicate {
				return TaskQueueUserDataReconciliationResult{}, serviceerror.NewDataLossf("duplicate target task queue user data row %s", key)
			}
			target[key] = row
		}
	}
	source, err := readTaskQueueUserDataV1NamespaceForValidation(ctx, session, namespaceID)
	if err != nil {
		return TaskQueueUserDataReconciliationResult{}, err
	}
	operations := make([]taskQueueUserDataReconciliationOperation, 0)
	for key, targetRow := range target {
		sourceRow, exists := source[key]
		if exists && taskQueueUserDataRowsEqual(targetRow, sourceRow) {
			continue
		}
		targetRow := targetRow
		operations = append(operations, taskQueueUserDataReconciliationOperation{copied: true, run: func(operationCtx context.Context) error {
			writeTime, err := taskQueueUserDataReconciliationWriteTime(targetRow.writeTime, sourceRow.writeTime)
			if err != nil {
				return err
			}
			return session.Query(
				templateBackfillTaskQueueUserDataV1,
				namespaceID,
				targetRow.buildID,
				targetRow.taskQueue,
				targetRow.data,
				targetRow.dataEncoding,
				targetRow.version,
				writeTime,
			).WithContext(operationCtx).Idempotent(true).Exec()
		}})
	}
	for key, sourceRow := range source {
		if _, exists := target[key]; exists {
			continue
		}
		sourceRow := sourceRow
		operations = append(operations, taskQueueUserDataReconciliationOperation{run: func(operationCtx context.Context) error {
			writeTime, err := taskQueueUserDataReconciliationWriteTime(0, sourceRow.writeTime)
			if err != nil {
				return err
			}
			return session.Query(
				templateDeleteTaskQueueUserDataV1,
				writeTime,
				namespaceID,
				sourceRow.buildID,
				sourceRow.taskQueue,
			).WithContext(operationCtx).Idempotent(true).Exec()
		}})
	}
	return runTaskQueueUserDataReconciliation(ctx, concurrency, operations)
}

func (d *userDataStore) readTaskQueueUserDataV2NamespaceCopies(
	ctx context.Context,
	namespaceID string,
	rejectPending bool,
) (map[string][]taskQueueUserDataV2Row, error) {
	namespaceState, err := d.stableTaskQueueUserDataV2Namespace(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	reads, err := d.readTaskQueueUserDataV2Buckets(
		ctx,
		func(bucketCtx context.Context, bucket int) (taskQueueUserDataV2BucketRead, error) {
			result := taskQueueUserDataV2BucketRead{observedTransactions: make(map[string]string)}
			iter := d.Session.Query(
				templateScanTaskQueueUserDataV2BucketForValidation,
				namespaceID,
				int16(bucket),
			).WithContext(bucketCtx).Iter()
			rowMap := make(map[string]any)
			for iter.MapScan(rowMap) {
				buildID, ok, err := optionalTaskQueueUserDataV2Field[string](rowMap, "build_id")
				if err != nil {
					_ = iter.Close()
					return taskQueueUserDataV2BucketRead{}, err
				}
				if !ok {
					_ = iter.Close()
					return taskQueueUserDataV2BucketRead{}, newFieldNotFoundError("build_id", rowMap)
				}
				row, err := decodeTaskQueueUserDataV2Row(rowMap, int16(bucket), buildID)
				if err != nil {
					_ = iter.Close()
					return taskQueueUserDataV2BucketRead{}, err
				}
				if rejectPending && row.pendingTxnID != "" {
					_ = iter.Close()
					return taskQueueUserDataV2BucketRead{}, serviceerror.NewUnavailable(
						"task queue user data reconciliation requires transaction recovery first",
					)
				}
				row, err = d.logicalTaskQueueUserDataV2RowObserved(
					bucketCtx,
					namespaceID,
					row,
					result.observedTransactions,
				)
				if err != nil {
					_ = iter.Close()
					return taskQueueUserDataV2BucketRead{}, err
				}
				result.rows = append(result.rows, row)
				rowMap = make(map[string]any)
			}
			if err := iter.Close(); err != nil {
				return taskQueueUserDataV2BucketRead{}, gocql.ConvertError("ReadTaskQueueUserDataV2ForReconciliation", err)
			}
			return result, nil
		},
	)
	if err != nil {
		return nil, err
	}
	rows, observedTransactions, err := mergeTaskQueueUserDataV2BucketReads(reads)
	if err != nil {
		return nil, err
	}
	if err := d.validateTaskQueueUserDataV2Snapshot(
		ctx,
		namespaceID,
		namespaceState.epoch,
		observedTransactions,
	); err != nil {
		return nil, err
	}
	byKey := make(map[string][]taskQueueUserDataV2Row)
	for _, row := range rows {
		key := taskQueueUserDataValidationKey(row.buildID, row.taskQueue)
		byKey[key] = append(byKey[key], row)
	}
	return byKey, nil
}

func taskQueueUserDataRowsEqual(first taskQueueUserDataV2Row, second taskQueueUserDataV2Row) bool {
	return first.present == second.present &&
		first.version == second.version &&
		first.dataEncoding == second.dataEncoding &&
		bytes.Equal(first.data, second.data)
}

func taskQueueUserDataReconciliationWriteTime(authoritative int64, stale int64) (int64, error) {
	if stale < authoritative && authoritative > 0 {
		return authoritative, nil
	}
	if stale == math.MaxInt64 {
		return 0, errors.New("task queue user data reconciliation timestamp overflow")
	}
	return nextTaskQueueUserDataMigrationWriteTime(stale + 1), nil
}

func nextTaskQueueUserDataMigrationWriteTime(minimum int64) int64 {
	for {
		previous := taskQueueUserDataMigrationClock.Load()
		next := max(time.Now().UTC().UnixMicro(), minimum, previous+1)
		if taskQueueUserDataMigrationClock.CompareAndSwap(previous, next) {
			return next
		}
	}
}

func runTaskQueueUserDataReconciliation(
	ctx context.Context,
	concurrency int,
	operations []taskQueueUserDataReconciliationOperation,
) (TaskQueueUserDataReconciliationResult, error) {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	var copied atomic.Int64
	var deleted atomic.Int64
	for _, operation := range operations {
		operation := operation
		group.Go(func() error {
			if err := operation.run(groupCtx); err != nil {
				return err
			}
			if operation.copied {
				copied.Add(1)
			} else {
				deleted.Add(1)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return TaskQueueUserDataReconciliationResult{
			Copied:  int(copied.Load()),
			Deleted: int(deleted.Load()),
		}, err
	}
	return TaskQueueUserDataReconciliationResult{
		Copied:  int(copied.Load()),
		Deleted: int(deleted.Load()),
	}, nil
}

func readTaskQueueUserDataV1NamespaceForValidation(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
) (map[string]taskQueueUserDataV2Row, error) {
	iter := session.Query(
		templateScanTaskQueueUserDataNamespaceForValidation,
		namespaceID,
	).WithContext(ctx).Iter()
	rows := make(map[string]taskQueueUserDataV2Row)
	for {
		var row taskQueueUserDataV2Row
		var dataTime, encodingTime, versionTime nullableInt64
		if !iter.Scan(
			&row.buildID,
			&row.taskQueue,
			&row.data,
			&row.dataEncoding,
			&row.version,
			&dataTime,
			&encodingTime,
			&versionTime,
		) {
			break
		}
		row.present = true
		row.data = bytes.Clone(row.data)
		row.writeTime = taskQueueUserDataMaxWriteTime(dataTime, encodingTime, versionTime)
		key := taskQueueUserDataValidationKey(row.buildID, row.taskQueue)
		if _, duplicate := rows[key]; duplicate {
			_ = iter.Close()
			return nil, serviceerror.NewDataLossf("duplicate source task queue user data row %s", key)
		}
		rows[key] = row
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ValidateTaskQueueUserDataV1Namespace", err)
	}
	return rows, nil
}

//nolint:revive // Validation decodes and cross-checks namespace, transaction, and bucket invariants.
func (d *userDataStore) readTaskQueueUserDataV2NamespaceForValidation(
	ctx context.Context,
	namespaceID string,
) (map[string]taskQueueUserDataV2Row, int, []string, error) {
	rows := make(map[string]taskQueueUserDataV2Row)
	physicalRows := 0
	var mismatches []string
	for bucket := range d.effectiveTaskQueueUserDataBucketCount() {
		iter := d.Session.Query(
			templateScanTaskQueueUserDataV2BucketForValidation,
			namespaceID,
			int16(bucket),
		).WithContext(ctx).Iter()
		rowMap := make(map[string]any)
		for iter.MapScan(rowMap) {
			buildID, ok, err := optionalTaskQueueUserDataV2Field[string](rowMap, "build_id")
			if err != nil {
				_ = iter.Close()
				return nil, 0, nil, err
			}
			if !ok {
				_ = iter.Close()
				return nil, 0, nil, newFieldNotFoundError("build_id", rowMap)
			}
			row, err := decodeTaskQueueUserDataV2Row(rowMap, int16(bucket), buildID)
			if err != nil {
				_ = iter.Close()
				return nil, 0, nil, err
			}
			if row.pendingTxnID != "" {
				mismatches = append(mismatches, fmt.Sprintf(
					"unmaterialized target transaction %s txn_id=%q",
					taskQueueUserDataValidationKey(row.buildID, row.taskQueue),
					row.pendingTxnID,
				))
			}
			row, err = d.logicalTaskQueueUserDataV2Row(ctx, namespaceID, row)
			if err != nil {
				_ = iter.Close()
				return nil, 0, nil, err
			}
			if row.present {
				physicalRows++
				expectedBucket, err := taskQueueUserDataBucket(row.taskQueue, d.effectiveTaskQueueUserDataBucketCount())
				if err != nil {
					_ = iter.Close()
					return nil, 0, nil, err
				}
				key := taskQueueUserDataValidationKey(row.buildID, row.taskQueue)
				if row.bucket != expectedBucket {
					mismatches = append(mismatches, fmt.Sprintf(
						"wrong target bucket %s actual=%d expected=%d",
						key,
						row.bucket,
						expectedBucket,
					))
				}
				if previous, duplicate := rows[key]; duplicate {
					mismatches = append(mismatches, fmt.Sprintf(
						"duplicate target row %s buckets=%d,%d",
						key,
						previous.bucket,
						row.bucket,
					))
					if previous.bucket == expectedBucket {
						rowMap = make(map[string]any)
						continue
					}
				}
				rows[key] = row
			}
			rowMap = make(map[string]any)
		}
		if err := iter.Close(); err != nil {
			return nil, 0, nil, gocql.ConvertError("ValidateTaskQueueUserDataV2Namespace", err)
		}
	}
	return rows, physicalRows, mismatches, nil
}

func validateTaskQueueUserDataBackfillOptions(options TaskQueueUserDataBackfillOptions) error {
	if options.PageSize <= 0 {
		return errors.New("task queue user data backfill page size must be positive")
	}
	if options.Concurrency <= 0 {
		return errors.New("task queue user data backfill concurrency must be positive")
	}
	_, err := taskQueueUserDataBucket("", effectiveTaskQueueUserDataBackfillBucketCount(options))
	return err
}

func validateTaskQueueUserDataRecoveryOptions(options TaskQueueUserDataRecoveryOptions) error {
	if options.PageSize <= 0 {
		return errors.New("task queue user data recovery page size must be positive")
	}
	if options.Concurrency <= 0 {
		return errors.New("task queue user data recovery concurrency must be positive")
	}
	_, err := taskQueueUserDataBucket("", effectiveTaskQueueUserDataRecoveryBucketCount(options))
	return err
}

func effectiveTaskQueueUserDataRecoveryBucketCount(options TaskQueueUserDataRecoveryOptions) int {
	if options.BucketCount <= 0 {
		return DefaultTaskQueueUserDataBucketCount
	}
	return options.BucketCount
}

func TaskQueueUserDataBackfillTokenRanges(count int) ([]TaskQueueUserDataBackfillTokenRange, error) {
	if count <= 0 {
		return nil, errors.New("task queue user data backfill token range count must be positive")
	}
	if count > maxTaskQueueUserDataBackfillTokenRangeCount {
		return nil, fmt.Errorf(
			"task queue user data backfill token range count must not exceed %d",
			maxTaskQueueUserDataBackfillTokenRangeCount,
		)
	}
	ranges := make([]TaskQueueUserDataBackfillTokenRange, count)
	for index := range count {
		startOffset, _ := bits.Div64(uint64(index), 0, uint64(count))
		startToken := int64(startOffset ^ (uint64(1) << 63))
		endToken := int64(^uint64(0) >> 1)
		if index+1 < count {
			nextOffset, _ := bits.Div64(uint64(index+1), 0, uint64(count))
			endToken = int64((nextOffset - 1) ^ (uint64(1) << 63))
		}
		ranges[index] = TaskQueueUserDataBackfillTokenRange{
			Index:      index,
			StartToken: startToken,
			EndToken:   endToken,
		}
	}
	return ranges, nil
}

func validateTaskQueueUserDataBackfillPartitioner(
	ctx context.Context,
	session gocql.Session,
	partitioner string,
) error {
	if partitioner == "" {
		if err := session.Query(
			templateGetTaskQueueUserDataBackfillPartitioner,
		).WithContext(ctx).Scan(&partitioner); err != nil {
			return fmt.Errorf("read Cassandra partitioner for task queue user data backfill: %w", err)
		}
	}
	if partitioner != taskQueueUserDataMurmur3Partitioner {
		return fmt.Errorf(
			"task queue user data backfill requires Cassandra partitioner %q, got %q",
			taskQueueUserDataMurmur3Partitioner,
			partitioner,
		)
	}
	return nil
}

func effectiveTaskQueueUserDataBackfillBucketCount(options TaskQueueUserDataBackfillOptions) int {
	if options.BucketCount <= 0 {
		return DefaultTaskQueueUserDataBucketCount
	}
	return options.BucketCount
}

func taskQueueUserDataValidationKey(buildID string, taskQueue string) string {
	return fmt.Sprintf("build_id=%q task_queue=%q", buildID, taskQueue)
}

func taskQueueUserDataMaxWriteTime(writeTimes ...nullableInt64) int64 {
	writeTime := int64(0)
	for _, candidate := range writeTimes {
		if candidate.valid {
			writeTime = max(writeTime, candidate.value)
		}
	}
	return writeTime
}
