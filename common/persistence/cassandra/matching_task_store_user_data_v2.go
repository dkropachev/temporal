package cassandra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"golang.org/x/sync/errgroup"
)

var errTaskQueueUserDataV2SnapshotChanged = errors.New("task queue user data transaction changed during read")

const (
	taskQueueUserDataV2TransactionLease     = 5 * time.Minute
	taskQueueUserDataV2TransactionRetention = time.Hour
	taskQueueUserDataV2FanoutConcurrency    = 8

	taskQueueUserDataV2TransactionPreparing = "preparing"
	taskQueueUserDataV2TransactionCommitted = "committed"
	taskQueueUserDataV2TransactionAborted   = "aborted"

	taskQueueUserDataV2PageTokenVersion = 2
	taskQueueUserDataV2Layout           = "task_queue_user_data_v2"
	taskQueueUserDataV2LayoutGeneration = 2

	templateGetTaskQueueUserDataV2RowQuery = `SELECT task_queue_name, data, data_encoding, version, present,
		pending_txn_id, pending_data, pending_data_encoding, pending_version, pending_present
		FROM task_queue_user_data_v2
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ? AND task_queue_name = ?`
	templateListTaskQueueUserDataV2Query = `SELECT task_queue_name, data, data_encoding, version, present,
		pending_txn_id, pending_data, pending_data_encoding, pending_version, pending_present
		FROM task_queue_user_data_v2
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ''`
	templateListTaskQueueUserDataV2PageQuery = `SELECT task_queue_name, data, data_encoding, version, present,
		pending_txn_id, pending_data, pending_data_encoding, pending_version, pending_present
		FROM task_queue_user_data_v2
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = '' LIMIT ?`
	templateListTaskQueueUserDataV2PageAfterQuery = `SELECT task_queue_name, data, data_encoding, version, present,
		pending_txn_id, pending_data, pending_data_encoding, pending_version, pending_present
		FROM task_queue_user_data_v2
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = '' AND task_queue_name > ? LIMIT ?`
	templateListTaskQueueNamesByBuildIDV2Query = `SELECT task_queue_name, present, pending_txn_id, pending_present
		FROM task_queue_user_data_v2
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ?`

	templateInsertTaskQueueUserDataV2TransactionQuery = `INSERT INTO task_queue_user_data_v2_txn
		(namespace_id, txn_id, state, expires_at) VALUES (?, ?, ?, ?) IF NOT EXISTS`
	templateGetTaskQueueUserDataV2TransactionQuery = `SELECT state, expires_at FROM task_queue_user_data_v2_txn
		WHERE namespace_id = ? AND txn_id = ?`
	templateRenewTaskQueueUserDataV2TransactionQuery = `UPDATE task_queue_user_data_v2_txn SET expires_at = ?
		WHERE namespace_id = ? AND txn_id = ? IF state = ?`
	templateCommitTaskQueueUserDataV2TransactionQuery = `UPDATE task_queue_user_data_v2_txn SET state = ?, expires_at = ?
		WHERE namespace_id = ? AND txn_id = ? IF state = ?`
	templateForceAbortTaskQueueUserDataV2TransactionQuery = `UPDATE task_queue_user_data_v2_txn SET state = ?, expires_at = ?
		WHERE namespace_id = ? AND txn_id = ? IF state = ?`
	templateInitializeTaskQueueUserDataV2NamespaceQuery = `INSERT INTO task_queue_user_data_v2_txn
		(namespace_id, txn_id, epoch) VALUES (?, '', 0) IF NOT EXISTS`
	templateGetTaskQueueUserDataV2NamespaceQuery = `SELECT active_txn_id, epoch, expires_at
		FROM task_queue_user_data_v2_txn WHERE namespace_id = ? AND txn_id = ''`
	templateAcquireTaskQueueUserDataV2NamespaceQuery = `UPDATE task_queue_user_data_v2_txn
		SET active_txn_id = ?, expires_at = ? WHERE namespace_id = ? AND txn_id = ''
		IF active_txn_id = null`
	templateRenewTaskQueueUserDataV2NamespaceQuery = `UPDATE task_queue_user_data_v2_txn
		SET expires_at = ? WHERE namespace_id = ? AND txn_id = '' IF active_txn_id = ?`
	templateCommitTaskQueueUserDataV2NamespaceQuery = `UPDATE task_queue_user_data_v2_txn
		SET active_txn_id = null, expires_at = null, epoch = ? WHERE namespace_id = ? AND txn_id = ''
		IF active_txn_id = ? AND epoch = ?`
	templateAbortTaskQueueUserDataV2NamespaceQuery = `UPDATE task_queue_user_data_v2_txn
		SET active_txn_id = null, expires_at = null, epoch = ? WHERE namespace_id = ? AND txn_id = ''
		IF active_txn_id = ? AND epoch = ?`
	templateExpireTerminalTaskQueueUserDataV2TransactionQuery = `UPDATE task_queue_user_data_v2_txn USING TTL ?
		SET state = ?, expires_at = ? WHERE namespace_id = ? AND txn_id = ? IF state = ?`

	templatePrepareNewTaskQueueUserDataV2Query = `INSERT INTO task_queue_user_data_v2
		(namespace_id, bucket_id, build_id, task_queue_name, version, present,
		 pending_txn_id, pending_data, pending_data_encoding, pending_version, pending_present)
		VALUES (?, ?, '', ?, 0, false, ?, ?, ?, ?, true) IF NOT EXISTS`
	templatePrepareExistingTaskQueueUserDataV2Query = `UPDATE task_queue_user_data_v2 SET
		pending_txn_id = ?, pending_data = ?, pending_data_encoding = ?, pending_version = ?, pending_present = true
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = '' AND task_queue_name = ?
		IF present = true AND version = ? AND pending_txn_id = null`
	templatePrepareNewTaskQueueUserDataV2MappingQuery = `INSERT INTO task_queue_user_data_v2
		(namespace_id, bucket_id, build_id, task_queue_name, present, pending_txn_id, pending_present)
		VALUES (?, ?, ?, ?, false, ?, true) IF NOT EXISTS`
	templatePrepareDeleteTaskQueueUserDataV2MappingQuery = `UPDATE task_queue_user_data_v2 SET
		pending_txn_id = ?, pending_present = false
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ? AND task_queue_name = ?
		IF present = true AND pending_txn_id = null`
	templateInsertTaskQueueUserDataV2Query = `INSERT INTO task_queue_user_data_v2
		(namespace_id, bucket_id, build_id, task_queue_name, data, data_encoding, version, present)
		VALUES (?, ?, '', ?, ?, ?, ?, true) IF NOT EXISTS`
	templateUpdateTaskQueueUserDataV2Query = `UPDATE task_queue_user_data_v2 SET
		data = ?, data_encoding = ?, version = ?
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = '' AND task_queue_name = ?
		IF present = true AND version = ? AND pending_txn_id = null`
	templateInsertTaskQueueUserDataV2MappingQuery = `INSERT INTO task_queue_user_data_v2
		(namespace_id, bucket_id, build_id, task_queue_name, present)
		VALUES (?, ?, ?, ?, true)`
	templateDeleteTaskQueueUserDataV2MappingQuery = `DELETE FROM task_queue_user_data_v2
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ? AND task_queue_name = ?`

	templateFinalizeTaskQueueUserDataV2Query = `UPDATE task_queue_user_data_v2 SET
		data = ?, data_encoding = ?, version = ?, present = true,
		pending_txn_id = null, pending_data = null, pending_data_encoding = null,
		pending_version = null, pending_present = null
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = '' AND task_queue_name = ?
		IF pending_txn_id = ?`
	templateFinalizeTaskQueueUserDataV2MappingQuery = `UPDATE task_queue_user_data_v2 SET
		present = true, pending_txn_id = null, pending_present = null
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ? AND task_queue_name = ?
		IF pending_txn_id = ?`
	templateFinalizeDeleteTaskQueueUserDataV2MappingQuery = `DELETE FROM task_queue_user_data_v2
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ? AND task_queue_name = ?
		IF pending_txn_id = ?`
	templateRollbackNewTaskQueueUserDataV2RowQuery = `DELETE FROM task_queue_user_data_v2
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ? AND task_queue_name = ?
		IF pending_txn_id = ? AND present = false`
	templateRollbackExistingTaskQueueUserDataV2RowQuery = `UPDATE task_queue_user_data_v2 SET
		pending_txn_id = null, pending_data = null, pending_data_encoding = null,
		pending_version = null, pending_present = null
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ? AND task_queue_name = ?
		IF pending_txn_id = ?`
	templateDeleteAbsentTaskQueueUserDataV2RowQuery = `DELETE FROM task_queue_user_data_v2
		WHERE namespace_id = ? AND bucket_id = ? AND build_id = ? AND task_queue_name = ?
		IF present = false AND pending_txn_id = null`
)

type taskQueueUserDataV2Row struct {
	taskQueue           string
	buildID             string
	bucket              int16
	data                []byte
	dataEncoding        string
	version             int64
	present             bool
	pendingTxnID        string
	pendingData         []byte
	pendingDataEncoding string
	pendingVersion      int64
	pendingPresent      bool
	writeTime           int64
	pendingWriteTime    int64
}

type taskQueueUserDataV2Transaction struct {
	state     string
	expiresAt time.Time
}

type taskQueueUserDataV2NamespaceState struct {
	activeTxnID string
	epoch       int64
	expiresAt   time.Time
}

type taskQueueUserDataV2Change struct {
	row            taskQueueUserDataV2Row
	newRow         bool
	targetPresent  bool
	targetData     []byte
	targetEncoding string
	targetVersion  int64
}

type taskQueueUserDataV2PageToken struct {
	Version          int    `json:"version"`
	Layout           string `json:"layout"`
	LayoutGeneration int    `json:"layout_generation"`
	NamespaceID      string `json:"namespace_id"`
	BuildID          string `json:"build_id"`
	BucketCount      int    `json:"bucket_count"`
	Epoch            int64  `json:"epoch"`
	AfterTaskQueue   string `json:"after_task_queue,omitempty"`
	HasCursor        bool   `json:"has_cursor,omitempty"`
}

type taskQueueUserDataV2BucketRead struct {
	rows                 []taskQueueUserDataV2Row
	observedTransactions map[string]string
}

type taskQueueUserDataV2Batch interface {
	Query(string, ...any)
}

func taskQueueUserDataBucket(taskQueue string, bucketCount int) (int16, error) {
	if bucketCount <= 0 || bucketCount > math.MaxInt16 {
		return 0, fmt.Errorf("task queue user data bucket count must be between 1 and %d: %d", math.MaxInt16, bucketCount)
	}
	const (
		fnvOffset32 = uint32(2166136261)
		fnvPrime32  = uint32(16777619)
	)
	hash := fnvOffset32
	for i := 0; i < len(taskQueue); i++ {
		hash ^= uint32(taskQueue[i])
		hash *= fnvPrime32
	}
	return int16(hash % uint32(bucketCount)), nil
}

func (d *userDataStore) effectiveTaskQueueUserDataBucketCount() int {
	if d.bucketCount <= 0 {
		return DefaultTaskQueueUserDataBucketCount
	}
	return d.bucketCount
}

func (d *userDataStore) getTaskQueueUserDataV2(
	ctx context.Context,
	request *p.GetTaskQueueUserDataRequest,
) (*p.InternalGetTaskQueueUserDataResponse, error) {
	bucket, err := taskQueueUserDataBucket(request.TaskQueue, d.effectiveTaskQueueUserDataBucketCount())
	if err != nil {
		return nil, err
	}
	row, err := d.readTaskQueueUserDataV2Row(ctx, request.NamespaceID, bucket, "", request.TaskQueue)
	if err != nil {
		return nil, gocql.ConvertError("GetTaskQueueDataV2", err)
	}
	row, err = d.logicalTaskQueueUserDataV2Row(ctx, request.NamespaceID, row)
	if err != nil {
		return nil, err
	}
	if !row.present {
		return nil, serviceerror.NewNotFound("task queue user data not found")
	}
	return &p.InternalGetTaskQueueUserDataResponse{
		Version:  row.version,
		UserData: p.NewDataBlob(row.data, row.dataEncoding),
	}, nil
}

//nolint:revive // The transaction coordinator handles optimistic versions, retries, and crash recovery in one state machine.
func (d *userDataStore) updateTaskQueueUserDataV2(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueUserDataRequest,
) error {
	for _, update := range request.Updates {
		if update.Applied != nil {
			*update.Applied = false
		}
	}
	if len(request.Updates) == 0 {
		return nil
	}

	fastChanges, singleBucket, err := d.singleBucketTaskQueueUserDataV2Changes(request)
	if err != nil {
		return err
	}
	if singleBucket {
		return d.applySingleBucketTaskQueueUserDataV2Changes(ctx, request, fastChanges)
	}
	changes, err := d.buildTaskQueueUserDataV2Changes(ctx, request)
	if err != nil {
		return err
	}
	txnID := uuid.NewString()
	namespaceEpoch, err := d.beginTaskQueueUserDataV2Transaction(ctx, request.NamespaceID, txnID)
	if err != nil {
		return err
	}
	prepared, err := d.prepareTaskQueueUserDataV2Changes(ctx, request.NamespaceID, txnID, changes)
	if err != nil {
		aborted, abortErr := d.abortTaskQueueUserDataV2Transaction(ctx, request.NamespaceID, txnID, namespaceEpoch)
		if abortErr != nil {
			return abortErr
		}
		if rollbackErr := d.rollbackTaskQueueUserDataV2Changes(ctx, request.NamespaceID, txnID, prepared); rollbackErr != nil {
			return rollbackErr
		}
		if aborted {
			if expireErr := d.expireTerminalTaskQueueUserDataV2Transaction(
				ctx,
				request.NamespaceID,
				txnID,
				taskQueueUserDataV2TransactionAborted,
			); expireErr != nil {
				return expireErr
			}
		}
		d.markTaskQueueUserDataV2Conflict(ctx, request)
		return err
	}

	committed, commitErr := d.commitTaskQueueUserDataV2Transaction(ctx, request.NamespaceID, txnID, namespaceEpoch)
	if commitErr != nil || !committed {
		state, stateErr := d.resolveAmbiguousTaskQueueUserDataV2Commit(ctx, request.NamespaceID, txnID, namespaceEpoch)
		if stateErr != nil || state != taskQueueUserDataV2TransactionCommitted {
			if state == taskQueueUserDataV2TransactionAborted {
				if rollbackErr := d.rollbackTaskQueueUserDataV2Changes(ctx, request.NamespaceID, txnID, prepared); rollbackErr != nil {
					return rollbackErr
				}
				if expireErr := d.expireTerminalTaskQueueUserDataV2Transaction(
					ctx,
					request.NamespaceID,
					txnID,
					taskQueueUserDataV2TransactionAborted,
				); expireErr != nil {
					return expireErr
				}
			}
			if commitErr != nil {
				return gocql.ConvertError("CommitTaskQueueUserDataV2Transaction", commitErr)
			}
			if stateErr != nil {
				return gocql.ConvertError("ResolveTaskQueueUserDataV2Transaction", stateErr)
			}
			return serviceerror.NewUnavailable("task queue user data transaction lost its prepare lease")
		}
	}

	for _, update := range request.Updates {
		if update.Applied != nil {
			*update.Applied = true
		}
	}
	d.scheduleTaskQueueUserDataV2Finalization(request.NamespaceID, txnID, changes)
	return nil
}

func (d *userDataStore) singleBucketTaskQueueUserDataV2Changes(
	request *p.InternalUpdateTaskQueueUserDataRequest,
) ([]taskQueueUserDataV2Change, bool, error) {
	taskQueues := make([]string, 0, len(request.Updates))
	for taskQueue := range request.Updates {
		taskQueues = append(taskQueues, taskQueue)
	}
	sort.Strings(taskQueues)
	changes := make([]taskQueueUserDataV2Change, 0, len(taskQueues))
	var requestBucket int16
	for index, taskQueue := range taskQueues {
		update := request.Updates[taskQueue]
		bucket, err := taskQueueUserDataBucket(taskQueue, d.effectiveTaskQueueUserDataBucketCount())
		if err != nil {
			return nil, false, err
		}
		if index == 0 {
			requestBucket = bucket
		} else if bucket != requestBucket {
			return nil, false, nil
		}
		changes = append(changes, taskQueueUserDataV2Change{
			row: taskQueueUserDataV2Row{
				taskQueue: taskQueue,
				bucket:    bucket,
				version:   update.Version,
				present:   update.Version > 0,
			},
			newRow:         update.Version == 0,
			targetPresent:  true,
			targetData:     update.UserData.Data,
			targetEncoding: update.UserData.EncodingType.String(),
			targetVersion:  update.Version + 1,
		})
		for _, buildID := range update.BuildIdsAdded {
			changes = append(changes, taskQueueUserDataV2Change{
				row: taskQueueUserDataV2Row{
					taskQueue: taskQueue,
					buildID:   buildID,
					bucket:    bucket,
				},
				targetPresent: true,
			})
		}
		for _, buildID := range update.BuildIdsRemoved {
			changes = append(changes, taskQueueUserDataV2Change{
				row: taskQueueUserDataV2Row{
					taskQueue: taskQueue,
					buildID:   buildID,
					bucket:    bucket,
				},
			})
		}
	}
	return changes, true, nil
}

func (d *userDataStore) scheduleTaskQueueUserDataV2Finalization(
	namespaceID string,
	txnID string,
	changes []taskQueueUserDataV2Change,
) {
	changes = append([]taskQueueUserDataV2Change(nil), changes...)
	go func() {
		for attempt := 0; attempt < 5; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), taskQueueUserDataV2TransactionLease)
			err := d.finalizeTaskQueueUserDataV2Changes(ctx, namespaceID, txnID, changes)
			if err == nil {
				err = d.expireTerminalTaskQueueUserDataV2Transaction(
					ctx,
					namespaceID,
					txnID,
					taskQueueUserDataV2TransactionCommitted,
				)
			}
			cancel()
			if err == nil {
				return
			}
			if attempt == 4 {
				d.logger.Error(
					"Unable to materialize committed task queue user data transaction",
					tag.NewStringTag("transaction-id", txnID),
					tag.Error(err),
				)
			}
		}
	}()
}

func (d *userDataStore) beginTaskQueueUserDataV2Transaction(
	ctx context.Context,
	namespaceID string,
	txnID string,
) (int64, error) {
	if _, err := d.Session.Query(
		templateInitializeTaskQueueUserDataV2NamespaceQuery,
		namespaceID,
	).WithContext(ctx).MapScanCAS(make(map[string]any)); err != nil {
		return 0, gocql.ConvertError("InitializeTaskQueueUserDataV2Namespace", err)
	}
	expiresAt := time.Now().UTC().Add(taskQueueUserDataV2TransactionLease)
	created, err := d.Session.Query(
		templateInsertTaskQueueUserDataV2TransactionQuery,
		namespaceID,
		txnID,
		taskQueueUserDataV2TransactionPreparing,
		expiresAt,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return 0, gocql.ConvertError("CreateTaskQueueUserDataV2Transaction", err)
	}
	if !created {
		return 0, serviceerror.NewUnavailable("task queue user data transaction ID collision")
	}
	acquired, err := d.Session.Query(
		templateAcquireTaskQueueUserDataV2NamespaceQuery,
		txnID,
		expiresAt,
		namespaceID,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return 0, gocql.ConvertError("AcquireTaskQueueUserDataV2Namespace", err)
	}
	if !acquired {
		aborted, abortErr := d.Session.Query(
			templateForceAbortTaskQueueUserDataV2TransactionQuery,
			taskQueueUserDataV2TransactionAborted,
			time.Now().UTC().Add(taskQueueUserDataV2TransactionRetention),
			namespaceID,
			txnID,
			taskQueueUserDataV2TransactionPreparing,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if abortErr != nil {
			return 0, gocql.ConvertError("AbortUnacquiredTaskQueueUserDataV2Transaction", abortErr)
		}
		if aborted {
			if expireErr := d.expireTerminalTaskQueueUserDataV2Transaction(
				ctx,
				namespaceID,
				txnID,
				taskQueueUserDataV2TransactionAborted,
			); expireErr != nil {
				return 0, expireErr
			}
		}
		return 0, serviceerror.NewUnavailable("another multi-bucket task queue user data update is in progress")
	}
	state, err := d.readTaskQueueUserDataV2Namespace(ctx, namespaceID)
	if err != nil {
		return 0, err
	}
	if state.activeTxnID != txnID {
		return 0, serviceerror.NewDataLoss("task queue user data namespace lock changed after acquisition")
	}
	return state.epoch, nil
}

func (d *userDataStore) commitTaskQueueUserDataV2Transaction(
	ctx context.Context,
	namespaceID string,
	txnID string,
	namespaceEpoch int64,
) (bool, error) {
	batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	addCommitTaskQueueUserDataV2TransactionToBatch(
		batch,
		namespaceID,
		txnID,
		namespaceEpoch,
		time.Now().UTC().Add(taskQueueUserDataV2TransactionRetention),
	)
	previous := make(map[string]any)
	applied, iter, err := d.Session.MapExecuteBatchCAS(batch, previous)
	if iter != nil {
		if closeErr := iter.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}
	return applied, err
}

func addCommitTaskQueueUserDataV2TransactionToBatch(
	batch taskQueueUserDataV2Batch,
	namespaceID string,
	txnID string,
	namespaceEpoch int64,
	cleanupAfter time.Time,
) {
	batch.Query(
		templateCommitTaskQueueUserDataV2TransactionQuery,
		taskQueueUserDataV2TransactionCommitted,
		cleanupAfter,
		namespaceID,
		txnID,
		taskQueueUserDataV2TransactionPreparing,
	)
	batch.Query(
		templateCommitTaskQueueUserDataV2NamespaceQuery,
		namespaceEpoch+1,
		namespaceID,
		txnID,
		namespaceEpoch,
	)
}

func taskQueueUserDataV2ChangesUseSingleBucket(changes []taskQueueUserDataV2Change) bool {
	if len(changes) == 0 {
		return true
	}
	bucket := changes[0].row.bucket
	for _, change := range changes[1:] {
		if change.row.bucket != bucket {
			return false
		}
	}
	return true
}

func (d *userDataStore) applySingleBucketTaskQueueUserDataV2Changes(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueUserDataRequest,
	changes []taskQueueUserDataV2Change,
) error {
	batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	for _, change := range changes {
		switch {
		case change.row.buildID == "" && change.newRow:
			batch.Query(
				templateInsertTaskQueueUserDataV2Query,
				request.NamespaceID,
				change.row.bucket,
				change.row.taskQueue,
				change.targetData,
				change.targetEncoding,
				change.targetVersion,
			)
		case change.row.buildID == "":
			batch.Query(
				templateUpdateTaskQueueUserDataV2Query,
				change.targetData,
				change.targetEncoding,
				change.targetVersion,
				request.NamespaceID,
				change.row.bucket,
				change.row.taskQueue,
				change.row.version,
			)
		case change.targetPresent:
			batch.Query(
				templateInsertTaskQueueUserDataV2MappingQuery,
				request.NamespaceID,
				change.row.bucket,
				change.row.buildID,
				change.row.taskQueue,
			)
		default:
			batch.Query(
				templateDeleteTaskQueueUserDataV2MappingQuery,
				request.NamespaceID,
				change.row.bucket,
				change.row.buildID,
				change.row.taskQueue,
			)
		}
	}
	previous := make(map[string]any)
	applied, iter, err := d.Session.MapExecuteBatchCAS(batch, previous)
	if iter != nil {
		if closeErr := iter.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}
	if err != nil {
		return gocql.ConvertError("UpdateTaskQueueUserDataV2", err)
	}
	if !applied {
		d.markTaskQueueUserDataV2Conflict(ctx, request)
		return &p.ConditionFailedError{Msg: "Failed to update task queues: concurrent task queue user data update"}
	}
	for _, update := range request.Updates {
		if update.Applied != nil {
			*update.Applied = true
		}
	}
	return nil
}

//nolint:revive // Change construction validates and coalesces all supported update operation forms.
func (d *userDataStore) buildTaskQueueUserDataV2Changes(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueUserDataRequest,
) ([]taskQueueUserDataV2Change, error) {
	taskQueues := make([]string, 0, len(request.Updates))
	for taskQueue := range request.Updates {
		taskQueues = append(taskQueues, taskQueue)
	}
	sort.Strings(taskQueues)

	changes := make([]taskQueueUserDataV2Change, 0, len(taskQueues))
	for _, taskQueue := range taskQueues {
		update := request.Updates[taskQueue]
		bucket, err := taskQueueUserDataBucket(taskQueue, d.effectiveTaskQueueUserDataBucketCount())
		if err != nil {
			return nil, err
		}
		row, found, err := d.readWritableTaskQueueUserDataV2Row(ctx, request.NamespaceID, bucket, "", taskQueue)
		if err != nil {
			return nil, err
		}
		if update.Version == 0 {
			if found && row.present {
				if update.Conflicting != nil {
					*update.Conflicting = true
				}
				return nil, taskQueueUserDataV2VersionConflict(taskQueue, update.Version, row.version)
			}
		} else if !found || !row.present || row.version != update.Version {
			if update.Conflicting != nil {
				*update.Conflicting = true
			}
			return nil, taskQueueUserDataV2VersionConflict(taskQueue, update.Version, row.version)
		}
		changes = append(changes, taskQueueUserDataV2Change{
			row: taskQueueUserDataV2Row{
				taskQueue: taskQueue,
				buildID:   "",
				bucket:    bucket,
				version:   row.version,
				present:   found && row.present,
			},
			newRow:         !found,
			targetPresent:  true,
			targetData:     update.UserData.Data,
			targetEncoding: update.UserData.EncodingType.String(),
			targetVersion:  update.Version + 1,
		})

		mappingTargets := make(map[string]bool, len(update.BuildIdsAdded)+len(update.BuildIdsRemoved))
		for _, buildID := range update.BuildIdsAdded {
			mappingTargets[buildID] = true
		}
		for _, buildID := range update.BuildIdsRemoved {
			mappingTargets[buildID] = false
		}
		buildIDs := make([]string, 0, len(mappingTargets))
		for buildID := range mappingTargets {
			buildIDs = append(buildIDs, buildID)
		}
		sort.Strings(buildIDs)
		for _, buildID := range buildIDs {
			mapping, mappingFound, err := d.readWritableTaskQueueUserDataV2Row(ctx, request.NamespaceID, bucket, buildID, taskQueue)
			if err != nil {
				return nil, err
			}
			targetPresent := mappingTargets[buildID]
			if (mappingFound && mapping.present == targetPresent) || (!mappingFound && !targetPresent) {
				continue
			}
			changes = append(changes, taskQueueUserDataV2Change{
				row: taskQueueUserDataV2Row{
					taskQueue: taskQueue,
					buildID:   buildID,
					bucket:    bucket,
					present:   mappingFound && mapping.present,
				},
				newRow:        !mappingFound,
				targetPresent: targetPresent,
			})
		}
	}
	return changes, nil
}

//nolint:revive // Preparation handles idempotent replay and conflicting row ownership states.
func (d *userDataStore) prepareTaskQueueUserDataV2Changes(
	ctx context.Context,
	namespaceID string,
	txnID string,
	changes []taskQueueUserDataV2Change,
) ([]taskQueueUserDataV2Change, error) {
	byBucket := make(map[int16][]taskQueueUserDataV2Change)
	for _, change := range changes {
		byBucket[change.row.bucket] = append(byBucket[change.row.bucket], change)
	}
	buckets := make([]int, 0, len(byBucket))
	for bucket := range byBucket {
		buckets = append(buckets, int(bucket))
	}
	sort.Ints(buckets)

	prepared := make([]taskQueueUserDataV2Change, 0, len(changes))
	for _, bucketValue := range buckets {
		expiresAt := time.Now().UTC().Add(taskQueueUserDataV2TransactionLease)
		renewed, err := d.Session.Query(
			templateRenewTaskQueueUserDataV2TransactionQuery,
			expiresAt,
			namespaceID,
			txnID,
			taskQueueUserDataV2TransactionPreparing,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return prepared, gocql.ConvertError("RenewTaskQueueUserDataV2Transaction", err)
		}
		if !renewed {
			return prepared, serviceerror.NewUnavailable("task queue user data transaction lost its prepare lease")
		}
		namespaceRenewed, err := d.Session.Query(
			templateRenewTaskQueueUserDataV2NamespaceQuery,
			expiresAt,
			namespaceID,
			txnID,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return prepared, gocql.ConvertError("RenewTaskQueueUserDataV2Namespace", err)
		}
		if !namespaceRenewed {
			return prepared, serviceerror.NewUnavailable("task queue user data transaction lost its namespace lock")
		}

		bucketChanges := byBucket[int16(bucketValue)]
		batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
		for _, change := range bucketChanges {
			switch {
			case change.row.buildID == "" && change.newRow:
				batch.Query(
					templatePrepareNewTaskQueueUserDataV2Query,
					namespaceID, change.row.bucket, change.row.taskQueue,
					txnID, change.targetData, change.targetEncoding, change.targetVersion,
				)
			case change.row.buildID == "":
				batch.Query(
					templatePrepareExistingTaskQueueUserDataV2Query,
					txnID, change.targetData, change.targetEncoding, change.targetVersion,
					namespaceID, change.row.bucket, change.row.taskQueue, change.row.version,
				)
			case change.targetPresent:
				batch.Query(
					templatePrepareNewTaskQueueUserDataV2MappingQuery,
					namespaceID, change.row.bucket, change.row.buildID, change.row.taskQueue, txnID,
				)
			default:
				batch.Query(
					templatePrepareDeleteTaskQueueUserDataV2MappingQuery,
					txnID, namespaceID, change.row.bucket, change.row.buildID, change.row.taskQueue,
				)
			}
		}
		previous := make(map[string]any)
		applied, iter, err := d.Session.MapExecuteBatchCAS(batch, previous)
		if iter != nil {
			if closeErr := iter.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}
		if err != nil {
			return prepared, gocql.ConvertError("PrepareTaskQueueUserDataV2Transaction", err)
		}
		if !applied {
			return prepared, &p.ConditionFailedError{Msg: "Failed to update task queues: concurrent task queue user data update"}
		}
		prepared = append(prepared, bucketChanges...)
	}
	return prepared, nil
}

func (d *userDataStore) finalizeTaskQueueUserDataV2Changes(
	ctx context.Context,
	namespaceID string,
	txnID string,
	changes []taskQueueUserDataV2Change,
) error {
	var firstErr error
	for _, change := range changes {
		var query string
		var args []any
		switch {
		case change.row.buildID == "":
			query = templateFinalizeTaskQueueUserDataV2Query
			args = []any{
				change.targetData, change.targetEncoding, change.targetVersion,
				namespaceID, change.row.bucket, change.row.taskQueue, txnID,
			}
		case change.targetPresent:
			query = templateFinalizeTaskQueueUserDataV2MappingQuery
			args = []any{namespaceID, change.row.bucket, change.row.buildID, change.row.taskQueue, txnID}
		default:
			query = templateFinalizeDeleteTaskQueueUserDataV2MappingQuery
			args = []any{namespaceID, change.row.bucket, change.row.buildID, change.row.taskQueue, txnID}
		}
		applied, err := d.Session.Query(query, args...).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			if firstErr == nil {
				firstErr = gocql.ConvertError("FinalizeTaskQueueUserDataV2Transaction", err)
			}
			continue
		}
		if !applied {
			resolved, resolveErr := d.taskQueueUserDataV2ParticipantResolved(ctx, namespaceID, txnID, change.row)
			if resolveErr != nil && firstErr == nil {
				firstErr = resolveErr
			} else if !resolved && firstErr == nil {
				firstErr = serviceerror.NewUnavailable("task queue user data transaction finalization was not applied")
			}
		}
	}
	return firstErr
}

func (d *userDataStore) rollbackTaskQueueUserDataV2Changes(
	ctx context.Context,
	namespaceID string,
	txnID string,
	changes []taskQueueUserDataV2Change,
) error {
	var firstErr error
	for _, change := range changes {
		query := templateRollbackExistingTaskQueueUserDataV2RowQuery
		if change.newRow {
			query = templateRollbackNewTaskQueueUserDataV2RowQuery
		}
		applied, err := d.Session.Query(
			query,
			namespaceID,
			change.row.bucket,
			change.row.buildID,
			change.row.taskQueue,
			txnID,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err == nil && !applied {
			var resolved bool
			resolved, err = d.taskQueueUserDataV2ParticipantResolved(ctx, namespaceID, txnID, change.row)
			if err == nil && !resolved {
				err = serviceerror.NewUnavailable("task queue user data transaction rollback was not applied")
			}
		}
		if err != nil {
			convertedErr := gocql.ConvertError("RollbackTaskQueueUserDataV2Transaction", err)
			if firstErr == nil {
				firstErr = convertedErr
			}
			d.logger.Error(
				"Unable to roll back task queue user data transaction participant",
				tag.NewStringTag("transaction-id", txnID),
				tag.Error(convertedErr),
			)
		}
	}
	return firstErr
}

func (d *userDataStore) taskQueueUserDataV2ParticipantResolved(
	ctx context.Context,
	namespaceID string,
	txnID string,
	row taskQueueUserDataV2Row,
) (bool, error) {
	current, err := d.readTaskQueueUserDataV2Row(ctx, namespaceID, row.bucket, row.buildID, row.taskQueue)
	if gocql.IsNotFoundError(err) {
		return true, nil
	}
	if err != nil {
		return false, gocql.ConvertError("ResolveTaskQueueUserDataV2Participant", err)
	}
	return current.pendingTxnID != txnID, nil
}

func (d *userDataStore) abortTaskQueueUserDataV2Transaction(
	ctx context.Context,
	namespaceID string,
	txnID string,
	namespaceEpoch int64,
) (bool, error) {
	batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	addAbortTaskQueueUserDataV2TransactionToBatch(
		batch,
		namespaceID,
		txnID,
		namespaceEpoch,
		time.Now().UTC().Add(taskQueueUserDataV2TransactionRetention),
	)
	previous := make(map[string]any)
	aborted, iter, err := d.Session.MapExecuteBatchCAS(batch, previous)
	if iter != nil {
		if closeErr := iter.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}
	if err != nil {
		return false, gocql.ConvertError("AbortTaskQueueUserDataV2Transaction", err)
	}
	return aborted, nil
}

func addAbortTaskQueueUserDataV2TransactionToBatch(
	batch taskQueueUserDataV2Batch,
	namespaceID string,
	txnID string,
	namespaceEpoch int64,
	cleanupAfter time.Time,
) {
	batch.Query(
		templateForceAbortTaskQueueUserDataV2TransactionQuery,
		taskQueueUserDataV2TransactionAborted,
		cleanupAfter,
		namespaceID,
		txnID,
		taskQueueUserDataV2TransactionPreparing,
	)
	batch.Query(
		templateAbortTaskQueueUserDataV2NamespaceQuery,
		namespaceEpoch+1,
		namespaceID,
		txnID,
		namespaceEpoch,
	)
}

func (d *userDataStore) expireTerminalTaskQueueUserDataV2Transaction(
	ctx context.Context,
	namespaceID string,
	txnID string,
	state string,
) error {
	if txnID == "" || (state != taskQueueUserDataV2TransactionCommitted && state != taskQueueUserDataV2TransactionAborted) {
		return fmt.Errorf("cannot expire task queue user data transaction %q in state %q", txnID, state)
	}
	cleanupAfter := time.Now().UTC().Add(taskQueueUserDataV2TransactionRetention)
	applied, err := d.Session.Query(
		templateExpireTerminalTaskQueueUserDataV2TransactionQuery,
		int(taskQueueUserDataV2TransactionRetention/time.Second),
		state,
		cleanupAfter,
		namespaceID,
		txnID,
		state,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("ExpireTaskQueueUserDataV2Transaction", err)
	}
	if applied {
		return nil
	}
	txn, err := d.readTaskQueueUserDataV2Transaction(ctx, namespaceID, txnID)
	if gocql.IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return gocql.ConvertError("ResolveTaskQueueUserDataV2TransactionExpiry", err)
	}
	if txn.state != state {
		return serviceerror.NewDataLossf(
			"task queue user data transaction %q changed from terminal state %q to %q",
			txnID,
			state,
			txn.state,
		)
	}
	return serviceerror.NewUnavailable("task queue user data transaction retention update was not applied")
}

func (d *userDataStore) resolveAmbiguousTaskQueueUserDataV2Commit(
	ctx context.Context,
	namespaceID string,
	txnID string,
	namespaceEpoch int64,
) (string, error) {
	for range 3 {
		txn, err := d.readTaskQueueUserDataV2Transaction(ctx, namespaceID, txnID)
		if err != nil {
			return "", gocql.ConvertError("ResolveTaskQueueUserDataV2Transaction", err)
		}
		if txn.state != taskQueueUserDataV2TransactionPreparing {
			return txn.state, nil
		}
		aborted, err := d.abortTaskQueueUserDataV2Transaction(ctx, namespaceID, txnID, namespaceEpoch)
		if err != nil {
			return "", err
		}
		if aborted {
			return taskQueueUserDataV2TransactionAborted, nil
		}
	}
	return "", serviceerror.NewUnavailable("task queue user data transaction commit resolution did not converge")
}

func (d *userDataStore) readWritableTaskQueueUserDataV2Row(
	ctx context.Context,
	namespaceID string,
	bucket int16,
	buildID string,
	taskQueue string,
) (taskQueueUserDataV2Row, bool, error) {
	for range 3 {
		row, err := d.readTaskQueueUserDataV2Row(ctx, namespaceID, bucket, buildID, taskQueue)
		if gocql.IsNotFoundError(err) {
			return taskQueueUserDataV2Row{}, false, nil
		}
		if err != nil {
			return taskQueueUserDataV2Row{}, false, gocql.ConvertError("ReadTaskQueueUserDataV2ForUpdate", err)
		}
		if row.pendingTxnID == "" {
			if !row.present {
				_, err := d.Session.Query(
					templateDeleteAbsentTaskQueueUserDataV2RowQuery,
					namespaceID,
					bucket,
					buildID,
					taskQueue,
				).WithContext(ctx).MapScanCAS(make(map[string]any))
				if err != nil {
					return taskQueueUserDataV2Row{}, false, gocql.ConvertError("CleanTaskQueueUserDataV2Row", err)
				}
				continue
			}
			return row, true, nil
		}
		resolved, err := d.recoverTaskQueueUserDataV2Row(ctx, namespaceID, row)
		if err != nil {
			return taskQueueUserDataV2Row{}, false, err
		}
		if !resolved {
			return taskQueueUserDataV2Row{}, false, serviceerror.NewUnavailable("task queue user data update is in progress")
		}
	}
	return taskQueueUserDataV2Row{}, false, serviceerror.NewUnavailable("task queue user data transaction recovery did not converge")
}

//nolint:revive // Row recovery resolves every committed, aborted, expired, and superseded transaction state.
func (d *userDataStore) recoverTaskQueueUserDataV2Row(
	ctx context.Context,
	namespaceID string,
	row taskQueueUserDataV2Row,
) (bool, error) {
	txn, err := d.readTaskQueueUserDataV2Transaction(ctx, namespaceID, row.pendingTxnID)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return false, serviceerror.NewDataLossf(
				"task queue user data row references missing transaction %q",
				row.pendingTxnID,
			)
		}
		return false, gocql.ConvertError("RecoverTaskQueueUserDataV2Transaction", err)
	}
	if txn.state == taskQueueUserDataV2TransactionPreparing && !time.Now().UTC().Before(txn.expiresAt) {
		namespaceState, err := d.readTaskQueueUserDataV2Namespace(ctx, namespaceID)
		if err != nil {
			return false, err
		}
		if namespaceState.activeTxnID != row.pendingTxnID {
			return false, serviceerror.NewDataLossf(
				"expired task queue user data transaction %q does not own namespace lock",
				row.pendingTxnID,
			)
		}
		aborted, err := d.abortTaskQueueUserDataV2Transaction(
			ctx,
			namespaceID,
			row.pendingTxnID,
			namespaceState.epoch,
		)
		if err != nil {
			return false, err
		}
		if aborted {
			txn.state = taskQueueUserDataV2TransactionAborted
		} else {
			txn, err = d.readTaskQueueUserDataV2Transaction(ctx, namespaceID, row.pendingTxnID)
			if err != nil {
				return false, gocql.ConvertError("ResolveExpiredTaskQueueUserDataV2Transaction", err)
			}
		}
	}

	switch txn.state {
	case taskQueueUserDataV2TransactionPreparing:
		return false, nil
	case taskQueueUserDataV2TransactionCommitted:
		change := taskQueueUserDataV2Change{
			row:            row,
			targetPresent:  row.pendingPresent,
			targetData:     row.pendingData,
			targetEncoding: row.pendingDataEncoding,
			targetVersion:  row.pendingVersion,
		}
		if err := d.finalizeTaskQueueUserDataV2Changes(
			ctx,
			namespaceID,
			row.pendingTxnID,
			[]taskQueueUserDataV2Change{change},
		); err != nil {
			return true, err
		}
		return true, d.expireUnreferencedTaskQueueUserDataV2Transaction(
			ctx,
			taskQueueUserDataV2TransactionKey{namespaceID: namespaceID, txnID: row.pendingTxnID},
		)
	case taskQueueUserDataV2TransactionAborted:
		query := templateRollbackExistingTaskQueueUserDataV2RowQuery
		if !row.present {
			query = templateRollbackNewTaskQueueUserDataV2RowQuery
		}
		applied, err := d.Session.Query(
			query,
			namespaceID,
			row.bucket,
			row.buildID,
			row.taskQueue,
			row.pendingTxnID,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return true, gocql.ConvertError("RollbackTaskQueueUserDataV2Transaction", err)
		}
		if !applied {
			resolved, resolveErr := d.taskQueueUserDataV2ParticipantResolved(ctx, namespaceID, row.pendingTxnID, row)
			if resolveErr != nil {
				return true, resolveErr
			}
			if !resolved {
				return true, serviceerror.NewUnavailable("task queue user data transaction rollback was not applied")
			}
		}
		return true, d.expireUnreferencedTaskQueueUserDataV2Transaction(
			ctx,
			taskQueueUserDataV2TransactionKey{namespaceID: namespaceID, txnID: row.pendingTxnID},
		)
	default:
		return false, serviceerror.NewDataLossf("unknown task queue user data transaction state %q", txn.state)
	}
}

func (d *userDataStore) logicalTaskQueueUserDataV2Row(
	ctx context.Context,
	namespaceID string,
	row taskQueueUserDataV2Row,
) (taskQueueUserDataV2Row, error) {
	return d.logicalTaskQueueUserDataV2RowObserved(ctx, namespaceID, row, nil)
}

func (d *userDataStore) logicalTaskQueueUserDataV2RowObserved(
	ctx context.Context,
	namespaceID string,
	row taskQueueUserDataV2Row,
	observedTransactions map[string]string,
) (taskQueueUserDataV2Row, error) {
	if row.pendingTxnID == "" {
		return row, nil
	}
	txn, err := d.readTaskQueueUserDataV2Transaction(ctx, namespaceID, row.pendingTxnID)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return taskQueueUserDataV2Row{}, serviceerror.NewDataLossf(
				"task queue user data row references missing transaction %q",
				row.pendingTxnID,
			)
		}
		return taskQueueUserDataV2Row{}, gocql.ConvertError("ResolveTaskQueueUserDataV2Read", err)
	}
	if observedTransactions != nil {
		if previousState, ok := observedTransactions[row.pendingTxnID]; ok && previousState != txn.state {
			return taskQueueUserDataV2Row{}, errTaskQueueUserDataV2SnapshotChanged
		}
		observedTransactions[row.pendingTxnID] = txn.state
	}
	switch txn.state {
	case taskQueueUserDataV2TransactionCommitted:
		row.data = row.pendingData
		row.dataEncoding = row.pendingDataEncoding
		row.version = row.pendingVersion
		row.present = row.pendingPresent
		row.writeTime = max(row.writeTime, row.pendingWriteTime)
		return row, nil
	case taskQueueUserDataV2TransactionPreparing,
		taskQueueUserDataV2TransactionAborted:
		return row, nil
	default:
		return taskQueueUserDataV2Row{}, serviceerror.NewDataLossf("unknown task queue user data transaction state %q", txn.state)
	}
}

func (d *userDataStore) taskQueueUserDataV2SnapshotUnchanged(
	ctx context.Context,
	namespaceID string,
	observedTransactions map[string]string,
) (bool, error) {
	for txnID, observedState := range observedTransactions {
		txn, err := d.readTaskQueueUserDataV2Transaction(ctx, namespaceID, txnID)
		if err != nil {
			return false, gocql.ConvertError("ValidateTaskQueueUserDataV2ReadSnapshot", err)
		}
		if txn.state != observedState {
			return false, nil
		}
	}
	return true, nil
}

func (d *userDataStore) readTaskQueueUserDataV2Transaction(
	ctx context.Context,
	namespaceID string,
	txnID string,
) (taskQueueUserDataV2Transaction, error) {
	var txn taskQueueUserDataV2Transaction
	err := d.Session.Query(
		templateGetTaskQueueUserDataV2TransactionQuery,
		namespaceID,
		txnID,
	).WithContext(ctx).Scan(&txn.state, &txn.expiresAt)
	return txn, err
}

func (d *userDataStore) readTaskQueueUserDataV2Namespace(
	ctx context.Context,
	namespaceID string,
) (taskQueueUserDataV2NamespaceState, error) {
	var state taskQueueUserDataV2NamespaceState
	err := d.Session.Query(
		templateGetTaskQueueUserDataV2NamespaceQuery,
		namespaceID,
	).WithContext(ctx).Scan(&state.activeTxnID, &state.epoch, &state.expiresAt)
	if gocql.IsNotFoundError(err) {
		return taskQueueUserDataV2NamespaceState{}, nil
	}
	if err != nil {
		return taskQueueUserDataV2NamespaceState{}, gocql.ConvertError("ReadTaskQueueUserDataV2Namespace", err)
	}
	return state, nil
}

func (d *userDataStore) stableTaskQueueUserDataV2Namespace(
	ctx context.Context,
	namespaceID string,
) (taskQueueUserDataV2NamespaceState, error) {
	for range 3 {
		state, err := d.readTaskQueueUserDataV2Namespace(ctx, namespaceID)
		if err != nil {
			return taskQueueUserDataV2NamespaceState{}, err
		}
		if state.activeTxnID == "" {
			return state, nil
		}
		if time.Now().UTC().Before(state.expiresAt) {
			return taskQueueUserDataV2NamespaceState{}, errTaskQueueUserDataV2SnapshotChanged
		}
		_, err = d.abortTaskQueueUserDataV2Transaction(
			ctx,
			namespaceID,
			state.activeTxnID,
			state.epoch,
		)
		if err != nil {
			return taskQueueUserDataV2NamespaceState{}, err
		}
	}
	return taskQueueUserDataV2NamespaceState{}, serviceerror.NewUnavailable("task queue user data namespace recovery did not converge")
}

func (d *userDataStore) readTaskQueueUserDataV2Row(
	ctx context.Context,
	namespaceID string,
	bucket int16,
	buildID string,
	taskQueue string,
) (taskQueueUserDataV2Row, error) {
	rowMap := make(map[string]any)
	err := d.Session.Query(
		templateGetTaskQueueUserDataV2RowQuery,
		namespaceID,
		bucket,
		buildID,
		taskQueue,
	).WithContext(ctx).MapScan(rowMap)
	if err != nil {
		return taskQueueUserDataV2Row{}, err
	}
	return decodeTaskQueueUserDataV2Row(rowMap, bucket, buildID)
}

//nolint:revive // Decoding validates the complete denormalized row and transaction marker combination.
func decodeTaskQueueUserDataV2Row(row map[string]any, bucket int16, buildID string) (taskQueueUserDataV2Row, error) {
	taskQueue, err := getTypedFieldFromRow[string]("task_queue_name", row)
	if err != nil {
		return taskQueueUserDataV2Row{}, err
	}
	decoded := taskQueueUserDataV2Row{
		taskQueue: taskQueue,
		buildID:   buildID,
		bucket:    bucket,
	}
	if value, ok, err := optionalTaskQueueUserDataV2Field[[]byte](row, "data"); err != nil {
		return taskQueueUserDataV2Row{}, err
	} else if ok {
		decoded.data = value
	}
	if value, ok, err := optionalTaskQueueUserDataV2Field[string](row, "data_encoding"); err != nil {
		return taskQueueUserDataV2Row{}, err
	} else if ok {
		decoded.dataEncoding = value
	}
	if value, ok, err := optionalTaskQueueUserDataV2Field[int64](row, "version"); err != nil {
		return taskQueueUserDataV2Row{}, err
	} else if ok {
		decoded.version = value
	}
	if value, ok, err := optionalTaskQueueUserDataV2Field[bool](row, "present"); err != nil {
		return taskQueueUserDataV2Row{}, err
	} else if ok {
		decoded.present = value
	}
	if value, ok, err := optionalTaskQueueUserDataV2Field[string](row, "pending_txn_id"); err != nil {
		return taskQueueUserDataV2Row{}, err
	} else if ok {
		decoded.pendingTxnID = value
	}
	if value, ok, err := optionalTaskQueueUserDataV2Field[[]byte](row, "pending_data"); err != nil {
		return taskQueueUserDataV2Row{}, err
	} else if ok {
		decoded.pendingData = value
	}
	if value, ok, err := optionalTaskQueueUserDataV2Field[string](row, "pending_data_encoding"); err != nil {
		return taskQueueUserDataV2Row{}, err
	} else if ok {
		decoded.pendingDataEncoding = value
	}
	if value, ok, err := optionalTaskQueueUserDataV2Field[int64](row, "pending_version"); err != nil {
		return taskQueueUserDataV2Row{}, err
	} else if ok {
		decoded.pendingVersion = value
	}
	if value, ok, err := optionalTaskQueueUserDataV2Field[bool](row, "pending_present"); err != nil {
		return taskQueueUserDataV2Row{}, err
	} else if ok {
		decoded.pendingPresent = value
	}
	for _, name := range []string{"migration_wt_data", "migration_wt_encoding", "migration_wt_version", "migration_wt_present"} {
		if value, ok, err := optionalTaskQueueUserDataV2Field[int64](row, name); err != nil {
			return taskQueueUserDataV2Row{}, err
		} else if ok {
			decoded.writeTime = max(decoded.writeTime, value)
		}
	}
	for _, name := range []string{
		"migration_wt_pending_data",
		"migration_wt_pending_encoding",
		"migration_wt_pending_version",
		"migration_wt_pending_present",
	} {
		if value, ok, err := optionalTaskQueueUserDataV2Field[int64](row, name); err != nil {
			return taskQueueUserDataV2Row{}, err
		} else if ok {
			decoded.pendingWriteTime = max(decoded.pendingWriteTime, value)
		}
	}
	return decoded, nil
}

func optionalTaskQueueUserDataV2Field[T any](row map[string]any, name string) (T, bool, error) {
	var zero T
	value, ok := row[name]
	if !ok || value == nil {
		return zero, false, nil
	}
	typed, ok := value.(T)
	if !ok {
		return zero, false, newPersistedTypeMismatchError(name, zero, value, row)
	}
	return typed, true, nil
}

func (d *userDataStore) listTaskQueueUserDataEntriesV2(
	ctx context.Context,
	request *p.ListTaskQueueUserDataEntriesRequest,
) (*p.InternalListTaskQueueUserDataEntriesResponse, error) {
	for range 3 {
		response, err := d.listTaskQueueUserDataEntriesV2Attempt(ctx, request)
		if errors.Is(err, errTaskQueueUserDataV2SnapshotChanged) {
			continue
		}
		return response, err
	}
	return nil, serviceerror.NewUnavailable("task queue user data read snapshot did not stabilize")
}

//nolint:revive // Stable pagination merges bucket reads while enforcing a namespace epoch snapshot.
func (d *userDataStore) listTaskQueueUserDataEntriesV2Attempt(
	ctx context.Context,
	request *p.ListTaskQueueUserDataEntriesRequest,
) (*p.InternalListTaskQueueUserDataEntriesResponse, error) {
	bucketCount := d.effectiveTaskQueueUserDataBucketCount()
	token, err := decodeTaskQueueUserDataV2PageToken(
		request.NextPageToken,
		request.NamespaceID,
		"",
		bucketCount,
	)
	if err != nil {
		return nil, err
	}
	namespaceState, err := d.stableTaskQueueUserDataV2Namespace(ctx, request.NamespaceID)
	if err != nil {
		return nil, err
	}
	if len(request.NextPageToken) > 0 && token.Epoch != namespaceState.epoch {
		return nil, errors.New("task queue user data changed; restart pagination")
	}
	token.Epoch = namespaceState.epoch
	response := &p.InternalListTaskQueueUserDataEntriesResponse{}
	pageSize := request.PageSize
	if pageSize <= 0 {
		pageSize = 1
	}
	reads, err := d.readTaskQueueUserDataV2Buckets(
		ctx,
		func(bucketCtx context.Context, bucket int) (taskQueueUserDataV2BucketRead, error) {
			query := d.Session.Query(
				templateListTaskQueueUserDataV2PageQuery,
				request.NamespaceID,
				int16(bucket),
				pageSize+1,
			)
			if token.HasCursor {
				query = d.Session.Query(
					templateListTaskQueueUserDataV2PageAfterQuery,
					request.NamespaceID,
					int16(bucket),
					token.AfterTaskQueue,
					pageSize+1,
				)
			}
			iter := query.WithContext(bucketCtx).PageSize(pageSize + 1).Iter()
			result := taskQueueUserDataV2BucketRead{observedTransactions: make(map[string]string)}
			rowMap := make(map[string]any)
			for iter.MapScan(rowMap) {
				row, err := decodeTaskQueueUserDataV2Row(rowMap, int16(bucket), "")
				if err != nil {
					_ = iter.Close()
					return taskQueueUserDataV2BucketRead{}, err
				}
				if err := validateTaskQueueUserDataV2RowBucket(row, bucketCount); err != nil {
					_ = iter.Close()
					return taskQueueUserDataV2BucketRead{}, err
				}
				row, err = d.logicalTaskQueueUserDataV2RowObserved(
					bucketCtx,
					request.NamespaceID,
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
				return taskQueueUserDataV2BucketRead{}, gocql.ConvertError("ListTaskQueueUserDataEntriesV2", err)
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
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].taskQueue == rows[j].taskQueue {
			return rows[i].bucket < rows[j].bucket
		}
		return rows[i].taskQueue < rows[j].taskQueue
	})
	for index := 1; index < len(rows); index++ {
		if rows[index-1].taskQueue == rows[index].taskQueue {
			return nil, serviceerror.NewDataLossf(
				"duplicate task queue user data v2 row %q in buckets %d and %d",
				rows[index].taskQueue,
				rows[index-1].bucket,
				rows[index].bucket,
			)
		}
	}
	processed := min(pageSize, len(rows))
	for _, row := range rows[:processed] {
		if row.present {
			response.Entries = append(response.Entries, p.InternalTaskQueueUserDataEntry{
				TaskQueue: row.taskQueue,
				Data:      p.NewDataBlob(row.data, row.dataEncoding),
				Version:   row.version,
			})
		}
	}
	if len(rows) > processed {
		response.NextPageToken, err = encodeTaskQueueUserDataV2PageToken(
			request.NamespaceID,
			"",
			bucketCount,
			token.Epoch,
			rows[processed-1].taskQueue,
			true,
		)
		if err != nil {
			return nil, err
		}
	}
	return d.finishTaskQueueUserDataV2ListSnapshot(ctx, request.NamespaceID, token.Epoch, response, observedTransactions)
}

func (d *userDataStore) finishTaskQueueUserDataV2ListSnapshot(
	ctx context.Context,
	namespaceID string,
	namespaceEpoch int64,
	response *p.InternalListTaskQueueUserDataEntriesResponse,
	observedTransactions map[string]string,
) (*p.InternalListTaskQueueUserDataEntriesResponse, error) {
	state, err := d.stableTaskQueueUserDataV2Namespace(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	if state.epoch != namespaceEpoch {
		return nil, errTaskQueueUserDataV2SnapshotChanged
	}
	unchanged, err := d.taskQueueUserDataV2SnapshotUnchanged(ctx, namespaceID, observedTransactions)
	if err != nil {
		return nil, err
	}
	if !unchanged {
		return nil, errTaskQueueUserDataV2SnapshotChanged
	}
	return response, nil
}

func (d *userDataStore) getTaskQueuesByBuildIDV2(
	ctx context.Context,
	request *p.GetTaskQueuesByBuildIdRequest,
) ([]string, error) {
	for range 3 {
		taskQueues, err := d.getTaskQueuesByBuildIDV2Attempt(ctx, request)
		if errors.Is(err, errTaskQueueUserDataV2SnapshotChanged) {
			continue
		}
		return taskQueues, err
	}
	return nil, serviceerror.NewUnavailable("task queue user data read snapshot did not stabilize")
}

//nolint:revive // Stable lookup merges bucket reads while enforcing a namespace epoch snapshot.
func (d *userDataStore) getTaskQueuesByBuildIDV2Attempt(
	ctx context.Context,
	request *p.GetTaskQueuesByBuildIdRequest,
) ([]string, error) {
	namespaceState, err := d.stableTaskQueueUserDataV2Namespace(ctx, request.NamespaceID)
	if err != nil {
		return nil, err
	}
	bucketCount := d.effectiveTaskQueueUserDataBucketCount()
	reads, err := d.readTaskQueueUserDataV2Buckets(
		ctx,
		func(bucketCtx context.Context, bucket int) (taskQueueUserDataV2BucketRead, error) {
			result := taskQueueUserDataV2BucketRead{observedTransactions: make(map[string]string)}
			var pageState []byte
			for {
				iter := d.Session.Query(
					templateListTaskQueueNamesByBuildIDV2Query,
					request.NamespaceID,
					int16(bucket),
					request.BuildID,
				).WithContext(bucketCtx).PageSize(listTaskQueueNamesByBuildIdPageSize).PageState(pageState).Iter()
				rowMap := make(map[string]any)
				for iter.MapScan(rowMap) {
					row, err := decodeTaskQueueUserDataV2Row(rowMap, int16(bucket), request.BuildID)
					if err != nil {
						_ = iter.Close()
						return taskQueueUserDataV2BucketRead{}, err
					}
					if err := validateTaskQueueUserDataV2RowBucket(row, bucketCount); err != nil {
						_ = iter.Close()
						return taskQueueUserDataV2BucketRead{}, err
					}
					row, err = d.logicalTaskQueueUserDataV2RowObserved(
						bucketCtx,
						request.NamespaceID,
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
				nextPageState := iter.PageState()
				if err := iter.Close(); err != nil {
					return taskQueueUserDataV2BucketRead{}, gocql.ConvertError("GetTaskQueuesByBuildIdV2", err)
				}
				if len(nextPageState) == 0 {
					break
				}
				pageState = nextPageState
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
		request.NamespaceID,
		namespaceState.epoch,
		observedTransactions,
	); err != nil {
		return nil, err
	}
	taskQueues := make([]string, 0, len(rows))
	seen := make(map[string]int16, len(rows))
	for _, row := range rows {
		if previousBucket, ok := seen[row.taskQueue]; ok {
			return nil, serviceerror.NewDataLossf(
				"duplicate task queue user data v2 mapping build_id=%q task_queue=%q in buckets %d and %d",
				request.BuildID,
				row.taskQueue,
				previousBucket,
				row.bucket,
			)
		}
		seen[row.taskQueue] = row.bucket
		if row.present {
			taskQueues = append(taskQueues, row.taskQueue)
		}
	}
	sort.Strings(taskQueues)
	return taskQueues, nil
}

func (d *userDataStore) countTaskQueuesByBuildIDV2(
	ctx context.Context,
	request *p.CountTaskQueuesByBuildIdRequest,
) (int, error) {
	for range 3 {
		count, err := d.countTaskQueuesByBuildIDV2Attempt(ctx, request)
		if errors.Is(err, errTaskQueueUserDataV2SnapshotChanged) {
			continue
		}
		return count, err
	}
	return 0, serviceerror.NewUnavailable("task queue user data read snapshot did not stabilize")
}

func (d *userDataStore) countTaskQueuesByBuildIDV2Attempt(
	ctx context.Context,
	request *p.CountTaskQueuesByBuildIdRequest,
) (int, error) {
	namespaceState, err := d.stableTaskQueueUserDataV2Namespace(ctx, request.NamespaceID)
	if err != nil {
		return 0, err
	}
	bucketCount := d.effectiveTaskQueueUserDataBucketCount()
	reads, err := d.readTaskQueueUserDataV2Buckets(
		ctx,
		func(bucketCtx context.Context, bucket int) (taskQueueUserDataV2BucketRead, error) {
			result := taskQueueUserDataV2BucketRead{observedTransactions: make(map[string]string)}
			iter := d.Session.Query(
				templateListTaskQueueNamesByBuildIDV2Query,
				request.NamespaceID,
				int16(bucket),
				request.BuildID,
			).WithContext(bucketCtx).PageSize(listTaskQueueNamesByBuildIdPageSize).Iter()
			rowMap := make(map[string]any)
			for iter.MapScan(rowMap) {
				row, err := decodeTaskQueueUserDataV2Row(rowMap, int16(bucket), request.BuildID)
				if err != nil {
					_ = iter.Close()
					return taskQueueUserDataV2BucketRead{}, err
				}
				if err := validateTaskQueueUserDataV2RowBucket(row, bucketCount); err != nil {
					_ = iter.Close()
					return taskQueueUserDataV2BucketRead{}, err
				}
				row, err = d.logicalTaskQueueUserDataV2RowObserved(
					bucketCtx,
					request.NamespaceID,
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
				return taskQueueUserDataV2BucketRead{}, gocql.ConvertError("CountTaskQueuesByBuildIdV2", err)
			}
			return result, nil
		},
	)
	if err != nil {
		return 0, err
	}
	rows, observedTransactions, err := mergeTaskQueueUserDataV2BucketReads(reads)
	if err != nil {
		return 0, err
	}
	if err := d.validateTaskQueueUserDataV2Snapshot(
		ctx,
		request.NamespaceID,
		namespaceState.epoch,
		observedTransactions,
	); err != nil {
		return 0, err
	}
	count := 0
	seen := make(map[string]int16, len(rows))
	for _, row := range rows {
		if previousBucket, ok := seen[row.taskQueue]; ok {
			return 0, serviceerror.NewDataLossf(
				"duplicate task queue user data v2 mapping build_id=%q task_queue=%q in buckets %d and %d",
				request.BuildID,
				row.taskQueue,
				previousBucket,
				row.bucket,
			)
		}
		seen[row.taskQueue] = row.bucket
		if row.present {
			count++
		}
	}
	if request.Limit > 0 {
		count = min(count, request.Limit)
	}
	return count, nil
}

func (d *userDataStore) readTaskQueueUserDataV2Buckets(
	ctx context.Context,
	read func(context.Context, int) (taskQueueUserDataV2BucketRead, error),
) ([]taskQueueUserDataV2BucketRead, error) {
	bucketCount := d.effectiveTaskQueueUserDataBucketCount()
	results := make([]taskQueueUserDataV2BucketRead, bucketCount)
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(min(bucketCount, taskQueueUserDataV2FanoutConcurrency))
	for bucket := range bucketCount {
		bucket := bucket
		group.Go(func() error {
			result, err := read(groupCtx, bucket)
			if err == nil {
				results[bucket] = result
			}
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func mergeTaskQueueUserDataV2BucketReads(
	reads []taskQueueUserDataV2BucketRead,
) ([]taskQueueUserDataV2Row, map[string]string, error) {
	var rows []taskQueueUserDataV2Row
	observedTransactions := make(map[string]string)
	for _, read := range reads {
		rows = append(rows, read.rows...)
		for txnID, state := range read.observedTransactions {
			if previousState, ok := observedTransactions[txnID]; ok && previousState != state {
				return nil, nil, errTaskQueueUserDataV2SnapshotChanged
			}
			observedTransactions[txnID] = state
		}
	}
	return rows, observedTransactions, nil
}

func validateTaskQueueUserDataV2RowBucket(row taskQueueUserDataV2Row, bucketCount int) error {
	expected, err := taskQueueUserDataBucket(row.taskQueue, bucketCount)
	if err != nil {
		return err
	}
	if row.bucket != expected {
		return serviceerror.NewDataLossf(
			"task queue user data v2 row build_id=%q task_queue=%q is in bucket %d, expected %d",
			row.buildID,
			row.taskQueue,
			row.bucket,
			expected,
		)
	}
	return nil
}

func (d *userDataStore) validateTaskQueueUserDataV2Snapshot(
	ctx context.Context,
	namespaceID string,
	epoch int64,
	observedTransactions map[string]string,
) error {
	finalNamespaceState, err := d.stableTaskQueueUserDataV2Namespace(ctx, namespaceID)
	if err != nil {
		return err
	}
	if finalNamespaceState.epoch != epoch {
		return errTaskQueueUserDataV2SnapshotChanged
	}
	unchanged, err := d.taskQueueUserDataV2SnapshotUnchanged(ctx, namespaceID, observedTransactions)
	if err != nil {
		return err
	}
	if !unchanged {
		return errTaskQueueUserDataV2SnapshotChanged
	}
	return nil
}

func encodeTaskQueueUserDataV2PageToken(
	namespaceID string,
	buildID string,
	bucketCount int,
	epoch int64,
	afterTaskQueue string,
	hasCursor bool,
) ([]byte, error) {
	return json.Marshal(taskQueueUserDataV2PageToken{
		Version:          taskQueueUserDataV2PageTokenVersion,
		Layout:           taskQueueUserDataV2Layout,
		LayoutGeneration: taskQueueUserDataV2LayoutGeneration,
		NamespaceID:      namespaceID,
		BuildID:          buildID,
		BucketCount:      bucketCount,
		Epoch:            epoch,
		AfterTaskQueue:   afterTaskQueue,
		HasCursor:        hasCursor,
	})
}

func decodeTaskQueueUserDataV2PageToken(
	data []byte,
	namespaceID string,
	buildID string,
	bucketCount int,
) (taskQueueUserDataV2PageToken, error) {
	if len(data) == 0 {
		return taskQueueUserDataV2PageToken{
			Version:          taskQueueUserDataV2PageTokenVersion,
			Layout:           taskQueueUserDataV2Layout,
			LayoutGeneration: taskQueueUserDataV2LayoutGeneration,
			NamespaceID:      namespaceID,
			BuildID:          buildID,
			BucketCount:      bucketCount,
		}, nil
	}
	var token taskQueueUserDataV2PageToken
	if err := json.Unmarshal(data, &token); err != nil {
		return taskQueueUserDataV2PageToken{}, fmt.Errorf("invalid task queue user data v2 page token: %w", err)
	}
	if token.Version != taskQueueUserDataV2PageTokenVersion {
		return taskQueueUserDataV2PageToken{}, fmt.Errorf("unsupported task queue user data v2 page token version %d", token.Version)
	}
	if token.Layout != taskQueueUserDataV2Layout || token.LayoutGeneration != taskQueueUserDataV2LayoutGeneration {
		return taskQueueUserDataV2PageToken{}, fmt.Errorf(
			"task queue user data v2 page token has layout %q generation %d",
			token.Layout,
			token.LayoutGeneration,
		)
	}
	if token.NamespaceID != namespaceID || token.BuildID != buildID {
		return taskQueueUserDataV2PageToken{}, errors.New("task queue user data v2 page token belongs to another request")
	}
	if token.BucketCount != bucketCount {
		return taskQueueUserDataV2PageToken{}, fmt.Errorf(
			"task queue user data v2 page token bucket count %d != configured %d",
			token.BucketCount,
			bucketCount,
		)
	}
	return token, nil
}

func taskQueueUserDataV2VersionConflict(taskQueue string, requested int64, actual int64) error {
	return &p.ConditionFailedError{
		Msg: fmt.Sprintf(
			"Failed to update task queues: task queue %q version %d != %d",
			taskQueue,
			requested,
			actual,
		),
	}
}

func (d *userDataStore) markTaskQueueUserDataV2Conflict(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueUserDataRequest,
) {
	for taskQueue, update := range request.Updates {
		bucket, err := taskQueueUserDataBucket(taskQueue, d.effectiveTaskQueueUserDataBucketCount())
		if err != nil {
			continue
		}
		row, err := d.readTaskQueueUserDataV2Row(ctx, request.NamespaceID, bucket, "", taskQueue)
		if err == nil && row.version != update.Version && update.Conflicting != nil {
			*update.Conflicting = true
			return
		}
	}
}
