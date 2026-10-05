package cassandra

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func TestTaskQueueUserDataBucketStable(t *testing.T) {
	const bucketCount = 64

	first, err := taskQueueUserDataBucket("task-queue", bucketCount)
	require.NoError(t, err)
	second, err := taskQueueUserDataBucket("task-queue", bucketCount)
	require.NoError(t, err)

	require.Equal(t, first, second)
	require.GreaterOrEqual(t, first, int16(0))
	require.Less(t, first, int16(bucketCount))
	require.Equal(t, int16(50), first)
}

func TestTaskQueueUserDataBucketRejectsInvalidCount(t *testing.T) {
	_, err := taskQueueUserDataBucket("task-queue", 0)
	require.Error(t, err)
	_, err = taskQueueUserDataBucket("task-queue", 1<<15)
	require.Error(t, err)
}

func TestTaskQueueUserDataRecoveryBucketCount(t *testing.T) {
	require.Equal(t, DefaultTaskQueueUserDataBucketCount, effectiveTaskQueueUserDataRecoveryBucketCount(TaskQueueUserDataRecoveryOptions{}))
	require.Equal(t, 16, effectiveTaskQueueUserDataRecoveryBucketCount(TaskQueueUserDataRecoveryOptions{BucketCount: 16}))
	require.Error(t, validateTaskQueueUserDataRecoveryOptions(TaskQueueUserDataRecoveryOptions{
		PageSize:    1,
		Concurrency: 1,
		BucketCount: 1 << 15,
	}))
}

func TestTaskQueueUserDataV2DetectsCrossBucketUpdate(t *testing.T) {
	require.True(t, taskQueueUserDataV2ChangesUseSingleBucket([]taskQueueUserDataV2Change{
		{row: taskQueueUserDataV2Row{bucket: 1}},
		{row: taskQueueUserDataV2Row{bucket: 1}},
	}))
	require.False(t, taskQueueUserDataV2ChangesUseSingleBucket([]taskQueueUserDataV2Change{
		{row: taskQueueUserDataV2Row{bucket: 1}},
		{row: taskQueueUserDataV2Row{bucket: 2}},
	}))
}

func TestTaskQueueUserDataV2PageTokenRoundTrip(t *testing.T) {
	encoded, err := encodeTaskQueueUserDataV2PageToken("namespace", "", 64, 23, "task-queue", true)
	require.NoError(t, err)

	decoded, err := decodeTaskQueueUserDataV2PageToken(encoded, "namespace", "", 64)
	require.NoError(t, err)
	require.Equal(t, taskQueueUserDataV2PageToken{
		Version:          taskQueueUserDataV2PageTokenVersion,
		Layout:           taskQueueUserDataV2Layout,
		LayoutGeneration: taskQueueUserDataV2LayoutGeneration,
		NamespaceID:      "namespace",
		BucketCount:      64,
		Epoch:            23,
		AfterTaskQueue:   "task-queue",
		HasCursor:        true,
	}, decoded)
}

func TestTaskQueueUserDataV2PageTokenRejectsWrongGenerationAndBucket(t *testing.T) {
	_, err := decodeTaskQueueUserDataV2PageToken([]byte(`{"version":1}`), "namespace", "", 64)
	require.ErrorContains(t, err, "version 1")
	encoded, err := encodeTaskQueueUserDataV2PageToken("namespace", "", 64, 1, "task-queue", true)
	require.NoError(t, err)
	_, err = decodeTaskQueueUserDataV2PageToken(encoded, "other-namespace", "", 64)
	require.ErrorContains(t, err, "another request")
	_, err = decodeTaskQueueUserDataV2PageToken(encoded, "namespace", "", 32)
	require.ErrorContains(t, err, "bucket count 64")
}

func TestTaskQueueUserDataV2BuildIDFanoutIsParallel(t *testing.T) {
	const bucketCount = 4
	taskQueues := make(map[int16]string, bucketCount)
	for candidate := 0; len(taskQueues) < bucketCount; candidate++ {
		name := fmt.Sprintf("task-queue-%d", candidate)
		bucket, err := taskQueueUserDataBucket(name, bucketCount)
		require.NoError(t, err)
		taskQueues[bucket] = name
	}

	var started atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			if stmt == templateGetTaskQueueUserDataV2NamespaceQuery {
				return &recordingQuery{scanFn: func(dest ...any) error {
					*dest[0].(*string) = ""
					*dest[1].(*int64) = 11
					*dest[2].(*time.Time) = time.Time{}
					return nil
				}}
			}
			require.Equal(t, templateListTaskQueueNamesByBuildIDV2Query, stmt)
			current := active.Add(1)
			for {
				observed := maxActive.Load()
				if current <= observed || maxActive.CompareAndSwap(observed, current) {
					break
				}
			}
			if started.Add(1) == 2 {
				releaseOnce.Do(func() { close(release) })
			}
			<-release
			active.Add(-1)
			bucket := args[1].(int16)
			return &recordingQuery{iter: &recordingIter{mapRows: []map[string]any{{
				"task_queue_name": taskQueues[bucket],
				"present":         true,
			}}}}
		},
	}
	store := newUserDataStore(session, nil, TaskQueueUserDataMigrationModeTargetOnly, bucketCount)

	result, err := store.getTaskQueuesByBuildIDV2Attempt(t.Context(), &p.GetTaskQueuesByBuildIdRequest{
		NamespaceID: "namespace",
		BuildID:     "build-id",
	})

	require.NoError(t, err)
	require.Len(t, result, bucketCount)
	require.Greater(t, maxActive.Load(), int32(1))
}

func TestTaskQueueUserDataMigrationModes(t *testing.T) {
	for _, mode := range []TaskQueueUserDataMigrationMode{
		TaskQueueUserDataMigrationModeSourceOnly,
		TaskQueueUserDataMigrationModeSourceDual,
		TaskQueueUserDataMigrationModeTargetShadow,
		TaskQueueUserDataMigrationModeTargetDual,
		TaskQueueUserDataMigrationModeTargetOnly,
		"",
	} {
		require.NoError(t, ValidateTaskQueueUserDataMigrationMode(mode))
	}
	require.Error(t, ValidateTaskQueueUserDataMigrationMode("invalid"))
}

func TestValidateTaskQueueUserDataMigrationModeSchema(t *testing.T) {
	tables := map[string][][]any{
		taskQueueUserDataV1TableName: {
			{"namespace_id", "partition_key", 0},
			{"build_id", "clustering", 0},
			{"task_queue_name", "clustering", 1},
			{"data", "regular", -1},
			{"data_encoding", "regular", -1},
			{"version", "regular", -1},
			{"migration_authority", "static", -1},
			{"migration_bucket_count", "static", -1},
			{"migration_generation", "static", -1},
		},
		taskQueueUserDataV2TableName: {
			{"namespace_id", "partition_key", 0},
			{"bucket_id", "partition_key", 1},
			{"build_id", "clustering", 0},
			{"task_queue_name", "clustering", 1},
			{"data", "regular", -1},
			{"data_encoding", "regular", -1},
			{"version", "regular", -1},
			{"present", "regular", -1},
			{"pending_txn_id", "regular", -1},
			{"pending_data", "regular", -1},
			{"pending_data_encoding", "regular", -1},
			{"pending_version", "regular", -1},
			{"pending_present", "regular", -1},
		},
		taskQueueUserDataV2TxnTableName: {
			{"namespace_id", "partition_key", 0},
			{"txn_id", "clustering", 0},
			{"state", "regular", -1},
			{"expires_at", "regular", -1},
			{"active_txn_id", "regular", -1},
			{"epoch", "regular", -1},
			{"migration_authority", "regular", -1},
			{"migration_bucket_count", "regular", -1},
			{"migration_generation", "regular", -1},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetTaskQueueUserDataV2SchemaColumns, stmt)
			require.Len(t, args, 2)
			return &recordingQuery{iter: &recordingIter{scanRows: tables[args[1].(string)]}}
		},
	}

	err := ValidateTaskQueueUserDataMigrationModeSchema(
		t.Context(),
		session,
		"temporal",
		TaskQueueUserDataMigrationModeTargetDual,
	)

	require.NoError(t, err)
	require.Len(t, session.queries, 3)
}

func TestSourceOnlyTaskQueueUserDataSchemaValidationDoesNotReadTargetSchema(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetTaskQueueUserDataV2SchemaColumns, stmt)
			require.Equal(t, taskQueueUserDataV1TableName, args[1])
			return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
				{"namespace_id", "partition_key", 0},
				{"build_id", "clustering", 0},
				{"task_queue_name", "clustering", 1},
				{"data", "regular", -1},
				{"data_encoding", "regular", -1},
				{"version", "regular", -1},
				{"migration_authority", "static", -1},
				{"migration_bucket_count", "static", -1},
				{"migration_generation", "static", -1},
			}}}
		},
	}

	err := ValidateTaskQueueUserDataMigrationModeSchema(
		t.Context(),
		session,
		"temporal",
		TaskQueueUserDataMigrationModeSourceOnly,
	)

	require.NoError(t, err)
	require.Len(t, session.queries, 1)
}

func TestTargetOnlyTaskQueueUserDataReadUsesOnlyV2(t *testing.T) {
	const taskQueue = "target-only-task-queue"
	bucket, err := taskQueueUserDataBucket(taskQueue, DefaultTaskQueueUserDataBucketCount)
	require.NoError(t, err)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetTaskQueueUserDataV2RowQuery, stmt)
			require.Equal(t, []any{"11111111-1111-1111-1111-111111111111", bucket, "", taskQueue}, args)
			return &recordingQuery{
				mapScanFn: func(row map[string]any) error {
					row["task_queue_name"] = taskQueue
					row["data"] = []byte("target-data")
					row["data_encoding"] = enumspb.ENCODING_TYPE_PROTO3.String()
					row["version"] = int64(7)
					row["present"] = true
					return nil
				},
			}
		},
	}
	store := newUserDataStore(
		session,
		nil,
		TaskQueueUserDataMigrationModeTargetOnly,
		DefaultTaskQueueUserDataBucketCount,
	)
	store.targetAuthorityCache.Store("11111111-1111-1111-1111-111111111111", struct{}{})

	response, err := store.GetTaskQueueUserData(t.Context(), &p.GetTaskQueueUserDataRequest{
		NamespaceID: "11111111-1111-1111-1111-111111111111",
		TaskQueue:   taskQueue,
	})

	require.NoError(t, err)
	require.Equal(t, int64(7), response.Version)
	require.Equal(t, []byte("target-data"), response.UserData.Data)
	require.Equal(t, []string{templateGetTaskQueueUserDataV2RowQuery}, recordedStatements(session.queries))
}

func TestTargetOnlyTaskQueueUserDataWriteStartsOnlyV2(t *testing.T) {
	expectedErr := errors.New("stop after target row query")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			require.Equal(t, templateGetTaskQueueUserDataV2RowQuery, stmt)
			return &recordingQuery{mapScanFn: func(map[string]any) error { return expectedErr }}
		},
	}
	store := newUserDataStore(
		session,
		nil,
		TaskQueueUserDataMigrationModeTargetOnly,
		2,
	)
	store.targetAuthorityCache.Store("11111111-1111-1111-1111-111111111111", struct{}{})

	err := store.UpdateTaskQueueUserData(t.Context(), &p.InternalUpdateTaskQueueUserDataRequest{
		NamespaceID: "11111111-1111-1111-1111-111111111111",
		Updates: map[string]*p.InternalSingleTaskQueueUserDataUpdate{
			"a": {
				UserData: p.NewDataBlob([]byte("data"), enumspb.ENCODING_TYPE_PROTO3.String()),
			},
			"b": {
				UserData: p.NewDataBlob([]byte("data"), enumspb.ENCODING_TYPE_PROTO3.String()),
			},
		},
	})

	require.Error(t, err)
	require.Len(t, session.queries, 1)
	require.Equal(t, templateGetTaskQueueUserDataV2RowQuery, session.queries[0].stmt)
}

func TestTargetOnlyTaskQueueUserDataScansUseOnlyV2(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*userDataStore) error
	}{
		{
			name: "list",
			run: func(store *userDataStore) error {
				_, err := store.ListTaskQueueUserDataEntries(t.Context(), &p.ListTaskQueueUserDataEntriesRequest{
					NamespaceID: "namespace",
					PageSize:    10,
				})
				return err
			},
		},
		{
			name: "get by build ID",
			run: func(store *userDataStore) error {
				_, err := store.GetTaskQueuesByBuildId(t.Context(), &p.GetTaskQueuesByBuildIdRequest{
					NamespaceID: "namespace",
					BuildID:     "build-id",
				})
				return err
			},
		},
		{
			name: "count",
			run: func(store *userDataStore) error {
				_, err := store.CountTaskQueuesByBuildId(t.Context(), &p.CountTaskQueuesByBuildIdRequest{
					NamespaceID: "namespace",
					BuildID:     "build-id",
				})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(stmt string, _ ...any) cgocql.Query {
					require.Contains(t, stmt, "task_queue_user_data_v2")
					require.NotEqual(t, templateListTaskQueueUserDataQuery, stmt)
					require.NotEqual(t, templateListTaskQueueNamesByBuildIdQuery, stmt)
					require.NotEqual(t, templateCountTaskQueueByBuildIDQuery, stmt)
					require.NotEqual(t, templateLimitedCountTaskQueueByBuildIDQuery, stmt)
					if stmt == templateGetTaskQueueUserDataV2NamespaceQuery {
						return &recordingQuery{scanFn: func(dest ...any) error {
							*dest[0].(*string) = ""
							*dest[1].(*int64) = 0
							*dest[2].(*time.Time) = time.Time{}
							return nil
						}}
					}
					return &recordingQuery{iter: &recordingIter{}}
				},
			}
			store := newUserDataStore(session, nil, TaskQueueUserDataMigrationModeTargetOnly, 2)
			store.targetAuthorityCache.Store("namespace", struct{}{})

			require.NoError(t, tc.run(&store))
			require.Len(t, session.queries, 4)
		})
	}
}

func TestLogicalTaskQueueUserDataV2RowUsesCommittedPendingValue(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetTaskQueueUserDataV2TransactionQuery, stmt)
			require.Equal(t, []any{"namespace", "transaction"}, args)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*string) = taskQueueUserDataV2TransactionCommitted
					*dest[1].(*time.Time) = time.Now().UTC()
					return nil
				},
			}
		},
	}
	store := newUserDataStore(session, nil, TaskQueueUserDataMigrationModeTargetOnly, 64)

	row, err := store.logicalTaskQueueUserDataV2Row(t.Context(), "namespace", taskQueueUserDataV2Row{
		data:                []byte("old"),
		version:             1,
		present:             true,
		pendingTxnID:        "transaction",
		pendingData:         []byte("new"),
		pendingDataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
		pendingVersion:      2,
		pendingPresent:      true,
	})

	require.NoError(t, err)
	require.Equal(t, []byte("new"), row.data)
	require.Equal(t, int64(2), row.version)
}

func TestLogicalTaskQueueUserDataV2RowUsesOldValueBeforeCommitAndAfterAbort(t *testing.T) {
	for _, state := range []string{
		taskQueueUserDataV2TransactionPreparing,
		taskQueueUserDataV2TransactionAborted,
	} {
		t.Run(state, func(t *testing.T) {
			session := &recordingSession{
				t: t,
				queryFn: func(string, ...any) cgocql.Query {
					return &recordingQuery{
						scanFn: func(dest ...any) error {
							*dest[0].(*string) = state
							*dest[1].(*time.Time) = time.Now().UTC()
							return nil
						},
					}
				},
			}
			store := newUserDataStore(session, nil, TaskQueueUserDataMigrationModeTargetOnly, 64)
			row, err := store.logicalTaskQueueUserDataV2Row(t.Context(), "namespace", taskQueueUserDataV2Row{
				data:           []byte("old"),
				version:        1,
				present:        true,
				pendingTxnID:   "transaction",
				pendingData:    []byte("new"),
				pendingVersion: 2,
				pendingPresent: true,
			})

			require.NoError(t, err)
			require.Equal(t, []byte("old"), row.data)
			require.Equal(t, int64(1), row.version)
		})
	}
}

func TestResolveAmbiguousTaskQueueUserDataV2CommitObservesCommitted(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			require.Equal(t, templateGetTaskQueueUserDataV2TransactionQuery, stmt)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*string) = taskQueueUserDataV2TransactionCommitted
					*dest[1].(*time.Time) = time.Now().UTC()
					return nil
				},
			}
		},
	}
	store := newUserDataStore(session, nil, TaskQueueUserDataMigrationModeTargetOnly, 64)

	state, err := store.resolveAmbiguousTaskQueueUserDataV2Commit(t.Context(), "namespace", "transaction", 7)

	require.NoError(t, err)
	require.Equal(t, taskQueueUserDataV2TransactionCommitted, state)
	require.Len(t, session.queries, 1)
}

func TestTaskQueueUserDataV2CoordinatorCommitIsSingleLinearizationBatch(t *testing.T) {
	batch := &taskQueueUserDataV2RecordingBatch{}
	cleanupAfter := time.Now().UTC()

	addCommitTaskQueueUserDataV2TransactionToBatch(batch, "namespace", "transaction", 7, cleanupAfter)

	require.Equal(t, []taskQueueUserDataV2RecordedStatement{
		{
			query: templateCommitTaskQueueUserDataV2TransactionQuery,
			args: []any{
				taskQueueUserDataV2TransactionCommitted,
				cleanupAfter,
				"namespace",
				"transaction",
				taskQueueUserDataV2TransactionPreparing,
			},
		},
		{
			query: templateCommitTaskQueueUserDataV2NamespaceQuery,
			args:  []any{int64(8), "namespace", "transaction", int64(7)},
		},
	}, batch.statements)
}

func TestTaskQueueUserDataV2CoordinatorAbortCompetesInSameLinearizationBatch(t *testing.T) {
	batch := &taskQueueUserDataV2RecordingBatch{}
	cleanupAfter := time.Now().UTC()

	addAbortTaskQueueUserDataV2TransactionToBatch(batch, "namespace", "transaction", 7, cleanupAfter)

	require.Equal(t, []taskQueueUserDataV2RecordedStatement{
		{
			query: templateForceAbortTaskQueueUserDataV2TransactionQuery,
			args: []any{
				taskQueueUserDataV2TransactionAborted,
				cleanupAfter,
				"namespace",
				"transaction",
				taskQueueUserDataV2TransactionPreparing,
			},
		},
		{
			query: templateAbortTaskQueueUserDataV2NamespaceQuery,
			args:  []any{int64(8), "namespace", "transaction", int64(7)},
		},
	}, batch.statements)
}

func TestExpireTaskQueueUserDataV2TransactionArmsTTL(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateExpireTerminalTaskQueueUserDataV2TransactionQuery, stmt)
			require.Equal(t, int(taskQueueUserDataV2TransactionRetention/time.Second), args[0])
			require.Equal(t, taskQueueUserDataV2TransactionCommitted, args[1])
			require.WithinDuration(t, time.Now().UTC().Add(taskQueueUserDataV2TransactionRetention), args[2].(time.Time), time.Second)
			require.Equal(t, []any{"namespace", "transaction", taskQueueUserDataV2TransactionCommitted}, args[3:])
			return &recordingQuery{mapScanCASFn: func(map[string]any) (bool, error) { return true, nil }}
		},
	}
	store := newUserDataStore(session, nil, TaskQueueUserDataMigrationModeTargetOnly, 64)

	err := store.expireTerminalTaskQueueUserDataV2Transaction(
		t.Context(),
		"namespace",
		"transaction",
		taskQueueUserDataV2TransactionCommitted,
	)

	require.NoError(t, err)
}

func TestLogicalTaskQueueUserDataV2RowDetectsTransactionChangeDuringRead(t *testing.T) {
	states := []string{
		taskQueueUserDataV2TransactionPreparing,
		taskQueueUserDataV2TransactionCommitted,
	}
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			state := states[0]
			states = states[1:]
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*string) = state
					*dest[1].(*time.Time) = time.Now().UTC()
					return nil
				},
			}
		},
	}
	store := newUserDataStore(session, nil, TaskQueueUserDataMigrationModeTargetOnly, 64)
	observed := make(map[string]string)
	row := taskQueueUserDataV2Row{pendingTxnID: "transaction", pendingPresent: true}

	_, err := store.logicalTaskQueueUserDataV2RowObserved(t.Context(), "namespace", row, observed)
	require.NoError(t, err)
	_, err = store.logicalTaskQueueUserDataV2RowObserved(t.Context(), "namespace", row, observed)

	require.ErrorIs(t, err, errTaskQueueUserDataV2SnapshotChanged)
}

func TestTaskQueueUserDataV2SnapshotValidationDetectsCommit(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*string) = taskQueueUserDataV2TransactionCommitted
					*dest[1].(*time.Time) = time.Now().UTC()
					return nil
				},
			}
		},
	}
	store := newUserDataStore(session, nil, TaskQueueUserDataMigrationModeTargetOnly, 64)

	unchanged, err := store.taskQueueUserDataV2SnapshotUnchanged(t.Context(), "namespace", map[string]string{
		"transaction": taskQueueUserDataV2TransactionPreparing,
	})

	require.NoError(t, err)
	require.False(t, unchanged)
}

func TestTaskQueueUserDataMirrorRequestDoesNotMutateResultPointers(t *testing.T) {
	applied := true
	conflicting := true
	request := &p.InternalUpdateTaskQueueUserDataRequest{
		NamespaceID: "namespace",
		Updates: map[string]*p.InternalSingleTaskQueueUserDataUpdate{
			"task-queue": {
				Version:     2,
				UserData:    &commonpb.DataBlob{Data: []byte("data")},
				Applied:     &applied,
				Conflicting: &conflicting,
			},
		},
	}

	mirror := taskQueueUserDataMirrorRequest(request)

	require.Nil(t, mirror.Updates["task-queue"].Applied)
	require.Nil(t, mirror.Updates["task-queue"].Conflicting)
	require.NotNil(t, request.Updates["task-queue"].Applied)
	require.NotNil(t, request.Updates["task-queue"].Conflicting)
}

func TestBackfillTaskQueueUserDataV2RangeUsesBucketAndSourceTimestamp(t *testing.T) {
	const taskQueue = "backfill-task-queue"
	const sourceWriteTime = int64(123456789)
	bucket, err := taskQueueUserDataBucket(taskQueue, 64)
	require.NoError(t, err)
	queryNumber := 0
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			queryNumber++
			switch queryNumber {
			case 1:
				require.Equal(t, templateScanTaskQueueUserDataForV2Backfill, stmt)
				require.Equal(t, []any{int64(-10), int64(10)}, args)
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{
						"11111111-1111-1111-1111-111111111111",
						"build-id",
						taskQueue,
						[]byte("data"),
						enumspb.ENCODING_TYPE_PROTO3.String(),
						int64(3),
						sourceWriteTime - 2,
						sourceWriteTime - 1,
						sourceWriteTime,
					},
				}}}
			case 2:
				require.Equal(t, templateBackfillTaskQueueUserDataV2, stmt)
				require.Equal(t, []any{
					"11111111-1111-1111-1111-111111111111",
					bucket,
					"build-id",
					taskQueue,
					[]byte("data"),
					enumspb.ENCODING_TYPE_PROTO3.String(),
					int64(3),
					sourceWriteTime,
				}, args)
				return &recordingQuery{}
			default:
				t.Fatalf("unexpected query %d", queryNumber)
				return nil
			}
		},
	}

	copied, err := BackfillTaskQueueUserDataV2Range(
		t.Context(),
		session,
		TaskQueueUserDataBackfillOptions{PageSize: 10, Concurrency: 1, BucketCount: 64},
		TaskQueueUserDataBackfillTokenRange{Index: 0, StartToken: -10, EndToken: 10},
	)

	require.NoError(t, err)
	require.Equal(t, int64(1), copied)
	require.Equal(t, 2, queryNumber)
}

func TestValidateTaskQueueUserDataV2RejectsWrongBucketAndDuplicate(t *testing.T) {
	const (
		taskQueue   = "validation-task-queue"
		bucketCount = 2
	)
	expectedBucket, err := taskQueueUserDataBucket(taskQueue, bucketCount)
	require.NoError(t, err)
	wrongBucket := int16(1) - expectedBucket
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateScanTaskQueueUserDataNamespaceForValidation:
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{{
					"", taskQueue, []byte("data"), enumspb.ENCODING_TYPE_PROTO3.String(), int64(1),
					int64(101), int64(101), int64(101),
				}}}}
			case templateScanTaskQueueUserDataV2BucketForValidation:
				bucket := args[1].(int16)
				return &recordingQuery{iter: &recordingIter{mapRows: []map[string]any{{
					"build_id":          "",
					"task_queue_name":   taskQueue,
					"data":              []byte("data"),
					"data_encoding":     enumspb.ENCODING_TYPE_PROTO3.String(),
					"version":           int64(1),
					"present":           true,
					"migration_wt_data": int64(101),
					"bucket":            bucket,
				}}}}
			default:
				t.Fatalf("unexpected query %q", stmt)
				return nil
			}
		},
	}

	result, err := ValidateTaskQueueUserDataV2Namespace(t.Context(), session, "namespace", bucketCount)

	require.NoError(t, err)
	require.False(t, result.Matches())
	require.Equal(t, 2, result.TargetRows)
	require.Contains(t, result.Mismatches, fmt.Sprintf(
		"wrong target bucket build_id=\"\" task_queue=\"%s\" actual=%d expected=%d",
		taskQueue,
		wrongBucket,
		expectedBucket,
	))
	require.Contains(t, result.Mismatches, fmt.Sprintf(
		"duplicate target row build_id=\"\" task_queue=\"%s\" buckets=%d,%d",
		taskQueue,
		min(expectedBucket, wrongBucket),
		max(expectedBucket, wrongBucket),
	))
}

func TestTaskQueueUserDataReconciliationTimestamp(t *testing.T) {
	writeTime, err := taskQueueUserDataReconciliationWriteTime(20, 10)
	require.NoError(t, err)
	require.Equal(t, int64(20), writeTime)

	writeTime, err = taskQueueUserDataReconciliationWriteTime(10, 20)
	require.NoError(t, err)
	require.Greater(t, writeTime, int64(20))

	_, err = taskQueueUserDataReconciliationWriteTime(10, int64(^uint64(0)>>1))
	require.ErrorContains(t, err, "overflow")

	first := taskQueueUserDataMigrationWriteTime(nullableInt64{})
	second := taskQueueUserDataMigrationWriteTime(nullableInt64{})
	require.Greater(t, second, first)
}

type taskQueueUserDataV2RecordedStatement struct {
	query string
	args  []any
}

type taskQueueUserDataV2RecordingBatch struct {
	statements []taskQueueUserDataV2RecordedStatement
}

func (b *taskQueueUserDataV2RecordingBatch) Query(query string, args ...any) {
	b.statements = append(b.statements, taskQueueUserDataV2RecordedStatement{query: query, args: args})
}
