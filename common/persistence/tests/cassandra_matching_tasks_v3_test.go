//go:build integration

package tests

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCassandraMatchingTasksV3TargetOnly(t *testing.T) {
	for _, fair := range []bool{false, true} {
		fair := fair
		name := "classic"
		if fair {
			name = "fair"
		}
		t.Run(name, func(t *testing.T) {
			testData, tearDown := setUpCassandraTest(t)
			defer tearDown()
			recreateMatchingTasksV3Schema(t, testData)
			sourceTable := "tasks"
			if fair {
				sourceTable = "tasks_v2"
			}
			session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
			require.NoError(t, session.Query("DROP TABLE "+sourceTable).Exec())
			session.Close()

			store, err := testData.Factory.NewTaskStoreWithMatchingTaskMigration(
				fair,
				config.CassandraMatchingTaskMigrationModeTargetOnly,
				2,
			)
			require.NoError(t, err)
			manager := p.NewTaskManager(store, serialization.NewSerializer())
			testMatchingTasksV3TargetOnly(t, testData.Factory, manager, fair)
		})
	}
}

func TestCassandraMatchingTasksV3OnlineMigration(t *testing.T) {
	for _, fair := range []bool{false, true} {
		fair := fair
		name := "classic"
		if fair {
			name = "fair"
		}
		t.Run(name, func(t *testing.T) {
			testData, tearDown := setUpCassandraTest(t)
			defer tearDown()
			recreateMatchingTasksV3Schema(t, testData)

			var sourceStore p.TaskStore
			var err error
			if fair {
				sourceStore, err = testData.Factory.NewFairTaskStore()
			} else {
				sourceStore, err = testData.Factory.NewTaskStore()
			}
			require.NoError(t, err)
			source := p.NewTaskManager(sourceStore, serialization.NewSerializer())
			info := newMatchingTasksV3QueueInfo()
			_, err = source.CreateTaskQueue(t.Context(), &p.CreateTaskQueueRequest{
				RangeID:       1,
				TaskQueueInfo: info,
			})
			require.NoError(t, err)
			require.NoError(t, createMatchingTasksV3(t.Context(), source, info, fair, 1, 1, 2))
			coldInfo := newMatchingTasksV3QueueInfo()
			_, err = source.CreateTaskQueue(t.Context(), &p.CreateTaskQueueRequest{
				RangeID:       1,
				TaskQueueInfo: coldInfo,
			})
			require.NoError(t, err)
			require.NoError(t, createMatchingTasksV3(t.Context(), source, coldInfo, fair, 1, 1))

			copied, err := testData.Factory.BackfillMatchingTasksV3(t.Context(), cassandra.MatchingTaskBackfillOptions{
				PageSize:        100,
				Concurrency:     4,
				TokenRangeCount: 1,
				BucketCount:     4,
				Fair:            fair,
			})
			require.NoError(t, err)
			require.Equal(t, int64(5), copied)

			result, err := testData.Factory.ValidateMatchingTasksV3Queue(
				t.Context(),
				info.NamespaceId,
				info.Name,
				info.TaskType,
				fair,
				4,
			)
			require.NoError(t, err)
			require.True(t, result.Matches(), result.Mismatches)
			require.False(t, result.TargetOnlyReady)

			sourceDualStore, err := testData.Factory.NewTaskStoreWithMatchingTaskMigration(
				fair,
				config.CassandraMatchingTaskMigrationModeSourceDual,
				4,
			)
			require.NoError(t, err)
			sourceDual := p.NewTaskManager(sourceDualStore, serialization.NewSerializer())
			require.NoError(t, createMatchingTasksV3(t.Context(), sourceDual, info, fair, 1, 3))
			sourceDualUpdate := &p.UpdateTaskQueueRequest{
				RangeID:       2,
				PrevRangeID:   1,
				TaskQueueInfo: info,
			}
			_, err = sourceDual.UpdateTaskQueue(t.Context(), sourceDualUpdate)
			require.NoError(t, err)
			_, err = sourceDual.UpdateTaskQueue(t.Context(), sourceDualUpdate)
			require.NoError(t, err)
			// ID 100000 models a batch that renewed its range before the batch commit. It is
			// mirrored into range 2's bucket while reconciliation infers range 1 from the ID.
			require.NoError(t, createMatchingTasksV3(t.Context(), sourceDual, info, fair, 2, 100000))
			// Model a process crash after the source commit but before its target mirror.
			require.NoError(t, createMatchingTasksV3(t.Context(), source, info, fair, 2, 100001))
			injectMatchingTasksV3TargetDivergence(t, testData, info, fair, 4)

			targetDualStore, err := testData.Factory.NewTaskStoreWithMatchingTaskMigration(
				fair,
				config.CassandraMatchingTaskMigrationModeTargetDual,
				4,
			)
			require.NoError(t, err)
			targetDual := p.NewTaskManager(targetDualStore, serialization.NewSerializer())
			targetDualUpdate := &p.UpdateTaskQueueRequest{
				RangeID:       3,
				PrevRangeID:   2,
				TaskQueueInfo: info,
			}
			_, err = targetDual.UpdateTaskQueue(t.Context(), targetDualUpdate)
			require.NoError(t, err)
			_, err = targetDual.UpdateTaskQueue(t.Context(), targetDualUpdate)
			require.NoError(t, err)
			require.NoError(t, createMatchingTasksV3(t.Context(), targetDual, info, fair, 3, 200001))

			_, err = sourceDual.UpdateTaskQueue(t.Context(), &p.UpdateTaskQueueRequest{
				RangeID: 4, PrevRangeID: 3, TaskQueueInfo: info,
			})
			require.Error(t, err)
			requireMatchingTaskSourceAuthority(t, testData, info, fair, 3, 3, 4)
			err = createMatchingTasksV3(t.Context(), sourceDual, info, fair, 3, 300001)
			require.Error(t, err)
			_, err = sourceDual.CompleteTasksLessThan(
				t.Context(),
				matchingTasksV3CompleteRequest(info, fair),
			)
			require.Error(t, err)

			result, err = testData.Factory.ValidateMatchingTasksV3Queue(
				t.Context(),
				info.NamespaceId,
				info.Name,
				info.TaskType,
				fair,
				4,
			)
			require.NoError(t, err)
			require.True(t, result.Matches(), result.Mismatches)
			require.True(t, result.CanEnterTargetOnly())

			// Simulate a crash after the source fence but before reconciliation/target activation.
			_, err = source.UpdateTaskQueue(t.Context(), &p.UpdateTaskQueueRequest{
				RangeID:       2,
				PrevRangeID:   1,
				TaskQueueInfo: coldInfo,
			})
			require.NoError(t, err)
			require.NoError(t, testData.Factory.CutoverMatchingTasksV3Queue(
				t.Context(),
				coldInfo.NamespaceId,
				coldInfo.Name,
				coldInfo.TaskType,
				fair,
				4,
			))
			coldResult, err := testData.Factory.ValidateMatchingTasksV3Queue(
				t.Context(),
				coldInfo.NamespaceId,
				coldInfo.Name,
				coldInfo.TaskType,
				fair,
				4,
			)
			require.NoError(t, err)
			require.True(t, coldResult.CanEnterTargetOnly(), coldResult)

			targetOnlyStore, err := testData.Factory.NewTaskStoreWithMatchingTaskMigration(
				fair,
				config.CassandraMatchingTaskMigrationModeTargetOnly,
				4,
			)
			require.NoError(t, err)
			targetOnly := p.NewTaskManager(targetOnlyStore, serialization.NewSerializer())
			tasks, err := targetOnly.GetTasks(t.Context(), matchingTasksV3GetRequest(info, fair, 10))
			require.NoError(t, err)
			require.Len(t, tasks.Tasks, 6)
			require.Equal(t, []int64{1, 2, 3, 100000, 100001, 200001}, matchingTasksV3IDs(tasks.Tasks))
		})
	}
}

func recreateMatchingTasksV3Schema(t testing.TB, testData CassandraTestData) {
	t.Helper()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	require.NoError(t, session.Query("DROP TABLE IF EXISTS tasks_v3").Exec())
	require.NoError(t, session.Query("DROP TABLE IF EXISTS tasks_v3_fair").Exec())
	session.Close()
	ApplySchemaUpdate(
		t,
		testData.Cfg,
		"../../../schema/cassandra/temporal/versioned/v1.16/matching_tasks_v3.cql",
		testData.Logger,
	)
}

func newMatchingTasksV3QueueInfo() *persistencespb.TaskQueueInfo {
	return &persistencespb.TaskQueueInfo{
		NamespaceId:    uuid.NewString(),
		Name:           "matching-v3-" + uuid.NewString(),
		TaskType:       enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		Kind:           enumspb.TASK_QUEUE_KIND_NORMAL,
		LastUpdateTime: timestamppb.Now(),
	}
}

func createMatchingTasksV3(
	ctx context.Context,
	manager p.TaskManager,
	info *persistencespb.TaskQueueInfo,
	fair bool,
	rangeID int64,
	ids ...int64,
) error {
	tasks := make([]*persistencespb.AllocatedTaskInfo, len(ids))
	for i, id := range ids {
		tasks[i] = &persistencespb.AllocatedTaskInfo{
			TaskId: id,
			Data: &persistencespb.TaskInfo{
				NamespaceId: info.NamespaceId,
				WorkflowId:  uuid.NewString(),
				RunId:       uuid.NewString(),
				CreateTime:  timestamppb.Now(),
			},
		}
		if fair {
			tasks[i].TaskPass = id
		} else {
			tasks[i].Data.ExpiryTime = timestamppb.New(time.Now().Add(time.Hour))
		}
	}
	_, err := manager.CreateTasks(ctx, &p.CreateTasksRequest{
		TaskQueueInfo: &p.PersistedTaskQueueInfo{Data: info, RangeID: rangeID},
		Tasks:         tasks,
	})
	return err
}

func matchingTasksV3GetRequest(
	info *persistencespb.TaskQueueInfo,
	fair bool,
	pageSize int,
) *p.GetTasksRequest {
	request := &p.GetTasksRequest{
		NamespaceID:        info.NamespaceId,
		TaskQueue:          info.Name,
		TaskType:           info.TaskType,
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: math.MaxInt64,
		PageSize:           pageSize,
	}
	if fair {
		request.InclusiveMinPass = 1
		request.UseLimit = true
	}
	return request
}

func matchingTasksV3IDs(tasks []*persistencespb.AllocatedTaskInfo) []int64 {
	ids := make([]int64, len(tasks))
	for i, task := range tasks {
		ids[i] = task.TaskId
	}
	return ids
}

func matchingTasksV3CompleteRequest(
	info *persistencespb.TaskQueueInfo,
	fair bool,
) *p.CompleteTasksLessThanRequest {
	request := &p.CompleteTasksLessThanRequest{
		NamespaceID:        info.NamespaceId,
		TaskQueueName:      info.Name,
		TaskType:           info.TaskType,
		ExclusiveMaxTaskID: math.MaxInt64,
		Limit:              100,
	}
	if fair {
		request.ExclusiveMaxPass = math.MaxInt64
	}
	return request
}

func injectMatchingTasksV3TargetDivergence(
	t *testing.T,
	testData CassandraTestData,
	info *persistencespb.TaskQueueInfo,
	fair bool,
	bucketCount int,
) {
	t.Helper()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	table := "tasks_v3"
	if fair {
		table = "tasks_v3_fair"
	}
	writeTime := time.Now().UTC().UnixMicro() + int64(time.Second/time.Microsecond)
	if fair {
		require.NoError(t, session.Query(
			`UPDATE tasks_v3_fair USING TIMESTAMP ? SET task = ?, task_encoding = ?
				WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? AND storage_bucket = ?
				AND type = ? AND pass = ? AND task_id = ?`,
			writeTime, []byte("stale"), enumspb.ENCODING_TYPE_PROTO3.String(),
			info.NamespaceId, info.Name, info.TaskType, int16(0), 0, int64(3), int64(3),
		).Exec())
		require.NoError(t, session.Query(
			`INSERT INTO tasks_v3_fair (namespace_id, task_queue_name, task_queue_type, storage_bucket,
				type, pass, task_id, task, task_encoding) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
			info.NamespaceId, info.Name, info.TaskType, int16(bucketCount-1), 0, int64(777), int64(777),
			[]byte("extra"), enumspb.ENCODING_TYPE_PROTO3.String(), writeTime,
		).Exec())
		return
	}
	require.Equal(t, "tasks_v3", table)
	require.NoError(t, session.Query(
		`UPDATE tasks_v3 USING TIMESTAMP ? SET task = ?, task_encoding = ?
			WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? AND storage_bucket = ?
			AND type = ? AND task_id = ?`,
		writeTime, []byte("stale"), enumspb.ENCODING_TYPE_PROTO3.String(),
		info.NamespaceId, info.Name, info.TaskType, int16(0), 0, int64(3),
	).Exec())
	require.NoError(t, session.Query(
		`INSERT INTO tasks_v3 (namespace_id, task_queue_name, task_queue_type, storage_bucket,
			type, task_id, task, task_encoding) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		info.NamespaceId, info.Name, info.TaskType, int16(bucketCount-1), 0, int64(777),
		[]byte("extra"), enumspb.ENCODING_TYPE_PROTO3.String(), writeTime,
	).Exec())
}

func requireMatchingTaskSourceAuthority(
	t *testing.T,
	testData CassandraTestData,
	info *persistencespb.TaskQueueInfo,
	fair bool,
	rangeID int64,
	authority int,
	bucketCount int16,
) {
	t.Helper()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	table := "tasks"
	clustering := "type = ? AND task_id = ?"
	args := []any{info.NamespaceId, info.Name, info.TaskType, 1, int64(-12345)}
	if fair {
		table = "tasks_v2"
		clustering = "type = ? AND pass = 0 AND task_id = ?"
	}
	var actualRangeID int64
	var actualAuthority int
	var actualBucketCount int16
	err := session.Query(
		fmt.Sprintf(
			"SELECT range_id, migration_authority, migration_bucket_count FROM %s "+
				"WHERE namespace_id = ? AND task_queue_name = ? AND task_queue_type = ? AND %s",
			table,
			clustering,
		),
		args...,
	).Scan(&actualRangeID, &actualAuthority, &actualBucketCount)
	require.NoError(t, err)
	require.Equal(t, rangeID, actualRangeID)
	require.Equal(t, authority, actualAuthority)
	require.Equal(t, bucketCount, actualBucketCount)
}

func testMatchingTasksV3TargetOnly(
	t *testing.T,
	factory *cassandra.Factory,
	manager p.TaskManager,
	fair bool,
) {
	ctx := context.Background()
	now := timestamppb.Now()
	info := &persistencespb.TaskQueueInfo{
		NamespaceId:    uuid.NewString(),
		Name:           "matching-v3-" + uuid.NewString(),
		TaskType:       enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		Kind:           enumspb.TASK_QUEUE_KIND_NORMAL,
		LastUpdateTime: now,
	}
	_, err := manager.CreateTaskQueue(ctx, &p.CreateTaskQueueRequest{RangeID: 1, TaskQueueInfo: info})
	require.NoError(t, err)

	create := func(rangeID int64, ids ...int64) error {
		tasks := make([]*persistencespb.AllocatedTaskInfo, len(ids))
		for i, id := range ids {
			tasks[i] = &persistencespb.AllocatedTaskInfo{
				TaskId: id,
				Data: &persistencespb.TaskInfo{
					NamespaceId: info.NamespaceId,
					WorkflowId:  uuid.NewString(),
					RunId:       uuid.NewString(),
					CreateTime:  now,
				},
			}
			if fair {
				tasks[i].TaskPass = id
			}
		}
		_, err := manager.CreateTasks(ctx, &p.CreateTasksRequest{
			TaskQueueInfo: &p.PersistedTaskQueueInfo{Data: info, RangeID: rangeID},
			Tasks:         tasks,
		})
		return err
	}
	require.NoError(t, create(1, 1, 2))

	_, err = manager.UpdateTaskQueue(ctx, &p.UpdateTaskQueueRequest{
		RangeID:       2,
		PrevRangeID:   1,
		TaskQueueInfo: info,
	})
	require.NoError(t, err)
	var conditionFailed *p.ConditionFailedError
	require.ErrorAs(t, create(1, 3), &conditionFailed)
	require.NoError(t, create(2, 100001))
	_, err = manager.UpdateTaskQueue(ctx, &p.UpdateTaskQueueRequest{
		RangeID:       3,
		PrevRangeID:   2,
		TaskQueueInfo: info,
	})
	require.NoError(t, err)
	require.ErrorAs(t, create(2, 100002), &conditionFailed)
	require.NoError(t, create(3, 200001))

	queue, err := manager.GetTaskQueue(ctx, &p.GetTaskQueueRequest{
		NamespaceID: info.NamespaceId,
		TaskQueue:   info.Name,
		TaskType:    info.TaskType,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), queue.RangeID)
	wrongBucketStore, err := factory.NewTaskStoreWithMatchingTaskMigration(
		fair,
		config.CassandraMatchingTaskMigrationModeTargetOnly,
		3,
	)
	require.NoError(t, err)
	wrongBucketManager := p.NewTaskManager(wrongBucketStore, serialization.NewSerializer())
	_, err = wrongBucketManager.GetTaskQueue(ctx, &p.GetTaskQueueRequest{
		NamespaceID: info.NamespaceId,
		TaskQueue:   info.Name,
		TaskType:    info.TaskType,
	})
	require.ErrorContains(t, err, "bucket count changed")

	getRequest := &p.GetTasksRequest{
		NamespaceID:        info.NamespaceId,
		TaskQueue:          info.Name,
		TaskType:           info.TaskType,
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: math.MaxInt64,
		PageSize:           2,
	}
	if fair {
		getRequest.InclusiveMinPass = 1
		getRequest.UseLimit = true
	}
	firstPage, err := manager.GetTasks(ctx, getRequest)
	require.NoError(t, err)
	require.Len(t, firstPage.Tasks, 2)
	require.NotEmpty(t, firstPage.NextPageToken)
	require.Equal(t, int64(1), firstPage.Tasks[0].TaskId)
	require.Equal(t, int64(2), firstPage.Tasks[1].TaskId)

	getRequest.NextPageToken = firstPage.NextPageToken
	secondPage, err := manager.GetTasks(ctx, getRequest)
	require.NoError(t, err)
	require.Len(t, secondPage.Tasks, 2)
	require.Equal(t, int64(100001), secondPage.Tasks[0].TaskId)
	require.Equal(t, int64(200001), secondPage.Tasks[1].TaskId)
	require.Empty(t, secondPage.NextPageToken)

	deleteRequest := &p.CompleteTasksLessThanRequest{
		NamespaceID:        info.NamespaceId,
		TaskQueueName:      info.Name,
		TaskType:           info.TaskType,
		ExclusiveMaxTaskID: 3,
		Limit:              100,
	}
	if fair {
		deleteRequest.ExclusiveMaxPass = 3
		deleteRequest.ExclusiveMaxTaskID = 0
	}
	_, err = manager.CompleteTasksLessThan(ctx, deleteRequest)
	require.NoError(t, err)

	getRequest.NextPageToken = nil
	if fair {
		getRequest.InclusiveMinPass = 1
	}
	remaining, err := manager.GetTasks(ctx, getRequest)
	require.NoError(t, err)
	require.Len(t, remaining.Tasks, 2)
	require.Equal(t, int64(100001), remaining.Tasks[0].TaskId)
	require.Equal(t, int64(200001), remaining.Tasks[1].TaskId)

	require.NoError(t, manager.DeleteTaskQueue(ctx, &p.DeleteTaskQueueRequest{
		TaskQueue: &p.TaskQueueKey{
			NamespaceID:   info.NamespaceId,
			TaskQueueName: info.Name,
			TaskQueueType: info.TaskType,
		},
		RangeID: 3,
	}))
	_, err = manager.GetTaskQueue(ctx, &p.GetTaskQueueRequest{
		NamespaceID: info.NamespaceId,
		TaskQueue:   info.Name,
		TaskType:    info.TaskType,
	})
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
}
