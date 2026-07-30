package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
)

type fakeWorkflowRun struct {
	id     string
	getErr func() error
}

func (r *fakeWorkflowRun) GetID() string {
	return r.id
}

func (*fakeWorkflowRun) GetRunID() string {
	return "run-id"
}

func (r *fakeWorkflowRun) Get(context.Context, any) error {
	return r.getErr()
}

func (r *fakeWorkflowRun) GetWithOptions(context.Context, any, client.WorkflowRunGetOptions) error {
	return r.getErr()
}

type fakeWorkflowServiceClient struct {
	workflowservice.WorkflowServiceClient
	describeTaskQueue func(*workflowservice.DescribeTaskQueueRequest) (*workflowservice.DescribeTaskQueueResponse, error)
}

func (c *fakeWorkflowServiceClient) DescribeTaskQueue(
	_ context.Context,
	request *workflowservice.DescribeTaskQueueRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DescribeTaskQueueResponse, error) {
	return c.describeTaskQueue(request)
}

type clientWithWorkflowService struct {
	client.Client
	workflowService workflowservice.WorkflowServiceClient
}

func (c *clientWithWorkflowService) WorkflowService() workflowservice.WorkflowServiceClient {
	return c.workflowService
}

func TestRegisterFlagsUpdatesConfig(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	var cfg runConfig
	registerFlags(flags, &cfg)

	err := flags.Parse([]string{
		"-address=frontend:7233",
		"-namespace=load-test",
		"-task-queue=load-task-queue",
		"-task-queues=5",
		"-workers-per-task-queue=2",
		"-workflows=7",
		"-concurrency=3",
		"-activities-each=2",
		"-signals-each=1",
		"-eager-start=true",
		"-eager-activities=true",
		"-payload-bytes=512",
		"-timeout=30s",
		"-register-namespace=false",
		"-worker=false",
		"-backlog-before-workers=true",
		"-backlog-wait-timeout=12s",
		"-cpu-profile=/tmp/cpu.pprof",
		"-heap-profile=/tmp/heap.pprof",
		"-server-pprof=http://frontend:7936",
		"-server-cpu-profile=/tmp/server-cpu.pprof",
		"-server-heap-profile=/tmp/server-heap.pprof",
		"-server-cpu-profile-duration=15s",
		"-metrics-snapshot-before=http://temporal:8000/metrics=/tmp/temporal.before.metrics",
		"-metrics-snapshot-after=http://temporal:8000/metrics=/tmp/temporal.after.metrics",
		"-metrics-snapshot=http://scylla:9180/metrics=/tmp/scylla.after.metrics",
		"-profile-summary=/tmp/cpu.pprof=/tmp/cpu.top.txt",
		"-profile-summary=/tmp/server-cpu.pprof=/tmp/server-cpu.top.txt",
		"-result-file=/tmp/scyllaload.result.json",
		"-run-metadata-file=/tmp/scyllaload.metadata.json",
	})

	require.NoError(t, err)
	require.Equal(t, "frontend:7233", cfg.address)
	require.Equal(t, "load-test", cfg.namespace)
	require.Equal(t, "load-task-queue", cfg.taskQueue)
	require.Equal(t, 5, cfg.taskQueues)
	require.Equal(t, 2, cfg.workersPerTaskQueue)
	require.Equal(t, 7, cfg.workflows)
	require.Equal(t, 3, cfg.concurrency)
	require.Equal(t, 2, cfg.activitiesEach)
	require.Equal(t, 1, cfg.signalsEach)
	require.True(t, cfg.eagerStart)
	require.True(t, cfg.eagerActivities)
	require.Equal(t, 512, cfg.payloadBytes)
	require.Equal(t, 30*time.Second, cfg.timeout)
	require.False(t, cfg.registerNS)
	require.False(t, cfg.runWorker)
	require.True(t, cfg.backlogBeforeWorkers)
	require.Equal(t, 12*time.Second, cfg.backlogWaitTimeout)
	require.Equal(t, "/tmp/cpu.pprof", cfg.cpuProfile)
	require.Equal(t, "/tmp/heap.pprof", cfg.heapProfile)
	require.Equal(t, "http://frontend:7936", cfg.serverPProf)
	require.Equal(t, "/tmp/server-cpu.pprof", cfg.serverCPU)
	require.Equal(t, "/tmp/server-heap.pprof", cfg.serverHeap)
	require.Equal(t, 15*time.Second, cfg.serverCPUTime)
	require.Equal(t, metricSnapshotFlags{
		{url: "http://temporal:8000/metrics", path: "/tmp/temporal.before.metrics"},
	}, cfg.metricSnapshotsBefore)
	require.Equal(t, metricSnapshotFlags{
		{url: "http://temporal:8000/metrics", path: "/tmp/temporal.after.metrics"},
		{url: "http://scylla:9180/metrics", path: "/tmp/scylla.after.metrics"},
	}, cfg.metricSnapshotsAfter)
	require.Equal(t, profileSummaryFlags{
		{profile: "/tmp/cpu.pprof", path: "/tmp/cpu.top.txt"},
		{profile: "/tmp/server-cpu.pprof", path: "/tmp/server-cpu.top.txt"},
	}, cfg.profileSummaries)
	require.Equal(t, "/tmp/scyllaload.result.json", cfg.resultFile)
	require.Equal(t, "/tmp/scyllaload.metadata.json", cfg.runMetadataFile)
}

func TestValidateConfigRequiresPositiveTaskQueuesAndWorkers(t *testing.T) {
	cfg := runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		serverCPUTime:       time.Second,
	}

	invalidTaskQueues := cfg
	invalidTaskQueues.taskQueues = 0
	require.ErrorContains(t, validateConfig(invalidTaskQueues), "-task-queues")

	invalidWorkers := cfg
	invalidWorkers.workersPerTaskQueue = 0
	require.ErrorContains(t, validateConfig(invalidWorkers), "-workers-per-task-queue")
}

func TestValidateConfigBacklogMode(t *testing.T) {
	cfg := runConfig{
		workflows:            1,
		concurrency:          1,
		taskQueues:           1,
		workersPerTaskQueue:  1,
		runWorker:            true,
		backlogBeforeWorkers: true,
		backlogWaitTimeout:   time.Second,
		serverCPUTime:        time.Second,
	}
	require.NoError(t, validateConfig(cfg))

	workerDisabled := cfg
	workerDisabled.runWorker = false
	require.ErrorContains(t, validateConfig(workerDisabled), "-worker")

	withSignals := cfg
	withSignals.signalsEach = 1
	require.ErrorContains(t, validateConfig(withSignals), "-signals-each")

	withEagerStart := cfg
	withEagerStart.eagerStart = true
	require.ErrorContains(t, validateConfig(withEagerStart), "-eager-start")

	invalidWait := cfg
	invalidWait.backlogWaitTimeout = 0
	require.ErrorContains(t, validateConfig(invalidWait), "-backlog-wait-timeout")
}

func TestLoadWorkflowRunsActivities(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(loadWorkflow)
	env.RegisterActivity(loadActivity)

	env.ExecuteWorkflow(loadWorkflow, workflowInput{
		Activities: 3,
		Payload:    []byte("payload"),
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestLoadWorkflowConsumesSignals(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(loadWorkflow)
	env.RegisterActivity(loadActivity)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalName, []byte("one"))
		env.SignalWorkflow(signalName, []byte("two"))
	}, 0)

	env.ExecuteWorkflow(loadWorkflow, workflowInput{
		Signals: 2,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestTaskQueueName(t *testing.T) {
	require.Equal(t, "load-task-queue", taskQueueName(runConfig{
		taskQueue:  "load-task-queue",
		taskQueues: 1,
	}, 3))
	require.Equal(t, "load-task-queue-3", taskQueueName(runConfig{
		taskQueue:  "load-task-queue",
		taskQueues: 5,
	}, 3))
}

func TestRunLoadCountsUnlaunchedWorkflowsAsFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	release := make(chan struct{})
	runner := func(context.Context, client.Client, runConfig, []byte, int64, int) workflowRunResult {
		close(started)
		<-release
		return workflowRunResult{
			completed: true,
			requests:  3,
		}
	}

	resultCh := make(chan runResult, 1)
	go func() {
		resultCh <- runLoadWithRunner(ctx, nil, runConfig{
			workflows:   3,
			concurrency: 1,
			cpuProfile:  "/tmp/cpu.pprof",
			heapProfile: "/tmp/heap.pprof",
			serverCPU:   "/tmp/server-cpu.pprof",
			serverHeap:  "/tmp/server-heap.pprof",
			metricSnapshotsBefore: metricSnapshotFlags{
				{url: "http://temporal:8000/metrics", path: "/tmp/temporal.before.metrics"},
			},
			metricSnapshotsAfter: metricSnapshotFlags{
				{url: "http://temporal:8000/metrics", path: "/tmp/temporal.after.metrics"},
				{url: "http://scylla:9180/metrics", path: "/tmp/scylla.after.metrics"},
			},
			profileSummaries: profileSummaryFlags{
				{profile: "/tmp/cpu.pprof", path: "/tmp/cpu.top.txt"},
				{profile: "/tmp/server-cpu.pprof", path: "/tmp/server-cpu.top.txt"},
			},
			resultFile:      "/tmp/scyllaload.result.json",
			runMetadataFile: "/tmp/scyllaload.metadata.json",
		}, runner)
	}()

	<-started
	cancel()
	close(release)

	result := <-resultCh
	require.Equal(t, int64(1), result.Completed)
	require.Equal(t, int64(2), result.Failed)
	require.Equal(t, int64(3), result.Requests)
	require.Equal(t, 3, result.Workflows)
	require.Equal(t, "/tmp/cpu.pprof", result.CPUProfile)
	require.Equal(t, "/tmp/heap.pprof", result.HeapProfile)
	require.Equal(t, "/tmp/server-cpu.pprof", result.ServerCPUProfile)
	require.Equal(t, "/tmp/server-heap.pprof", result.ServerHeapProfile)
	require.Equal(t, []string{"/tmp/temporal.before.metrics"}, result.MetricSnapshotsBefore)
	require.Equal(t, []string{"/tmp/temporal.after.metrics", "/tmp/scylla.after.metrics"}, result.MetricSnapshotsAfter)
	require.Equal(t, []string{"/tmp/cpu.top.txt", "/tmp/server-cpu.top.txt"}, result.ProfileSummaries)
	require.Equal(t, "/tmp/scyllaload.result.json", result.ResultFile)
	require.Equal(t, "/tmp/scyllaload.metadata.json", result.RunMetadataFile)
}

func TestRunLoadReportsFrontendRequestThroughput(t *testing.T) {
	runner := func(context.Context, client.Client, runConfig, []byte, int64, int) workflowRunResult {
		return workflowRunResult{
			completed: true,
			requests:  2,
		}
	}

	result := runLoadWithRunner(t.Context(), nil, runConfig{
		workflows:   3,
		concurrency: 2,
	}, runner)

	require.Equal(t, int64(3), result.Completed)
	require.Equal(t, int64(0), result.Failed)
	require.Equal(t, int64(6), result.Requests)
	require.Positive(t, result.WorkflowsPerSec)
	require.Positive(t, result.RequestsPerSec)
	require.GreaterOrEqual(t, result.RequestsPerSec, result.WorkflowsPerSec)
}

func TestRunLoadDistributesWorkflowsAcrossTaskQueues(t *testing.T) {
	taskQueueCounts := make(map[string]int)
	runner := func(_ context.Context, _ client.Client, cfg runConfig, _ []byte, _ int64, workflowIndex int) workflowRunResult {
		taskQueueCounts[taskQueueName(cfg, workflowIndex%cfg.taskQueues)]++
		return workflowRunResult{
			completed: true,
			requests:  1,
		}
	}

	result := runLoadWithRunner(t.Context(), nil, runConfig{
		taskQueue:           "load-task-queue",
		taskQueues:          4,
		workersPerTaskQueue: 8,
		workflows:           8,
		concurrency:         1,
		activitiesEach:      1,
	}, runner)

	require.Equal(t, int64(8), result.Completed)
	require.Equal(t, 4, result.TaskQueues)
	require.Equal(t, 8, result.WorkersPerTaskQueue)
	require.Equal(t, map[string]int{
		"load-task-queue-0": 2,
		"load-task-queue-1": 2,
		"load-task-queue-2": 2,
		"load-task-queue-3": 2,
	}, taskQueueCounts)
}

func TestRunBacklogLoadStartsWorkersAfterTasksArePersisted(t *testing.T) {
	var starts atomic.Int64
	var workersStarted atomic.Bool
	starter := func(context.Context, client.Client, runConfig, []byte, int64, int) (client.WorkflowRun, error) {
		if workersStarted.Load() {
			return nil, errors.New("workers started before enqueue completed")
		}
		starts.Add(1)
		return &fakeWorkflowRun{
			id: "workflow",
			getErr: func() error {
				if !workersStarted.Load() {
					return errors.New("workflow drained before workers started")
				}
				return nil
			},
		}, nil
	}
	waiter := func(_ context.Context, _ client.Client, _ runConfig, expected int64) (int64, error) {
		if workersStarted.Load() || expected != 4 || starts.Load() != 4 {
			return 0, errors.New("backlog wait ran before enqueue completed")
		}
		return expected, nil
	}
	startWorkers := func(client.Client, runConfig) ([]worker.Worker, error) {
		workersStarted.Store(true)
		return nil, nil
	}

	result, workers, err := runBacklogLoadWithDependencies(t.Context(), nil, runConfig{
		taskQueue:            "load-task-queue",
		taskQueues:           2,
		workersPerTaskQueue:  8,
		workflows:            4,
		concurrency:          2,
		backlogBeforeWorkers: true,
	}, starter, waiter, startWorkers)

	require.NoError(t, err)
	require.Nil(t, workers)
	require.Equal(t, int64(4), result.Enqueued)
	require.Equal(t, int64(4), result.BacklogTasks)
	require.Equal(t, int64(4), result.Completed)
	require.Zero(t, result.EnqueueFailed)
	require.Zero(t, result.DrainFailed)
	require.Zero(t, result.Failed)
	require.Positive(t, result.EnqueueRequestsPerSec)
	require.Positive(t, result.WorkerStartElapsed)
	require.Positive(t, result.DrainWorkflowsPerSec)
}

func TestRunBacklogLoadIncludesWorkerStartupInDrainElapsed(t *testing.T) {
	starter := func(context.Context, client.Client, runConfig, []byte, int64, int) (client.WorkflowRun, error) {
		return &fakeWorkflowRun{
			id:     "workflow",
			getErr: func() error { return nil },
		}, nil
	}
	waiter := func(_ context.Context, _ client.Client, _ runConfig, expected int64) (int64, error) {
		return expected, nil
	}
	workerStart := make(chan struct{})
	releaseWorkerStart := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseWorkerStart:
		default:
			close(releaseWorkerStart)
		}
	})
	startWorkers := func(client.Client, runConfig) ([]worker.Worker, error) {
		close(workerStart)
		<-releaseWorkerStart
		return nil, nil
	}
	type backlogLoadOutcome struct {
		result runResult
		err    error
	}
	resultCh := make(chan backlogLoadOutcome, 1)
	go func() {
		result, _, err := runBacklogLoadWithDependencies(t.Context(), nil, runConfig{
			workflows:            1,
			concurrency:          1,
			backlogBeforeWorkers: true,
		}, starter, waiter, startWorkers)
		resultCh <- backlogLoadOutcome{result: result, err: err}
	}()

	<-workerStart
	workerStartBlockedAt := time.Now()
	require.Eventually(t, func() bool {
		return time.Since(workerStartBlockedAt) >= 50*time.Millisecond
	}, time.Second, time.Millisecond)
	close(releaseWorkerStart)
	outcome := <-resultCh

	require.NoError(t, outcome.err)
	require.GreaterOrEqual(t, outcome.result.DrainElapsed, outcome.result.WorkerStartElapsed)
}

func TestRunBacklogLoadReportsEnqueueAndDrainFailures(t *testing.T) {
	starter := func(_ context.Context, _ client.Client, _ runConfig, _ []byte, _ int64, index int) (client.WorkflowRun, error) {
		if index == 0 {
			return nil, errors.New("enqueue failed")
		}
		return &fakeWorkflowRun{
			id: "workflow",
			getErr: func() error {
				if index == 2 {
					return errors.New("drain failed")
				}
				return nil
			},
		}, nil
	}
	waiter := func(_ context.Context, _ client.Client, _ runConfig, expected int64) (int64, error) {
		return expected, nil
	}
	startWorkers := func(client.Client, runConfig) ([]worker.Worker, error) {
		return nil, nil
	}

	result, _, err := runBacklogLoadWithDependencies(t.Context(), nil, runConfig{
		workflows:            3,
		concurrency:          2,
		backlogBeforeWorkers: true,
	}, starter, waiter, startWorkers)

	require.NoError(t, err)
	require.Equal(t, int64(2), result.Enqueued)
	require.Equal(t, int64(1), result.EnqueueFailed)
	require.Equal(t, int64(1), result.Completed)
	require.Equal(t, int64(1), result.DrainFailed)
	require.Equal(t, int64(2), result.Failed)
}

func TestWorkflowTaskBacklogCountUsesReportedStats(t *testing.T) {
	taskQueueNames := []string{"load-task-queue-0", "load-task-queue-1"}
	backlogCounts := []int64{3, 4}
	call := 0
	service := &fakeWorkflowServiceClient{
		describeTaskQueue: func(request *workflowservice.DescribeTaskQueueRequest) (*workflowservice.DescribeTaskQueueResponse, error) {
			require.Less(t, call, len(taskQueueNames))
			require.Equal(t, "load-test", request.GetNamespace())
			require.Equal(t, taskQueueNames[call], request.GetTaskQueue().GetName())
			require.Equal(t, enumspb.TASK_QUEUE_KIND_NORMAL, request.GetTaskQueue().GetKind())
			require.Equal(t, enumspb.TASK_QUEUE_TYPE_WORKFLOW, request.GetTaskQueueType())
			require.True(t, request.GetReportStats())

			response := &workflowservice.DescribeTaskQueueResponse{
				Stats: &taskqueuepb.TaskQueueStats{
					ApproximateBacklogCount: backlogCounts[call],
				},
			}
			call++
			return response, nil
		},
	}
	c := &clientWithWorkflowService{workflowService: service}

	backlog, err := workflowTaskBacklogCount(t.Context(), c, runConfig{
		namespace:  "load-test",
		taskQueue:  "load-task-queue",
		taskQueues: 2,
	})

	require.NoError(t, err)
	require.Equal(t, len(taskQueueNames), call)
	require.Equal(t, int64(7), backlog)
}

func TestServerProfilesFetchPPROFEndpoints(t *testing.T) {
	var profileCalled bool
	var heapCalled bool
	var profileSeconds string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/pprof/profile":
			profileCalled = true
			profileSeconds = r.URL.Query().Get("seconds")
			_, _ = w.Write([]byte("cpu profile"))
		case "/debug/pprof/heap":
			heapCalled = true
			_, _ = w.Write([]byte("heap profile"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	cpuPath := dir + "/cpu.pprof"
	heapPath := dir + "/heap.pprof"
	cfg := runConfig{
		serverPProf:   server.URL,
		serverCPU:     cpuPath,
		serverHeap:    heapPath,
		serverCPUTime: 2 * time.Second,
	}

	waitCPU, err := startServerCPUProfile(t.Context(), cfg)
	require.NoError(t, err)
	require.NoError(t, waitCPU())
	require.NoError(t, writeServerHeapProfile(t.Context(), cfg))

	cpuBytes, err := os.ReadFile(cpuPath)
	require.NoError(t, err)
	require.Equal(t, "cpu profile", string(cpuBytes))
	heapBytes, err := os.ReadFile(heapPath)
	require.NoError(t, err)
	require.Equal(t, "heap profile", string(heapBytes))
	require.True(t, profileCalled)
	require.Equal(t, "2", profileSeconds)
	require.True(t, heapCalled)
}

func TestValidateConfigRequiresServerPPROFForServerProfiles(t *testing.T) {
	err := validateConfig(runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		serverCPU:           "/tmp/server-cpu.pprof",
		serverCPUTime:       time.Second,
	})
	require.ErrorContains(t, err, "-server-pprof")

	err = validateConfig(runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		serverHeap:          "/tmp/server-heap.pprof",
		serverCPUTime:       time.Second,
	})
	require.ErrorContains(t, err, "-server-pprof")
}

func TestMetricSnapshotFlags(t *testing.T) {
	var snapshots metricSnapshotFlags

	require.NoError(t, snapshots.Set("http://temporal:8000/metrics=/tmp/temporal.metrics"))
	require.NoError(t, snapshots.Set("http://scylla:9180/metrics=/tmp/scylla.metrics"))

	require.Equal(t, "http://temporal:8000/metrics=/tmp/temporal.metrics,http://scylla:9180/metrics=/tmp/scylla.metrics", snapshots.String())
	require.Equal(t, []string{"/tmp/temporal.metrics", "/tmp/scylla.metrics"}, snapshots.paths())
	require.Error(t, snapshots.Set("missing-output-path"))
}

func TestProfileSummaryFlags(t *testing.T) {
	var summaries profileSummaryFlags

	require.NoError(t, summaries.Set("/tmp/cpu.pprof=/tmp/cpu.top.txt"))
	require.NoError(t, summaries.Set("/tmp/server-cpu.pprof=/tmp/server-cpu.top.txt"))

	require.Equal(t, "/tmp/cpu.pprof=/tmp/cpu.top.txt,/tmp/server-cpu.pprof=/tmp/server-cpu.top.txt", summaries.String())
	require.Equal(t, []string{"/tmp/cpu.top.txt", "/tmp/server-cpu.top.txt"}, summaries.paths())
	require.Error(t, summaries.Set("missing-output-path"))
}

func TestWriteMetricSnapshots(t *testing.T) {
	metricBody := "temporal_persistence_requests 1\n"
	var requestedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		_, _ = w.Write([]byte(metricBody))
	}))
	defer server.Close()

	outputPath := t.TempDir() + "/metrics.prom"
	err := writeMetricSnapshots(t.Context(), []metricSnapshot{
		{url: server.URL + "/metrics", path: outputPath},
	})
	require.NoError(t, err)

	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Equal(t, metricBody, string(data))
	require.Equal(t, "/metrics", requestedPath)
}

func TestWriteRunMetadata(t *testing.T) {
	var pprofPath string
	var metricsPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/pprof/":
			pprofPath = r.URL.Path
		case "/metrics":
			metricsPath = r.URL.Path
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	t.Setenv("CASSANDRA_SEEDS", "node1,node2,node3")
	t.Setenv("CASSANDRA_MAX_CONNS", "12")
	t.Setenv("CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE", "2")
	t.Setenv("CASSANDRA_MAX_PREPARED_STMTS", "1000")

	outputPath := t.TempDir() + "/metadata.json"
	require.NoError(t, writeRunMetadata(t.Context(), runConfig{
		address:             "127.0.0.1:7233",
		namespace:           "scylla-load",
		taskQueue:           "scylla-load",
		taskQueues:          5,
		workersPerTaskQueue: 2,
		workflows:           7,
		concurrency:         3,
		activitiesEach:      2,
		signalsEach:         1,
		eagerStart:          true,
		eagerActivities:     true,
		payloadBytes:        512,
		serverPProf:         server.URL,
		runMetadataFile:     outputPath,
		metricSnapshotsBefore: metricSnapshotFlags{
			{url: server.URL + "/metrics", path: "/tmp/temporal.before.metrics"},
		},
	}))

	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var metadata runMetadata
	require.NoError(t, json.Unmarshal(data, &metadata))
	require.False(t, metadata.StartedAt.IsZero())
	metadata.StartedAt = time.Time{}
	require.Equal(t, "12", metadata.Environment["CASSANDRA_MAX_CONNS"])
	require.Equal(t, "2", metadata.Environment["CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE"])
	require.Equal(t, "1000", metadata.Environment["CASSANDRA_MAX_PREPARED_STMTS"])
	require.Equal(t, "node1,node2,node3", metadata.Environment["CASSANDRA_SEEDS"])
	metadata.Environment = nil
	require.Equal(t, runMetadata{
		Address:             "127.0.0.1:7233",
		Namespace:           "scylla-load",
		TaskQueue:           "scylla-load",
		TaskQueues:          5,
		WorkersPerTaskQueue: 2,
		Workflows:           7,
		Concurrency:         3,
		ActivitiesEach:      2,
		SignalsEach:         1,
		EagerStart:          true,
		EagerActivities:     true,
		PayloadBytes:        512,
		GoVersion:           runtime.Version(),
		GOOS:                runtime.GOOS,
		GOARCH:              runtime.GOARCH,
		NumCPU:              runtime.NumCPU(),
		GOMAXPROCS:          runtime.GOMAXPROCS(0),
		PProfEndpoint: &endpointCheck{
			Name:   "pprof",
			URL:    server.URL + "/debug/pprof/",
			Status: "200 OK",
		},
		MetricSnapshotEndpoints: []endpointCheck{
			{
				Name:   "metrics",
				URL:    server.URL + "/metrics",
				Status: "200 OK",
			},
		},
	}, metadata)
	require.Equal(t, "/debug/pprof/", pprofPath)
	require.Equal(t, "/metrics", metricsPath)
}

func TestWriteProfileSummaries(t *testing.T) {
	dir := t.TempDir()
	fakeGo := dir + "/go"
	argsPath := dir + "/args.txt"
	err := os.WriteFile(fakeGo, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > "+argsPath+"\nprintf 'flat flat%% sum%% cum cum%% name\\n10ms 100%% 100%% 10ms 100%% test.hot\\n'\n"), 0o755)
	require.NoError(t, err)

	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+oldPath)

	summaryPath := dir + "/cpu.top.txt"
	require.NoError(t, writeProfileSummaries(t.Context(), []profileSummary{
		{profile: "/tmp/cpu.pprof", path: summaryPath},
	}))

	args, err := os.ReadFile(argsPath)
	require.NoError(t, err)
	require.Equal(t, "tool\npprof\n-top\n/tmp/cpu.pprof\n", string(args))
	summary, err := os.ReadFile(summaryPath)
	require.NoError(t, err)
	require.Contains(t, string(summary), "test.hot")
}

func TestWriteResultFile(t *testing.T) {
	resultPath := t.TempDir() + "/result.json"
	result := runResult{
		Address:   "127.0.0.1:7233",
		Namespace: "scylla-load",
		Workflows: 7,
		PreparedStats: &prepStats{
			MetricEndpoints:            3,
			PrepareRequests:            4,
			ReprepareAttempts:          1,
			ClientReprepareAttempts:    1,
			PreparedCacheEntriesBefore: 100,
			PreparedCacheEntriesAfter:  101,
		},
		ResultFile: resultPath,
	}

	require.NoError(t, writeResultFile(resultPath, result))

	data, err := os.ReadFile(resultPath)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"address": "127.0.0.1:7233",
		"namespace": "scylla-load",
		"taskQueue": "",
		"taskQueues": 0,
		"workersPerTaskQueue": 0,
		"workflows": 7,
		"concurrency": 0,
		"activitiesEach": 0,
		"signalsEach": 0,
		"eagerStart": false,
		"eagerActivities": false,
		"payloadBytes": 0,
		"elapsed": 0,
		"completed": 0,
		"failed": 0,
		"workflowsPerSec": 0,
		"requests": 0,
		"requestsPerSec": 0,
		"scyllaPreparedStatements": {
			"metricEndpoints": 3,
			"prepareRequests": 4,
			"reprepareAttempts": 1,
			"clientReprepareAttempts": 1,
			"forwardedReprepareAttempts": 0,
			"statementsParsed": 0,
			"preparedCacheEvictions": 0,
			"oneOffPreparedCacheEvictions": 0,
			"authorizedPreparedCacheEvictions": 0,
			"preparedCacheEntriesBefore": 100,
			"preparedCacheEntriesAfter": 101
		},
		"resultFile": "`+resultPath+`"
	}`, string(data))
}
