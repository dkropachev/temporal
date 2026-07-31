package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync/atomic"
	"syscall"
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

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
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
	executeWorkflow func(
		context.Context,
		client.StartWorkflowOptions,
		any,
		...any,
	) (client.WorkflowRun, error)
	terminateWorkflow func(context.Context, string, string, string, ...any) error
	signalWorkflow    func(context.Context, string, string, string, any) error
}

func (c *clientWithWorkflowService) WorkflowService() workflowservice.WorkflowServiceClient {
	return c.workflowService
}

func (c *clientWithWorkflowService) ExecuteWorkflow(
	ctx context.Context,
	options client.StartWorkflowOptions,
	workflow any,
	args ...any,
) (client.WorkflowRun, error) {
	return c.executeWorkflow(ctx, options, workflow, args...)
}

func (c *clientWithWorkflowService) TerminateWorkflow(
	ctx context.Context,
	workflowID string,
	runID string,
	reason string,
	details ...any,
) error {
	if c.terminateWorkflow == nil {
		return nil
	}
	return c.terminateWorkflow(ctx, workflowID, runID, reason, details...)
}

func (c *clientWithWorkflowService) SignalWorkflow(
	ctx context.Context,
	workflowID string,
	runID string,
	signalName string,
	arg any,
) error {
	if c.signalWorkflow == nil {
		return nil
	}
	return c.signalWorkflow(ctx, workflowID, runID, signalName, arg)
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
		"-target-workflows-per-second=12.5",
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
	require.InDelta(t, 12.5, cfg.targetRPS, 0)
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
		timeout:             time.Second,
		serverCPUTime:       time.Second,
	}

	invalidTaskQueues := cfg
	invalidTaskQueues.taskQueues = 0
	require.ErrorContains(t, validateConfig(invalidTaskQueues), "-task-queues")

	invalidWorkers := cfg
	invalidWorkers.workersPerTaskQueue = 0
	require.ErrorContains(t, validateConfig(invalidWorkers), "-workers-per-task-queue")
}

func TestValidateConfigRequiresPositiveTimeout(t *testing.T) {
	cfg := runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		serverCPUTime:       time.Second,
	}

	require.ErrorContains(t, validateConfig(cfg), "-timeout")
}

func TestValidateConfigRejectsNegativeTargetRate(t *testing.T) {
	cfg := runConfig{
		workflows: 1, concurrency: 1, taskQueues: 1, workersPerTaskQueue: 1,
		targetRPS: -1, timeout: time.Second, serverCPUTime: time.Second,
	}

	require.ErrorContains(t, validateConfig(cfg), "-target-workflows-per-second")
}

func TestWorkflowLatencyPercentiles(t *testing.T) {
	p50, p95, p99 := workflowLatencyPercentiles([]time.Duration{
		10 * time.Millisecond,
		time.Millisecond,
		5 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
	})

	require.Equal(t, 3*time.Millisecond, p50)
	require.Equal(t, 10*time.Millisecond, p95)
	require.Equal(t, 10*time.Millisecond, p99)
}

func TestWaitForLaunchSlotHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := waitForLaunchSlot(ctx, time.Now(), 1, 1)

	require.ErrorIs(t, err, context.Canceled)
}

func TestValidateConfigRequiresWorkerForEagerStart(t *testing.T) {
	cfg := runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		eagerStart:          true,
		timeout:             time.Second,
	}

	err := validateConfig(cfg)

	require.ErrorContains(t, err, "-worker must be enabled when -eager-start is set")
}

func TestValidateConfigRequiresExactServerCPUProfileDuration(t *testing.T) {
	cfg := runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		timeout:             time.Second,
		serverPProf:         "http://127.0.0.1:7936",
		serverCPU:           "/tmp/server.pprof",
		serverCPUTime:       1500 * time.Millisecond,
	}

	err := validateConfig(cfg)

	require.ErrorContains(t, err, "whole number of seconds")
	cfg.serverCPUTime = 2 * time.Second
	require.NoError(t, validateConfig(cfg))
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
		timeout:              time.Second,
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

func TestValidateConfigRejectsOutputPathCollisions(t *testing.T) {
	cfg := runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		timeout:             time.Second,
		serverCPUTime:       time.Second,
		metricSnapshotsBefore: metricSnapshotFlags{
			{url: "http://node/metrics", path: "/tmp/scyllaload.metrics"},
		},
		metricSnapshotsAfter: metricSnapshotFlags{
			{url: "http://node/metrics", path: "/tmp/../tmp/scyllaload.metrics"},
		},
	}

	err := validateConfig(cfg)

	require.ErrorContains(t, err, "same output path")
	require.ErrorContains(t, err, "-metrics-snapshot-before[0]")
	require.ErrorContains(t, err, "-metrics-snapshot-after[0]")
}

func TestValidateConfigRejectsProfileSummaryInputOutputCollision(t *testing.T) {
	dir := t.TempDir()
	cfg := runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		timeout:             time.Second,
		serverCPUTime:       time.Second,
		profileSummaries: profileSummaryFlags{
			{
				profile: dir + "/cpu.pprof",
				path:    dir + "/profiles/../cpu.pprof",
			},
		},
	}

	err := validateConfig(cfg)

	require.ErrorContains(t, err, "profile input")
	require.ErrorContains(t, err, "-profile-summary[0]")
	require.ErrorContains(t, err, "same path")
}

func TestValidateConfigRejectsProfileInputCollisionWithNonProfileOutput(t *testing.T) {
	dir := t.TempDir()
	resultPath := filepath.Join(dir, "result.json")
	cfg := runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		timeout:             time.Second,
		serverCPUTime:       time.Second,
		resultFile:          resultPath,
		profileSummaries: profileSummaryFlags{{
			profile: resultPath,
			path:    filepath.Join(dir, "result.top.txt"),
		}},
	}

	err := validateConfig(cfg)

	require.ErrorContains(t, err, "profile input")
	require.ErrorContains(t, err, "-result-file")
}

func TestValidateConfigAllowsGeneratedProfileAsSummaryInput(t *testing.T) {
	dir := t.TempDir()
	profilePath := filepath.Join(dir, "cpu.pprof")
	cfg := runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		timeout:             time.Second,
		serverCPUTime:       time.Second,
		cpuProfile:          profilePath,
		profileSummaries: profileSummaryFlags{{
			profile: profilePath,
			path:    filepath.Join(dir, "cpu.top.txt"),
		}},
	}

	require.NoError(t, validateConfig(cfg))
}

func TestValidateConfigRejectsDuplicateMetricSnapshotURLs(t *testing.T) {
	cfg := runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		timeout:             time.Second,
		serverCPUTime:       time.Second,
		metricSnapshotsAfter: metricSnapshotFlags{
			{url: "http://node/metrics", path: "/tmp/scyllaload.metrics-1"},
			{url: "http://node/metrics", path: "/tmp/scyllaload.metrics-2"},
		},
	}

	err := validateConfig(cfg)

	require.ErrorContains(t, err, "duplicates URL")
	require.ErrorContains(t, err, "-metrics-snapshot-after[1]")
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

func TestStartOneWorkflowBoundsExecutionLifetime(t *testing.T) {
	const startNanos = int64(123)
	var options client.StartWorkflowOptions
	c := &clientWithWorkflowService{
		executeWorkflow: func(
			_ context.Context,
			startOptions client.StartWorkflowOptions,
			_ any,
			_ ...any,
		) (client.WorkflowRun, error) {
			options = startOptions
			return &fakeWorkflowRun{
				id:     startOptions.ID,
				getErr: func() error { return nil },
			}, nil
		},
	}
	cfg := runConfig{
		taskQueue:  "load-task-queue",
		taskQueues: 2,
		timeout:    5 * time.Minute,
	}

	_, err := startOneWorkflow(t.Context(), c, cfg, []byte("payload"), startNanos, 3)

	require.NoError(t, err)
	require.Equal(t, workflowID(startNanos, 3), options.ID)
	require.Equal(t, "load-task-queue-1", options.TaskQueue)
	require.Equal(t, cfg.timeout, options.WorkflowExecutionTimeout)
}

func TestStartOneWorkflowUsesRemainingRunDeadline(t *testing.T) {
	var options client.StartWorkflowOptions
	c := &clientWithWorkflowService{
		executeWorkflow: func(
			_ context.Context,
			startOptions client.StartWorkflowOptions,
			_ any,
			_ ...any,
		) (client.WorkflowRun, error) {
			options = startOptions
			return &fakeWorkflowRun{
				id:     startOptions.ID,
				getErr: func() error { return nil },
			}, nil
		},
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	_, err := startOneWorkflow(ctx, c, runConfig{
		taskQueue:  "load-task-queue",
		taskQueues: 1,
		timeout:    5 * time.Minute,
	}, nil, 123, 0)

	require.NoError(t, err)
	require.Positive(t, options.WorkflowExecutionTimeout)
	require.LessOrEqual(t, options.WorkflowExecutionTimeout, time.Second)
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

	type outcome struct {
		result runResult
		err    error
	}
	resultCh := make(chan outcome, 1)
	go func() {
		result, err := runLoadWithRunner(ctx, nil, runConfig{
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
		resultCh <- outcome{result: result, err: err}
	}()

	<-started
	cancel()
	close(release)

	loadOutcome := <-resultCh
	require.NoError(t, loadOutcome.err)
	result := loadOutcome.result
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

func TestRunLoadTerminatesWorkflowAfterCanceledOperation(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		signalsEach int
		cancelOnGet bool
	}{
		{
			name:        "signal",
			signalsEach: 1,
		},
		{
			name:        "get",
			cancelOnGet: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var terminateContextErr error
			var terminateDeadline time.Time
			var terminatedID string
			var terminatedRunID string
			var terminationReason string
			c := &clientWithWorkflowService{
				executeWorkflow: func(
					context.Context,
					client.StartWorkflowOptions,
					any,
					...any,
				) (client.WorkflowRun, error) {
					return &fakeWorkflowRun{
						id: "workflow",
						getErr: func() error {
							if testCase.cancelOnGet {
								cancel()
							}
							return ctx.Err()
						},
					}, nil
				},
				signalWorkflow: func(context.Context, string, string, string, any) error {
					cancel()
					return ctx.Err()
				},
				terminateWorkflow: func(
					terminationCtx context.Context,
					workflowID string,
					runID string,
					reason string,
					_ ...any,
				) error {
					terminateContextErr = terminationCtx.Err()
					terminateDeadline, _ = terminationCtx.Deadline()
					terminatedID = workflowID
					terminatedRunID = runID
					terminationReason = reason
					return nil
				},
			}

			result, err := runLoad(ctx, c, runConfig{
				taskQueue:   "load-task-queue",
				taskQueues:  1,
				workflows:   1,
				concurrency: 1,
				signalsEach: testCase.signalsEach,
				timeout:     time.Minute,
			})

			require.NoError(t, err)
			require.Zero(t, result.Completed)
			require.Equal(t, int64(1), result.Failed)
			require.NoError(t, terminateContextErr)
			require.WithinDuration(t, time.Now().Add(postRunCleanupTimeout), terminateDeadline, time.Second)
			require.Equal(t, "workflow", terminatedID)
			require.Equal(t, "run-id", terminatedRunID)
			require.Equal(t, incompleteWorkflowTerminationReason, terminationReason)
		})
	}
}

func TestRunLoadReportsWorkflowTerminationFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	terminationErr := errors.New("termination failed")
	c := &clientWithWorkflowService{
		executeWorkflow: func(
			context.Context,
			client.StartWorkflowOptions,
			any,
			...any,
		) (client.WorkflowRun, error) {
			return &fakeWorkflowRun{
				id: "workflow",
				getErr: func() error {
					cancel()
					return ctx.Err()
				},
			}, nil
		},
		terminateWorkflow: func(context.Context, string, string, string, ...any) error {
			return terminationErr
		},
	}

	_, err := runLoad(ctx, c, runConfig{
		taskQueue:   "load-task-queue",
		taskQueues:  1,
		workflows:   1,
		concurrency: 1,
		timeout:     time.Minute,
	})

	require.ErrorIs(t, err, terminationErr)
	require.ErrorContains(t, err, "terminate incomplete workflows")
}

func TestLoadSignalsCancelAndTerminateRunningWorkflows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("sending console signals to the current process is unsupported on Windows")
	}
	for _, testCase := range []struct {
		name   string
		signal os.Signal
	}{
		{name: "interrupt", signal: os.Interrupt},
		{name: "terminate", signal: syscall.SIGTERM},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, stopSignals := loadSignalContext(t.Context())
			defer stopSignals()
			getStarted := make(chan struct{})
			var terminated atomic.Int64
			c := &clientWithWorkflowService{
				executeWorkflow: func(
					context.Context,
					client.StartWorkflowOptions,
					any,
					...any,
				) (client.WorkflowRun, error) {
					return &fakeWorkflowRun{
						id: "workflow",
						getErr: func() error {
							close(getStarted)
							<-ctx.Done()
							return ctx.Err()
						},
					}, nil
				},
				terminateWorkflow: func(context.Context, string, string, string, ...any) error {
					terminated.Add(1)
					return nil
				},
			}
			type outcome struct {
				result runResult
				err    error
			}
			outcomeCh := make(chan outcome, 1)
			go func() {
				result, err := runLoad(ctx, c, runConfig{
					taskQueue:   "load-task-queue",
					taskQueues:  1,
					workflows:   1,
					concurrency: 1,
					timeout:     time.Minute,
				})
				outcomeCh <- outcome{result: result, err: err}
			}()

			<-getStarted
			process, err := os.FindProcess(os.Getpid())
			require.NoError(t, err)
			require.NoError(t, process.Signal(testCase.signal))
			loadOutcome := <-outcomeCh

			require.NoError(t, loadOutcome.err)
			require.Zero(t, loadOutcome.result.Completed)
			require.Equal(t, int64(1), loadOutcome.result.Failed)
			require.Equal(t, int64(1), terminated.Load())
		})
	}
}

func TestTerminateIncompleteWorkflowRunsBoundsConcurrency(t *testing.T) {
	const concurrency = 2
	runs := make([]client.WorkflowRun, 4)
	for index := range runs {
		runs[index] = &fakeWorkflowRun{
			id:     fmt.Sprintf("workflow-%d", index),
			getErr: func() error { return nil },
		}
	}
	release := make(chan struct{})
	entered := make(chan struct{}, len(runs))
	var active atomic.Int64
	var maximum atomic.Int64
	c := &clientWithWorkflowService{
		terminateWorkflow: func(context.Context, string, string, string, ...any) error {
			current := active.Add(1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			active.Add(-1)
			return nil
		},
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- terminateIncompleteWorkflowRuns(t.Context(), c, runs, concurrency)
	}()

	for range concurrency {
		<-entered
	}
	require.Never(t, func() bool {
		return active.Load() > concurrency
	}, 50*time.Millisecond, time.Millisecond)
	close(release)

	require.NoError(t, <-errCh)
	require.Equal(t, int64(concurrency), maximum.Load())
}

func TestTerminateIncompleteWorkflowRunsSkipsNilRuns(t *testing.T) {
	runs := []client.WorkflowRun{
		nil,
		&fakeWorkflowRun{id: "workflow-1", getErr: func() error { return nil }},
		nil,
		&fakeWorkflowRun{id: "workflow-2", getErr: func() error { return nil }},
	}
	terminated := make(chan string, len(runs))
	c := &clientWithWorkflowService{
		terminateWorkflow: func(
			_ context.Context,
			workflowID string,
			_ string,
			_ string,
			_ ...any,
		) error {
			terminated <- workflowID
			return nil
		},
	}

	err := terminateIncompleteWorkflowRuns(t.Context(), c, runs, len(runs))

	require.NoError(t, err)
	close(terminated)
	var workflowIDs []string
	for workflowID := range terminated {
		workflowIDs = append(workflowIDs, workflowID)
	}
	require.ElementsMatch(t, []string{"workflow-1", "workflow-2"}, workflowIDs)
}

func TestRunLoadReportsFrontendRequestThroughput(t *testing.T) {
	runner := func(context.Context, client.Client, runConfig, []byte, int64, int) workflowRunResult {
		return workflowRunResult{
			completed: true,
			requests:  2,
		}
	}

	result, err := runLoadWithRunner(t.Context(), nil, runConfig{
		workflows:   3,
		concurrency: 2,
	}, runner)

	require.NoError(t, err)
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

	result, err := runLoadWithRunner(t.Context(), nil, runConfig{
		taskQueue:           "load-task-queue",
		taskQueues:          4,
		workersPerTaskQueue: 8,
		workflows:           8,
		concurrency:         1,
		activitiesEach:      1,
	}, runner)

	require.NoError(t, err)
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
	counter := func(context.Context, client.Client, runConfig) ([]int64, error) {
		return []int64{0, 0}, nil
	}
	waiter := func(
		_ context.Context,
		_ client.Client,
		_ runConfig,
		baseline []int64,
		expected []int64,
	) (int64, error) {
		if workersStarted.Load() ||
			!slices.Equal(baseline, []int64{0, 0}) ||
			!slices.Equal(expected, []int64{2, 2}) ||
			starts.Load() != 4 {
			return 0, errors.New("backlog wait ran before enqueue completed")
		}
		return sumInt64(expected), nil
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
	}, starter, counter, waiter, startWorkers)

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

func TestRunBacklogLoadRejectsNonemptyBaseline(t *testing.T) {
	var starterCalled atomic.Bool
	var waiterCalled atomic.Bool
	var workerCalled atomic.Bool
	starter := func(context.Context, client.Client, runConfig, []byte, int64, int) (client.WorkflowRun, error) {
		starterCalled.Store(true)
		return nil, nil
	}
	counter := func(context.Context, client.Client, runConfig) ([]int64, error) {
		return []int64{0, 3}, nil
	}
	waiter := func(
		context.Context,
		client.Client,
		runConfig,
		[]int64,
		[]int64,
	) (int64, error) {
		waiterCalled.Store(true)
		return 0, nil
	}
	startWorkers := func(client.Client, runConfig) ([]worker.Worker, error) {
		workerCalled.Store(true)
		return nil, nil
	}

	result, workers, err := runBacklogLoadWithDependencies(t.Context(), nil, runConfig{
		taskQueues:           2,
		workflows:            4,
		concurrency:          2,
		backlogBeforeWorkers: true,
	}, starter, counter, waiter, startWorkers)

	require.ErrorContains(t, err, "backlog baseline must be empty")
	require.ErrorContains(t, err, "1=3")
	require.Nil(t, workers)
	require.Zero(t, result.Enqueued)
	require.Equal(t, int64(4), result.Failed)
	require.False(t, starterCalled.Load())
	require.False(t, waiterCalled.Load())
	require.False(t, workerCalled.Load())
}

func TestRunBacklogLoadIncludesWorkerStartupInDrainElapsed(t *testing.T) {
	starter := func(context.Context, client.Client, runConfig, []byte, int64, int) (client.WorkflowRun, error) {
		return &fakeWorkflowRun{
			id:     "workflow",
			getErr: func() error { return nil },
		}, nil
	}
	counter := func(context.Context, client.Client, runConfig) ([]int64, error) {
		return []int64{0}, nil
	}
	waiter := func(
		_ context.Context,
		_ client.Client,
		_ runConfig,
		_ []int64,
		expected []int64,
	) (int64, error) {
		return sumInt64(expected), nil
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
			taskQueues:           1,
			workflows:            1,
			concurrency:          1,
			backlogBeforeWorkers: true,
		}, starter, counter, waiter, startWorkers)
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
	counter := func(context.Context, client.Client, runConfig) ([]int64, error) {
		return []int64{0}, nil
	}
	waiter := func(
		_ context.Context,
		_ client.Client,
		_ runConfig,
		_ []int64,
		expected []int64,
	) (int64, error) {
		return sumInt64(expected), nil
	}
	startWorkers := func(client.Client, runConfig) ([]worker.Worker, error) {
		return nil, nil
	}
	var terminated atomic.Int64
	c := &clientWithWorkflowService{
		terminateWorkflow: func(context.Context, string, string, string, ...any) error {
			terminated.Add(1)
			return nil
		},
	}

	result, _, err := runBacklogLoadWithDependencies(t.Context(), c, runConfig{
		taskQueues:           1,
		workflows:            3,
		concurrency:          2,
		backlogBeforeWorkers: true,
	}, starter, counter, waiter, startWorkers)

	require.NoError(t, err)
	require.Equal(t, int64(2), result.Enqueued)
	require.Equal(t, int64(1), result.EnqueueFailed)
	require.Equal(t, int64(1), result.Completed)
	require.Equal(t, int64(1), result.DrainFailed)
	require.Equal(t, int64(2), result.Failed)
	require.Equal(t, int64(1), terminated.Load())
}

func TestRunBacklogLoadTerminatesRunsBeforeDrain(t *testing.T) {
	for _, stage := range []string{"backlog wait", "worker start"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			triggerErr := errors.New(stage + " failed")
			terminationErr := errors.New("termination failed")
			var terminated atomic.Int64
			var canceledTerminationContext atomic.Bool
			var wrongTerminationReason atomic.Bool
			c := &clientWithWorkflowService{
				terminateWorkflow: func(
					terminationCtx context.Context,
					_ string,
					_ string,
					reason string,
					_ ...any,
				) error {
					if terminationCtx.Err() != nil {
						canceledTerminationContext.Store(true)
					}
					if reason != incompleteWorkflowTerminationReason {
						wrongTerminationReason.Store(true)
					}
					terminated.Add(1)
					return terminationErr
				},
			}
			starter := func(
				context.Context,
				client.Client,
				runConfig,
				[]byte,
				int64,
				int,
			) (client.WorkflowRun, error) {
				return &fakeWorkflowRun{
					id:     "workflow",
					getErr: func() error { return nil },
				}, nil
			}
			counter := func(context.Context, client.Client, runConfig) ([]int64, error) {
				return []int64{0}, nil
			}
			waiter := func(
				context.Context,
				client.Client,
				runConfig,
				[]int64,
				[]int64,
			) (int64, error) {
				if stage == "backlog wait" {
					cancel()
					return 0, triggerErr
				}
				return 2, nil
			}
			startWorkers := func(client.Client, runConfig) ([]worker.Worker, error) {
				cancel()
				return nil, triggerErr
			}

			result, workers, err := runBacklogLoadWithDependencies(ctx, c, runConfig{
				taskQueues:           1,
				workflows:            2,
				concurrency:          2,
				backlogBeforeWorkers: true,
			}, starter, counter, waiter, startWorkers)

			require.ErrorIs(t, err, triggerErr)
			require.ErrorIs(t, err, terminationErr)
			require.Nil(t, workers)
			require.Equal(t, int64(2), result.Enqueued)
			require.Zero(t, result.Completed)
			require.Equal(t, int64(2), result.Failed)
			require.Equal(t, int64(2), terminated.Load())
			require.False(t, canceledTerminationContext.Load())
			require.False(t, wrongTerminationReason.Load())
		})
	}
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

	backlog, err := workflowTaskBacklogCounts(t.Context(), c, runConfig{
		namespace:  "load-test",
		taskQueue:  "load-task-queue",
		taskQueues: 2,
	})

	require.NoError(t, err)
	require.Equal(t, len(taskQueueNames), call)
	require.Equal(t, []int64{3, 4}, backlog)
}

func TestConfirmedWorkflowTaskBacklogRequiresEachQueueDelta(t *testing.T) {
	baseline := []int64{100, 0}
	expected := []int64{1, 1}

	require.Equal(t, int64(1), confirmedWorkflowTaskBacklog(
		baseline,
		expected,
		[]int64{102, 0},
	))
	require.Equal(t, int64(2), confirmedWorkflowTaskBacklog(
		baseline,
		expected,
		[]int64{101, 1},
	))
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

func TestRunWithClientOmitsFailedProfileArtifactsFromResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "profile failed", http.StatusInternalServerError)
	}))
	defer server.Close()
	c := &clientWithWorkflowService{
		executeWorkflow: func(
			context.Context,
			client.StartWorkflowOptions,
			any,
			...any,
		) (client.WorkflowRun, error) {
			return &fakeWorkflowRun{
				id:     "workflow",
				getErr: func() error { return nil },
			}, nil
		},
	}
	dir := t.TempDir()
	serverCPUPath := filepath.Join(dir, "server-cpu.pprof")
	serverHeapPath := filepath.Join(dir, "server-heap.pprof")
	summaryPath := filepath.Join(dir, "server-cpu.top.txt")
	resultPath := filepath.Join(dir, "result.json")
	for _, path := range []string{serverCPUPath, serverHeapPath, summaryPath} {
		require.NoError(t, os.WriteFile(path, []byte("existing"), 0o600))
	}

	err := runWithClient(t.Context(), c, runConfig{
		namespace:           "load-test",
		taskQueue:           "load-task-queue",
		taskQueues:          1,
		workersPerTaskQueue: 1,
		workflows:           1,
		concurrency:         1,
		timeout:             time.Minute,
		serverPProf:         server.URL,
		serverCPU:           serverCPUPath,
		serverHeap:          serverHeapPath,
		serverCPUTime:       time.Second,
		profileSummaries: profileSummaryFlags{{
			profile: serverCPUPath,
			path:    summaryPath,
		}},
		resultFile: resultPath,
	})

	require.ErrorContains(t, err, "write server CPU profile")
	require.ErrorContains(t, err, "write server heap profile")
	resultBytes, readErr := os.ReadFile(resultPath)
	require.NoError(t, readErr)
	var result runResult
	require.NoError(t, json.Unmarshal(resultBytes, &result))
	require.Empty(t, result.ServerCPUProfile)
	require.Zero(t, result.ServerCPUProfileDuration)
	require.Empty(t, result.ServerHeapProfile)
	require.Nil(t, result.ProfileSummaries)
	for _, path := range []string{serverCPUPath, serverHeapPath, summaryPath} {
		data, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.Equal(t, "existing", string(data))
	}
}

func TestRunWithClientUnwindsProfilesOnError(t *testing.T) {
	profileStarted := make(chan struct{})
	profileCanceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(profileStarted)
		<-request.Context().Done()
		close(profileCanceled)
	}))
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
	})

	describeCalls := 0
	service := &fakeWorkflowServiceClient{
		describeTaskQueue: func(*workflowservice.DescribeTaskQueueRequest) (*workflowservice.DescribeTaskQueueResponse, error) {
			describeCalls++
			if describeCalls == 1 {
				return &workflowservice.DescribeTaskQueueResponse{
					Stats: &taskqueuepb.TaskQueueStats{},
				}, nil
			}
			select {
			case <-profileStarted:
			case <-t.Context().Done():
				return nil, t.Context().Err()
			}
			return nil, errors.New("describe task queue failed")
		},
	}
	c := &clientWithWorkflowService{
		workflowService: service,
		executeWorkflow: func(
			context.Context,
			client.StartWorkflowOptions,
			any,
			...any,
		) (client.WorkflowRun, error) {
			return &fakeWorkflowRun{
				id:     "workflow",
				getErr: func() error { return nil },
			}, nil
		},
	}
	dir := t.TempDir()

	err := runWithClient(t.Context(), c, runConfig{
		namespace:            "load-test",
		taskQueue:            "load-task-queue",
		taskQueues:           1,
		workersPerTaskQueue:  1,
		workflows:            1,
		concurrency:          1,
		runWorker:            true,
		backlogBeforeWorkers: true,
		backlogWaitTimeout:   time.Second,
		cpuProfile:           dir + "/load.pprof",
		serverPProf:          server.URL,
		serverCPU:            dir + "/server.pprof",
		serverCPUTime:        time.Hour,
	})

	require.ErrorContains(t, err, "run persisted task backlog load")
	require.ErrorContains(t, err, "describe task queue failed")
	select {
	case <-profileCanceled:
	default:
		t.Fatal("server CPU profile request was not canceled")
	}
	stopCPUProfile, err := startCPUProfile(dir + "/after-error.pprof")
	require.NoError(t, err)
	require.NoError(t, stopCPUProfile())
}

func TestStartCPUProfilePreservesExistingFileOnStartFailure(t *testing.T) {
	activeProfile, err := os.CreateTemp(t.TempDir(), "active-*.pprof")
	require.NoError(t, err)
	require.NoError(t, pprof.StartCPUProfile(activeProfile))
	defer func() {
		pprof.StopCPUProfile()
		require.NoError(t, activeProfile.Close())
	}()

	dir := t.TempDir()
	path := filepath.Join(dir, "cpu.pprof")
	require.NoError(t, os.WriteFile(path, []byte("existing"), 0o600))

	stopCPUProfile, err := startCPUProfile(path)

	require.Error(t, err)
	require.Nil(t, stopCPUProfile)
	data, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, "existing", string(data))
	tempFiles, globErr := filepath.Glob(filepath.Join(dir, ".cpu.pprof.tmp-*"))
	require.NoError(t, globErr)
	require.Empty(t, tempFiles)
}

func TestStartCPUProfilePreservesExistingFileOnAsynchronousWriteFailure(t *testing.T) {
	writeErr := errors.New("profile write failed")
	dir := t.TempDir()
	path := filepath.Join(dir, "cpu.pprof")
	require.NoError(t, os.WriteFile(path, []byte("existing"), 0o600))

	stopCPUProfile, err := startCPUProfileWithWriter(path, func(io.Writer) io.Writer {
		return failingWriter{err: writeErr}
	})
	require.NoError(t, err)

	err = stopCPUProfile()

	require.ErrorIs(t, err, writeErr)
	data, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, "existing", string(data))
	tempFiles, globErr := filepath.Glob(filepath.Join(dir, ".cpu.pprof.tmp-*"))
	require.NoError(t, globErr)
	require.Empty(t, tempFiles)
}

func TestStartCPUProfilePublishesOnStop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cpu.pprof")
	require.NoError(t, os.WriteFile(path, []byte("existing"), 0o600))

	stopCPUProfile, err := startCPUProfile(path)
	require.NoError(t, err)
	duringProfile, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "existing", string(duringProfile))

	require.NoError(t, stopCPUProfile())
	profile, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotEmpty(t, profile)
	require.NotEqual(t, "existing", string(profile))
	tempFiles, err := filepath.Glob(filepath.Join(dir, ".cpu.pprof.tmp-*"))
	require.NoError(t, err)
	require.Empty(t, tempFiles)
}

func TestRunWithClientReportsCPUProfileStopFailure(t *testing.T) {
	root := t.TempDir()
	profileDir := filepath.Join(root, "profiles")
	movedProfileDir := filepath.Join(root, "profiles-moved")
	resultPath := filepath.Join(root, "result.json")
	require.NoError(t, os.Mkdir(profileDir, 0o700))
	renameErrCh := make(chan error, 1)
	c := &clientWithWorkflowService{
		executeWorkflow: func(
			context.Context,
			client.StartWorkflowOptions,
			any,
			...any,
		) (client.WorkflowRun, error) {
			renameErrCh <- os.Rename(profileDir, movedProfileDir)
			return &fakeWorkflowRun{
				id:     "workflow",
				getErr: func() error { return nil },
			}, nil
		},
	}

	err := runWithClient(t.Context(), c, runConfig{
		namespace:           "load-test",
		taskQueue:           "load-task-queue",
		taskQueues:          1,
		workersPerTaskQueue: 1,
		workflows:           1,
		concurrency:         1,
		timeout:             time.Minute,
		cpuProfile:          filepath.Join(profileDir, "cpu.pprof"),
		resultFile:          resultPath,
	})

	require.NoError(t, <-renameErrCh)
	require.ErrorContains(t, err, "write CPU profile")
	require.ErrorIs(t, err, fs.ErrNotExist)
	resultBytes, readErr := os.ReadFile(resultPath)
	require.NoError(t, readErr)
	var result runResult
	require.NoError(t, json.Unmarshal(resultBytes, &result))
	require.Empty(t, result.CPUProfile)
}

func TestRunWithClientWritesResultsAfterRunContextCanceled(t *testing.T) {
	metricsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("temporal_persistence_requests 1\n"))
	}))
	defer metricsServer.Close()

	ctx, cancel := context.WithCancel(t.Context())
	c := &clientWithWorkflowService{
		executeWorkflow: func(
			context.Context,
			client.StartWorkflowOptions,
			any,
			...any,
		) (client.WorkflowRun, error) {
			return &fakeWorkflowRun{
				id: "workflow",
				getErr: func() error {
					cancel()
					return ctx.Err()
				},
			}, nil
		},
	}
	dir := t.TempDir()
	metricsPath := dir + "/metrics.after"
	resultPath := dir + "/result.json"

	err := runWithClient(ctx, c, runConfig{
		namespace:           "load-test",
		taskQueue:           "load-task-queue",
		taskQueues:          1,
		workersPerTaskQueue: 1,
		workflows:           1,
		concurrency:         1,
		metricSnapshotsAfter: metricSnapshotFlags{
			{url: metricsServer.URL, path: metricsPath},
		},
		resultFile: resultPath,
	})

	require.ErrorContains(t, err, "load completed 0 of 1 workflows")
	metrics, readErr := os.ReadFile(metricsPath)
	require.NoError(t, readErr)
	require.Equal(t, "temporal_persistence_requests 1\n", string(metrics))
	resultBytes, readErr := os.ReadFile(resultPath)
	require.NoError(t, readErr)
	var result runResult
	require.NoError(t, json.Unmarshal(resultBytes, &result))
	require.Zero(t, result.Completed)
	require.Equal(t, int64(1), result.Failed)
}

func TestRunWithClientOmitsPreparedStatsWhenPostSnapshotFails(t *testing.T) {
	const (
		beforeMetrics = "# TYPE scylla_transport_cql_requests_count counter\n" +
			"scylla_transport_cql_requests_count{kind=\"PREPARE\",shard=\"0\"} 10\n"
		staleAfterMetrics = "# TYPE scylla_transport_cql_requests_count counter\n" +
			"scylla_transport_cql_requests_count{kind=\"PREPARE\",shard=\"0\"} 12\n"
	)
	var requests atomic.Int32
	metricsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			_, _ = w.Write([]byte(beforeMetrics))
			return
		}
		http.Error(w, "snapshot failed", http.StatusInternalServerError)
	}))
	defer metricsServer.Close()

	c := &clientWithWorkflowService{
		executeWorkflow: func(
			context.Context,
			client.StartWorkflowOptions,
			any,
			...any,
		) (client.WorkflowRun, error) {
			return &fakeWorkflowRun{
				id:     "workflow",
				getErr: func() error { return nil },
			}, nil
		},
	}
	dir := t.TempDir()
	beforePath := dir + "/metrics.before"
	afterPath := dir + "/metrics.after"
	resultPath := dir + "/result.json"
	require.NoError(t, os.WriteFile(afterPath, []byte(staleAfterMetrics), 0o600))

	err := runWithClient(t.Context(), c, runConfig{
		namespace:           "load-test",
		taskQueue:           "load-task-queue",
		taskQueues:          1,
		workersPerTaskQueue: 1,
		workflows:           1,
		concurrency:         1,
		metricSnapshotsBefore: metricSnapshotFlags{
			{url: metricsServer.URL, path: beforePath},
		},
		metricSnapshotsAfter: metricSnapshotFlags{
			{url: metricsServer.URL, path: afterPath},
		},
		resultFile: resultPath,
	})

	require.ErrorContains(t, err, "write post-run metric snapshots")
	resultBytes, readErr := os.ReadFile(resultPath)
	require.NoError(t, readErr)
	var result runResult
	require.NoError(t, json.Unmarshal(resultBytes, &result))
	require.Equal(t, int64(1), result.Completed)
	require.Nil(t, result.PreparedStats)
	require.Nil(t, result.MetricSnapshotsAfter)
}

func TestValidateConfigRequiresServerPPROFForServerProfiles(t *testing.T) {
	err := validateConfig(runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		serverCPU:           "/tmp/server-cpu.pprof",
		timeout:             time.Second,
		serverCPUTime:       time.Second,
	})
	require.ErrorContains(t, err, "-server-pprof")

	err = validateConfig(runConfig{
		workflows:           1,
		concurrency:         1,
		taskQueues:          1,
		workersPerTaskQueue: 1,
		serverHeap:          "/tmp/server-heap.pprof",
		timeout:             time.Second,
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
	snapshots := []metricSnapshot{
		{url: server.URL + "/metrics", path: outputPath},
	}
	beforeCapture := time.Now()
	err := writeMetricSnapshots(t.Context(), snapshots)
	afterCapture := time.Now()
	require.NoError(t, err)

	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Equal(t, metricBody, string(data))
	require.Equal(t, "/metrics", requestedPath)
	require.False(t, snapshots[0].captureStartedAt.Before(beforeCapture))
	require.False(t, snapshots[0].captureFinishedAt.Before(snapshots[0].captureStartedAt))
	require.False(t, afterCapture.Before(snapshots[0].captureFinishedAt))
}

func TestWriteMetricSnapshotsContinuesAfterFailure(t *testing.T) {
	metricBody := "temporal_persistence_requests 1\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/failed" {
			http.Error(w, "failed", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(metricBody))
	}))
	defer server.Close()

	dir := t.TempDir()
	successPath := dir + "/success.prom"
	err := writeMetricSnapshots(t.Context(), []metricSnapshot{
		{url: server.URL + "/failed", path: dir + "/failed.prom"},
		{url: server.URL + "/success", path: successPath},
	})

	require.ErrorContains(t, err, server.URL+"/failed")
	require.ErrorContains(t, err, dir+"/failed.prom")
	data, readErr := os.ReadFile(successPath)
	require.NoError(t, readErr)
	require.Equal(t, metricBody, string(data))
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
		serverCPU:           "/tmp/server.pprof",
		serverCPUTime:       2 * time.Second,
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
		Address:                  "127.0.0.1:7233",
		Namespace:                "scylla-load",
		TaskQueue:                "scylla-load",
		TaskQueues:               5,
		WorkersPerTaskQueue:      2,
		Workflows:                7,
		Concurrency:              3,
		ActivitiesEach:           2,
		SignalsEach:              1,
		EagerStart:               true,
		EagerActivities:          true,
		PayloadBytes:             512,
		ServerCPUProfileDuration: 2 * time.Second,
		GoVersion:                runtime.Version(),
		GOOS:                     runtime.GOOS,
		GOARCH:                   runtime.GOARCH,
		NumCPU:                   runtime.NumCPU(),
		GOMAXPROCS:               runtime.GOMAXPROCS(0),
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

func TestWriteProfileSummariesContinuesAfterFailure(t *testing.T) {
	dir := t.TempDir()
	fakeGo := dir + "/go"
	script := "#!/bin/sh\n" +
		"if [ \"$4\" = \"/tmp/failed.pprof\" ]; then echo failed >&2; exit 1; fi\n" +
		"printf 'flat flat%% sum%% cum cum%% name\\n10ms 100%% 100%% 10ms 100%% test.hot\\n'\n"
	require.NoError(t, os.WriteFile(fakeGo, []byte(script), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	successPath := dir + "/success.top.txt"
	err := writeProfileSummaries(t.Context(), []profileSummary{
		{profile: "/tmp/failed.pprof", path: dir + "/failed.top.txt"},
		{profile: "/tmp/success.pprof", path: successPath},
	})

	require.ErrorContains(t, err, "/tmp/failed.pprof")
	summary, readErr := os.ReadFile(successPath)
	require.NoError(t, readErr)
	require.Contains(t, string(summary), "test.hot")
}

func TestFilterFailedProfileSummaries(t *testing.T) {
	dir := t.TempDir()
	failedProfile := filepath.Join(dir, "failed.pprof")
	producerErr := errors.New("profile fetch failed")
	failedProfiles := make(map[string]error)
	recordFailedProfile(failedProfiles, failedProfile, producerErr)

	filtered, err := filterFailedProfileSummaries(profileSummaryFlags{
		{profile: failedProfile, path: filepath.Join(dir, "failed.top.txt")},
		{profile: filepath.Join(dir, "success.pprof"), path: filepath.Join(dir, "success.top.txt")},
	}, failedProfiles)

	require.ErrorIs(t, err, producerErr)
	require.ErrorContains(t, err, "skip profile summary")
	require.Equal(t, profileSummaryFlags{{
		profile: filepath.Join(dir, "success.pprof"),
		path:    filepath.Join(dir, "success.top.txt"),
	}}, filtered)
}

func TestWriteOutputFilePreservesExistingFileOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o600))
	writeErr := errors.New("write failed")

	err := writeOutputFile(path, func(file *os.File) error {
		_, err := file.WriteString("new")
		require.NoError(t, err)
		return writeErr
	})

	require.ErrorIs(t, err, writeErr)
	data, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, "old", string(data))
	tempFiles, globErr := filepath.Glob(filepath.Join(filepath.Dir(path), ".output.tmp-*"))
	require.NoError(t, globErr)
	require.Empty(t, tempFiles)
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
