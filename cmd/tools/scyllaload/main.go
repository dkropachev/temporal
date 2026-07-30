package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
)

const signalName = "load-signal"

type (
	runConfig struct {
		address               string
		namespace             string
		taskQueue             string
		taskQueues            int
		workersPerTaskQueue   int
		workflows             int
		concurrency           int
		activitiesEach        int
		signalsEach           int
		eagerStart            bool
		eagerActivities       bool
		payloadBytes          int
		timeout               time.Duration
		registerNS            bool
		runWorker             bool
		backlogBeforeWorkers  bool
		backlogWaitTimeout    time.Duration
		cpuProfile            string
		heapProfile           string
		serverPProf           string
		serverCPU             string
		serverHeap            string
		serverCPUTime         time.Duration
		metricSnapshotsBefore metricSnapshotFlags
		metricSnapshotsAfter  metricSnapshotFlags
		profileSummaries      profileSummaryFlags
		resultFile            string
		runMetadataFile       string
	}

	workflowInput struct {
		Activities int
		Signals    int
		Eager      bool
		Payload    []byte
	}

	runResult struct {
		Address               string        `json:"address"`
		Namespace             string        `json:"namespace"`
		TaskQueue             string        `json:"taskQueue"`
		TaskQueues            int           `json:"taskQueues"`
		WorkersPerTaskQueue   int           `json:"workersPerTaskQueue"`
		Workflows             int           `json:"workflows"`
		Concurrency           int           `json:"concurrency"`
		ActivitiesEach        int           `json:"activitiesEach"`
		SignalsEach           int           `json:"signalsEach"`
		EagerStart            bool          `json:"eagerStart"`
		EagerActivities       bool          `json:"eagerActivities"`
		PayloadBytes          int           `json:"payloadBytes"`
		Elapsed               time.Duration `json:"elapsed"`
		Completed             int64         `json:"completed"`
		Failed                int64         `json:"failed"`
		WorkflowsPerSec       float64       `json:"workflowsPerSec"`
		Requests              int64         `json:"requests"`
		RequestsPerSec        float64       `json:"requestsPerSec"`
		BacklogBeforeWorkers  bool          `json:"backlogBeforeWorkers,omitempty"`
		EnqueueElapsed        time.Duration `json:"enqueueElapsed,omitempty"`
		Enqueued              int64         `json:"enqueued,omitempty"`
		EnqueueFailed         int64         `json:"enqueueFailed,omitempty"`
		EnqueueRequestsPerSec float64       `json:"enqueueRequestsPerSec,omitempty"`
		BacklogWaitElapsed    time.Duration `json:"backlogWaitElapsed,omitempty"`
		BacklogTasks          int64         `json:"backlogTasks,omitempty"`
		WorkerStartElapsed    time.Duration `json:"workerStartElapsed,omitempty"`
		DrainElapsed          time.Duration `json:"drainElapsed,omitempty"`
		DrainFailed           int64         `json:"drainFailed,omitempty"`
		DrainWorkflowsPerSec  float64       `json:"drainWorkflowsPerSec,omitempty"`
		CPUProfile            string        `json:"cpuProfile,omitempty"`
		HeapProfile           string        `json:"heapProfile,omitempty"`
		ServerCPUProfile      string        `json:"serverCpuProfile,omitempty"`
		ServerHeapProfile     string        `json:"serverHeapProfile,omitempty"`
		MetricSnapshotsBefore []string      `json:"metricSnapshotsBefore,omitempty"`
		MetricSnapshotsAfter  []string      `json:"metricSnapshotsAfter,omitempty"`
		PreparedStats         *prepStats    `json:"scyllaPreparedStatements,omitempty"`
		ProfileSummaries      []string      `json:"profileSummaries,omitempty"`
		ResultFile            string        `json:"resultFile,omitempty"`
		RunMetadataFile       string        `json:"runMetadataFile,omitempty"`
	}

	runMetadata struct {
		StartedAt               time.Time         `json:"startedAt"`
		Address                 string            `json:"address"`
		Namespace               string            `json:"namespace"`
		TaskQueue               string            `json:"taskQueue"`
		TaskQueues              int               `json:"taskQueues"`
		WorkersPerTaskQueue     int               `json:"workersPerTaskQueue"`
		Workflows               int               `json:"workflows"`
		Concurrency             int               `json:"concurrency"`
		ActivitiesEach          int               `json:"activitiesEach"`
		SignalsEach             int               `json:"signalsEach"`
		EagerStart              bool              `json:"eagerStart"`
		EagerActivities         bool              `json:"eagerActivities"`
		PayloadBytes            int               `json:"payloadBytes"`
		BacklogBeforeWorkers    bool              `json:"backlogBeforeWorkers,omitempty"`
		BacklogWaitTimeout      time.Duration     `json:"backlogWaitTimeout,omitempty"`
		GoVersion               string            `json:"goVersion"`
		GOOS                    string            `json:"goos"`
		GOARCH                  string            `json:"goarch"`
		NumCPU                  int               `json:"numCpu"`
		GOMAXPROCS              int               `json:"gomaxprocs"`
		Environment             map[string]string `json:"environment,omitempty"`
		PProfEndpoint           *endpointCheck    `json:"pprofEndpoint,omitempty"`
		MetricSnapshotEndpoints []endpointCheck   `json:"metricSnapshotEndpoints,omitempty"`
	}

	endpointCheck struct {
		Name   string `json:"name"`
		URL    string `json:"url"`
		Status string `json:"status,omitempty"`
		Error  string `json:"error,omitempty"`
	}

	metricSnapshot struct {
		url  string
		path string
	}

	profileSummary struct {
		profile string
		path    string
	}

	metricSnapshotFlags []metricSnapshot

	profileSummaryFlags []profileSummary

	nopLogger struct{}
)

func main() {
	var cfg runConfig
	registerFlags(flag.CommandLine, &cfg)
	flag.Parse()
	if err := validateConfig(cfg); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	c, err := client.Dial(client.Options{
		HostPort:  cfg.address,
		Namespace: cfg.namespace,
		Logger:    nopLogger{},
	})
	if err != nil {
		log.Fatalf("dial Temporal: %v", err)
	}
	defer c.Close()

	if cfg.registerNS {
		if err := ensureNamespace(ctx, c, cfg.namespace); err != nil {
			log.Fatalf("register namespace: %v", err)
		}
	}

	var workers []worker.Worker
	if !cfg.backlogBeforeWorkers {
		workers, err = startWorker(c, cfg)
		if err != nil {
			log.Fatalf("start worker: %v", err)
		}
	}
	defer func() {
		stopWorkers(workers)
	}()

	stopCPUProfile, err := startCPUProfile(cfg.cpuProfile)
	if err != nil {
		log.Fatalf("start CPU profile: %v", err)
	}
	serverCPUProfileDone, err := startServerCPUProfile(ctx, cfg)
	if err != nil {
		log.Fatalf("start server CPU profile: %v", err)
	}
	if err := writeMetricSnapshots(ctx, cfg.metricSnapshotsBefore); err != nil {
		log.Fatalf("write pre-run metric snapshots: %v", err)
	}
	if err := writeRunMetadata(ctx, cfg); err != nil {
		log.Fatalf("write run metadata: %v", err)
	}
	var result runResult
	if cfg.backlogBeforeWorkers {
		result, workers, err = runBacklogLoad(ctx, c, cfg)
		if err != nil {
			log.Fatalf("run persisted task backlog load: %v", err)
		}
	} else {
		result = runLoad(ctx, c, cfg)
	}
	stopCPUProfile()
	if err := serverCPUProfileDone(); err != nil {
		log.Fatalf("write server CPU profile: %v", err)
	}
	if err := writeHeapProfile(cfg.heapProfile); err != nil {
		log.Fatalf("write heap profile: %v", err)
	}
	if err := writeServerHeapProfile(ctx, cfg); err != nil {
		log.Fatalf("write server heap profile: %v", err)
	}
	if err := writeMetricSnapshots(ctx, cfg.metricSnapshotsAfter); err != nil {
		log.Fatalf("write post-run metric snapshots: %v", err)
	}
	result.PreparedStats, err = readScyllaPreparedStatementMetrics(
		cfg.metricSnapshotsBefore,
		cfg.metricSnapshotsAfter,
	)
	if err != nil {
		log.Fatalf("read Scylla prepared statement metrics: %v", err)
	}
	if err := writeProfileSummaries(ctx, cfg.profileSummaries); err != nil {
		log.Fatalf("write profile summaries: %v", err)
	}
	if err := writeResult(result); err != nil {
		log.Fatalf("write result: %v", err)
	}
	if err := writeResultFile(cfg.resultFile, result); err != nil {
		log.Fatalf("write result file: %v", err)
	}
	if result.Failed > 0 || result.Completed != int64(cfg.workflows) {
		os.Exit(1)
	}
}

func registerFlags(flags *flag.FlagSet, cfg *runConfig) {
	flags.StringVar(&cfg.address, "address", "127.0.0.1:7233", "Temporal frontend host:port")
	flags.StringVar(&cfg.namespace, "namespace", "scylla-load", "Temporal namespace")
	flags.StringVar(&cfg.taskQueue, "task-queue", "scylla-load", "Temporal task queue")
	flags.IntVar(&cfg.taskQueues, "task-queues", 1, "number of task queues to distribute workflow starts across")
	flags.IntVar(&cfg.workersPerTaskQueue, "workers-per-task-queue", 1, "workers to start for each task queue")
	flags.IntVar(&cfg.workflows, "workflows", 1000, "number of workflows to execute")
	flags.IntVar(&cfg.concurrency, "concurrency", 100, "maximum concurrent workflow executions")
	flags.IntVar(&cfg.activitiesEach, "activities-each", 1, "activities executed by each workflow")
	flags.IntVar(&cfg.signalsEach, "signals-each", 0, "signals sent to each workflow before completion")
	flags.BoolVar(&cfg.eagerStart, "eager-start", false, "request eager workflow start from a colocated worker")
	flags.BoolVar(&cfg.eagerActivities, "eager-activities", false, "request eager activity dispatch from workflow tasks")
	flags.IntVar(&cfg.payloadBytes, "payload-bytes", 128, "payload size for workflow inputs, activities, and signals")
	flags.DurationVar(&cfg.timeout, "timeout", 10*time.Minute, "overall load run timeout")
	flags.BoolVar(&cfg.registerNS, "register-namespace", true, "register namespace if it does not exist")
	flags.BoolVar(&cfg.runWorker, "worker", true, "run a worker in this process")
	flags.BoolVar(&cfg.backlogBeforeWorkers, "backlog-before-workers", false, "persist workflow tasks before starting workers")
	flags.DurationVar(&cfg.backlogWaitTimeout, "backlog-wait-timeout", 30*time.Second, "maximum time to wait for workflow tasks to enter the persisted backlog")
	flags.StringVar(&cfg.cpuProfile, "cpu-profile", "", "write Go CPU profile for the load generator")
	flags.StringVar(&cfg.heapProfile, "heap-profile", "", "write Go heap profile for the load generator after the run")
	flags.StringVar(&cfg.serverPProf, "server-pprof", "", "Temporal server pprof base URL, for example http://127.0.0.1:7936")
	flags.StringVar(&cfg.serverCPU, "server-cpu-profile", "", "write Temporal server CPU profile from -server-pprof during the run")
	flags.StringVar(&cfg.serverHeap, "server-heap-profile", "", "write Temporal server heap profile from -server-pprof after the run")
	flags.DurationVar(&cfg.serverCPUTime, "server-cpu-profile-duration", 30*time.Second, "Temporal server CPU profile duration")
	flags.Var(&cfg.metricSnapshotsBefore, "metrics-snapshot-before", "fetch a Prometheus metrics snapshot before the run, formatted as URL=output_path; repeatable")
	flags.Var(&cfg.metricSnapshotsAfter, "metrics-snapshot-after", "fetch a Prometheus metrics snapshot after the run, formatted as URL=output_path; repeatable")
	flags.Var(&cfg.metricSnapshotsAfter, "metrics-snapshot", "alias for -metrics-snapshot-after")
	flags.Var(&cfg.profileSummaries, "profile-summary", "write go tool pprof -top output, formatted as profile_path=summary_path; repeatable")
	flags.StringVar(&cfg.resultFile, "result-file", "", "write the load result JSON to this file in addition to stdout")
	flags.StringVar(&cfg.runMetadataFile, "run-metadata-file", "", "write run environment and endpoint metadata JSON to this file")
}

func validateConfig(cfg runConfig) error {
	if cfg.workflows <= 0 {
		return errors.New("-workflows must be positive")
	}
	if cfg.concurrency <= 0 {
		return errors.New("-concurrency must be positive")
	}
	if cfg.taskQueues <= 0 {
		return errors.New("-task-queues must be positive")
	}
	if cfg.workersPerTaskQueue <= 0 {
		return errors.New("-workers-per-task-queue must be positive")
	}
	if cfg.activitiesEach < 0 {
		return errors.New("-activities-each must be non-negative")
	}
	if cfg.signalsEach < 0 {
		return errors.New("-signals-each must be non-negative")
	}
	if cfg.payloadBytes < 0 {
		return errors.New("-payload-bytes must be non-negative")
	}
	if cfg.backlogBeforeWorkers {
		if !cfg.runWorker {
			return errors.New("-worker must be enabled when -backlog-before-workers is set")
		}
		if cfg.signalsEach != 0 {
			return errors.New("-signals-each must be zero when -backlog-before-workers is set")
		}
		if cfg.eagerStart {
			return errors.New("-eager-start must be disabled when -backlog-before-workers is set")
		}
		if cfg.backlogWaitTimeout <= 0 {
			return errors.New("-backlog-wait-timeout must be positive when -backlog-before-workers is set")
		}
	}
	if cfg.serverCPU != "" && cfg.serverPProf == "" {
		return errors.New("-server-pprof must be set when -server-cpu-profile is set")
	}
	if cfg.serverHeap != "" && cfg.serverPProf == "" {
		return errors.New("-server-pprof must be set when -server-heap-profile is set")
	}
	if cfg.serverCPUTime <= 0 {
		return errors.New("-server-cpu-profile-duration must be positive")
	}
	return nil
}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

func startWorker(c client.Client, cfg runConfig) ([]worker.Worker, error) {
	if !cfg.runWorker {
		return nil, nil
	}
	workers := make([]worker.Worker, 0, cfg.taskQueues*cfg.workersPerTaskQueue)
	for taskQueueIndex := 0; taskQueueIndex < cfg.taskQueues; taskQueueIndex++ {
		taskQueue := taskQueueName(cfg, taskQueueIndex)
		for workerIndex := 0; workerIndex < cfg.workersPerTaskQueue; workerIndex++ {
			w := worker.New(c, taskQueue, worker.Options{})
			w.RegisterWorkflow(loadWorkflow)
			w.RegisterActivity(loadActivity)
			if err := w.Start(); err != nil {
				for _, started := range workers {
					started.Stop()
				}
				return nil, err
			}
			workers = append(workers, w)
		}
	}
	return workers, nil
}

func stopWorkers(workers []worker.Worker) {
	for _, w := range workers {
		w.Stop()
	}
}

func runLoad(ctx context.Context, c client.Client, cfg runConfig) runResult {
	return runLoadWithRunner(ctx, c, cfg, runOneWorkflow)
}

type (
	workflowRunner  func(context.Context, client.Client, runConfig, []byte, int64, int) workflowRunResult
	workflowStarter func(context.Context, client.Client, runConfig, []byte, int64, int) (client.WorkflowRun, error)
	backlogWaiter   func(context.Context, client.Client, runConfig, int64) (int64, error)
	workerStarter   func(client.Client, runConfig) ([]worker.Worker, error)
)

type workflowRunResult struct {
	completed bool
	requests  int64
}

type backlogEnqueueResult struct {
	runs     []client.WorkflowRun
	elapsed  time.Duration
	enqueued int64
	requests int64
}

func runLoadWithRunner(ctx context.Context, c client.Client, cfg runConfig, runner workflowRunner) runResult {
	payload := makePayload(cfg.payloadBytes)
	start := time.Now()
	var completed atomic.Int64
	var failed atomic.Int64
	var requests atomic.Int64
	sem := make(chan struct{}, cfg.concurrency)
	var wg sync.WaitGroup

	launched := 0
launch:
	for i := 0; i < cfg.workflows; i++ {
		select {
		case <-ctx.Done():
			break launch
		case sem <- struct{}{}:
		}
		launched++
		workflowIndex := i
		wg.Go(func() {
			defer func() { <-sem }()
			runResult := runner(ctx, c, cfg, payload, start.UnixNano(), workflowIndex)
			requests.Add(runResult.requests)
			if runResult.completed {
				completed.Add(1)
			} else {
				failed.Add(1)
			}
		})
	}
	if launched < cfg.workflows {
		failed.Add(int64(cfg.workflows - launched))
	}

	wg.Wait()
	elapsed := time.Since(start)
	result := newRunResult(cfg)
	result.Elapsed = elapsed
	result.Completed = completed.Load()
	result.Failed = failed.Load()
	result.Requests = requests.Load()
	if elapsed > 0 {
		result.WorkflowsPerSec = float64(result.Completed) / elapsed.Seconds()
		result.RequestsPerSec = float64(result.Requests) / elapsed.Seconds()
	}
	return result
}

func newRunResult(cfg runConfig) runResult {
	return runResult{
		Address:               cfg.address,
		Namespace:             cfg.namespace,
		TaskQueue:             cfg.taskQueue,
		TaskQueues:            cfg.taskQueues,
		WorkersPerTaskQueue:   cfg.workersPerTaskQueue,
		Workflows:             cfg.workflows,
		Concurrency:           cfg.concurrency,
		ActivitiesEach:        cfg.activitiesEach,
		SignalsEach:           cfg.signalsEach,
		EagerStart:            cfg.eagerStart,
		EagerActivities:       cfg.eagerActivities,
		PayloadBytes:          cfg.payloadBytes,
		BacklogBeforeWorkers:  cfg.backlogBeforeWorkers,
		CPUProfile:            cfg.cpuProfile,
		HeapProfile:           cfg.heapProfile,
		ServerCPUProfile:      cfg.serverCPU,
		ServerHeapProfile:     cfg.serverHeap,
		MetricSnapshotsBefore: cfg.metricSnapshotsBefore.paths(),
		MetricSnapshotsAfter:  cfg.metricSnapshotsAfter.paths(),
		ProfileSummaries:      cfg.profileSummaries.paths(),
		ResultFile:            cfg.resultFile,
		RunMetadataFile:       cfg.runMetadataFile,
	}
}

func runOneWorkflow(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
	payload []byte,
	startNanos int64,
	workflowIndex int,
) workflowRunResult {
	workflowID := workflowID(startNanos, workflowIndex)
	result := workflowRunResult{requests: 1}
	run, err := startOneWorkflow(ctx, c, cfg, payload, startNanos, workflowIndex)
	if err != nil {
		log.Printf("start workflow %s: %v", workflowID, err)
		return result
	}

	for signalIndex := 0; signalIndex < cfg.signalsEach; signalIndex++ {
		result.requests++
		if err := c.SignalWorkflow(ctx, workflowID, run.GetRunID(), signalName, payload); err != nil {
			log.Printf("signal workflow %s: %v", workflowID, err)
			return result
		}
	}

	if err := run.Get(ctx, nil); err != nil {
		log.Printf("workflow %s failed: %v", workflowID, err)
		return result
	}
	result.completed = true
	return result
}

func startOneWorkflow(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
	payload []byte,
	startNanos int64,
	workflowIndex int,
) (client.WorkflowRun, error) {
	return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:               workflowID(startNanos, workflowIndex),
		TaskQueue:        taskQueueName(cfg, workflowIndex%cfg.taskQueues),
		EnableEagerStart: cfg.eagerStart,
	}, loadWorkflow, workflowInput{
		Activities: cfg.activitiesEach,
		Signals:    cfg.signalsEach,
		Eager:      cfg.eagerActivities,
		Payload:    payload,
	})
}

func workflowID(startNanos int64, workflowIndex int) string {
	return fmt.Sprintf("scylla-load-%d-%d", startNanos, workflowIndex)
}

func runBacklogLoad(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
) (runResult, []worker.Worker, error) {
	return runBacklogLoadWithDependencies(ctx, c, cfg, startOneWorkflow, waitForWorkflowTaskBacklog, startWorker)
}

func runBacklogLoadWithDependencies(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
	starter workflowStarter,
	waiter backlogWaiter,
	startWorkers workerStarter,
) (runResult, []worker.Worker, error) {
	result := newRunResult(cfg)
	start := time.Now()
	enqueueResult := enqueueWorkflowBacklog(ctx, c, cfg, starter, start)
	result.EnqueueElapsed = enqueueResult.elapsed
	result.Enqueued = enqueueResult.enqueued
	result.EnqueueFailed = int64(cfg.workflows) - result.Enqueued
	result.Requests = enqueueResult.requests
	if result.EnqueueElapsed > 0 {
		result.EnqueueRequestsPerSec = float64(result.Requests) / result.EnqueueElapsed.Seconds()
	}

	backlogWaitStart := time.Now()
	backlogTasks, err := waiter(ctx, c, cfg, result.Enqueued)
	result.BacklogWaitElapsed = time.Since(backlogWaitStart)
	result.BacklogTasks = backlogTasks
	if err != nil {
		return result, nil, err
	}

	drainStart := time.Now()
	workerStart := time.Now()
	workers, err := startWorkers(c, cfg)
	result.WorkerStartElapsed = time.Since(workerStart)
	if err != nil {
		return result, nil, err
	}

	completed := drainWorkflowBacklog(ctx, enqueueResult.runs, cfg.concurrency)
	result.DrainElapsed = time.Since(drainStart)
	result.Completed = completed
	result.DrainFailed = result.Enqueued - result.Completed
	result.Failed = int64(cfg.workflows) - result.Completed
	result.Elapsed = time.Since(start)
	if result.Elapsed > 0 {
		result.WorkflowsPerSec = float64(result.Completed) / result.Elapsed.Seconds()
		result.RequestsPerSec = float64(result.Requests) / result.Elapsed.Seconds()
	}
	if result.DrainElapsed > 0 {
		result.DrainWorkflowsPerSec = float64(result.Completed) / result.DrainElapsed.Seconds()
	}
	return result, workers, nil
}

func enqueueWorkflowBacklog(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
	starter workflowStarter,
	start time.Time,
) backlogEnqueueResult {
	payload := makePayload(cfg.payloadBytes)
	runs := make([]client.WorkflowRun, cfg.workflows)
	sem := make(chan struct{}, cfg.concurrency)
	var wg sync.WaitGroup
	var enqueued atomic.Int64
	var requests atomic.Int64

enqueue:
	for workflowIndex := 0; workflowIndex < cfg.workflows; workflowIndex++ {
		select {
		case <-ctx.Done():
			break enqueue
		case sem <- struct{}{}:
		}
		index := workflowIndex
		wg.Go(func() {
			defer func() { <-sem }()
			requests.Add(1)
			run, err := starter(ctx, c, cfg, payload, start.UnixNano(), index)
			if err != nil {
				log.Printf("start workflow %s: %v", workflowID(start.UnixNano(), index), err)
				return
			}
			runs[index] = run
			enqueued.Add(1)
		})
	}
	wg.Wait()
	return backlogEnqueueResult{
		runs:     runs,
		elapsed:  time.Since(start),
		enqueued: enqueued.Load(),
		requests: requests.Load(),
	}
}

func drainWorkflowBacklog(
	ctx context.Context,
	runs []client.WorkflowRun,
	concurrency int,
) int64 {
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var completed atomic.Int64

drain:
	for _, run := range runs {
		if run == nil {
			continue
		}
		select {
		case <-ctx.Done():
			break drain
		case sem <- struct{}{}:
		}
		workflowRun := run
		wg.Go(func() {
			defer func() { <-sem }()
			if err := workflowRun.Get(ctx, nil); err != nil {
				log.Printf("workflow %s failed: %v", workflowRun.GetID(), err)
				return
			}
			completed.Add(1)
		})
	}
	wg.Wait()
	return completed.Load()
}

func waitForWorkflowTaskBacklog(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
	expected int64,
) (int64, error) {
	if expected == 0 {
		return 0, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, cfg.backlogWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var backlog int64
	for {
		var err error
		backlog, err = workflowTaskBacklogCount(waitCtx, c, cfg)
		if err != nil {
			return backlog, err
		}
		if backlog >= expected {
			return backlog, nil
		}

		select {
		case <-waitCtx.Done():
			return backlog, fmt.Errorf("wait for workflow task backlog: got %d of %d tasks: %w", backlog, expected, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func workflowTaskBacklogCount(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
) (int64, error) {
	var backlog int64
	for taskQueueIndex := 0; taskQueueIndex < cfg.taskQueues; taskQueueIndex++ {
		response, err := c.WorkflowService().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace: cfg.namespace,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueueName(cfg, taskQueueIndex),
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			ReportStats:   true,
		})
		if err != nil {
			return backlog, err
		}
		backlog += response.GetStats().GetApproximateBacklogCount()
	}
	return backlog, nil
}

func taskQueueName(cfg runConfig, index int) string {
	if cfg.taskQueues == 1 {
		return cfg.taskQueue
	}
	return fmt.Sprintf("%s-%d", cfg.taskQueue, index)
}

func makePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	return payload
}

func writeResult(result runResult) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func writeResultFile(path string, result runResult) error {
	if path == "" {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func writeRunMetadata(ctx context.Context, cfg runConfig) error {
	if cfg.runMetadataFile == "" {
		return nil
	}
	metadata := runMetadata{
		StartedAt:            time.Now().UTC(),
		Address:              cfg.address,
		Namespace:            cfg.namespace,
		TaskQueue:            cfg.taskQueue,
		TaskQueues:           cfg.taskQueues,
		WorkersPerTaskQueue:  cfg.workersPerTaskQueue,
		Workflows:            cfg.workflows,
		Concurrency:          cfg.concurrency,
		ActivitiesEach:       cfg.activitiesEach,
		SignalsEach:          cfg.signalsEach,
		EagerStart:           cfg.eagerStart,
		EagerActivities:      cfg.eagerActivities,
		PayloadBytes:         cfg.payloadBytes,
		BacklogBeforeWorkers: cfg.backlogBeforeWorkers,
		BacklogWaitTimeout:   cfg.backlogWaitTimeout,
		GoVersion:            runtime.Version(),
		GOOS:                 runtime.GOOS,
		GOARCH:               runtime.GOARCH,
		NumCPU:               runtime.NumCPU(),
		GOMAXPROCS:           runtime.GOMAXPROCS(0),
		Environment:          selectedEnvironment(),
	}
	if cfg.serverPProf != "" {
		pprofURL, err := serverPProfURL(cfg.serverPProf, "/debug/pprof/", nil)
		if err != nil {
			return err
		}
		metadata.PProfEndpoint = ptr(endpointStatus(ctx, "pprof", pprofURL))
	}
	for _, snapshot := range append(cfg.metricSnapshotsBefore, cfg.metricSnapshotsAfter...) {
		metadata.MetricSnapshotEndpoints = append(metadata.MetricSnapshotEndpoints, endpointStatus(ctx, "metrics", snapshot.url))
	}
	f, err := os.Create(cfg.runMetadataFile)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func selectedEnvironment() map[string]string {
	keys := []string{
		"CASSANDRA_SEEDS",
		"CASSANDRA_MAX_CONNS",
		"CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE",
		"CASSANDRA_MAX_PREPARED_STMTS",
		"CASSANDRA_USER",
		"DB",
		"GOMAXPROCS",
		"NUM_HISTORY_SHARDS",
		"PPROF_PORT",
		"PROMETHEUS_ENDPOINT",
	}
	values := make(map[string]string)
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			values[key] = value
		}
	}
	return values
}

func endpointStatus(ctx context.Context, name string, endpoint string) endpointCheck {
	check := endpointCheck{Name: name, URL: endpoint}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		check.Error = err.Error()
		return check
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		check.Error = err.Error()
		return check
	}
	defer func() { _ = resp.Body.Close() }()
	check.Status = resp.Status
	return check
}

func ptr[T any](value T) *T {
	return &value
}

func startCPUProfile(path string) (func(), error) {
	if path == "" {
		return func() {}, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		pprof.StopCPUProfile()
		_ = f.Close()
	}, nil
}

func writeHeapProfile(path string) error {
	if path == "" {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func startServerCPUProfile(ctx context.Context, cfg runConfig) (func() error, error) {
	if cfg.serverCPU == "" {
		return func() error { return nil }, nil
	}
	profileURL, err := serverPProfURL(cfg.serverPProf, "/debug/pprof/profile", map[string]string{
		"seconds": strconv.FormatInt(serverCPUProfileSeconds(cfg.serverCPUTime), 10),
	})
	if err != nil {
		return nil, err
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- fetchProfile(ctx, profileURL, cfg.serverCPU)
	}()
	return func() error {
		return <-errCh
	}, nil
}

func writeServerHeapProfile(ctx context.Context, cfg runConfig) error {
	if cfg.serverHeap == "" {
		return nil
	}
	profileURL, err := serverPProfURL(cfg.serverPProf, "/debug/pprof/heap", nil)
	if err != nil {
		return err
	}
	return fetchProfile(ctx, profileURL, cfg.serverHeap)
}

func writeMetricSnapshots(ctx context.Context, snapshots []metricSnapshot) error {
	for _, snapshot := range snapshots {
		if err := fetchProfile(ctx, snapshot.url, snapshot.path); err != nil {
			return err
		}
	}
	return nil
}

func writeProfileSummaries(ctx context.Context, summaries []profileSummary) error {
	for _, summary := range summaries {
		if err := writeProfileSummary(ctx, summary); err != nil {
			return err
		}
	}
	return nil
}

func writeProfileSummary(ctx context.Context, summary profileSummary) error {
	cmd := exec.CommandContext(ctx, "go", "tool", "pprof", "-top", summary.profile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("summarize profile %s: %w: %s", summary.profile, err, strings.TrimSpace(string(output)))
	}
	f, err := os.Create(summary.path)
	if err != nil {
		return err
	}
	if _, err := f.Write(output); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func serverCPUProfileSeconds(duration time.Duration) int64 {
	seconds := int64(duration / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}

func serverPProfURL(base string, endpoint string, query map[string]string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = endpoint
	values := u.Query()
	for key, value := range query {
		values.Set(key, value)
	}
	u.RawQuery = values.Encode()
	return u.String(), nil
}

func fetchProfile(ctx context.Context, profileURL string, outputPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, profileURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch %s: %s", profileURL, resp.Status)
	}
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (f *metricSnapshotFlags) String() string {
	parts := make([]string, 0, len(*f))
	for _, snapshot := range *f {
		parts = append(parts, snapshot.url+"="+snapshot.path)
	}
	return strings.Join(parts, ",")
}

func (f *metricSnapshotFlags) Set(value string) error {
	metricURL, outputPath, ok := strings.Cut(value, "=")
	if !ok || metricURL == "" || outputPath == "" {
		return fmt.Errorf("invalid metrics snapshot %q, expected URL=output_path", value)
	}
	*f = append(*f, metricSnapshot{
		url:  metricURL,
		path: outputPath,
	})
	return nil
}

func (f metricSnapshotFlags) paths() []string {
	if len(f) == 0 {
		return nil
	}
	paths := make([]string, 0, len(f))
	for _, snapshot := range f {
		paths = append(paths, snapshot.path)
	}
	return paths
}

func (f *profileSummaryFlags) String() string {
	parts := make([]string, 0, len(*f))
	for _, summary := range *f {
		parts = append(parts, summary.profile+"="+summary.path)
	}
	return strings.Join(parts, ",")
}

func (f *profileSummaryFlags) Set(value string) error {
	profilePath, outputPath, ok := strings.Cut(value, "=")
	if !ok || profilePath == "" || outputPath == "" {
		return fmt.Errorf("invalid profile summary %q, expected profile_path=summary_path", value)
	}
	*f = append(*f, profileSummary{
		profile: profilePath,
		path:    outputPath,
	})
	return nil
}

func (f profileSummaryFlags) paths() []string {
	if len(f) == 0 {
		return nil
	}
	paths := make([]string, 0, len(f))
	for _, summary := range f {
		paths = append(paths, summary.path)
	}
	return paths
}

func ensureNamespace(ctx context.Context, c client.Client, namespace string) error {
	_, err := c.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	if err == nil {
		return nil
	}
	if _, ok := err.(*serviceerror.NamespaceAlreadyExists); ok {
		return nil
	}
	return err
}

func loadWorkflow(ctx workflow.Context, input workflowInput) error {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:   time.Minute,
		DisableEagerExecution: !input.Eager,
	})
	for i := 0; i < input.Activities; i++ {
		var size int
		if err := workflow.ExecuteActivity(activityCtx, loadActivity, input.Payload).Get(activityCtx, &size); err != nil {
			return err
		}
	}

	signalChannel := workflow.GetSignalChannel(ctx, signalName)
	for i := 0; i < input.Signals; i++ {
		var payload []byte
		signalChannel.Receive(ctx, &payload)
	}
	return nil
}

func loadActivity(_ context.Context, payload []byte) (int, error) {
	return len(payload), nil
}
