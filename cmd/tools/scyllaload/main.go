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
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	signalName                          = "load-signal"
	postRunCleanupTimeout               = 30 * time.Second
	incompleteWorkflowTerminationReason = "scyllaload run ended before workflow completion"
)

type (
	runConfig struct {
		address               string
		namespace             string
		taskQueue             string
		taskQueues            int
		workersPerTaskQueue   int
		workflows             int
		concurrency           int
		targetRPS             float64
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
		Address                  string        `json:"address"`
		Namespace                string        `json:"namespace"`
		TaskQueue                string        `json:"taskQueue"`
		TaskQueues               int           `json:"taskQueues"`
		WorkersPerTaskQueue      int           `json:"workersPerTaskQueue"`
		Workflows                int           `json:"workflows"`
		Concurrency              int           `json:"concurrency"`
		ActivitiesEach           int           `json:"activitiesEach"`
		SignalsEach              int           `json:"signalsEach"`
		EagerStart               bool          `json:"eagerStart"`
		EagerActivities          bool          `json:"eagerActivities"`
		PayloadBytes             int           `json:"payloadBytes"`
		Elapsed                  time.Duration `json:"elapsed"`
		Completed                int64         `json:"completed"`
		Failed                   int64         `json:"failed"`
		WorkflowsPerSec          float64       `json:"workflowsPerSec"`
		Requests                 int64         `json:"requests"`
		RequestsPerSec           float64       `json:"requestsPerSec"`
		TargetWorkflowsPerSec    float64       `json:"targetWorkflowsPerSec,omitempty"`
		WorkflowLatencyP50       time.Duration `json:"workflowLatencyP50,omitempty"`
		WorkflowLatencyP95       time.Duration `json:"workflowLatencyP95,omitempty"`
		WorkflowLatencyP99       time.Duration `json:"workflowLatencyP99,omitempty"`
		BacklogBeforeWorkers     bool          `json:"backlogBeforeWorkers,omitempty"`
		EnqueueElapsed           time.Duration `json:"enqueueElapsed,omitempty"`
		Enqueued                 int64         `json:"enqueued,omitempty"`
		EnqueueFailed            int64         `json:"enqueueFailed,omitempty"`
		EnqueueRequestsPerSec    float64       `json:"enqueueRequestsPerSec,omitempty"`
		BacklogWaitElapsed       time.Duration `json:"backlogWaitElapsed,omitempty"`
		BacklogTasks             int64         `json:"backlogTasks,omitempty"`
		WorkerStartElapsed       time.Duration `json:"workerStartElapsed,omitempty"`
		DrainElapsed             time.Duration `json:"drainElapsed,omitempty"`
		DrainFailed              int64         `json:"drainFailed,omitempty"`
		DrainWorkflowsPerSec     float64       `json:"drainWorkflowsPerSec,omitempty"`
		CPUProfile               string        `json:"cpuProfile,omitempty"`
		HeapProfile              string        `json:"heapProfile,omitempty"`
		ServerCPUProfile         string        `json:"serverCpuProfile,omitempty"`
		ServerCPUProfileDuration time.Duration `json:"serverCpuProfileDuration,omitempty"`
		ServerHeapProfile        string        `json:"serverHeapProfile,omitempty"`
		MetricSnapshotsBefore    []string      `json:"metricSnapshotsBefore,omitempty"`
		MetricSnapshotsAfter     []string      `json:"metricSnapshotsAfter,omitempty"`
		PreparedStats            *prepStats    `json:"scyllaPreparedStatements,omitempty"`
		ProfileSummaries         []string      `json:"profileSummaries,omitempty"`
		ResultFile               string        `json:"resultFile,omitempty"`
		RunMetadataFile          string        `json:"runMetadataFile,omitempty"`
	}

	runMetadata struct {
		StartedAt                time.Time         `json:"startedAt"`
		Address                  string            `json:"address"`
		Namespace                string            `json:"namespace"`
		TaskQueue                string            `json:"taskQueue"`
		TaskQueues               int               `json:"taskQueues"`
		WorkersPerTaskQueue      int               `json:"workersPerTaskQueue"`
		Workflows                int               `json:"workflows"`
		Concurrency              int               `json:"concurrency"`
		TargetWorkflowsPerSec    float64           `json:"targetWorkflowsPerSec,omitempty"`
		ActivitiesEach           int               `json:"activitiesEach"`
		SignalsEach              int               `json:"signalsEach"`
		EagerStart               bool              `json:"eagerStart"`
		EagerActivities          bool              `json:"eagerActivities"`
		PayloadBytes             int               `json:"payloadBytes"`
		BacklogBeforeWorkers     bool              `json:"backlogBeforeWorkers,omitempty"`
		BacklogWaitTimeout       time.Duration     `json:"backlogWaitTimeout,omitempty"`
		ServerCPUProfileDuration time.Duration     `json:"serverCpuProfileDuration,omitempty"`
		GoVersion                string            `json:"goVersion"`
		GOOS                     string            `json:"goos"`
		GOARCH                   string            `json:"goarch"`
		NumCPU                   int               `json:"numCpu"`
		GOMAXPROCS               int               `json:"gomaxprocs"`
		Environment              map[string]string `json:"environment,omitempty"`
		PProfEndpoint            *endpointCheck    `json:"pprofEndpoint,omitempty"`
		MetricSnapshotEndpoints  []endpointCheck   `json:"metricSnapshotEndpoints,omitempty"`
	}

	endpointCheck struct {
		Name   string `json:"name"`
		URL    string `json:"url"`
		Status string `json:"status,omitempty"`
		Error  string `json:"error,omitempty"`
	}

	metricSnapshot struct {
		url               string
		path              string
		captureStartedAt  time.Time
		captureFinishedAt time.Time
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
	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg runConfig) error {
	signalCtx, stopSignals := loadSignalContext(context.Background())
	defer stopSignals()
	ctx, cancel := context.WithTimeout(signalCtx, cfg.timeout)
	defer cancel()

	c, err := client.DialContext(ctx, client.Options{
		HostPort:  cfg.address,
		Namespace: cfg.namespace,
		Logger:    nopLogger{},
	})
	if err != nil {
		return fmt.Errorf("dial Temporal: %w", err)
	}
	defer c.Close()

	if cfg.registerNS {
		if err := ensureNamespace(ctx, c, cfg.namespace); err != nil {
			return fmt.Errorf("register namespace: %w", err)
		}
	}

	return runWithClient(ctx, c, cfg)
}

func runWithClient(ctx context.Context, c client.Client, cfg runConfig) (retErr error) {
	var workers []worker.Worker
	var err error
	if !cfg.backlogBeforeWorkers {
		workers, err = startWorker(c, cfg)
		if err != nil {
			return fmt.Errorf("start worker: %w", err)
		}
	}
	defer func() {
		stopWorkers(workers)
	}()

	var backlogBaseline []int64
	if cfg.backlogBeforeWorkers {
		backlogBaseline, err = workflowTaskBacklogCounts(ctx, c, cfg)
		if err != nil {
			return fmt.Errorf("read workflow task backlog baseline: %w", err)
		}
		if err := requireEmptyWorkflowTaskBacklog(backlogBaseline); err != nil {
			return err
		}
	}
	if err := writeMetricSnapshots(ctx, cfg.metricSnapshotsBefore); err != nil {
		return fmt.Errorf("write pre-run metric snapshots: %w", err)
	}
	if err := writeRunMetadata(ctx, cfg); err != nil {
		return fmt.Errorf("write run metadata: %w", err)
	}
	stopCPUProfile, err := startCPUProfile(cfg.cpuProfile)
	if err != nil {
		return fmt.Errorf("start CPU profile: %w", err)
	}
	cpuProfileStopped := false
	defer func() {
		if !cpuProfileStopped {
			if err := stopCPUProfile(); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("write CPU profile: %w", err))
			}
		}
	}()

	serverCPUProfileCtx, cancelServerCPUProfile := context.WithCancel(ctx)
	serverCPUProfileDone, err := startServerCPUProfile(serverCPUProfileCtx, cfg)
	if err != nil {
		cancelServerCPUProfile()
		return fmt.Errorf("start server CPU profile: %w", err)
	}
	serverCPUProfileFinished := false
	defer func() {
		cancelServerCPUProfile()
		if !serverCPUProfileFinished {
			_ = serverCPUProfileDone()
		}
	}()

	var result runResult
	var runErr error
	if cfg.backlogBeforeWorkers {
		result, workers, runErr = runBacklogLoad(ctx, c, cfg, backlogBaseline)
		if runErr != nil {
			runErr = fmt.Errorf("run persisted task backlog load: %w", runErr)
		}
	} else {
		result, runErr = runLoad(ctx, c, cfg)
		if runErr != nil {
			runErr = fmt.Errorf("run load: %w", runErr)
		}
	}

	cpuProfileErr := stopCPUProfile()
	cpuProfileStopped = true
	stopWorkers(workers)
	workers = nil
	if runErr != nil {
		cancelServerCPUProfile()
	}
	serverCPUProfileErr := serverCPUProfileDone()
	serverCPUProfileFinished = true

	cleanupCtx, cancelCleanup := context.WithTimeout(
		context.WithoutCancel(ctx),
		postRunCleanupTimeout,
	)
	defer cancelCleanup()

	return finalizeRunResult(
		cleanupCtx,
		cfg,
		&result,
		runErr,
		cpuProfileErr,
		serverCPUProfileErr,
	)
}

func finalizeRunResult(
	ctx context.Context,
	cfg runConfig,
	result *runResult,
	runErr error,
	cpuProfileErr error,
	serverCPUProfileErr error,
) error {
	var resultErrs []error
	failedProfiles := make(map[string]error)
	if runErr != nil {
		resultErrs = append(resultErrs, runErr)
	}
	if cpuProfileErr != nil {
		resultErrs = append(resultErrs, fmt.Errorf("write CPU profile: %w", cpuProfileErr))
		recordFailedProfile(failedProfiles, cfg.cpuProfile, cpuProfileErr)
		result.CPUProfile = ""
	}
	if serverCPUProfileErr != nil {
		resultErrs = append(resultErrs, fmt.Errorf("write server CPU profile: %w", serverCPUProfileErr))
		recordFailedProfile(failedProfiles, cfg.serverCPU, serverCPUProfileErr)
		result.ServerCPUProfile = ""
		result.ServerCPUProfileDuration = 0
	}
	if err := writeHeapProfile(cfg.heapProfile); err != nil {
		resultErrs = append(resultErrs, fmt.Errorf("write heap profile: %w", err))
		recordFailedProfile(failedProfiles, cfg.heapProfile, err)
		result.HeapProfile = ""
	}
	if err := writeServerHeapProfile(ctx, cfg); err != nil {
		resultErrs = append(resultErrs, fmt.Errorf("write server heap profile: %w", err))
		recordFailedProfile(failedProfiles, cfg.serverHeap, err)
		result.ServerHeapProfile = ""
	}
	postSnapshotErr := writeMetricSnapshots(ctx, cfg.metricSnapshotsAfter)
	if postSnapshotErr != nil {
		resultErrs = append(resultErrs, fmt.Errorf("write post-run metric snapshots: %w", postSnapshotErr))
		result.MetricSnapshotsAfter = nil
	} else {
		var err error
		result.PreparedStats, err = readScyllaPreparedStatementMetrics(
			cfg.metricSnapshotsBefore,
			cfg.metricSnapshotsAfter,
		)
		if err != nil {
			resultErrs = append(resultErrs, fmt.Errorf("read Scylla prepared statement metrics: %w", err))
		}
	}
	profileSummaries, skippedSummaryErr := filterFailedProfileSummaries(
		cfg.profileSummaries,
		failedProfiles,
	)
	if skippedSummaryErr != nil {
		resultErrs = append(resultErrs, skippedSummaryErr)
	}
	result.ProfileSummaries = profileSummaries.paths()
	if err := writeProfileSummaries(ctx, profileSummaries); err != nil {
		resultErrs = append(resultErrs, fmt.Errorf("write profile summaries: %w", err))
		result.ProfileSummaries = nil
	}
	if err := writeResultFile(cfg.resultFile, *result); err != nil {
		resultErrs = append(resultErrs, fmt.Errorf("write result file: %w", err))
		result.ResultFile = ""
	}
	if err := writeResult(*result); err != nil {
		resultErrs = append(resultErrs, fmt.Errorf("write result: %w", err))
	}
	if result.Failed > 0 || result.Completed != int64(cfg.workflows) {
		resultErrs = append(resultErrs, fmt.Errorf(
			"load completed %d of %d workflows (%d failed)",
			result.Completed,
			cfg.workflows,
			result.Failed,
		))
	}
	return errors.Join(resultErrs...)
}

func registerFlags(flags *flag.FlagSet, cfg *runConfig) {
	flags.StringVar(&cfg.address, "address", "127.0.0.1:7233", "Temporal frontend host:port")
	flags.StringVar(&cfg.namespace, "namespace", "scylla-load", "Temporal namespace")
	flags.StringVar(&cfg.taskQueue, "task-queue", "scylla-load", "Temporal task queue")
	flags.IntVar(&cfg.taskQueues, "task-queues", 1, "number of task queues to distribute workflow starts across")
	flags.IntVar(&cfg.workersPerTaskQueue, "workers-per-task-queue", 1, "workers to start for each task queue")
	flags.IntVar(&cfg.workflows, "workflows", 1000, "number of workflows to execute")
	flags.IntVar(&cfg.concurrency, "concurrency", 100, "maximum concurrent workflow executions")
	flags.Float64Var(&cfg.targetRPS, "target-workflows-per-second", 0, "pace workflow starts at this rate; zero submits as fast as possible")
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
	flags.StringVar(&cfg.serverCPU, "server-cpu-profile", "", "write a Temporal server CPU profile starting with the run")
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
	if cfg.targetRPS < 0 {
		return errors.New("-target-workflows-per-second must be non-negative")
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
	if cfg.timeout <= 0 {
		return errors.New("-timeout must be positive")
	}
	if cfg.eagerStart && !cfg.runWorker {
		return errors.New("-worker must be enabled when -eager-start is set")
	}
	if err := validateBacklogConfig(cfg); err != nil {
		return err
	}
	if cfg.serverCPU != "" && cfg.serverPProf == "" {
		return errors.New("-server-pprof must be set when -server-cpu-profile is set")
	}
	if cfg.serverHeap != "" && cfg.serverPProf == "" {
		return errors.New("-server-pprof must be set when -server-heap-profile is set")
	}
	if cfg.serverCPU != "" {
		if _, err := serverCPUProfileSeconds(cfg.serverCPUTime); err != nil {
			return err
		}
	}
	if err := validateMetricSnapshotURLs("-metrics-snapshot-before", cfg.metricSnapshotsBefore); err != nil {
		return err
	}
	if err := validateMetricSnapshotURLs("-metrics-snapshot-after", cfg.metricSnapshotsAfter); err != nil {
		return err
	}
	return validateOutputPaths(cfg)
}

func validateBacklogConfig(cfg runConfig) error {
	if !cfg.backlogBeforeWorkers {
		return nil
	}
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
	return nil
}

func loadSignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

func configuredServerCPUProfileDuration(cfg runConfig) time.Duration {
	if cfg.serverCPU == "" {
		return 0
	}
	return cfg.serverCPUTime
}

func validateMetricSnapshotURLs(name string, snapshots metricSnapshotFlags) error {
	seen := make(map[string]int, len(snapshots))
	for index, snapshot := range snapshots {
		if previous, ok := seen[snapshot.url]; ok {
			return fmt.Errorf(
				"%s[%d] duplicates URL %q from %s[%d]",
				name,
				index,
				snapshot.url,
				name,
				previous,
			)
		}
		seen[snapshot.url] = index
	}
	return nil
}

func validateOutputPaths(cfg runConfig) error {
	outputs := []struct {
		name string
		path string
	}{
		{name: "-cpu-profile", path: cfg.cpuProfile},
		{name: "-heap-profile", path: cfg.heapProfile},
		{name: "-server-cpu-profile", path: cfg.serverCPU},
		{name: "-server-heap-profile", path: cfg.serverHeap},
		{name: "-result-file", path: cfg.resultFile},
		{name: "-run-metadata-file", path: cfg.runMetadataFile},
	}
	for i, snapshot := range cfg.metricSnapshotsBefore {
		outputs = append(outputs, struct {
			name string
			path string
		}{
			name: fmt.Sprintf("-metrics-snapshot-before[%d]", i),
			path: snapshot.path,
		})
	}
	for i, snapshot := range cfg.metricSnapshotsAfter {
		outputs = append(outputs, struct {
			name string
			path string
		}{
			name: fmt.Sprintf("-metrics-snapshot-after[%d]", i),
			path: snapshot.path,
		})
	}
	for i, summary := range cfg.profileSummaries {
		outputs = append(outputs, struct {
			name string
			path string
		}{
			name: fmt.Sprintf("-profile-summary[%d]", i),
			path: summary.path,
		})
	}

	seen := make(map[string]string, len(outputs))
	for _, output := range outputs {
		if output.path == "" {
			continue
		}
		path, err := filepath.Abs(output.path)
		if err != nil {
			return fmt.Errorf("resolve %s output path: %w", output.name, err)
		}
		if previous, ok := seen[path]; ok {
			return fmt.Errorf(
				"%s and %s resolve to the same output path %q",
				previous,
				output.name,
				path,
			)
		}
		seen[path] = output.name
	}

	generatedProfiles := make(map[string]struct{}, 4)
	for _, path := range []string{
		cfg.cpuProfile,
		cfg.heapProfile,
		cfg.serverCPU,
		cfg.serverHeap,
	} {
		if path == "" {
			continue
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("resolve generated profile path %q: %w", path, err)
		}
		generatedProfiles[absolute] = struct{}{}
	}
	for i, summary := range cfg.profileSummaries {
		path, err := filepath.Abs(summary.profile)
		if err != nil {
			return fmt.Errorf("resolve -profile-summary[%d] profile input path: %w", i, err)
		}
		output, isOutput := seen[path]
		_, isGeneratedProfile := generatedProfiles[path]
		if isOutput && !isGeneratedProfile {
			return fmt.Errorf(
				"-profile-summary[%d] profile input and %s output resolve to the same path %q",
				i,
				output,
				path,
			)
		}
	}
	return nil
}

func recordFailedProfile(failed map[string]error, path string, err error) {
	if path == "" || err == nil {
		return
	}
	failed[normalizedOutputPath(path)] = err
}

func filterFailedProfileSummaries(
	summaries profileSummaryFlags,
	failedProfiles map[string]error,
) (profileSummaryFlags, error) {
	filtered := make(profileSummaryFlags, 0, len(summaries))
	var errs []error
	for _, summary := range summaries {
		producerErr, failed := failedProfiles[normalizedOutputPath(summary.profile)]
		if !failed {
			filtered = append(filtered, summary)
			continue
		}
		errs = append(errs, fmt.Errorf(
			"skip profile summary %s because profile %s failed: %w",
			summary.path,
			summary.profile,
			producerErr,
		))
	}
	return filtered, errors.Join(errs...)
}

func normalizedOutputPath(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return absolute
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

func runLoad(ctx context.Context, c client.Client, cfg runConfig) (runResult, error) {
	return runLoadWithRunner(ctx, c, cfg, runOneWorkflow)
}

type (
	workflowRunner  func(context.Context, client.Client, runConfig, []byte, int64, int) workflowRunResult
	workflowStarter func(context.Context, client.Client, runConfig, []byte, int64, int) (client.WorkflowRun, error)
	backlogCounter  func(context.Context, client.Client, runConfig) ([]int64, error)
	backlogWaiter   func(context.Context, client.Client, runConfig, []int64, []int64) (int64, error)
	workerStarter   func(client.Client, runConfig) ([]worker.Worker, error)
)

type workflowRunResult struct {
	completed bool
	requests  int64
	run       client.WorkflowRun
	latency   time.Duration
}

type backlogEnqueueResult struct {
	runs                []client.WorkflowRun
	enqueuedByTaskQueue []int64
	elapsed             time.Duration
	enqueued            int64
	requests            int64
}

func runLoadWithRunner(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
	runner workflowRunner,
) (runResult, error) {
	payload := makePayload(cfg.payloadBytes)
	start := time.Now()
	var completed atomic.Int64
	var failed atomic.Int64
	var requests atomic.Int64
	sem := make(chan struct{}, cfg.concurrency)
	var wg sync.WaitGroup
	var incompleteMu sync.Mutex
	var incomplete []client.WorkflowRun
	var latenciesMu sync.Mutex
	latencies := make([]time.Duration, 0, cfg.workflows)

	launched := 0
launch:
	for i := 0; i < cfg.workflows; i++ {
		if err := waitForLaunchSlot(ctx, start, cfg.targetRPS, i); err != nil {
			break launch
		}
		select {
		case <-ctx.Done():
			break launch
		case sem <- struct{}{}:
		}
		launched++
		workflowIndex := i
		wg.Go(func() {
			defer func() { <-sem }()
			workflowStart := time.Now()
			runResult := runner(ctx, c, cfg, payload, start.UnixNano(), workflowIndex)
			runResult.latency = time.Since(workflowStart)
			requests.Add(runResult.requests)
			if runResult.completed {
				completed.Add(1)
				latenciesMu.Lock()
				latencies = append(latencies, runResult.latency)
				latenciesMu.Unlock()
			} else {
				failed.Add(1)
				if runResult.run != nil {
					incompleteMu.Lock()
					incomplete = append(incomplete, runResult.run)
					incompleteMu.Unlock()
				}
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
	result.TargetWorkflowsPerSec = cfg.targetRPS
	result.WorkflowLatencyP50, result.WorkflowLatencyP95, result.WorkflowLatencyP99 = workflowLatencyPercentiles(latencies)
	if elapsed > 0 {
		result.WorkflowsPerSec = float64(result.Completed) / elapsed.Seconds()
		result.RequestsPerSec = float64(result.Requests) / elapsed.Seconds()
	}
	return result, terminateIncompleteWorkflowRuns(ctx, c, incomplete, cfg.concurrency)
}

func waitForLaunchSlot(ctx context.Context, started time.Time, targetRPS float64, index int) error {
	if targetRPS == 0 || index == 0 {
		return ctx.Err()
	}
	due := started.Add(time.Duration(float64(index) / targetRPS * float64(time.Second)))
	wait := time.Until(due)
	if wait <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func workflowLatencyPercentiles(latencies []time.Duration) (p50 time.Duration, p95 time.Duration, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	ordered := slices.Clone(latencies)
	slices.Sort(ordered)
	percentile := func(p float64) time.Duration {
		index := int(float64(len(ordered))*p+0.999999999) - 1
		return ordered[max(0, min(index, len(ordered)-1))]
	}
	return percentile(0.50), percentile(0.95), percentile(0.99)
}

func newRunResult(cfg runConfig) runResult {
	return runResult{
		Address:                  cfg.address,
		Namespace:                cfg.namespace,
		TaskQueue:                cfg.taskQueue,
		TaskQueues:               cfg.taskQueues,
		WorkersPerTaskQueue:      cfg.workersPerTaskQueue,
		Workflows:                cfg.workflows,
		Concurrency:              cfg.concurrency,
		TargetWorkflowsPerSec:    cfg.targetRPS,
		ActivitiesEach:           cfg.activitiesEach,
		SignalsEach:              cfg.signalsEach,
		EagerStart:               cfg.eagerStart,
		EagerActivities:          cfg.eagerActivities,
		PayloadBytes:             cfg.payloadBytes,
		BacklogBeforeWorkers:     cfg.backlogBeforeWorkers,
		CPUProfile:               cfg.cpuProfile,
		HeapProfile:              cfg.heapProfile,
		ServerCPUProfile:         cfg.serverCPU,
		ServerCPUProfileDuration: configuredServerCPUProfileDuration(cfg),
		ServerHeapProfile:        cfg.serverHeap,
		MetricSnapshotsBefore:    cfg.metricSnapshotsBefore.paths(),
		MetricSnapshotsAfter:     cfg.metricSnapshotsAfter.paths(),
		ProfileSummaries:         cfg.profileSummaries.paths(),
		ResultFile:               cfg.resultFile,
		RunMetadataFile:          cfg.runMetadataFile,
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
	result.run = run

	for signalIndex := 0; signalIndex < cfg.signalsEach; signalIndex++ {
		result.requests++
		if err := c.SignalWorkflow(ctx, workflowID, run.GetRunID(), signalName, payload); err != nil {
			log.Printf("signal workflow %s: %v", workflowID, err)
			return result
		}
	}

	if err := run.Get(ctx, nil); err != nil {
		log.Printf("workflow %s failed: %v", workflowID, err)
		var executionErr *temporal.WorkflowExecutionError
		if errors.As(err, &executionErr) {
			result.run = nil
		}
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
	executionTimeout, err := remainingWorkflowExecutionTimeout(ctx, cfg.timeout)
	if err != nil {
		return nil, err
	}
	return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       workflowID(startNanos, workflowIndex),
		TaskQueue:                taskQueueName(cfg, workflowIndex%cfg.taskQueues),
		WorkflowExecutionTimeout: executionTimeout,
		EnableEagerStart:         cfg.eagerStart,
	}, loadWorkflow, workflowInput{
		Activities: cfg.activitiesEach,
		Signals:    cfg.signalsEach,
		Eager:      cfg.eagerActivities,
		Payload:    payload,
	})
}

func remainingWorkflowExecutionTimeout(ctx context.Context, configured time.Duration) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return configured, nil
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0, context.DeadlineExceeded
	}
	return min(configured, remaining), nil
}

func workflowID(startNanos int64, workflowIndex int) string {
	return fmt.Sprintf("scylla-load-%d-%d", startNanos, workflowIndex)
}

func runBacklogLoad(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
	baseline []int64,
) (runResult, []worker.Worker, error) {
	return runBacklogLoadWithDependencies(
		ctx,
		c,
		cfg,
		startOneWorkflow,
		func(context.Context, client.Client, runConfig) ([]int64, error) {
			return slices.Clone(baseline), nil
		},
		waitForWorkflowTaskBacklog,
		startWorker,
	)
}

func runBacklogLoadWithDependencies(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
	starter workflowStarter,
	counter backlogCounter,
	waiter backlogWaiter,
	startWorkers workerStarter,
) (runResult, []worker.Worker, error) {
	result := newRunResult(cfg)
	start := time.Now()
	baseline, err := counter(ctx, c, cfg)
	if err != nil {
		finishBacklogResult(&result, cfg, start)
		return result, nil, fmt.Errorf("read workflow task backlog baseline: %w", err)
	}
	if err := requireEmptyWorkflowTaskBacklog(baseline); err != nil {
		finishBacklogResult(&result, cfg, start)
		return result, nil, err
	}
	enqueueStart := time.Now()
	enqueueResult := enqueueWorkflowBacklog(ctx, c, cfg, starter, enqueueStart)
	result.EnqueueElapsed = enqueueResult.elapsed
	result.Enqueued = enqueueResult.enqueued
	result.EnqueueFailed = int64(cfg.workflows) - result.Enqueued
	result.Requests = enqueueResult.requests
	if result.EnqueueElapsed > 0 {
		result.EnqueueRequestsPerSec = float64(result.Requests) / result.EnqueueElapsed.Seconds()
	}

	backlogWaitStart := time.Now()
	backlogTasks, err := waiter(
		ctx,
		c,
		cfg,
		baseline,
		enqueueResult.enqueuedByTaskQueue,
	)
	result.BacklogWaitElapsed = time.Since(backlogWaitStart)
	result.BacklogTasks = backlogTasks
	if err != nil {
		terminationErr := terminateIncompleteWorkflowRuns(
			ctx,
			c,
			enqueueResult.runs,
			cfg.concurrency,
		)
		finishBacklogResult(&result, cfg, start)
		return result, nil, errors.Join(err, terminationErr)
	}

	drainStart := time.Now()
	workerStart := time.Now()
	workers, err := startWorkers(c, cfg)
	result.WorkerStartElapsed = time.Since(workerStart)
	if err != nil {
		stopWorkers(workers)
		terminationErr := terminateIncompleteWorkflowRuns(
			ctx,
			c,
			enqueueResult.runs,
			cfg.concurrency,
		)
		finishBacklogResult(&result, cfg, start)
		return result, nil, errors.Join(err, terminationErr)
	}

	completed, incomplete := drainWorkflowBacklog(ctx, enqueueResult.runs, cfg.concurrency)
	result.DrainElapsed = time.Since(drainStart)
	result.Completed = completed
	terminationErr := terminateIncompleteWorkflowRuns(ctx, c, incomplete, cfg.concurrency)
	finishBacklogResult(&result, cfg, start)
	return result, workers, terminationErr
}

func finishBacklogResult(result *runResult, cfg runConfig, start time.Time) {
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
	enqueuedByTaskQueue := make([]int64, cfg.taskQueues)
	for index, run := range runs {
		if run != nil {
			enqueuedByTaskQueue[index%cfg.taskQueues]++
		}
	}
	return backlogEnqueueResult{
		runs:                runs,
		enqueuedByTaskQueue: enqueuedByTaskQueue,
		elapsed:             time.Since(start),
		enqueued:            enqueued.Load(),
		requests:            requests.Load(),
	}
}

func drainWorkflowBacklog(
	ctx context.Context,
	runs []client.WorkflowRun,
	concurrency int,
) (int64, []client.WorkflowRun) {
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var completed atomic.Int64
	incomplete := make([]atomic.Bool, len(runs))
	for index, run := range runs {
		incomplete[index].Store(run != nil)
	}

drain:
	for index, run := range runs {
		if run == nil {
			continue
		}
		select {
		case <-ctx.Done():
			break drain
		case sem <- struct{}{}:
		}
		runIndex := index
		workflowRun := run
		wg.Go(func() {
			defer func() { <-sem }()
			if err := workflowRun.Get(ctx, nil); err != nil {
				log.Printf("workflow %s failed: %v", workflowRun.GetID(), err)
				var executionErr *temporal.WorkflowExecutionError
				if errors.As(err, &executionErr) {
					incomplete[runIndex].Store(false)
				}
				return
			}
			incomplete[runIndex].Store(false)
			completed.Add(1)
		})
	}
	wg.Wait()
	incompleteRuns := make([]client.WorkflowRun, 0, len(runs)-int(completed.Load()))
	for index, run := range runs {
		if incomplete[index].Load() {
			incompleteRuns = append(incompleteRuns, run)
		}
	}
	return completed.Load(), incompleteRuns
}

func terminateIncompleteWorkflowRuns(
	ctx context.Context,
	c client.Client,
	runs []client.WorkflowRun,
	concurrency int,
) error {
	runCount := 0
	for _, run := range runs {
		if run != nil {
			runCount++
		}
	}
	if runCount == 0 {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		postRunCleanupTimeout,
	)
	defer cancel()

	sem := make(chan struct{}, min(max(concurrency, 1), runCount))
	var wg sync.WaitGroup
	var errsMu sync.Mutex
	var errs []error
terminate:
	for _, run := range runs {
		if run == nil {
			continue
		}
		select {
		case <-cleanupCtx.Done():
			errsMu.Lock()
			errs = append(errs, cleanupCtx.Err())
			errsMu.Unlock()
			break terminate
		case sem <- struct{}{}:
		}
		workflowRun := run
		wg.Go(func() {
			defer func() { <-sem }()
			if err := c.TerminateWorkflow(
				cleanupCtx,
				workflowRun.GetID(),
				workflowRun.GetRunID(),
				incompleteWorkflowTerminationReason,
			); err != nil {
				errsMu.Lock()
				errs = append(errs, fmt.Errorf(
					"terminate workflow %s: %w",
					workflowRun.GetID(),
					err,
				))
				errsMu.Unlock()
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("terminate incomplete workflows: %w", err)
	}
	return nil
}

func waitForWorkflowTaskBacklog(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
	baseline []int64,
	expected []int64,
) (int64, error) {
	if len(baseline) != cfg.taskQueues || len(expected) != cfg.taskQueues {
		return 0, fmt.Errorf(
			"workflow task backlog counts have %d baseline and %d expected queues, want %d",
			len(baseline),
			len(expected),
			cfg.taskQueues,
		)
	}
	expectedTotal := sumInt64(expected)
	if expectedTotal == 0 {
		return 0, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, cfg.backlogWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var confirmed int64
	for {
		backlog, err := workflowTaskBacklogCounts(waitCtx, c, cfg)
		if err != nil {
			return confirmed, err
		}
		confirmed = confirmedWorkflowTaskBacklog(baseline, expected, backlog)
		if confirmed == expectedTotal {
			return confirmed, nil
		}

		select {
		case <-waitCtx.Done():
			return confirmed, fmt.Errorf(
				"wait for workflow task backlog: confirmed %d of %d tasks: %w",
				confirmed,
				expectedTotal,
				waitCtx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func workflowTaskBacklogCounts(
	ctx context.Context,
	c client.Client,
	cfg runConfig,
) ([]int64, error) {
	backlog := make([]int64, cfg.taskQueues)
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
		backlog[taskQueueIndex] = response.GetStats().GetApproximateBacklogCount()
	}
	return backlog, nil
}

func confirmedWorkflowTaskBacklog(baseline []int64, expected []int64, backlog []int64) int64 {
	var confirmed int64
	for index := range expected {
		if index >= len(baseline) || index >= len(backlog) {
			break
		}
		delta := max(backlog[index]-baseline[index], 0)
		confirmed += min(delta, expected[index])
	}
	return confirmed
}

func requireEmptyWorkflowTaskBacklog(backlog []int64) error {
	var occupied []string
	for index, count := range backlog {
		if count != 0 {
			occupied = append(occupied, fmt.Sprintf("%d=%d", index, count))
		}
	}
	if len(occupied) != 0 {
		return fmt.Errorf(
			"workflow task backlog baseline must be empty; task queue indexes with tasks: %s",
			strings.Join(occupied, ", "),
		)
	}
	return nil
}

func sumInt64(values []int64) int64 {
	var total int64
	for _, value := range values {
		total += value
	}
	return total
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
	return writeOutputFile(path, func(f *os.File) error {
		encoder := json.NewEncoder(f)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	})
}

func writeRunMetadata(ctx context.Context, cfg runConfig) error {
	if cfg.runMetadataFile == "" {
		return nil
	}
	metadata := runMetadata{
		StartedAt:                time.Now().UTC(),
		Address:                  cfg.address,
		Namespace:                cfg.namespace,
		TaskQueue:                cfg.taskQueue,
		TaskQueues:               cfg.taskQueues,
		WorkersPerTaskQueue:      cfg.workersPerTaskQueue,
		Workflows:                cfg.workflows,
		Concurrency:              cfg.concurrency,
		TargetWorkflowsPerSec:    cfg.targetRPS,
		ActivitiesEach:           cfg.activitiesEach,
		SignalsEach:              cfg.signalsEach,
		EagerStart:               cfg.eagerStart,
		EagerActivities:          cfg.eagerActivities,
		PayloadBytes:             cfg.payloadBytes,
		BacklogBeforeWorkers:     cfg.backlogBeforeWorkers,
		BacklogWaitTimeout:       cfg.backlogWaitTimeout,
		ServerCPUProfileDuration: configuredServerCPUProfileDuration(cfg),
		GoVersion:                runtime.Version(),
		GOOS:                     runtime.GOOS,
		GOARCH:                   runtime.GOARCH,
		NumCPU:                   runtime.NumCPU(),
		GOMAXPROCS:               runtime.GOMAXPROCS(0),
		Environment:              selectedEnvironment(),
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
	return writeOutputFile(cfg.runMetadataFile, func(f *os.File) error {
		encoder := json.NewEncoder(f)
		encoder.SetIndent("", "  ")
		return encoder.Encode(metadata)
	})
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

func startCPUProfile(path string) (func() error, error) {
	return startCPUProfileWithWriter(path, nil)
}

func startCPUProfileWithWriter(
	path string,
	decorate func(io.Writer) io.Writer,
) (func() error, error) {
	if path == "" {
		return func() error { return nil }, nil
	}
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return nil, err
	}
	tempPath := f.Name()
	var destination io.Writer = f
	if decorate != nil {
		destination = decorate(destination)
	}
	profileWriter := &errorTrackingWriter{writer: destination}
	if err := pprof.StartCPUProfile(profileWriter); err != nil {
		return nil, errors.Join(err, f.Close(), os.Remove(tempPath))
	}
	return func() error {
		pprof.StopCPUProfile()
		if err := errors.Join(profileWriter.Err(), f.Close()); err != nil {
			_ = os.Remove(tempPath)
			return err
		}
		if err := os.Rename(tempPath, path); err != nil {
			_ = os.Remove(tempPath)
			return err
		}
		return nil
	}, nil
}

type errorTrackingWriter struct {
	mu     sync.Mutex
	writer io.Writer
	err    error
}

func (w *errorTrackingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if n != len(data) && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.mu.Lock()
		if w.err == nil {
			w.err = err
		}
		w.mu.Unlock()
	}
	return n, err
}

func (w *errorTrackingWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func writeHeapProfile(path string) error {
	if path == "" {
		return nil
	}
	return writeOutputFile(path, func(f *os.File) error {
		return pprof.WriteHeapProfile(f)
	})
}

func startServerCPUProfile(ctx context.Context, cfg runConfig) (func() error, error) {
	if cfg.serverCPU == "" {
		return func() error { return nil }, nil
	}
	seconds, err := serverCPUProfileSeconds(cfg.serverCPUTime)
	if err != nil {
		return nil, err
	}
	profileURL, err := serverPProfURL(cfg.serverPProf, "/debug/pprof/profile", map[string]string{
		"seconds": strconv.FormatInt(seconds, 10),
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
	var errs []error
	for index := range snapshots {
		snapshot := &snapshots[index]
		snapshot.captureStartedAt = time.Now()
		snapshot.captureFinishedAt = time.Time{}
		if err := fetchProfile(ctx, snapshot.url, snapshot.path); err != nil {
			errs = append(errs, fmt.Errorf(
				"fetch %s to %s: %w",
				snapshot.url,
				snapshot.path,
				err,
			))
		}
		snapshot.captureFinishedAt = time.Now()
	}
	return errors.Join(errs...)
}

func writeProfileSummaries(ctx context.Context, summaries []profileSummary) error {
	var errs []error
	for _, summary := range summaries {
		if err := writeProfileSummary(ctx, summary); err != nil {
			errs = append(errs, fmt.Errorf(
				"summarize %s to %s: %w",
				summary.profile,
				summary.path,
				err,
			))
		}
	}
	return errors.Join(errs...)
}

func writeProfileSummary(ctx context.Context, summary profileSummary) error {
	cmd := exec.CommandContext(ctx, "go", "tool", "pprof", "-top", summary.profile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("summarize profile %s: %w: %s", summary.profile, err, strings.TrimSpace(string(output)))
	}
	return writeOutputFile(summary.path, func(f *os.File) error {
		_, err := f.Write(output)
		return err
	})
}

func serverCPUProfileSeconds(duration time.Duration) (int64, error) {
	if duration <= 0 {
		return 0, errors.New("-server-cpu-profile-duration must be positive")
	}
	if duration%time.Second != 0 {
		return 0, errors.New("-server-cpu-profile-duration must be a whole number of seconds")
	}
	return int64(duration / time.Second), nil
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
	return writeOutputFile(outputPath, func(f *os.File) error {
		_, err := io.Copy(f, resp.Body)
		return err
	})
}

func writeOutputFile(path string, write func(*os.File) error) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := f.Name()
	defer func() {
		_ = os.Remove(tempPath)
	}()
	if err := write(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
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
