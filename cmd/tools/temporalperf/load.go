package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	loadSignalName                      = "temporal-perf-signal"
	incompleteWorkflowTerminationReason = "temporalperf run ended before workflow completion"
	postRunCleanupTimeout               = 30 * time.Second
)

type (
	loadConfig struct {
		taskQueue           string
		taskQueues          int
		workersPerTaskQueue int
		workflows           int
		concurrency         int
		targetRPS           float64
		launchDuration      time.Duration
		activitiesEach      int
		signalsEach         int
		payloadBytes        int
		timeout             time.Duration
	}

	loadWorkflowInput struct {
		Activities int
		Signals    int
		Payload    []byte
	}

	loadMetadata struct {
		StartedAt           time.Time `json:"startedAt"`
		Address             string    `json:"address"`
		Namespace           string    `json:"namespace"`
		TaskQueue           string    `json:"taskQueue"`
		TaskQueues          int       `json:"taskQueues"`
		WorkersPerTaskQueue int       `json:"workersPerTaskQueue"`
		Workflows           int       `json:"workflows"`
		Concurrency         int       `json:"concurrency"`
		TargetRPS           float64   `json:"targetWorkflowsPerSec,omitempty"`
		ActivitiesEach      int       `json:"activitiesEach"`
		SignalsEach         int       `json:"signalsEach"`
		PayloadBytes        int       `json:"payloadBytes"`
		GoVersion           string    `json:"goVersion"`
		GOOS                string    `json:"goos"`
		GOARCH              string    `json:"goarch"`
		NumCPU              int       `json:"numCpu"`
		GOMAXPROCS          int       `json:"gomaxprocs"`
	}

	workflowRunResult struct {
		completed bool
		requests  int64
		run       client.WorkflowRun
		latency   time.Duration
		err       error
	}

	workflowRunner func(context.Context, client.Client, loadConfig, []byte, int64, int) workflowRunResult

	loadRunState struct {
		completed atomic.Int64
		failed    atomic.Int64
		requests  atomic.Int64

		incompleteMu    sync.Mutex
		incomplete      []client.WorkflowRun
		latenciesMu     sync.Mutex
		latencies       []time.Duration
		completionTimes []time.Time
		firstComplete   time.Time
		lastComplete    time.Time
		errs            []error
	}

	loadCompletionError struct {
		completed int64
		expected  int
		failed    int64
	}

	loadSaturationError struct {
		launched int
		expected int
	}

	nopLogger struct{}
)

func (e *loadCompletionError) Error() string {
	return fmt.Sprintf(
		"load completed %d of %d workflows (%d failed)",
		e.completed,
		e.expected,
		e.failed,
	)
}

func (e *loadSaturationError) Error() string {
	return fmt.Sprintf("load launched %d of %d workflows during the measurement window", e.launched, e.expected)
}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

func (s *loadRunState) record(index int, result workflowRunResult, loadLog *log.Logger) {
	s.requests.Add(result.requests)
	if result.completed {
		s.completed.Add(1)
		s.latenciesMu.Lock()
		s.latencies = append(s.latencies, result.latency)
		completedAt := time.Now()
		s.completionTimes = append(s.completionTimes, completedAt)
		if s.firstComplete.IsZero() || completedAt.Before(s.firstComplete) {
			s.firstComplete = completedAt
		}
		if completedAt.After(s.lastComplete) {
			s.lastComplete = completedAt
		}
		s.latenciesMu.Unlock()
		return
	}
	s.failed.Add(1)
	if result.err != nil {
		loadLog.Printf("workflow %d: %v", index, result.err)
		s.incompleteMu.Lock()
		s.errs = append(s.errs, result.err)
		s.incompleteMu.Unlock()
	}
	if result.run != nil {
		s.incompleteMu.Lock()
		s.incomplete = append(s.incomplete, result.run)
		s.incompleteMu.Unlock()
	}
}

func runTemporalSample(
	ctx context.Context,
	cfg config,
	warmup runRequest,
	request runRequest,
	dir string,
) (runRecord, error) {
	taskQueue := loadTaskQueue(request)
	c, err := client.DialContext(ctx, client.Options{
		HostPort: cfg.address, Namespace: cfg.namespace, Logger: nopLogger{},
	})
	if err != nil {
		return runRecord{}, fmt.Errorf("dial Temporal: %w", err)
	}
	defer c.Close()
	if err := ensureLoadNamespace(ctx, c, cfg.namespace); err != nil {
		return runRecord{}, fmt.Errorf("register namespace: %w", err)
	}

	workers, err := startLoadWorkers(c, loadConfigForRequest(cfg, request, taskQueue))
	if err != nil {
		return runRecord{}, fmt.Errorf("start workers: %w", err)
	}
	defer stopLoadWorkers(workers)

	warmupRecord, err := runTemporalPhase(ctx, c, cfg, warmup, taskQueue, dir, "warmup")
	if err != nil {
		return warmupRecord, fmt.Errorf("warmup: %w", err)
	}
	return runTemporalPhase(ctx, c, cfg, request, taskQueue, dir, "measure")
}

func runTemporalPhase(
	ctx context.Context,
	c client.Client,
	cfg config,
	request runRequest,
	taskQueue string,
	dir string,
	phase string,
) (record runRecord, retErr error) {
	resultPath := filepath.Join(dir, phase+".result.json")
	metadataPath := filepath.Join(dir, phase+".metadata.json")
	logPath := filepath.Join(dir, phase+".log")
	loadCfg := loadConfigForRequest(cfg, request, taskQueue)
	record = runRecord{
		Profile: request.profile, PayloadBytes: request.payloadBytes, Stage: request.stage,
		TargetRatio: request.targetRatio, TargetRPS: request.targetRPS, Trial: request.trial,
		ResultFile: resultPath, MetadataFile: metadataPath,
	}

	logFile, err := os.Create(logPath)
	if err != nil {
		return record, fmt.Errorf("create load log: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, logFile.Close())
	}()
	loadLog := log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)

	metadata := loadMetadata{
		StartedAt: time.Now().UTC(), Address: cfg.address, Namespace: cfg.namespace,
		TaskQueue: taskQueue, TaskQueues: cfg.taskQueues,
		WorkersPerTaskQueue: cfg.workersPerTaskQueue, Workflows: request.workflows,
		Concurrency: cfg.concurrency, TargetRPS: request.targetRPS,
		ActivitiesEach: request.profile.Activities, SignalsEach: request.profile.Signals,
		PayloadBytes: request.payloadBytes, GoVersion: runtime.Version(), GOOS: runtime.GOOS,
		GOARCH: runtime.GOARCH, NumCPU: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	if err := writeJSON(metadataPath, metadata); err != nil {
		return record, fmt.Errorf("write load metadata: %w", err)
	}

	phaseCtx, cancelPhase := context.WithTimeout(ctx, cfg.sampleTimeout)
	defer cancelPhase()
	record.Result, err = executeTemporalLoad(phaseCtx, c, loadCfg, loadLog)
	if writeErr := writeJSON(resultPath, record.Result); writeErr != nil {
		err = errors.Join(err, fmt.Errorf("write load result: %w", writeErr))
	}
	if _, writeErr := fmt.Fprintf(
		logFile,
		"workflows=%d completed=%d failed=%d throughput=%.2f workflows/s elapsed=%s launch_elapsed=%s\n",
		record.Result.Workflows,
		record.Result.Completed,
		record.Result.Failed,
		record.Result.WorkflowsPerSec,
		record.Result.Elapsed,
		record.Result.LaunchElapsed,
	); writeErr != nil {
		err = errors.Join(err, fmt.Errorf("write load summary: %w", writeErr))
	}
	return record, err
}

func loadConfigForRequest(cfg config, request runRequest, taskQueue string) loadConfig {
	return loadConfig{
		taskQueue: taskQueue, taskQueues: cfg.taskQueues,
		workersPerTaskQueue: cfg.workersPerTaskQueue, workflows: request.workflows,
		concurrency: cfg.concurrency, targetRPS: request.targetRPS,
		launchDuration: request.duration, activitiesEach: request.profile.Activities,
		signalsEach: request.profile.Signals, payloadBytes: request.payloadBytes,
		timeout: cfg.sampleTimeout,
	}
}

func loadTaskQueue(request runRequest) string {
	return fmt.Sprintf(
		"perf-%s-p%d-%s-r%d",
		request.profile.Name,
		request.payloadBytes,
		request.stage,
		request.trial,
	)
}

func executeTemporalLoad(
	ctx context.Context,
	c client.Client,
	cfg loadConfig,
	loadLog *log.Logger,
) (loadResult, error) {
	return executeTemporalLoadWithRunner(ctx, c, cfg, loadLog, runOneWorkflow)
}

func executeTemporalLoadWithRunner(
	ctx context.Context,
	c client.Client,
	cfg loadConfig,
	loadLog *log.Logger,
	runner workflowRunner,
) (loadResult, error) {
	payload := makeLoadPayload(cfg.payloadBytes)
	startedAt := time.Now()
	state := loadRunState{
		latencies:       make([]time.Duration, 0, cfg.workflows),
		completionTimes: make([]time.Time, 0, cfg.workflows),
	}
	sem := make(chan struct{}, cfg.concurrency)
	var wg sync.WaitGroup
	launchCtx := ctx
	cancelLaunch := func() {}
	launchDuration := cfg.launchDuration
	if cfg.targetRPS > 0 {
		if launchDuration <= 0 {
			launchDuration = time.Duration(float64(cfg.workflows) / cfg.targetRPS * float64(time.Second))
		}
		launchCtx, cancelLaunch = context.WithDeadline(ctx, startedAt.Add(launchDuration))
	}

	launched := 0
launch:
	for workflowIndex := 0; workflowIndex < cfg.workflows; workflowIndex++ {
		if err := waitForLaunchSlot(launchCtx, startedAt, cfg.targetRPS, workflowIndex); err != nil {
			break launch
		}
		select {
		case <-launchCtx.Done():
			break launch
		case sem <- struct{}{}:
		}
		launched++
		index := workflowIndex
		wg.Go(func() {
			defer func() { <-sem }()
			workflowStartedAt := time.Now()
			runResult := runner(ctx, c, cfg, payload, startedAt.UnixNano(), index)
			runResult.latency = time.Since(workflowStartedAt)
			state.record(index, runResult, loadLog)
		})
	}
	cancelLaunch()
	var pacingErr error
	if launched < cfg.workflows {
		state.failed.Add(int64(cfg.workflows - launched))
	} else if cfg.targetRPS > 0 {
		pacingErr = waitUntil(ctx, startedAt.Add(launchDuration))
	}
	launchElapsed := time.Since(startedAt)
	wg.Wait()
	elapsed := time.Since(startedAt)
	result := loadResult{
		Workflows: cfg.workflows, Completed: state.completed.Load(), Failed: state.failed.Load(),
		Elapsed: elapsed, LaunchElapsed: launchElapsed, Launched: int64(launched), Requests: state.requests.Load(),
		TargetWorkflowsPerSec: cfg.targetRPS,
	}
	result.WorkflowLatencyP50, result.WorkflowLatencyP95, result.WorkflowLatencyP99 =
		workflowLatencyPercentiles(state.latencies)
	if !state.firstComplete.IsZero() && !state.lastComplete.IsZero() {
		result.CompletionElapsed = state.lastComplete.Sub(state.firstComplete)
	}
	if launchElapsed > 0 {
		result.LaunchesPerSec = float64(result.Launched) / launchElapsed.Seconds()
	}
	if elapsed > 0 {
		result.RequestsPerSec = float64(result.Requests) / elapsed.Seconds()
	}
	if cfg.targetRPS > 0 && result.Completed > 1 && result.CompletionElapsed > 0 {
		result.WorkflowsPerSec = pacedWorkflowThroughput(state.completionTimes)
	} else if elapsed > 0 {
		result.WorkflowsPerSec = float64(result.Completed) / elapsed.Seconds()
	}

	var completionErr error
	workflowFailed := int64(launched) - result.Completed
	if workflowFailed > 0 {
		completionErr = &loadCompletionError{
			completed: result.Completed,
			expected:  launched,
			failed:    workflowFailed,
		}
	}
	var saturationErr error
	if launched < cfg.workflows && ctx.Err() == nil {
		saturationErr = &loadSaturationError{launched: launched, expected: cfg.workflows}
	}
	terminationErr := terminateIncompleteWorkflowRuns(ctx, c, state.incomplete, cfg.concurrency)
	return result, errors.Join(
		completionErr,
		saturationErr,
		pacingErr,
		ctx.Err(),
		errors.Join(state.errs...),
		terminationErr,
	)
}

func pacedWorkflowThroughput(completionTimes []time.Time) float64 {
	if len(completionTimes) < 2 {
		return 0
	}
	ordered := slices.Clone(completionTimes)
	slices.SortFunc(ordered, func(a time.Time, b time.Time) int {
		return a.Compare(b)
	})
	trim := 0
	if len(ordered) >= 20 {
		trim = len(ordered) / 20
	}
	first := trim
	last := len(ordered) - 1 - trim
	elapsed := ordered[last].Sub(ordered[first])
	if elapsed <= 0 {
		first = 0
		last = len(ordered) - 1
		elapsed = ordered[last].Sub(ordered[first])
		if elapsed <= 0 {
			return 0
		}
	}
	return float64(last-first) / elapsed.Seconds()
}

func runOneWorkflow(
	ctx context.Context,
	c client.Client,
	cfg loadConfig,
	payload []byte,
	startNanos int64,
	workflowIndex int,
) workflowRunResult {
	id := loadWorkflowID(startNanos, workflowIndex)
	result := workflowRunResult{requests: 1}
	run, err := startOneWorkflow(ctx, c, cfg, payload, startNanos, workflowIndex)
	if err != nil {
		result.err = fmt.Errorf("start workflow %s: %w", id, err)
		return result
	}
	result.run = run

	for range cfg.signalsEach {
		result.requests++
		if err := c.SignalWorkflow(ctx, id, run.GetRunID(), loadSignalName, payload); err != nil {
			result.err = fmt.Errorf("signal workflow %s: %w", id, err)
			return result
		}
	}
	if err := run.Get(ctx, nil); err != nil {
		result.err = fmt.Errorf("wait for workflow %s: %w", id, err)
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
	cfg loadConfig,
	payload []byte,
	startNanos int64,
	workflowIndex int,
) (client.WorkflowRun, error) {
	executionTimeout, err := remainingWorkflowExecutionTimeout(ctx, cfg.timeout)
	if err != nil {
		return nil, err
	}
	return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       loadWorkflowID(startNanos, workflowIndex),
		TaskQueue:                loadTaskQueueName(cfg.taskQueue, cfg.taskQueues, workflowIndex%cfg.taskQueues),
		WorkflowExecutionTimeout: executionTimeout,
	}, temporalPerfWorkflow, loadWorkflowInput{
		Activities: cfg.activitiesEach,
		Signals:    cfg.signalsEach,
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

func waitForLaunchSlot(ctx context.Context, startedAt time.Time, targetRPS float64, index int) error {
	if targetRPS == 0 || index == 0 {
		return ctx.Err()
	}
	due := startedAt.Add(time.Duration(float64(index) / targetRPS * float64(time.Second)))
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

func waitUntil(ctx context.Context, deadline time.Time) error {
	wait := time.Until(deadline)
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

func startLoadWorkers(c client.Client, cfg loadConfig) ([]worker.Worker, error) {
	workers := make([]worker.Worker, 0, cfg.taskQueues*cfg.workersPerTaskQueue)
	for taskQueueIndex := 0; taskQueueIndex < cfg.taskQueues; taskQueueIndex++ {
		taskQueue := loadTaskQueueName(cfg.taskQueue, cfg.taskQueues, taskQueueIndex)
		for range cfg.workersPerTaskQueue {
			w := worker.New(c, taskQueue, worker.Options{})
			w.RegisterWorkflow(temporalPerfWorkflow)
			w.RegisterActivity(temporalPerfActivity)
			if err := w.Start(); err != nil {
				stopLoadWorkers(workers)
				return nil, err
			}
			workers = append(workers, w)
		}
	}
	return workers, nil
}

func stopLoadWorkers(workers []worker.Worker) {
	for _, w := range workers {
		w.Stop()
	}
}

func terminateIncompleteWorkflowRuns(
	ctx context.Context,
	c client.Client,
	runs []client.WorkflowRun,
	concurrency int,
) error {
	if len(runs) == 0 {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), postRunCleanupTimeout)
	defer cancel()

	sem := make(chan struct{}, min(max(concurrency, 1), len(runs)))
	var wg sync.WaitGroup
	var errsMu sync.Mutex
	var errs []error
terminate:
	for _, run := range runs {
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
				errs = append(errs, fmt.Errorf("terminate workflow %s: %w", workflowRun.GetID(), err))
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

func ensureLoadNamespace(ctx context.Context, c client.Client, namespace string) error {
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

func temporalPerfWorkflow(ctx workflow.Context, input loadWorkflowInput) error {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})
	for range input.Activities {
		var size int
		if err := workflow.ExecuteActivity(activityCtx, temporalPerfActivity, input.Payload).
			Get(activityCtx, &size); err != nil {
			return err
		}
	}

	signalChannel := workflow.GetSignalChannel(ctx, loadSignalName)
	for range input.Signals {
		var payload []byte
		signalChannel.Receive(ctx, &payload)
	}
	return nil
}

func temporalPerfActivity(_ context.Context, payload []byte) (int, error) {
	return len(payload), nil
}

func loadTaskQueueName(base string, taskQueues int, index int) string {
	if taskQueues == 1 {
		return base
	}
	return fmt.Sprintf("%s-%d", base, index)
}

func loadWorkflowID(startNanos int64, workflowIndex int) string {
	return fmt.Sprintf("temporal-perf-%d-%d", startNanos, workflowIndex)
}

func makeLoadPayload(size int) []byte {
	payload := make([]byte, size)
	for index := range payload {
		payload[index] = byte(index)
	}
	return payload
}

func isOnlyLoadSaturationError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*loadSaturationError); ok {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !isOnlyLoadSaturationError(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return isOnlyLoadSaturationError(wrapped.Unwrap())
	}
	return false
}
