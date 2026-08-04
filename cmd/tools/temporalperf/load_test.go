package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
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

type fakeTemporalClient struct {
	client.Client
	executeWorkflow func(context.Context, client.StartWorkflowOptions, any, ...any) (client.WorkflowRun, error)
	signalWorkflow  func(context.Context, string, string, string, any) error
	terminate       func(context.Context, string, string, string, ...any) error
}

func (c *fakeTemporalClient) ExecuteWorkflow(
	ctx context.Context,
	options client.StartWorkflowOptions,
	workflow any,
	args ...any,
) (client.WorkflowRun, error) {
	return c.executeWorkflow(ctx, options, workflow, args...)
}

func (c *fakeTemporalClient) SignalWorkflow(
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

func (c *fakeTemporalClient) TerminateWorkflow(
	ctx context.Context,
	workflowID string,
	runID string,
	reason string,
	details ...any,
) error {
	if c.terminate == nil {
		return nil
	}
	return c.terminate(ctx, workflowID, runID, reason, details...)
}

func (c *fakeTemporalClient) WorkflowService() workflowservice.WorkflowServiceClient {
	return nil
}

func TestTemporalPerfWorkflowRunsActivities(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(temporalPerfWorkflow)
	env.RegisterActivity(temporalPerfActivity)

	env.ExecuteWorkflow(temporalPerfWorkflow, loadWorkflowInput{
		Activities: 3,
		Payload:    []byte("payload"),
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestTemporalPerfWorkflowConsumesSignals(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(temporalPerfWorkflow)
	env.RegisterActivity(temporalPerfActivity)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(loadSignalName, []byte("one"))
		env.SignalWorkflow(loadSignalName, []byte("two"))
	}, 0)

	env.ExecuteWorkflow(temporalPerfWorkflow, loadWorkflowInput{Signals: 2})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestStartOneWorkflowDistributesTaskQueuesAndBoundsTimeout(t *testing.T) {
	var options client.StartWorkflowOptions
	c := &fakeTemporalClient{
		executeWorkflow: func(
			_ context.Context,
			startOptions client.StartWorkflowOptions,
			_ any,
			_ ...any,
		) (client.WorkflowRun, error) {
			options = startOptions
			return &fakeWorkflowRun{id: startOptions.ID, getErr: func() error { return nil }}, nil
		},
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	cfg := loadConfig{taskQueue: "perf", taskQueues: 2, timeout: 5 * time.Minute}

	_, err := startOneWorkflow(ctx, c, cfg, []byte("payload"), 123, 3)

	require.NoError(t, err)
	require.Equal(t, loadWorkflowID(123, 3), options.ID)
	require.Equal(t, "perf-1", options.TaskQueue)
	require.Positive(t, options.WorkflowExecutionTimeout)
	require.LessOrEqual(t, options.WorkflowExecutionTimeout, time.Second)
}

func TestPacedThroughputExcludesCompletionTail(t *testing.T) {
	runner := func(ctx context.Context, _ client.Client, _ loadConfig, _ []byte, _ int64, _ int) workflowRunResult {
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return workflowRunResult{err: ctx.Err()}
		case <-timer.C:
			return workflowRunResult{completed: true, requests: 1}
		}
	}

	result, err := executeTemporalLoadWithRunner(
		t.Context(),
		nil,
		loadConfig{workflows: 4, concurrency: 4, targetRPS: 20, launchDuration: 200 * time.Millisecond},
		log.New(io.Discard, "", 0),
		runner,
	)

	require.NoError(t, err)
	require.Greater(t, result.Elapsed, result.LaunchElapsed)
	require.InDelta(t, 20, result.LaunchesPerSec, 3)
	require.InDelta(t, 20, result.WorkflowsPerSec, 3)
}

func TestPacedThroughputDetectsAccumulatingBacklog(t *testing.T) {
	runner := func(ctx context.Context, _ client.Client, _ loadConfig, _ []byte, _ int64, index int) workflowRunResult {
		timer := time.NewTimer(time.Duration(index+1) * 50 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return workflowRunResult{err: ctx.Err()}
		case <-timer.C:
			return workflowRunResult{completed: true, requests: 1}
		}
	}

	result, err := executeTemporalLoadWithRunner(
		t.Context(),
		nil,
		loadConfig{workflows: 4, concurrency: 4, targetRPS: 20, launchDuration: 200 * time.Millisecond},
		log.New(io.Discard, "", 0),
		runner,
	)

	require.NoError(t, err)
	require.InDelta(t, 20, result.LaunchesPerSec, 3)
	require.InDelta(t, 10, result.WorkflowsPerSec, 3)
}

func TestPacedWorkflowThroughputExcludesTailStraggler(t *testing.T) {
	startedAt := time.Now()
	completionTimes := make([]time.Time, 0, 21)
	for index := range 20 {
		completionTimes = append(completionTimes, startedAt.Add(time.Duration(index)*10*time.Millisecond))
	}
	completionTimes = append(completionTimes, startedAt.Add(2*time.Second))

	require.InDelta(t, 100, pacedWorkflowThroughput(completionTimes), 0.001)
}

func TestPacedLoadStopsLaunchingAtMeasurementDeadline(t *testing.T) {
	runner := func(ctx context.Context, _ client.Client, _ loadConfig, _ []byte, _ int64, _ int) workflowRunResult {
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return workflowRunResult{err: ctx.Err()}
		case <-timer.C:
			return workflowRunResult{completed: true, requests: 1}
		}
	}

	result, err := executeTemporalLoadWithRunner(
		t.Context(),
		nil,
		loadConfig{workflows: 100, concurrency: 1, targetRPS: 1000, launchDuration: 50 * time.Millisecond},
		log.New(io.Discard, "", 0),
		runner,
	)

	var saturationErr *loadSaturationError
	require.ErrorAs(t, err, &saturationErr)
	require.Less(t, result.Launched, int64(result.Workflows))
	require.Less(t, result.Elapsed, time.Second)
}

func TestExecuteTemporalLoadCountsUnlaunchedWorkflowsAsFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	release := make(chan struct{})
	runner := func(context.Context, client.Client, loadConfig, []byte, int64, int) workflowRunResult {
		close(started)
		<-release
		return workflowRunResult{completed: true, requests: 1}
	}
	type outcome struct {
		result loadResult
		err    error
	}
	outcomeCh := make(chan outcome, 1)
	go func() {
		result, err := executeTemporalLoadWithRunner(
			ctx,
			nil,
			loadConfig{workflows: 3, concurrency: 1},
			log.New(io.Discard, "", 0),
			runner,
		)
		outcomeCh <- outcome{result: result, err: err}
	}()

	<-started
	cancel()
	close(release)
	loadOutcome := <-outcomeCh

	require.ErrorIs(t, loadOutcome.err, context.Canceled)
	require.Equal(t, int64(1), loadOutcome.result.Completed)
	require.Equal(t, int64(2), loadOutcome.result.Failed)
}

func TestPacedLoadReportsCancellationDuringFinalLaunchWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	completed := make(chan struct{})
	runner := func(context.Context, client.Client, loadConfig, []byte, int64, int) workflowRunResult {
		close(completed)
		return workflowRunResult{completed: true, requests: 1}
	}
	type outcome struct {
		result loadResult
		err    error
	}
	outcomeCh := make(chan outcome, 1)
	go func() {
		result, err := executeTemporalLoadWithRunner(
			ctx,
			nil,
			loadConfig{workflows: 1, concurrency: 1, targetRPS: 1, launchDuration: time.Second},
			log.New(io.Discard, "", 0),
			runner,
		)
		outcomeCh <- outcome{result: result, err: err}
	}()

	<-completed
	cancel()
	loadOutcome := <-outcomeCh

	require.ErrorIs(t, loadOutcome.err, context.Canceled)
	require.Equal(t, int64(1), loadOutcome.result.Completed)
}

func TestExecuteTemporalLoadReportsTerminationFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	terminationErr := errors.New("termination failed")
	c := &fakeTemporalClient{
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
		terminate: func(context.Context, string, string, string, ...any) error {
			return terminationErr
		},
	}

	_, err := executeTemporalLoad(
		ctx,
		c,
		loadConfig{taskQueue: "perf", taskQueues: 1, workflows: 1, concurrency: 1, timeout: time.Minute},
		log.New(io.Discard, "", 0),
	)

	require.ErrorIs(t, err, terminationErr)
	require.ErrorContains(t, err, "terminate incomplete workflows")
	require.False(t, isOnlyLoadSaturationError(err))
}

func TestExecuteTemporalLoadPreservesWorkflowInfrastructureFailure(t *testing.T) {
	infrastructureErr := errors.New("frontend unavailable")
	runner := func(context.Context, client.Client, loadConfig, []byte, int64, int) workflowRunResult {
		return workflowRunResult{requests: 1, err: infrastructureErr}
	}

	result, err := executeTemporalLoadWithRunner(
		t.Context(),
		nil,
		loadConfig{workflows: 1, concurrency: 1},
		log.New(io.Discard, "", 0),
		runner,
	)

	require.ErrorIs(t, err, infrastructureErr)
	require.Equal(t, int64(1), result.Failed)
	require.False(t, isOnlyLoadSaturationError(err))
}

func TestIsOnlyLoadSaturationErrorRejectsJoinedInfrastructureFailure(t *testing.T) {
	saturationErr := &loadSaturationError{launched: 9, expected: 10}

	require.True(t, isOnlyLoadSaturationError(fmt.Errorf("measure: %w", saturationErr)))
	require.False(t, isOnlyLoadSaturationError(errors.Join(
		saturationErr,
		errors.New("cleanup failed"),
	)))
}
