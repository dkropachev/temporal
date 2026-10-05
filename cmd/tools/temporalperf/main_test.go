package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParsePayloadsUsesAtMostFourSortedBuckets(t *testing.T) {
	payloads, err := parsePayloads("1048576,128,4096,65536,128")
	require.NoError(t, err)
	require.Equal(t, []int{128, 4096, 65536, 1048576}, payloads)

	_, err = parsePayloads("1,2,3,4,5")
	require.ErrorContains(t, err, "one to four")
}

func TestDefaultMatrixIncludesEveryProfileAndPayloadBucket(t *testing.T) {
	cfg, err := parseFlags(nil)
	require.NoError(t, err)

	profiles, err := parseProfiles(cfg.profiles)
	require.NoError(t, err)
	payloads, err := parsePayloads(cfg.payloads)
	require.NoError(t, err)
	require.Len(t, profiles, 3)
	require.Equal(t, []int{128, 4096, 65536, 1048576}, payloads)
}

func TestValidateConfigRejectsNonpositiveTimeout(t *testing.T) {
	cfg, err := parseFlags(nil)
	require.NoError(t, err)
	cfg.resetCommand = ":"
	cfg.timeout = 0

	require.ErrorContains(t, validateConfig(cfg), "-timeout")
}

func TestValidateConfigRejectsNonFiniteCalibrationValues(t *testing.T) {
	base, err := parseFlags(nil)
	require.NoError(t, err)
	base.resetCommand = ":"

	for name, mutate := range map[string]func(*config){
		"initial RPS NaN":        func(cfg *config) { cfg.initialRPS = math.NaN() },
		"maximum RPS infinity":   func(cfg *config) { cfg.maxRPS = math.Inf(1) },
		"growth factor infinity": func(cfg *config) { cfg.growthFactor = math.Inf(1) },
		"success ratio NaN":      func(cfg *config) { cfg.successRatio = math.NaN() },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			mutate(&cfg)
			require.Error(t, validateConfig(cfg))
		})
	}
}

func TestValidateConfigRejectsWorkflowCountOverflow(t *testing.T) {
	cfg, err := parseFlags(nil)
	require.NoError(t, err)
	cfg.resetCommand = ":"
	cfg.maxRPS = math.MaxFloat64

	require.ErrorContains(t, validateConfig(cfg), "too many workflows")
}

func TestParseProfilesReturnsStableProfileDefinitions(t *testing.T) {
	profiles, err := parseProfiles("tiny,medium,big")
	require.NoError(t, err)
	require.Equal(t, []workflowProfile{
		{Name: "tiny", Activities: 1},
		{Name: "medium", Activities: 5},
		{Name: "big", Activities: 20},
	}, profiles)
}

func TestCalibrateStopsAtFirstUnsustainableRate(t *testing.T) {
	cfg := config{initialRPS: 10, growthFactor: 2, maxRPS: 100, successRatio: 0.95, measurement: time.Second}
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		achieved := request.targetRPS
		var err error
		if request.targetRPS == 40 {
			achieved = 30
			err = &loadSaturationError{launched: request.workflows - 1, expected: request.workflows}
		}
		workflows := request.workflows
		return runRecord{Result: loadResult{
			Workflows: workflows, Completed: int64(workflows), WorkflowsPerSec: achieved,
		}}, err
	}

	maximum, ceiling, runs, err := calibrate(t.Context(), cfg, executor, profileCatalog["tiny"], 128)

	require.NoError(t, err)
	require.False(t, ceiling)
	require.InDelta(t, 20, maximum, 0.001)
	require.Len(t, runs, 3)
}

func TestCalibrateReportsConfiguredCeiling(t *testing.T) {
	cfg := config{initialRPS: 10, growthFactor: 2, maxRPS: 20, successRatio: 0.95, measurement: time.Second}
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		return runRecord{Result: loadResult{
			Workflows:       request.workflows,
			Completed:       int64(request.workflows),
			WorkflowsPerSec: request.targetRPS,
		}}, nil
	}

	maximum, ceiling, runs, err := calibrate(t.Context(), cfg, executor, profileCatalog["medium"], 4096)

	require.NoError(t, err)
	require.True(t, ceiling)
	require.InDelta(t, 20, maximum, 0.001)
	require.Len(t, runs, 2)
}

func TestCalibrateDoesNotExceedSustainableOfferedRate(t *testing.T) {
	cfg := config{initialRPS: 10, growthFactor: 2, maxRPS: 20, successRatio: 0.95, measurement: time.Second}
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		return runRecord{Result: loadResult{
			Workflows: request.workflows, Completed: int64(request.workflows),
			WorkflowsPerSec: request.targetRPS * 1.2,
		}}, nil
	}

	maximum, ceiling, _, err := calibrate(t.Context(), cfg, executor, profileCatalog["tiny"], 128)

	require.NoError(t, err)
	require.True(t, ceiling)
	require.InDelta(t, 20, maximum, 0.001)
}

func TestCalibrateDoesNotHideInfrastructureFailure(t *testing.T) {
	cfg := config{initialRPS: 10, growthFactor: 2, maxRPS: 100, successRatio: 0.95, measurement: time.Second}
	expectedErr := errors.New("reset failed")
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		if request.targetRPS == 20 {
			return runRecord{}, expectedErr
		}
		return runRecord{Result: loadResult{
			Workflows: request.workflows, Completed: int64(request.workflows), WorkflowsPerSec: request.targetRPS,
		}}, nil
	}

	maximum, _, _, err := calibrate(t.Context(), cfg, executor, profileCatalog["tiny"], 128)

	require.ErrorIs(t, err, expectedErr)
	require.InDelta(t, 10, maximum, 0.001)
}

func TestCalibrateTreatsWarmupSaturationAsBoundary(t *testing.T) {
	cfg := config{initialRPS: 10, growthFactor: 2, maxRPS: 100, successRatio: 0.95, measurement: time.Second}
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		if request.targetRPS == 20 {
			return runRecord{}, fmt.Errorf(
				"warmup: %w",
				&loadSaturationError{launched: request.workflows - 1, expected: request.workflows},
			)
		}
		return runRecord{Result: loadResult{
			Workflows: request.workflows, Completed: int64(request.workflows), WorkflowsPerSec: request.targetRPS,
		}}, nil
	}

	maximum, ceiling, runs, err := calibrate(t.Context(), cfg, executor, profileCatalog["tiny"], 128)

	require.NoError(t, err)
	require.False(t, ceiling)
	require.InDelta(t, 10, maximum, 0.001)
	require.Len(t, runs, 2)
}

func TestCalibrateDoesNotHideWorkflowInfrastructureFailure(t *testing.T) {
	cfg := config{initialRPS: 10, growthFactor: 2, maxRPS: 100, successRatio: 0.95, measurement: time.Second}
	infrastructureErr := errors.New("frontend unavailable")
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		result := loadResult{
			Workflows: request.workflows, Completed: int64(request.workflows),
			WorkflowsPerSec: request.targetRPS,
		}
		if request.targetRPS == 20 {
			result.Completed--
			result.Failed = 1
			return runRecord{Result: result}, errors.Join(
				&loadCompletionError{completed: result.Completed, expected: result.Workflows, failed: 1},
				infrastructureErr,
			)
		}
		return runRecord{Result: result}, nil
	}

	maximum, _, _, err := calibrate(t.Context(), cfg, executor, profileCatalog["tiny"], 128)

	require.ErrorIs(t, err, infrastructureErr)
	require.InDelta(t, 10, maximum, 0.001)
}

func TestCalibrateDoesNotHideCleanupFailureAtSaturation(t *testing.T) {
	cfg := config{initialRPS: 10, growthFactor: 2, maxRPS: 100, successRatio: 0.95, measurement: time.Second}
	cleanupErr := errors.New("cleanup failed")
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		result := loadResult{
			Workflows: request.workflows, Completed: int64(request.workflows),
			WorkflowsPerSec: request.targetRPS,
		}
		if request.targetRPS == 20 {
			result.Failed = 1
			result.Completed--
			return runRecord{Result: result}, errors.Join(
				&loadSaturationError{launched: result.Workflows - 1, expected: result.Workflows},
				cleanupErr,
			)
		}
		return runRecord{Result: result}, nil
	}

	maximum, _, _, err := calibrate(t.Context(), cfg, executor, profileCatalog["tiny"], 128)

	require.ErrorIs(t, err, cleanupErr)
	require.InDelta(t, 10, maximum, 0.001)
}

func TestRunCaseUsesCalibratedTargetsAndFixedBatch(t *testing.T) {
	cfg := config{
		initialRPS: 10, growthFactor: 2, maxRPS: 20, successRatio: 0.95,
		measurement: time.Second, trials: 2, batchWorkflows: 123,
	}
	var requests []runRequest
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		requests = append(requests, request)
		return runRecord{Result: loadResult{
			Workflows:       request.workflows,
			Completed:       int64(request.workflows),
			WorkflowsPerSec: request.targetRPS,
		}}, nil
	}

	result, err := runCase(t.Context(), cfg, executor, profileCatalog["big"], 65536)

	require.NoError(t, err)
	require.InDelta(t, 20, result.MaximumThroughput, 0.001)
	require.Len(t, requests, 9)
	require.Equal(t, []float64{10, 16, 18}, []float64{
		requests[2].targetRPS,
		requests[4].targetRPS,
		requests[6].targetRPS,
	})
	require.Equal(t, "batch", requests[8].stage)
	require.Equal(t, 123, requests[8].workflows)
	require.Zero(t, requests[8].targetRPS)
	require.Len(t, result.Targets, 3)
	require.InDelta(t, 18, result.Targets[2].TargetRPS, 0.001)
}

func TestWorkflowsForDurationRoundsDownWithinPacingWindow(t *testing.T) {
	require.Equal(t, 1898, workflowsForDuration(126.53373214438052, 15*time.Second))
	require.Equal(t, 20, workflowsForDuration(20, time.Second))
	require.Equal(t, 1, workflowsForDuration(0.5, time.Second))
}

func TestSummarizeTargetUsesTrialMedians(t *testing.T) {
	summary := summarizeTarget(0.8, 80, []loadResult{
		{WorkflowsPerSec: 79, WorkflowLatencyP50: 3, WorkflowLatencyP95: 30, WorkflowLatencyP99: 300},
		{WorkflowsPerSec: 81, WorkflowLatencyP50: 1, WorkflowLatencyP95: 10, WorkflowLatencyP99: 100},
		{WorkflowsPerSec: 80, WorkflowLatencyP50: 2, WorkflowLatencyP95: 20, WorkflowLatencyP99: 200},
	})

	require.InDelta(t, 80, summary.ThroughputMedian, 0.001)
	require.Equal(t, time.Duration(2), summary.LatencyP50Median)
	require.Equal(t, time.Duration(20), summary.LatencyP95Median)
	require.Equal(t, time.Duration(200), summary.LatencyP99Median)
}

func TestSummarizeTargetAveragesEvenTrialMedians(t *testing.T) {
	summary := summarizeTarget(0.8, 80, []loadResult{
		{WorkflowsPerSec: 79, WorkflowLatencyP50: 1, WorkflowLatencyP95: 10, WorkflowLatencyP99: 100},
		{WorkflowsPerSec: 81, WorkflowLatencyP50: 3, WorkflowLatencyP95: 30, WorkflowLatencyP99: 300},
	})

	require.InDelta(t, 80, summary.ThroughputMedian, 0.001)
	require.Equal(t, time.Duration(2), summary.LatencyP50Median)
	require.Equal(t, time.Duration(20), summary.LatencyP95Median)
	require.Equal(t, time.Duration(200), summary.LatencyP99Median)
}

func TestArtifactNameSeparatesTargets(t *testing.T) {
	base := runRequest{profile: profileCatalog["tiny"], payloadBytes: 128, stage: "steady", trial: 1}
	fifty := base
	fifty.targetRatio = 0.5
	fifty.targetRPS = 100
	ninety := base
	ninety.targetRatio = 0.9
	ninety.targetRPS = 180
	require.NotEqual(t, runArtifactName(fifty), runArtifactName(ninety))
}

func TestArtifactNamePreservesCloselySpacedTargets(t *testing.T) {
	base := runRequest{profile: profileCatalog["tiny"], payloadBytes: 128, stage: "calibration", trial: 1}
	first := base
	first.targetRPS = 1.0004
	second := base
	second.targetRPS = 1.00049

	require.NotEqual(t, runArtifactName(first), runArtifactName(second))
}

func TestCommandExecutorResetsWarmsMeasuresAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	lifecycle := filepath.Join(dir, "lifecycle.log")
	cfg := config{
		address: "127.0.0.1:7233", namespace: "temporal-perf", outputDir: dir,
		resetCommand:   "printf 'reset\\n' >> " + lifecycle,
		cleanupCommand: "printf 'cleanup\\n' >> " + lifecycle,
		taskQueues:     1, workersPerTaskQueue: 1, concurrency: 1,
		warmup: time.Second, initialRPS: 2, timeout: time.Minute,
	}
	request := runRequest{
		profile: profileCatalog["medium"], payloadBytes: 4096, stage: "steady",
		targetRatio: 0.8, targetRPS: 10, workflows: 20, trial: 2,
	}

	loadCalls := 0
	executeLoad := func(
		ctx context.Context,
		cfg config,
		warmup runRequest,
		measure runRequest,
		dir string,
	) (runRecord, error) {
		loadCalls++
		require.Equal(t, 10, warmup.workflows)
		require.Equal(t, time.Second, warmup.duration)
		return fakeLoadExecutor(ctx, cfg, warmup, measure, dir)
	}
	record, err := commandExecutorWithLoad(cfg, executeLoad)(t.Context(), request)

	require.NoError(t, err)
	require.Equal(t, 1, loadCalls)
	require.Equal(t, int64(20), record.Result.Completed)
	lifecycleData, err := os.ReadFile(lifecycle)
	require.NoError(t, err)
	require.Equal(t, "reset\ncleanup\n", string(lifecycleData))
	artifactDir := filepath.Join(dir, runArtifactName(request))
	warmupLog, err := os.ReadFile(filepath.Join(artifactDir, "warmup.log"))
	require.NoError(t, err)
	measureLog, err := os.ReadFile(filepath.Join(artifactDir, "measure.log"))
	require.NoError(t, err)
	require.Equal(t, strings.Fields(string(warmupLog))[0], strings.Fields(string(measureLog))[0])
	require.Contains(t, string(warmupLog), "workflows=10")
	require.Contains(t, string(measureLog), "workflows=20")
}

func TestRunHookWritesStdoutAndStderrToLog(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "hook.log")

	err := runHook(
		t.Context(),
		"printf stdout; printf stderr >&2; exit 7",
		os.Environ(),
		logPath,
	)

	require.Error(t, err)
	contents, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	require.Equal(t, "stdoutstderr", string(contents))
}

func TestRunSuiteSmoke(t *testing.T) {
	dir := t.TempDir()
	cfg := config{
		address: "127.0.0.1:7233", namespace: "temporal-perf",
		outputDir:    filepath.Join(dir, "results"),
		resetCommand: ":", cleanupCommand: ":", profiles: "tiny", payloads: "128",
		taskQueues: 1, workersPerTaskQueue: 1, concurrency: 1,
		warmup: time.Millisecond, measurement: time.Millisecond, trials: 1,
		initialRPS: 1, growthFactor: 2, maxRPS: 1, successRatio: 0.95,
		batchWorkflows: 1, timeout: time.Minute,
	}

	require.NoError(t, runSuiteWithExecutor(
		t.Context(),
		cfg,
		commandExecutorWithLoad(cfg, fakeLoadExecutor),
	))
	var result suiteResult
	require.NoError(t, readJSON(filepath.Join(cfg.outputDir, "suite.json"), &result))
	require.NotZero(t, result.FinishedAt)
	require.Len(t, result.Cases, 1)
	require.Len(t, result.Cases[0].Targets, 3)
	require.Len(t, result.Cases[0].Runs, 5)
	require.Equal(t, "batch", result.Cases[0].Runs[4].Stage)
}

func TestRunSuitePersistsPartialCaseOnFailure(t *testing.T) {
	dir := t.TempDir()
	cfg := config{
		outputDir: filepath.Join(dir, "results"), profiles: "tiny", payloads: "128",
		measurement: time.Second, trials: 1, initialRPS: 1, growthFactor: 2,
		maxRPS: 1, successRatio: 0.95, batchWorkflows: 1,
	}
	expectedErr := errors.New("sample failed")
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		record := runRecord{Stage: request.stage, Result: loadResult{
			Workflows: request.workflows, Completed: int64(request.workflows),
			WorkflowsPerSec: request.targetRPS,
		}}
		if request.stage == "steady" {
			return record, expectedErr
		}
		return record, nil
	}

	err := runSuiteWithExecutor(t.Context(), cfg, executor)

	require.ErrorIs(t, err, expectedErr)
	var result suiteResult
	require.NoError(t, readJSON(filepath.Join(cfg.outputDir, "suite.json"), &result))
	require.NotZero(t, result.FinishedAt)
	require.Len(t, result.Cases, 1)
	require.Len(t, result.Cases[0].Runs, 2)
	require.Equal(t, "steady", result.Cases[0].Runs[1].Stage)
}

func TestRunSuiteResumesAtNextCalibrationSample(t *testing.T) {
	cfg := newResumeTestConfig(t)
	contentionErr := errors.New("external contention")
	var firstRequests []runRequest
	firstExecutor := func(_ context.Context, request runRequest) (runRecord, error) {
		firstRequests = append(firstRequests, request)
		if request.stage == "calibration" && request.targetRPS == 20 {
			return writeResumeTestRun(t, cfg, request, false), contentionErr
		}
		return writeResumeTestRun(t, cfg, request, true), nil
	}

	err := runSuiteWithExecutor(t.Context(), cfg, firstExecutor)
	require.ErrorIs(t, err, contentionErr)
	require.Len(t, firstRequests, 2)
	var partial suiteCheckpoint
	require.NoError(t, readJSON(filepath.Join(cfg.outputDir, checkpointFileName), &partial))
	require.False(t, partial.Complete)
	require.Len(t, partial.Cases, 1)
	require.Len(t, partial.Cases[0].Runs, 1)
	firstRunReset := filepath.Join(cfg.outputDir, runArtifactName(firstRequests[0]), "reset.log")
	firstRunResetHash := fileSHA256ForTest(t, firstRunReset)

	var resumedRequests []runRequest
	resumedExecutor := func(_ context.Context, request runRequest) (runRecord, error) {
		resumedRequests = append(resumedRequests, request)
		if request.stage == "calibration" && request.targetRPS == 20 {
			record := writeResumeTestRun(t, cfg, request, false)
			return record, &loadSaturationError{launched: record.Result.Workflows - 1, expected: record.Result.Workflows}
		}
		return writeResumeTestRun(t, cfg, request, true), nil
	}

	require.NoError(t, runSuiteWithExecutor(t.Context(), cfg, resumedExecutor))
	require.NotEmpty(t, resumedRequests)
	require.Equal(t, "calibration", resumedRequests[0].stage)
	require.InDelta(t, 20, resumedRequests[0].targetRPS, 0.001)
	require.Equal(t, firstRunResetHash, fileSHA256ForTest(t, firstRunReset))
	rejectedRun := filepath.Join(
		cfg.outputDir,
		"rejected-samples",
		runArtifactName(firstRequests[1])+"-1",
	)
	info, err := os.Stat(rejectedRun)
	require.NoError(t, err)
	require.True(t, info.IsDir())

	var checkpoint suiteCheckpoint
	require.NoError(t, readJSON(filepath.Join(cfg.outputDir, checkpointFileName), &checkpoint))
	require.True(t, checkpoint.Complete)
	require.Len(t, checkpoint.Cases[0].Runs, 6)
	require.Equal(t, checkpointBoundary, checkpoint.Cases[0].Runs[1].Acceptance)
	artifactPaths := make([]string, 0, len(checkpoint.Cases[0].Runs[0].Artifacts))
	for _, artifact := range checkpoint.Cases[0].Runs[0].Artifacts {
		artifactPaths = append(artifactPaths, artifact.Path)
	}
	require.Contains(t, artifactPaths, "sample-capture-state.json")
	require.Contains(t, artifactPaths, "sample-clean.json")
	require.Contains(t, artifactPaths, observabilityManifest)
	require.Contains(t, artifactPaths, "observability/host-monitor.tsv")
	var result suiteResult
	require.NoError(t, readJSON(filepath.Join(cfg.outputDir, "suite.json"), &result))
	require.Equal(t, suiteVersion, result.Version)
	require.Len(t, result.Cases, 1)
	require.Len(t, result.Cases[0].Runs, 6)
	require.InDelta(t, 10, result.Cases[0].MaximumThroughput, 0.001)
}

func TestRunSuiteCheckpointsWarmupSaturationBoundary(t *testing.T) {
	cfg := newResumeTestConfig(t)
	cfg.measurement = 2 * time.Second
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		if request.stage == "calibration" && request.targetRPS == 20 {
			record := writeResumeWarmupBoundary(t, cfg, request)
			return record, &loadSaturationError{
				launched: record.Result.Workflows - 1, expected: record.Result.Workflows,
			}
		}
		return writeResumeTestRun(t, cfg, request, true), nil
	}

	require.NoError(t, runSuiteWithExecutor(t.Context(), cfg, executor))
	var checkpoint suiteCheckpoint
	require.NoError(t, readJSON(filepath.Join(cfg.outputDir, checkpointFileName), &checkpoint))
	boundary := checkpoint.Cases[0].Runs[1]
	require.Equal(t, checkpointBoundary, boundary.Acceptance)
	require.Equal(t, workflowsForDuration(20, cfg.warmup), boundary.Record.Result.Workflows)
	require.NotEqual(t, workflowsForDuration(20, cfg.measurement), boundary.Record.Result.Workflows)
	require.Equal(t, "warmup.result.json", filepath.Base(boundary.Record.ResultFile))
}

func TestRunSuiteDoesNotCheckpointSaturationJoinedWithCleanupFailure(t *testing.T) {
	cfg := newResumeTestConfig(t)
	cleanupErr := errors.New("sample contaminated")
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		if request.stage == "calibration" && request.targetRPS == 20 {
			record := writeResumeTestRun(t, cfg, request, false)
			return record, errors.Join(
				&loadSaturationError{launched: record.Result.Workflows - 1, expected: record.Result.Workflows},
				cleanupErr,
			)
		}
		return writeResumeTestRun(t, cfg, request, true), nil
	}

	err := runSuiteWithExecutor(t.Context(), cfg, executor)
	require.ErrorIs(t, err, cleanupErr)
	var checkpoint suiteCheckpoint
	require.NoError(t, readJSON(filepath.Join(cfg.outputDir, checkpointFileName), &checkpoint))
	require.Len(t, checkpoint.Cases, 1)
	require.Len(t, checkpoint.Cases[0].Runs, 1)
	require.Equal(t, checkpointSuccess, checkpoint.Cases[0].Runs[0].Acceptance)
}

func TestRunSuiteDoesNotCheckpointMalformedCalibrationBoundary(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*loadResult)
	}{
		{name: "workflow count", mutate: func(result *loadResult) { result.Workflows++ }},
		{name: "launched bound", mutate: func(result *loadResult) { result.Launched = int64(result.Workflows + 1) }},
		{name: "completion accounting", mutate: func(result *loadResult) { result.Failed++ }},
		{name: "negative requests", mutate: func(result *loadResult) { result.Requests = -1 }},
		{name: "zero elapsed", mutate: func(result *loadResult) { result.Elapsed = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := newResumeTestConfig(t)
			executor := func(_ context.Context, request runRequest) (runRecord, error) {
				if request.stage == "calibration" && request.targetRPS == 20 {
					record := writeResumeTestRun(t, cfg, request, false)
					test.mutate(&record.Result)
					require.NoError(t, writeJSON(record.ResultFile, record.Result))
					return record, &loadSaturationError{
						launched: record.Result.Workflows - 1, expected: record.Result.Workflows,
					}
				}
				return writeResumeTestRun(t, cfg, request, true), nil
			}

			err := runSuiteWithExecutor(t.Context(), cfg, executor)
			require.ErrorContains(t, err, "boundary result is invalid")
			var checkpoint suiteCheckpoint
			require.NoError(t, readJSON(filepath.Join(cfg.outputDir, checkpointFileName), &checkpoint))
			require.Len(t, checkpoint.Cases[0].Runs, 1)
		})
	}
}

func TestCompleteCheckpointResultMatchesSuiteValidatorRequirements(t *testing.T) {
	base := completeResumeTestResult(runRequest{targetRPS: 10, workflows: 10})
	require.True(t, completeCheckpointResult(base, 10))
	mutations := map[string]func(*loadResult){
		"workflows":          func(result *loadResult) { result.Workflows = 0 },
		"failed":             func(result *loadResult) { result.Failed = 1 },
		"launched":           func(result *loadResult) { result.Launched-- },
		"completed":          func(result *loadResult) { result.Completed-- },
		"requests":           func(result *loadResult) { result.Requests = 0 },
		"elapsed":            func(result *loadResult) { result.Elapsed = 0 },
		"completion elapsed": func(result *loadResult) { result.CompletionElapsed = 0 },
		"throughput":         func(result *loadResult) { result.WorkflowsPerSec = 0 },
		"request throughput": func(result *loadResult) { result.RequestsPerSec = 0 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			result := base
			mutate(&result)
			require.False(t, completeCheckpointResult(result, 10))
		})
	}
}

func TestRunSuiteResumesInterruptedSteadyAndBatchSamples(t *testing.T) {
	for _, test := range []struct {
		name  string
		match func(runRequest) bool
	}{
		{
			name: "steady trial",
			match: func(request runRequest) bool {
				return request.stage == "steady" && request.targetRatio == 0.8 && request.trial == 2
			},
		},
		{
			name:  "batch",
			match: func(request runRequest) bool { return request.stage == "batch" },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := newResumeTestConfig(t)
			cfg.trials = 2
			interrupted := false
			var interruptedRequest runRequest
			firstExecutor := func(_ context.Context, request runRequest) (runRecord, error) {
				record, err := completedResumeTestRun(t, cfg, request)
				if !interrupted && test.match(request) {
					interrupted = true
					interruptedRequest = request
					return record, errors.New("external contention")
				}
				return record, err
			}
			require.Error(t, runSuiteWithExecutor(t.Context(), cfg, firstExecutor))
			require.True(t, interrupted)

			var resumedRequests []runRequest
			resumedExecutor := func(_ context.Context, request runRequest) (runRecord, error) {
				resumedRequests = append(resumedRequests, request)
				return completedResumeTestRun(t, cfg, request)
			}
			require.NoError(t, runSuiteWithExecutor(t.Context(), cfg, resumedExecutor))
			require.NotEmpty(t, resumedRequests)
			require.Equal(t, interruptedRequest, resumedRequests[0])

			var result suiteResult
			require.NoError(t, readJSON(filepath.Join(cfg.outputDir, "suite.json"), &result))
			require.Len(t, result.Cases[0].Runs, 9)
			for _, target := range result.Cases[0].Targets {
				require.NotZero(t, target.ThroughputMedian)
			}
		})
	}
}

func TestRunSuiteRejectsResumeMismatchBeforeExecution(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*config)
	}{
		{name: "resume ID", mutate: func(cfg *config) { cfg.resumeID = "different-protocol" }},
		{name: "configuration", mutate: func(cfg *config) { cfg.namespace += "-different" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := createPartialResumeCheckpoint(t)
			test.mutate(&cfg)
			calls := 0
			err := runSuiteWithExecutor(t.Context(), cfg, func(context.Context, runRequest) (runRecord, error) {
				calls++
				return runRecord{}, nil
			})
			require.Error(t, err)
			require.Zero(t, calls)
		})
	}
}

func TestCheckpointSpecificationBindsEverySemanticConfigField(t *testing.T) {
	cfg := newResumeTestConfig(t)
	_, originalHash, err := checkpointSpecForConfig(cfg)
	require.NoError(t, err)
	mutations := map[string]func(*config){
		"address":              func(cfg *config) { cfg.address += "0" },
		"namespace":            func(cfg *config) { cfg.namespace += "x" },
		"output directory":     func(cfg *config) { cfg.outputDir += "x" },
		"reset command":        func(cfg *config) { cfg.resetCommand += " " },
		"cleanup command":      func(cfg *config) { cfg.cleanupCommand += " " },
		"server config":        func(cfg *config) { cfg.serverConfigFile = "/tmp/config" },
		"server binary":        func(cfg *config) { cfg.serverBinary += "x" },
		"server start timeout": func(cfg *config) { cfg.serverStartTimeout++ },
		"server stop timeout":  func(cfg *config) { cfg.serverStopTimeout++ },
		"target-only":          func(cfg *config) { cfg.requireTargetOnly = !cfg.requireTargetOnly },
		"profiles":             func(cfg *config) { cfg.profiles = "big" },
		"payloads":             func(cfg *config) { cfg.payloads = "129" },
		"task queues":          func(cfg *config) { cfg.taskQueues++ },
		"workers":              func(cfg *config) { cfg.workersPerTaskQueue++ },
		"concurrency":          func(cfg *config) { cfg.concurrency++ },
		"warmup":               func(cfg *config) { cfg.warmup++ },
		"measurement":          func(cfg *config) { cfg.measurement++ },
		"trials":               func(cfg *config) { cfg.trials++ },
		"initial RPS":          func(cfg *config) { cfg.initialRPS++ },
		"growth factor":        func(cfg *config) { cfg.growthFactor += 0.1 },
		"maximum RPS":          func(cfg *config) { cfg.maxRPS++ },
		"success ratio":        func(cfg *config) { cfg.successRatio -= 0.01 },
		"batch workflows":      func(cfg *config) { cfg.batchWorkflows++ },
		"sample timeout":       func(cfg *config) { cfg.sampleTimeout++ },
		"suite timeout":        func(cfg *config) { cfg.timeout++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := cfg
			mutate(&changed)
			_, changedHash, err := checkpointSpecForConfig(changed)
			require.NoError(t, err)
			require.NotEqual(t, originalHash, changedHash)
		})
	}
}

func TestRunSuiteRejectsInvalidCheckpointBeforeExecution(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, config, *suiteCheckpoint)
	}{
		{
			name: "out-of-order run",
			mutate: func(_ *testing.T, _ config, checkpoint *suiteCheckpoint) {
				checkpoint.Cases[0].Runs[0].Record.TargetRPS = 20
			},
		},
		{
			name: "duplicate run",
			mutate: func(_ *testing.T, _ config, checkpoint *suiteCheckpoint) {
				checkpoint.Cases[0].Runs = append(checkpoint.Cases[0].Runs, checkpoint.Cases[0].Runs[0])
			},
		},
		{
			name: "artifact changed",
			mutate: func(t *testing.T, cfg config, checkpoint *suiteCheckpoint) {
				path := filepath.Join(cfg.outputDir, runArtifactName(runRequest{
					profile: profileCatalog["tiny"], payloadBytes: 128, stage: "calibration",
					targetRPS: 10, workflows: 10, duration: time.Second, trial: 1,
				}), "reset.log")
				require.NoError(t, os.WriteFile(path, []byte("changed"), 0o644))
			},
		},
		{
			name: "clean marker missing",
			mutate: func(t *testing.T, cfg config, _ *suiteCheckpoint) {
				path := filepath.Join(cfg.outputDir, runArtifactName(runRequest{
					profile: profileCatalog["tiny"], payloadBytes: 128, stage: "calibration",
					targetRPS: 10, workflows: 10, duration: time.Second, trial: 1,
				}), "sample-clean.json")
				require.NoError(t, os.Remove(path))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := createPartialResumeCheckpoint(t)
			checkpointPath := filepath.Join(cfg.outputDir, checkpointFileName)
			var checkpoint suiteCheckpoint
			require.NoError(t, readJSON(checkpointPath, &checkpoint))
			test.mutate(t, cfg, &checkpoint)
			require.NoError(t, writeJSON(checkpointPath, checkpoint))
			calls := 0
			err := runSuiteWithExecutor(t.Context(), cfg, func(context.Context, runRequest) (runRecord, error) {
				calls++
				return runRecord{}, nil
			})
			require.Error(t, err)
			require.Zero(t, calls)
		})
	}
}

func TestRunSuiteRejectsTruncatedCheckpointBeforeExecution(t *testing.T) {
	cfg := createPartialResumeCheckpoint(t)
	checkpointPath := filepath.Join(cfg.outputDir, checkpointFileName)
	require.NoError(t, os.WriteFile(checkpointPath, []byte("{\n"), 0o644))
	calls := 0

	err := runSuiteWithExecutor(t.Context(), cfg, func(context.Context, runRequest) (runRecord, error) {
		calls++
		return runRecord{}, nil
	})

	require.ErrorContains(t, err, "read resume checkpoint")
	require.Zero(t, calls)
}

func TestCheckpointRejectsUnsafeOrDuplicateObservabilityManifest(t *testing.T) {
	for _, test := range []struct {
		name     string
		manifest func(*testing.T, string) string
	}{
		{
			name: "outside observability directory",
			manifest: func(t *testing.T, runDir string) string {
				return fmt.Sprintf("%s  reset.log\n", fileSHA256ForTest(t, filepath.Join(runDir, "reset.log")))
			},
		},
		{
			name: "parent traversal",
			manifest: func(t *testing.T, runDir string) string {
				return fmt.Sprintf(
					"%s  observability/../reset.log\n",
					fileSHA256ForTest(t, filepath.Join(runDir, "reset.log")),
				)
			},
		},
		{
			name: "duplicate path",
			manifest: func(t *testing.T, runDir string) string {
				path := filepath.Join(runDir, "observability/host-monitor.tsv")
				line := fmt.Sprintf("%s  observability/host-monitor.tsv\n", fileSHA256ForTest(t, path))
				return line + line
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := createPartialResumeCheckpoint(t)
			runDir := filepath.Join(cfg.outputDir, runArtifactName(runRequest{
				profile: profileCatalog["tiny"], payloadBytes: 128, stage: "calibration",
				targetRPS: 10, workflows: 10, duration: time.Second, trial: 1,
			}))
			require.NoError(t, os.WriteFile(
				filepath.Join(runDir, observabilityManifest),
				[]byte(test.manifest(t, runDir)),
				0o644,
			))
			calls := 0
			err := runSuiteWithExecutor(t.Context(), cfg, func(context.Context, runRequest) (runRecord, error) {
				calls++
				return runRecord{}, nil
			})
			require.Error(t, err)
			require.Zero(t, calls)
		})
	}
}

func TestCheckpointRejectsInvalidTimestampAndCompleteState(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*suiteCheckpoint)
	}{
		{
			name: "timestamp order",
			mutate: func(checkpoint *suiteCheckpoint) {
				checkpoint.UpdatedAt = checkpoint.StartedAt.Add(-time.Second)
			},
		},
		{
			name: "incomplete marked complete",
			mutate: func(checkpoint *suiteCheckpoint) {
				checkpoint.Complete = true
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := createPartialResumeCheckpoint(t)
			checkpointPath := filepath.Join(cfg.outputDir, checkpointFileName)
			var checkpoint suiteCheckpoint
			require.NoError(t, readJSON(checkpointPath, &checkpoint))
			test.mutate(&checkpoint)
			require.NoError(t, writeJSON(checkpointPath, checkpoint))
			calls := 0
			err := runSuiteWithExecutor(t.Context(), cfg, func(context.Context, runRequest) (runRecord, error) {
				calls++
				return runRecord{}, nil
			})
			require.Error(t, err)
			require.Zero(t, calls)
		})
	}
}

func TestCheckpointWriteFailureStopsBeforeNextSample(t *testing.T) {
	cfg := newResumeTestConfig(t)
	checkpointPath := filepath.Join(cfg.outputDir, checkpointFileName)
	calls := 0
	executor := func(_ context.Context, request runRequest) (runRecord, error) {
		calls++
		record := writeResumeTestRun(t, cfg, request, true)
		require.NoError(t, os.Rename(checkpointPath, checkpointPath+".saved"))
		require.NoError(t, os.Mkdir(checkpointPath, 0o755))
		return record, nil
	}

	err := runSuiteWithExecutor(t.Context(), cfg, executor)
	require.ErrorContains(t, err, "write resume checkpoint")
	require.Equal(t, 1, calls)
	var checkpoint suiteCheckpoint
	require.NoError(t, readJSON(checkpointPath+".saved", &checkpoint))
	require.Empty(t, checkpoint.Cases)
}

func TestCompleteCheckpointExecutesNoSamples(t *testing.T) {
	cfg := newResumeTestConfig(t)
	require.NoError(t, runSuiteWithExecutor(t.Context(), cfg, func(
		_ context.Context,
		request runRequest,
	) (runRecord, error) {
		return completedResumeTestRun(t, cfg, request)
	}))

	calls := 0
	require.NoError(t, runSuiteWithExecutor(t.Context(), cfg, func(
		context.Context,
		runRequest,
	) (runRecord, error) {
		calls++
		return runRecord{}, nil
	}))
	require.Zero(t, calls)
}

func TestHookEnvironmentIncludesResumeSampleIdentity(t *testing.T) {
	artifactDir := filepath.Join(t.TempDir(), "sample")
	cfg := config{resumeID: "protocol:baseline:tiny:182"}
	request := runRequest{
		profile: profileCatalog["tiny"], payloadBytes: 182, stage: "steady",
		targetRatio: 0.8, targetRPS: 12.5, trial: 2,
	}
	environment := make(map[string]string)
	for _, value := range hookEnvironment(cfg, request, artifactDir) {
		name, value, found := strings.Cut(value, "=")
		if found {
			environment[name] = value
		}
	}
	require.Equal(t, cfg.resumeID, environment["TEMPORALPERF_RESUME_ID"])
	require.Equal(t, runArtifactName(request), environment["TEMPORALPERF_RUN_NAME"])
	require.Equal(t, "12.5", environment["TEMPORALPERF_TARGET_RPS"])
	require.Equal(t, "0.8", environment["TEMPORALPERF_TARGET_RATIO"])
	require.Equal(t, artifactDir, environment["TEMPORALPERF_ARTIFACT_DIR"])
}

func newResumeTestConfig(t *testing.T) config {
	t.Helper()
	return config{
		address: "127.0.0.1:7233", namespace: "temporal-perf-resume",
		outputDir: t.TempDir(), resetCommand: ":", cleanupCommand: ":",
		serverBinary: defaultServerBinary, serverStartTimeout: time.Minute,
		serverStopTimeout: time.Minute, resumeID: "protocol:baseline:tiny:128",
		profiles: "tiny", payloads: "128", taskQueues: 1, workersPerTaskQueue: 1,
		concurrency: 8, warmup: time.Second, measurement: time.Second, trials: 1,
		initialRPS: 10, growthFactor: 2, maxRPS: 100, successRatio: 0.95,
		batchWorkflows: 3, sampleTimeout: time.Minute, timeout: time.Hour,
	}
}

func createPartialResumeCheckpoint(t *testing.T) config {
	t.Helper()
	cfg := newResumeTestConfig(t)
	err := runSuiteWithExecutor(t.Context(), cfg, func(_ context.Context, request runRequest) (runRecord, error) {
		if request.stage == "calibration" && request.targetRPS == 20 {
			return writeResumeTestRun(t, cfg, request, false), errors.New("stop after one sample")
		}
		return writeResumeTestRun(t, cfg, request, true), nil
	})
	require.Error(t, err)
	return cfg
}

func completedResumeTestRun(t *testing.T, cfg config, request runRequest) (runRecord, error) {
	t.Helper()
	if request.stage == "calibration" && request.targetRPS == 20 {
		record := writeResumeTestRun(t, cfg, request, false)
		return record, &loadSaturationError{launched: record.Result.Workflows - 1, expected: record.Result.Workflows}
	}
	return writeResumeTestRun(t, cfg, request, true), nil
}

func writeResumeTestRun(t *testing.T, cfg config, request runRequest, successful bool) runRecord {
	t.Helper()
	runName := runArtifactName(request)
	runDir := filepath.Join(cfg.outputDir, runName)
	require.NoError(t, os.MkdirAll(filepath.Join(runDir, "observability"), 0o755))

	result := completeResumeTestResult(request)
	if !successful {
		result.Completed--
		result.Failed = 1
		result.WorkflowsPerSec = request.targetRPS / 2
	}
	warmupResult := completeResumeTestResult(runRequest{
		targetRPS: request.targetRPS, workflows: max(1, int(request.targetRPS)),
	})
	for name, value := range map[string]any{
		"warmup.result.json":    warmupResult,
		"warmup.metadata.json":  struct{}{},
		"measure.result.json":   result,
		"measure.metadata.json": struct{}{},
		"sample-capture-state.json": map[string]any{
			"resumeId": cfg.resumeID, "runName": runName,
		},
		"sample-clean.json": sampleCleanMarker{
			Version: sampleCleanMarkerVersion, ResumeID: cfg.resumeID, RunName: runName, Clean: true,
		},
	} {
		require.NoError(t, writeJSON(filepath.Join(runDir, name), value))
	}
	for _, name := range []string{"cleanup.log", "reset.log", "warmup.log", "measure.log"} {
		require.NoError(t, os.WriteFile(filepath.Join(runDir, name), []byte(name+"\n"), 0o644))
	}
	evidenceRelativePath := "observability/host-monitor.tsv"
	evidence := []byte("timestamp\tcompeting_jobs\n2026-09-09T00:00:00Z\t0\n")
	require.NoError(t, os.WriteFile(filepath.Join(runDir, evidenceRelativePath), evidence, 0o644))
	evidenceSum := sha256.Sum256(evidence)
	manifest := fmt.Sprintf("%x  %s\n", evidenceSum, evidenceRelativePath)
	require.NoError(t, os.WriteFile(filepath.Join(runDir, observabilityManifest), []byte(manifest), 0o644))

	return runRecord{
		Profile: request.profile, PayloadBytes: request.payloadBytes, Stage: request.stage,
		TargetRatio: request.targetRatio, TargetRPS: request.targetRPS, Trial: request.trial,
		Result: result, ResultFile: filepath.Join(runDir, "measure.result.json"),
		MetadataFile: filepath.Join(runDir, "measure.metadata.json"),
	}
}

func writeResumeWarmupBoundary(t *testing.T, cfg config, request runRequest) runRecord {
	t.Helper()
	record := writeResumeTestRun(t, cfg, request, false)
	warmupRequest := request
	warmupRequest.workflows = workflowsForDuration(request.targetRPS, cfg.warmup)
	result := completeResumeTestResult(warmupRequest)
	result.Completed--
	result.Failed = 1
	result.WorkflowsPerSec = request.targetRPS / 2
	runDir := filepath.Dir(record.ResultFile)
	record.Result = result
	record.ResultFile = filepath.Join(runDir, "warmup.result.json")
	record.MetadataFile = filepath.Join(runDir, "warmup.metadata.json")
	require.NoError(t, writeJSON(record.ResultFile, result))
	for _, name := range []string{"measure.log", "measure.metadata.json", "measure.result.json"} {
		require.NoError(t, os.Remove(filepath.Join(runDir, name)))
	}
	return record
}

func completeResumeTestResult(request runRequest) loadResult {
	throughput := request.targetRPS
	if throughput == 0 {
		throughput = float64(request.workflows)
	}
	return loadResult{
		Workflows: request.workflows, Completed: int64(request.workflows),
		Failed: 0, Elapsed: time.Second, LaunchElapsed: time.Second,
		CompletionElapsed: time.Second, Launched: int64(request.workflows),
		LaunchesPerSec: throughput, WorkflowsPerSec: throughput,
		Requests: int64(request.workflows), RequestsPerSec: throughput,
		TargetWorkflowsPerSec: request.targetRPS,
		WorkflowLatencyP50:    time.Millisecond, WorkflowLatencyP95: 2 * time.Millisecond,
		WorkflowLatencyP99: 3 * time.Millisecond,
	}
}

func fileSHA256ForTest(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum)
}

func fakeLoadExecutor(
	ctx context.Context,
	cfg config,
	warmup runRequest,
	request runRequest,
	dir string,
) (runRecord, error) {
	if _, err := fakeLoadPhase(ctx, cfg, warmup, dir, "warmup"); err != nil {
		return runRecord{}, err
	}
	return fakeLoadPhase(ctx, cfg, request, dir, "measure")
}

func fakeLoadPhase(
	_ context.Context,
	_ config,
	request runRequest,
	dir string,
	phase string,
) (runRecord, error) {
	resultPath := filepath.Join(dir, phase+".result.json")
	metadataPath := filepath.Join(dir, phase+".metadata.json")
	throughput := request.targetRPS
	if throughput == 0 {
		throughput = float64(request.workflows)
	}
	result := loadResult{
		Workflows: request.workflows, Completed: int64(request.workflows),
		WorkflowsPerSec: throughput,
	}
	if err := writeJSON(resultPath, result); err != nil {
		return runRecord{}, err
	}
	if err := writeJSON(metadataPath, struct{}{}); err != nil {
		return runRecord{}, err
	}
	logLine := fmt.Sprintf(
		"task_queue=perf-%s-p%d-%s-r%d phase=%s workflows=%d\n",
		request.profile.Name,
		request.payloadBytes,
		request.stage,
		request.trial,
		phase,
		request.workflows,
	)
	if err := os.WriteFile(filepath.Join(dir, phase+".log"), []byte(logLine), 0o644); err != nil {
		return runRecord{}, err
	}
	return runRecord{
		Profile: request.profile, PayloadBytes: request.payloadBytes, Stage: request.stage,
		TargetRatio: request.targetRatio, TargetRPS: request.targetRPS, Trial: request.trial,
		Result: result, ResultFile: resultPath, MetadataFile: metadataPath,
	}, nil
}
