package main

import (
	"context"
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
