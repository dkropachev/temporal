package main

import (
	"context"
	"errors"
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
			err = errors.New("workflows did not complete")
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

func TestCommandExecutorResetsWarmsMeasuresAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	binary := writeFakeScyllaload(t, dir)
	lifecycle := filepath.Join(dir, "lifecycle.log")
	cfg := config{
		address: "127.0.0.1:7233", namespace: "temporal-perf", scyllaload: binary, outputDir: dir,
		resetCommand:   "printf 'reset\\n' >> " + lifecycle,
		cleanupCommand: "printf 'cleanup\\n' >> " + lifecycle,
		taskQueues:     1, workersPerTaskQueue: 1, concurrency: 1,
		warmup: time.Second, initialRPS: 2, timeout: time.Minute,
	}
	request := runRequest{
		profile: profileCatalog["medium"], payloadBytes: 4096, stage: "steady",
		targetRatio: 0.8, targetRPS: 10, workflows: 20, trial: 2,
	}

	record, err := commandExecutor(cfg)(t.Context(), request)

	require.NoError(t, err)
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

func TestRunSuiteSmoke(t *testing.T) {
	dir := t.TempDir()
	cfg := config{
		address: "127.0.0.1:7233", namespace: "temporal-perf",
		scyllaload: writeFakeScyllaload(t, dir), outputDir: filepath.Join(dir, "results"),
		resetCommand: ":", cleanupCommand: ":", profiles: "tiny", payloads: "128",
		taskQueues: 1, workersPerTaskQueue: 1, concurrency: 1,
		warmup: time.Millisecond, measurement: time.Millisecond, trials: 1,
		initialRPS: 1, growthFactor: 2, maxRPS: 1, successRatio: 0.95,
		batchWorkflows: 1, timeout: time.Minute,
	}

	require.NoError(t, runSuite(t.Context(), cfg))
	var result suiteResult
	require.NoError(t, readJSON(filepath.Join(cfg.outputDir, "suite.json"), &result))
	require.NotZero(t, result.FinishedAt)
	require.Len(t, result.Cases, 1)
	require.Len(t, result.Cases[0].Targets, 3)
	require.Len(t, result.Cases[0].Runs, 5)
	require.Equal(t, "batch", result.Cases[0].Runs[4].Stage)
}

func writeFakeScyllaload(t *testing.T, dir string) string {
	binary := filepath.Join(dir, "fake-scyllaload")
	require.NoError(t, os.WriteFile(binary, []byte(`#!/bin/sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    -workflows) workflows="$2" ;;
    -target-workflows-per-second) target="$2" ;;
    -result-file) result="$2" ;;
    -run-metadata-file) metadata="$2" ;;
    -task-queue) task_queue="$2" ;;
  esac
  shift 2
done
printf '{"workflows":%s,"completed":%s,"workflowsPerSec":%s}\n' "$workflows" "$workflows" "$target" > "$result"
printf '{}\n' > "$metadata"
printf 'task_queue=%s workflows=%s\n' "$task_queue" "$workflows"
`), 0o755))
	return binary
}
