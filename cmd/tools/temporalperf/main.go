package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const suiteVersion = 2

const (
	defaultTemporalAddress = "127.0.0.1:7233"
	defaultServerBinary    = "./temporalperf-server"
)

type (
	config struct {
		address             string
		addressExplicit     bool
		namespace           string
		outputDir           string
		resetCommand        string
		cleanupCommand      string
		serverConfigFile    string
		serverBinary        string
		serverStartTimeout  time.Duration
		serverStopTimeout   time.Duration
		profiles            string
		payloads            string
		taskQueues          int
		workersPerTaskQueue int
		concurrency         int
		warmup              time.Duration
		measurement         time.Duration
		trials              int
		initialRPS          float64
		growthFactor        float64
		maxRPS              float64
		successRatio        float64
		batchWorkflows      int
		sampleTimeout       time.Duration
		timeout             time.Duration
	}

	workflowProfile struct {
		Name       string `json:"name"`
		Activities int    `json:"activities"`
		Signals    int    `json:"signals"`
	}

	loadResult struct {
		Workflows             int           `json:"workflows"`
		Completed             int64         `json:"completed"`
		Failed                int64         `json:"failed"`
		Elapsed               time.Duration `json:"elapsed"`
		LaunchElapsed         time.Duration `json:"launchElapsed"`
		CompletionElapsed     time.Duration `json:"completionElapsed"`
		Launched              int64         `json:"launched"`
		LaunchesPerSec        float64       `json:"launchesPerSec"`
		WorkflowsPerSec       float64       `json:"workflowsPerSec"`
		Requests              int64         `json:"requests"`
		RequestsPerSec        float64       `json:"requestsPerSec"`
		TargetWorkflowsPerSec float64       `json:"targetWorkflowsPerSec"`
		WorkflowLatencyP50    time.Duration `json:"workflowLatencyP50"`
		WorkflowLatencyP95    time.Duration `json:"workflowLatencyP95"`
		WorkflowLatencyP99    time.Duration `json:"workflowLatencyP99"`
	}

	runRecord struct {
		Profile      workflowProfile `json:"profile"`
		PayloadBytes int             `json:"payloadBytes"`
		Stage        string          `json:"stage"`
		TargetRatio  float64         `json:"targetRatio,omitempty"`
		TargetRPS    float64         `json:"targetRPS,omitempty"`
		Trial        int             `json:"trial"`
		Result       loadResult      `json:"result"`
		ResultFile   string          `json:"resultFile"`
		MetadataFile string          `json:"metadataFile"`
	}

	caseResult struct {
		Profile            workflowProfile `json:"profile"`
		PayloadBytes       int             `json:"payloadBytes"`
		MaximumThroughput  float64         `json:"maximumThroughput"`
		CalibrationCeiling bool            `json:"calibrationCeiling"`
		Targets            []targetSummary `json:"targets"`
		Runs               []runRecord     `json:"runs"`
	}

	targetSummary struct {
		TargetRatio      float64       `json:"targetRatio"`
		TargetRPS        float64       `json:"targetRPS"`
		ThroughputMedian float64       `json:"throughputMedian"`
		LatencyP50Median time.Duration `json:"latencyP50Median"`
		LatencyP95Median time.Duration `json:"latencyP95Median"`
		LatencyP99Median time.Duration `json:"latencyP99Median"`
	}

	suiteResult struct {
		Version          int               `json:"version"`
		StartedAt        time.Time         `json:"startedAt"`
		FinishedAt       time.Time         `json:"finishedAt"`
		Address          string            `json:"address"`
		Namespace        string            `json:"namespace"`
		ServerConfigFile string            `json:"serverConfigFile,omitempty"`
		ServerBinary     string            `json:"serverBinary,omitempty"`
		Warmup           time.Duration     `json:"warmup"`
		Measurement      time.Duration     `json:"measurement"`
		Trials           int               `json:"trials"`
		Payloads         []int             `json:"payloads"`
		Profiles         []workflowProfile `json:"profiles"`
		Cases            []caseResult      `json:"cases"`
	}

	runRequest struct {
		profile      workflowProfile
		payloadBytes int
		stage        string
		targetRatio  float64
		targetRPS    float64
		workflows    int
		duration     time.Duration
		trial        int
	}

	runExecutor  func(context.Context, runRequest) (runRecord, error)
	loadExecutor func(context.Context, config, runRequest, runRequest, string) (runRecord, error)
)

var profileCatalog = map[string]workflowProfile{
	"tiny":   {Name: "tiny", Activities: 1},
	"medium": {Name: "medium", Activities: 5},
	"big":    {Name: "big", Activities: 20},
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
	if err := prepareConfig(&cfg); err != nil {
		log.Fatal(err)
	}
	if err := validateConfig(cfg); err != nil {
		log.Fatal(err)
	}
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, cancel := context.WithTimeout(signalCtx, cfg.timeout)
	defer cancel()
	if err := runSuite(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

func parseFlags(args []string) (config, error) {
	var cfg config
	flags := flag.NewFlagSet("temporalperf", flag.ContinueOnError)
	flags.StringVar(&cfg.address, "address", defaultTemporalAddress, "Temporal frontend host:port")
	flags.StringVar(&cfg.namespace, "namespace", "temporal-perf", "benchmark namespace")
	flags.StringVar(&cfg.outputDir, "output-dir", "temporalperf-results", "artifact directory")
	flags.StringVar(&cfg.resetCommand, "reset-command", "", "command that prepares database/server state before every sample; required in external-server mode")
	flags.StringVar(&cfg.cleanupCommand, "cleanup-command", "", "command run after every sample, including failed samples")
	flags.StringVar(&cfg.serverConfigFile, "config-file", "", "Temporal server config file; starts a real server for every sample")
	flags.StringVar(&cfg.serverBinary, "server-binary", defaultServerBinary, "Temporal server binary used with -config-file")
	flags.DurationVar(&cfg.serverStartTimeout, "server-start-timeout", 2*time.Minute, "maximum time for a managed Temporal server to become healthy")
	flags.DurationVar(&cfg.serverStopTimeout, "server-stop-timeout", time.Minute, "maximum time for a managed Temporal server to stop gracefully")
	flags.StringVar(&cfg.profiles, "profiles", "tiny,medium,big", "comma-separated workflow profiles")
	flags.StringVar(&cfg.payloads, "payload-bytes", "128,4096,65536,1048576", "comma-separated payload sizes; at most four")
	flags.IntVar(&cfg.taskQueues, "task-queues", 16, "task queues per sample")
	flags.IntVar(&cfg.workersPerTaskQueue, "workers-per-task-queue", 8, "workers per task queue")
	flags.IntVar(&cfg.concurrency, "concurrency", 640, "maximum in-flight workflows")
	flags.DurationVar(&cfg.warmup, "warmup", 30*time.Second, "warmup duration before every measured sample")
	flags.DurationVar(&cfg.measurement, "measurement", time.Minute, "steady-state measurement duration")
	flags.IntVar(&cfg.trials, "trials", 3, "samples at each 50/80/90 percent target")
	flags.Float64Var(&cfg.initialRPS, "initial-rps", 25, "first offered rate during calibration")
	flags.Float64Var(&cfg.growthFactor, "growth-factor", 1.5, "calibration offered-rate multiplier")
	flags.Float64Var(&cfg.maxRPS, "max-rps", 100000, "calibration safety ceiling")
	flags.Float64Var(&cfg.successRatio, "success-ratio", 0.95, "minimum achieved/offered ratio for a sustainable rate")
	flags.IntVar(&cfg.batchWorkflows, "batch-workflows", 10000, "fixed-N workflows submitted without rate limiting")
	flags.DurationVar(&cfg.sampleTimeout, "sample-timeout", 10*time.Minute, "maximum warmup or measurement sample duration")
	flags.DurationVar(&cfg.timeout, "timeout", 24*time.Hour, "overall suite timeout")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "address" {
			cfg.addressExplicit = true
		}
	})
	return cfg, nil
}

func validateConfig(cfg config) error {
	if cfg.resetCommand == "" && cfg.serverConfigFile == "" {
		return errors.New("-reset-command is required in external-server mode")
	}
	if err := validateManagedServerConfig(cfg); err != nil {
		return err
	}
	if cfg.warmup <= 0 || cfg.measurement <= 0 {
		return errors.New("-warmup and -measurement must be positive")
	}
	if cfg.trials <= 0 || cfg.taskQueues <= 0 || cfg.workersPerTaskQueue <= 0 || cfg.concurrency <= 0 {
		return errors.New("-trials, -task-queues, -workers-per-task-queue, and -concurrency must be positive")
	}
	if !isFinite(cfg.initialRPS) || !isFinite(cfg.maxRPS) || !isFinite(cfg.growthFactor) ||
		cfg.initialRPS <= 0 || cfg.maxRPS < cfg.initialRPS || cfg.growthFactor <= 1 {
		return errors.New("invalid calibration bounds")
	}
	if cfg.maxRPS*max(cfg.warmup, cfg.measurement).Seconds() >= float64(math.MaxInt) {
		return errors.New("-max-rps produces too many workflows for the measurement duration")
	}
	if !isFinite(cfg.successRatio) || cfg.successRatio <= 0 || cfg.successRatio > 1 {
		return errors.New("-success-ratio must be in (0,1]")
	}
	if cfg.batchWorkflows <= 0 {
		return errors.New("-batch-workflows must be positive")
	}
	if cfg.timeout <= 0 {
		return errors.New("-timeout must be positive")
	}
	if cfg.sampleTimeout <= max(cfg.warmup, cfg.measurement) {
		return errors.New("-sample-timeout must be greater than -warmup and -measurement")
	}
	profiles, err := parseProfiles(cfg.profiles)
	if err != nil {
		return err
	}
	if len(profiles) == 0 {
		return errors.New("at least one profile is required")
	}
	_, err = parsePayloads(cfg.payloads)
	return err
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func runSuite(ctx context.Context, cfg config) error {
	return runSuiteWithExecutor(ctx, cfg, commandExecutor(cfg))
}

func runSuiteWithExecutor(ctx context.Context, cfg config, executor runExecutor) error {
	profiles, _ := parseProfiles(cfg.profiles)
	payloads, _ := parsePayloads(cfg.payloads)
	if err := os.MkdirAll(cfg.outputDir, 0o755); err != nil {
		return err
	}
	serverBinary := ""
	if cfg.serverConfigFile != "" {
		serverBinary = cfg.serverBinary
	}
	result := suiteResult{
		Version: suiteVersion, StartedAt: time.Now().UTC(), Address: cfg.address,
		Namespace: cfg.namespace, ServerConfigFile: cfg.serverConfigFile,
		ServerBinary: serverBinary, Warmup: cfg.warmup, Measurement: cfg.measurement,
		Trials: cfg.trials, Payloads: payloads, Profiles: profiles,
	}
	for _, profile := range profiles {
		for _, payload := range payloads {
			caseResult, caseErr := runCase(ctx, cfg, executor, profile, payload)
			result.Cases = append(result.Cases, caseResult)
			if caseErr != nil {
				result.FinishedAt = time.Now().UTC()
				writeErr := writeJSON(filepath.Join(cfg.outputDir, "suite.json"), result)
				return errors.Join(
					fmt.Errorf("profile %s payload %d: %w", profile.Name, payload, caseErr),
					writeErr,
				)
			}
			if err := writeJSON(filepath.Join(cfg.outputDir, "suite.json"), result); err != nil {
				return err
			}
		}
	}
	result.FinishedAt = time.Now().UTC()
	return writeJSON(filepath.Join(cfg.outputDir, "suite.json"), result)
}

func runCase(ctx context.Context, cfg config, execute runExecutor, profile workflowProfile, payload int) (caseResult, error) {
	result := caseResult{Profile: profile, PayloadBytes: payload}
	maximum, ceiling, calibrationRuns, err := calibrate(ctx, cfg, execute, profile, payload)
	result.Runs = append(result.Runs, calibrationRuns...)
	if err != nil {
		return result, err
	}
	result.MaximumThroughput = maximum
	result.CalibrationCeiling = ceiling
	for _, ratio := range []float64{0.50, 0.80, 0.90} {
		target := maximum * ratio
		var trials []loadResult
		for trial := 1; trial <= cfg.trials; trial++ {
			run, err := execute(ctx, runRequest{
				profile: profile, payloadBytes: payload, stage: "steady",
				targetRatio: ratio, targetRPS: target,
				workflows: workflowsForDuration(target, cfg.measurement), duration: cfg.measurement, trial: trial,
			})
			result.Runs = append(result.Runs, run)
			if err != nil {
				return result, err
			}
			trials = append(trials, run.Result)
		}
		result.Targets = append(result.Targets, summarizeTarget(ratio, target, trials))
	}
	batch, err := execute(ctx, runRequest{
		profile: profile, payloadBytes: payload, stage: "batch",
		workflows: cfg.batchWorkflows, trial: 1,
	})
	result.Runs = append(result.Runs, batch)
	return result, err
}

func calibrate(
	ctx context.Context,
	cfg config,
	execute runExecutor,
	profile workflowProfile,
	payload int,
) (float64, bool, []runRecord, error) {
	var runs []runRecord
	maximum := 0.0
	for offered := cfg.initialRPS; offered <= cfg.maxRPS; offered *= cfg.growthFactor {
		run, err := execute(ctx, runRequest{
			profile: profile, payloadBytes: payload, stage: "calibration",
			targetRPS: offered, workflows: workflowsForDuration(offered, cfg.measurement),
			duration: cfg.measurement, trial: 1,
		})
		runs = append(runs, run)
		if err != nil {
			if maximum > 0 && isOnlyLoadSaturationError(err) {
				return maximum, false, runs, nil
			}
			return maximum, false, runs, err
		}
		if !sustainable(run.Result, offered, cfg.successRatio) {
			if maximum == 0 {
				return 0, false, runs, fmt.Errorf("initial rate %.2f workflows/s is not sustainable", offered)
			}
			return maximum, false, runs, nil
		}
		maximum = max(maximum, min(offered, run.Result.WorkflowsPerSec))
		if offered > cfg.maxRPS/cfg.growthFactor {
			return maximum, true, runs, nil
		}
	}
	return maximum, true, runs, nil
}

func sustainable(result loadResult, offered float64, successRatio float64) bool {
	return result.Failed == 0 && result.Completed == int64(result.Workflows) &&
		result.WorkflowsPerSec >= offered*successRatio
}

func summarizeTarget(ratio float64, target float64, results []loadResult) targetSummary {
	throughputs := make([]float64, 0, len(results))
	p50s := make([]time.Duration, 0, len(results))
	p95s := make([]time.Duration, 0, len(results))
	p99s := make([]time.Duration, 0, len(results))
	for _, result := range results {
		throughputs = append(throughputs, result.WorkflowsPerSec)
		p50s = append(p50s, result.WorkflowLatencyP50)
		p95s = append(p95s, result.WorkflowLatencyP95)
		p99s = append(p99s, result.WorkflowLatencyP99)
	}
	slices.Sort(throughputs)
	slices.Sort(p50s)
	slices.Sort(p95s)
	slices.Sort(p99s)
	middle := len(results) / 2
	throughputMedian := throughputs[middle]
	p50Median := p50s[middle]
	p95Median := p95s[middle]
	p99Median := p99s[middle]
	if len(results)%2 == 0 {
		throughputMedian = throughputs[middle-1] + (throughputs[middle]-throughputs[middle-1])/2
		p50Median = p50s[middle-1] + (p50s[middle]-p50s[middle-1])/2
		p95Median = p95s[middle-1] + (p95s[middle]-p95s[middle-1])/2
		p99Median = p99s[middle-1] + (p99s[middle]-p99s[middle-1])/2
	}
	return targetSummary{
		TargetRatio: ratio, TargetRPS: target,
		ThroughputMedian: throughputMedian, LatencyP50Median: p50Median,
		LatencyP95Median: p95Median, LatencyP99Median: p99Median,
	}
}

func workflowsForDuration(rps float64, duration time.Duration) int {
	return max(1, int(rps*duration.Seconds()))
}

func commandExecutor(cfg config) runExecutor {
	return commandExecutorWithLoadAndServer(cfg, runTemporalSample, startManagedServer)
}

func commandExecutorWithLoad(cfg config, executeLoad loadExecutor) runExecutor {
	return commandExecutorWithLoadAndServer(cfg, executeLoad, startManagedServer)
}

func commandExecutorWithLoadAndServer(
	cfg config,
	executeLoad loadExecutor,
	startServer serverStarter,
) runExecutor {
	return func(ctx context.Context, request runRequest) (record runRecord, retErr error) {
		name := runArtifactName(request)
		dir := filepath.Join(cfg.outputDir, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return record, err
		}
		env := hookEnvironment(cfg, request, dir)
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			cleanupCommand := cfg.cleanupCommand
			if cleanupCommand == "" {
				cleanupCommand = cfg.resetCommand
			}
			if err := runHook(cleanupCtx, cleanupCommand, env, filepath.Join(dir, "cleanup.log")); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("cleanup: %w", err))
			}
		}()
		if err := runHook(ctx, cfg.resetCommand, env, filepath.Join(dir, "reset.log")); err != nil {
			return record, fmt.Errorf("reset: %w", err)
		}
		stopServer, err := startServer(ctx, cfg, dir)
		if err != nil {
			return record, fmt.Errorf("start Temporal server: %w", err)
		}
		if stopServer != nil {
			defer func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), cfg.serverStopTimeout)
				defer cancel()
				if err := stopServer(stopCtx); err != nil {
					retErr = errors.Join(retErr, fmt.Errorf("stop Temporal server: %w", err))
				}
			}()
		}

		warmupRPS := request.targetRPS
		if warmupRPS == 0 {
			warmupRPS = cfg.initialRPS
		}
		warmup := request
		warmup.targetRPS = warmupRPS
		warmup.workflows = workflowsForDuration(warmupRPS, cfg.warmup)
		warmup.duration = cfg.warmup
		record, err = executeLoad(ctx, cfg, warmup, request, dir)
		return record, err
	}
}

func runArtifactName(request runRequest) string {
	target := "unlimited"
	if request.targetRPS > 0 {
		target = strings.ReplaceAll(strconv.FormatFloat(request.targetRPS, 'g', -1, 64), ".", "_") + "rps"
	}
	ratio := ""
	if request.targetRatio > 0 {
		ratio = fmt.Sprintf("-%dpct", int(math.Round(request.targetRatio*100)))
	}
	return fmt.Sprintf(
		"%s-p%d-%s%s-%s-r%d",
		request.profile.Name,
		request.payloadBytes,
		request.stage,
		ratio,
		target,
		request.trial,
	)
}

func hookEnvironment(cfg config, request runRequest, artifactDir string) []string {
	return append(os.Environ(),
		"TEMPORALPERF_ADDRESS="+cfg.address,
		"TEMPORALPERF_NAMESPACE="+cfg.namespace,
		"TEMPORALPERF_CONFIG_FILE="+cfg.serverConfigFile,
		"TEMPORALPERF_PROFILE="+request.profile.Name,
		"TEMPORALPERF_PAYLOAD_BYTES="+strconv.Itoa(request.payloadBytes),
		"TEMPORALPERF_STAGE="+request.stage,
		"TEMPORALPERF_TRIAL="+strconv.Itoa(request.trial),
		"TEMPORALPERF_ARTIFACT_DIR="+artifactDir,
	)
}

func runHook(ctx context.Context, command string, env []string, logPath string) error {
	if command == "" {
		return nil
	}
	logFile, err := os.Create(logPath)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.Env = env
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	return errors.Join(cmd.Run(), logFile.Close())
}

func parseProfiles(value string) ([]workflowProfile, error) {
	var profiles []workflowProfile
	seen := make(map[string]struct{})
	for _, name := range strings.Split(value, ",") {
		name = strings.TrimSpace(name)
		profile, ok := profileCatalog[name]
		if !ok {
			return nil, fmt.Errorf("unknown workflow profile %q", name)
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		profiles = append(profiles, profile)
	}
	return profiles, nil
}

func parsePayloads(value string) ([]int, error) {
	var payloads []int
	for _, raw := range strings.Split(value, ",") {
		size, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || size < 0 {
			return nil, fmt.Errorf("invalid payload size %q", raw)
		}
		payloads = append(payloads, size)
	}
	slices.Sort(payloads)
	payloads = slices.Compact(payloads)
	if len(payloads) == 0 || len(payloads) > 4 {
		return nil, errors.New("payload size list must contain one to four values")
	}
	return payloads, nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(path), ".temporalperf-*.json")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := errors.Join(temporary.Chmod(0o644), writeAllAndClose(temporary, data)); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func writeAllAndClose(file *os.File, data []byte) error {
	_, writeErr := file.Write(data)
	return errors.Join(writeErr, file.Close())
}

func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}
