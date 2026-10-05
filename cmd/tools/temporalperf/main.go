package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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
	checkpointVersion        = 1
	sampleCleanMarkerVersion = 1
	checkpointFileName       = "checkpoint.json"
	observabilityManifest    = "observability.sha256"
	checkpointSuccess        = "success"
	checkpointBoundary       = "calibration-boundary"
)

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
		requireTargetOnly   bool
		resumeID            string
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
		TargetOnly       bool              `json:"cassandraTargetOnlyRequired,omitempty"`
		Warmup           time.Duration     `json:"warmup"`
		Measurement      time.Duration     `json:"measurement"`
		Trials           int               `json:"trials"`
		Payloads         []int             `json:"payloads"`
		Profiles         []workflowProfile `json:"profiles"`
		Cases            []caseResult      `json:"cases"`
	}

	checkpointSpec struct {
		SuiteVersion        int               `json:"suiteVersion"`
		Address             string            `json:"address"`
		Namespace           string            `json:"namespace"`
		OutputDir           string            `json:"outputDir"`
		ResetCommand        string            `json:"resetCommand"`
		CleanupCommand      string            `json:"cleanupCommand"`
		ServerConfigFile    string            `json:"serverConfigFile"`
		ServerBinary        string            `json:"serverBinary"`
		ServerStartTimeout  time.Duration     `json:"serverStartTimeout"`
		ServerStopTimeout   time.Duration     `json:"serverStopTimeout"`
		RequireTargetOnly   bool              `json:"requireTargetOnly"`
		Profiles            []workflowProfile `json:"profiles"`
		Payloads            []int             `json:"payloads"`
		TaskQueues          int               `json:"taskQueues"`
		WorkersPerTaskQueue int               `json:"workersPerTaskQueue"`
		Concurrency         int               `json:"concurrency"`
		Warmup              time.Duration     `json:"warmup"`
		Measurement         time.Duration     `json:"measurement"`
		Trials              int               `json:"trials"`
		InitialRPS          float64           `json:"initialRPS"`
		GrowthFactor        float64           `json:"growthFactor"`
		MaxRPS              float64           `json:"maxRPS"`
		SuccessRatio        float64           `json:"successRatio"`
		BatchWorkflows      int               `json:"batchWorkflows"`
		SampleTimeout       time.Duration     `json:"sampleTimeout"`
		Timeout             time.Duration     `json:"timeout"`
	}

	checkpointArtifact struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	}

	checkpointRun struct {
		Record     runRecord            `json:"record"`
		Acceptance string               `json:"acceptance"`
		Artifacts  []checkpointArtifact `json:"artifacts"`
	}

	checkpointCase struct {
		Profile      workflowProfile `json:"profile"`
		PayloadBytes int             `json:"payloadBytes"`
		Runs         []checkpointRun `json:"runs"`
	}

	suiteCheckpoint struct {
		Version    int              `json:"version"`
		ResumeID   string           `json:"resumeId"`
		Spec       checkpointSpec   `json:"spec"`
		SpecSHA256 string           `json:"specSha256"`
		StartedAt  time.Time        `json:"startedAt"`
		UpdatedAt  time.Time        `json:"updatedAt"`
		Complete   bool             `json:"complete"`
		Cases      []checkpointCase `json:"cases"`
	}

	sampleCleanMarker struct {
		Version  int    `json:"version"`
		ResumeID string `json:"resumeId"`
		RunName  string `json:"runName"`
		Clean    bool   `json:"clean"`
	}

	resumableCaseState struct {
		Result  caseResult
		Next    *runRequest
		Maximum float64
		Done    bool
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
	flags.BoolVar(&cfg.requireTargetOnly, "require-cassandra-target-only", false, "reject managed-server configs that can read or write legacy Cassandra layouts")
	flags.StringVar(&cfg.resumeID, "resume-id", "", "immutable protocol identifier used to checkpoint and resume clean samples")
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
	if cfg.resumeID != "" {
		if strings.TrimSpace(cfg.resumeID) == "" {
			return errors.New("-resume-id must not be blank")
		}
		if cfg.resetCommand == "" || cfg.cleanupCommand == "" {
			return errors.New("-resume-id requires explicit -reset-command and -cleanup-command")
		}
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
	var err error
	cfg, err = withAbsoluteOutputDir(cfg)
	if err != nil {
		return err
	}
	return runSuiteWithExecutor(ctx, cfg, commandExecutor(cfg))
}

func runSuiteWithExecutor(ctx context.Context, cfg config, executor runExecutor) error {
	var err error
	cfg, err = withAbsoluteOutputDir(cfg)
	if err != nil {
		return err
	}
	if cfg.resumeID != "" {
		return runSuiteWithCheckpoint(ctx, cfg, executor)
	}
	return runSuiteWithoutCheckpoint(ctx, cfg, executor)
}

func withAbsoluteOutputDir(cfg config) (config, error) {
	outputDir, err := filepath.Abs(cfg.outputDir)
	if err != nil {
		return config{}, fmt.Errorf("resolve output directory: %w", err)
	}
	cfg.outputDir = filepath.Clean(outputDir)
	return cfg, nil
}

func runSuiteWithoutCheckpoint(ctx context.Context, cfg config, executor runExecutor) error {
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
		TargetOnly: cfg.requireTargetOnly, Trials: cfg.trials, Payloads: payloads, Profiles: profiles,
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

func runSuiteWithCheckpoint(ctx context.Context, cfg config, executor runExecutor) error {
	if err := os.MkdirAll(cfg.outputDir, 0o755); err != nil {
		return err
	}
	checkpointPath := filepath.Join(cfg.outputDir, checkpointFileName)
	checkpoint, err := loadOrCreateCheckpoint(checkpointPath, cfg)
	if err != nil {
		return err
	}
	if err := validateCheckpoint(cfg, checkpoint); err != nil {
		return fmt.Errorf("validate resume checkpoint: %w", err)
	}

	profiles, _ := parseProfiles(cfg.profiles)
	payloads, _ := parsePayloads(cfg.payloads)
	caseIndex := 0
	for _, profile := range profiles {
		for _, payload := range payloads {
			if len(checkpoint.Cases) == caseIndex {
				checkpoint.Cases = append(checkpoint.Cases, checkpointCase{
					Profile: profile, PayloadBytes: payload,
				})
			}
			caseErr := runCheckpointCase(ctx, cfg, executor, checkpoint, caseIndex, checkpointPath)
			if caseErr != nil {
				writeErr := writePartialCheckpointSuite(cfg, checkpoint)
				return errors.Join(
					fmt.Errorf("profile %s payload %d: %w", profile.Name, payload, caseErr),
					writeErr,
				)
			}
			caseIndex++
		}
	}

	checkpoint.Complete = true
	if err := persistCheckpoint(checkpointPath, checkpoint); err != nil {
		checkpoint.Complete = false
		return err
	}
	result, err := suiteFromCheckpoint(cfg, checkpoint)
	if err != nil {
		return err
	}
	result.FinishedAt = time.Now().UTC()
	return writeJSON(filepath.Join(cfg.outputDir, "suite.json"), result)
}

func loadOrCreateCheckpoint(path string, cfg config) (*suiteCheckpoint, error) {
	var checkpoint suiteCheckpoint
	if err := readJSON(path, &checkpoint); err == nil {
		return &checkpoint, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read resume checkpoint: %w", err)
	}

	spec, specSHA256, err := checkpointSpecForConfig(cfg)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	checkpoint = suiteCheckpoint{
		Version: checkpointVersion, ResumeID: cfg.resumeID,
		Spec: spec, SpecSHA256: specSHA256, StartedAt: now, UpdatedAt: now,
	}
	if err := writeJSON(path, checkpoint); err != nil {
		return nil, fmt.Errorf("initialize resume checkpoint: %w", err)
	}
	return &checkpoint, nil
}

func checkpointSpecForConfig(cfg config) (checkpointSpec, string, error) {
	profiles, err := parseProfiles(cfg.profiles)
	if err != nil {
		return checkpointSpec{}, "", err
	}
	payloads, err := parsePayloads(cfg.payloads)
	if err != nil {
		return checkpointSpec{}, "", err
	}
	spec := checkpointSpec{
		SuiteVersion: suiteVersion, Address: cfg.address, Namespace: cfg.namespace,
		OutputDir: cfg.outputDir, ResetCommand: cfg.resetCommand, CleanupCommand: cfg.cleanupCommand,
		ServerConfigFile: cfg.serverConfigFile, ServerBinary: cfg.serverBinary,
		ServerStartTimeout: cfg.serverStartTimeout, ServerStopTimeout: cfg.serverStopTimeout,
		RequireTargetOnly: cfg.requireTargetOnly, Profiles: profiles, Payloads: payloads,
		TaskQueues: cfg.taskQueues, WorkersPerTaskQueue: cfg.workersPerTaskQueue,
		Concurrency: cfg.concurrency, Warmup: cfg.warmup, Measurement: cfg.measurement,
		Trials: cfg.trials, InitialRPS: cfg.initialRPS, GrowthFactor: cfg.growthFactor,
		MaxRPS: cfg.maxRPS, SuccessRatio: cfg.successRatio, BatchWorkflows: cfg.batchWorkflows,
		SampleTimeout: cfg.sampleTimeout, Timeout: cfg.timeout,
	}
	hash, err := jsonSHA256(spec)
	if err != nil {
		return checkpointSpec{}, "", fmt.Errorf("hash checkpoint specification: %w", err)
	}
	return spec, hash, nil
}

func validateCheckpoint(cfg config, checkpoint *suiteCheckpoint) error {
	if err := validateCheckpointHeader(cfg, checkpoint); err != nil {
		return err
	}
	allCasesComplete, err := validateCheckpointCases(cfg, checkpoint)
	if err != nil {
		return err
	}
	if checkpoint.Complete && !allCasesComplete {
		return errors.New("checkpoint is marked complete but its sample sequence is incomplete")
	}
	return nil
}

func validateCheckpointHeader(cfg config, checkpoint *suiteCheckpoint) error {
	if checkpoint.Version != checkpointVersion {
		return fmt.Errorf("checkpoint version is %d, expected %d", checkpoint.Version, checkpointVersion)
	}
	if checkpoint.ResumeID != cfg.resumeID {
		return fmt.Errorf("resume ID is %q, expected %q", checkpoint.ResumeID, cfg.resumeID)
	}
	if checkpoint.StartedAt.IsZero() || checkpoint.UpdatedAt.IsZero() {
		return errors.New("checkpoint timestamps are missing")
	}
	if checkpoint.UpdatedAt.Before(checkpoint.StartedAt) {
		return errors.New("checkpoint update timestamp precedes its start timestamp")
	}
	_, expectedSpecSHA256, err := checkpointSpecForConfig(cfg)
	if err != nil {
		return err
	}
	actualSpecSHA256, err := jsonSHA256(checkpoint.Spec)
	if err != nil {
		return fmt.Errorf("hash stored checkpoint specification: %w", err)
	}
	if checkpoint.SpecSHA256 != actualSpecSHA256 {
		return errors.New("checkpoint specification checksum is invalid")
	}
	if checkpoint.SpecSHA256 != expectedSpecSHA256 {
		return errors.New("checkpoint configuration or protocol inputs changed")
	}
	return nil
}

func validateCheckpointCases(cfg config, checkpoint *suiteCheckpoint) (bool, error) {
	profiles, _ := parseProfiles(cfg.profiles)
	payloads, _ := parsePayloads(cfg.payloads)
	expectedCaseCount := len(profiles) * len(payloads)
	if len(checkpoint.Cases) > expectedCaseCount {
		return false, fmt.Errorf("checkpoint has %d cases, expected at most %d", len(checkpoint.Cases), expectedCaseCount)
	}
	caseIndex := 0
	allCasesComplete := true
	for _, profile := range profiles {
		for _, payload := range payloads {
			if caseIndex >= len(checkpoint.Cases) {
				allCasesComplete = false
				caseIndex++
				continue
			}
			checkpointCase := checkpoint.Cases[caseIndex]
			if checkpointCase.Profile != profile || checkpointCase.PayloadBytes != payload {
				return false, fmt.Errorf("checkpoint case %d does not match profile %s payload %d", caseIndex, profile.Name, payload)
			}
			state, err := reconstructCheckpointCase(cfg, checkpointCase)
			if err != nil {
				return false, fmt.Errorf("checkpoint case %d: %w", caseIndex, err)
			}
			if !state.Done {
				allCasesComplete = false
				if caseIndex+1 < len(checkpoint.Cases) {
					return false, fmt.Errorf("checkpoint case %d is incomplete before a later case", caseIndex)
				}
			}
			caseIndex++
		}
	}
	return allCasesComplete, nil
}

func persistCheckpoint(path string, checkpoint *suiteCheckpoint) error {
	checkpoint.UpdatedAt = time.Now().UTC()
	if err := writeJSON(path, checkpoint); err != nil {
		return fmt.Errorf("write resume checkpoint: %w", err)
	}
	return nil
}

func writePartialCheckpointSuite(cfg config, checkpoint *suiteCheckpoint) error {
	result, err := suiteFromCheckpoint(cfg, checkpoint)
	if err != nil {
		return err
	}
	result.FinishedAt = time.Now().UTC()
	return writeJSON(filepath.Join(cfg.outputDir, "suite.json"), result)
}

func suiteFromCheckpoint(cfg config, checkpoint *suiteCheckpoint) (suiteResult, error) {
	profiles, _ := parseProfiles(cfg.profiles)
	payloads, _ := parsePayloads(cfg.payloads)
	serverBinary := ""
	if cfg.serverConfigFile != "" {
		serverBinary = cfg.serverBinary
	}
	result := suiteResult{
		Version: suiteVersion, StartedAt: checkpoint.StartedAt, Address: cfg.address,
		Namespace: cfg.namespace, ServerConfigFile: cfg.serverConfigFile,
		ServerBinary: serverBinary, Warmup: cfg.warmup, Measurement: cfg.measurement,
		TargetOnly: cfg.requireTargetOnly, Trials: cfg.trials, Payloads: payloads, Profiles: profiles,
	}
	for caseIndex, checkpointCase := range checkpoint.Cases {
		state, err := reconstructCheckpointCase(cfg, checkpointCase)
		if err != nil {
			return suiteResult{}, fmt.Errorf("reconstruct checkpoint case %d: %w", caseIndex, err)
		}
		result.Cases = append(result.Cases, state.Result)
	}
	return result, nil
}

func jsonSHA256(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum), nil
}

func runCheckpointCase(
	ctx context.Context,
	cfg config,
	execute runExecutor,
	checkpoint *suiteCheckpoint,
	caseIndex int,
	checkpointPath string,
) error {
	checkpointCase := &checkpoint.Cases[caseIndex]
	for {
		state, err := reconstructCheckpointCase(cfg, *checkpointCase)
		if err != nil {
			return err
		}
		if state.Done {
			return nil
		}
		request := *state.Next
		if err := archiveUncommittedRunDir(cfg.outputDir, request); err != nil {
			return err
		}
		record, runErr := execute(ctx, request)
		acceptance, err := classifyCheckpointRun(cfg, request, state.Maximum, record, runErr)
		if err != nil {
			return err
		}
		acceptedRun, err := makeCheckpointRun(cfg, request, record, acceptance)
		if err != nil {
			return err
		}

		checkpointCase.Runs = append(checkpointCase.Runs, acceptedRun)
		if err := persistCheckpoint(checkpointPath, checkpoint); err != nil {
			checkpointCase.Runs = checkpointCase.Runs[:len(checkpointCase.Runs)-1]
			return err
		}
	}
}

func classifyCheckpointRun(
	cfg config,
	request runRequest,
	maximum float64,
	record runRecord,
	runErr error,
) (string, error) {
	if request.stage != "calibration" {
		if runErr != nil {
			return "", runErr
		}
		if !completeCheckpointResult(record.Result, request.workflows) {
			return "", fmt.Errorf("%s sample did not complete all %d workflows", request.stage, request.workflows)
		}
		if request.stage == "steady" && !sustainable(record.Result, request.targetRPS, cfg.successRatio) {
			return "", fmt.Errorf("steady sample at %.2f workflows/s is not sustainable", request.targetRPS)
		}
		return checkpointSuccess, nil
	}

	if runErr != nil {
		if maximum <= 0 || !isOnlyLoadSaturationError(runErr) {
			return "", runErr
		}
		if sustainable(record.Result, request.targetRPS, cfg.successRatio) {
			return "", errors.New("calibration saturation error returned a sustainable result")
		}
		expectedWorkflows, err := checkpointRunWorkflows(cfg, request, record, checkpointBoundary)
		if err != nil {
			return "", err
		}
		if !validCheckpointBoundary(record.Result, expectedWorkflows) {
			return "", errors.New("calibration saturation boundary result is invalid")
		}
		return checkpointBoundary, nil
	}
	if sustainable(record.Result, request.targetRPS, cfg.successRatio) {
		if !completeCheckpointResult(record.Result, request.workflows) {
			return "", fmt.Errorf("calibration sample did not complete all %d workflows", request.workflows)
		}
		return checkpointSuccess, nil
	}
	if maximum <= 0 {
		return "", fmt.Errorf("initial rate %.2f workflows/s is not sustainable", request.targetRPS)
	}
	expectedWorkflows, err := checkpointRunWorkflows(cfg, request, record, checkpointBoundary)
	if err != nil {
		return "", err
	}
	if !validCheckpointBoundary(record.Result, expectedWorkflows) {
		return "", errors.New("calibration boundary result is invalid")
	}
	return checkpointBoundary, nil
}

func completeCheckpointResult(result loadResult, expectedWorkflows int) bool {
	return expectedWorkflows > 0 && result.Workflows == expectedWorkflows &&
		result.Failed == 0 && result.Launched == int64(expectedWorkflows) &&
		result.Completed == int64(expectedWorkflows) && result.Requests > 0 &&
		result.Elapsed > 0 && result.CompletionElapsed > 0 &&
		result.WorkflowsPerSec > 0 && result.RequestsPerSec > 0
}

func validCheckpointBoundary(result loadResult, expectedWorkflows int) bool {
	return expectedWorkflows > 0 && result.Workflows == expectedWorkflows &&
		result.Launched >= 0 && result.Launched <= int64(expectedWorkflows) &&
		result.Completed >= 0 && result.Completed <= result.Launched &&
		result.Failed >= 0 && result.Completed+result.Failed == int64(expectedWorkflows) &&
		result.Requests >= 0 && result.Elapsed > 0
}

func reconstructCheckpointCase(cfg config, checkpointCase checkpointCase) (resumableCaseState, error) {
	result := caseResult{Profile: checkpointCase.Profile, PayloadBytes: checkpointCase.PayloadBytes}
	runIndex, maximum, calibrationCeiling, next, err := reconstructCheckpointCalibration(
		cfg,
		checkpointCase,
		&result,
	)
	if err != nil {
		return resumableCaseState{}, err
	}
	if next != nil {
		return resumableCaseState{Result: result, Next: next, Maximum: maximum}, nil
	}
	result.MaximumThroughput = maximum
	result.CalibrationCeiling = calibrationCeiling

	runIndex, next, err = reconstructCheckpointSteady(cfg, checkpointCase, &result, runIndex, maximum)
	if err != nil {
		return resumableCaseState{}, err
	}
	if next != nil {
		return resumableCaseState{Result: result, Next: next, Maximum: maximum}, nil
	}
	return reconstructCheckpointBatch(cfg, checkpointCase, result, runIndex, maximum)
}

func reconstructCheckpointCalibration(
	cfg config,
	checkpointCase checkpointCase,
	result *caseResult,
) (int, float64, bool, *runRequest, error) {
	runIndex := 0
	maximum := 0.0
	for offered := cfg.initialRPS; offered <= cfg.maxRPS; offered *= cfg.growthFactor {
		request := runRequest{
			profile: checkpointCase.Profile, payloadBytes: checkpointCase.PayloadBytes,
			stage: "calibration", targetRPS: offered,
			workflows: workflowsForDuration(offered, cfg.measurement), duration: cfg.measurement, trial: 1,
		}
		if runIndex == len(checkpointCase.Runs) {
			return runIndex, maximum, false, &request, nil
		}
		checkpointRun := checkpointCase.Runs[runIndex]
		if err := validateCheckpointRun(cfg, request, checkpointRun); err != nil {
			return 0, 0, false, nil, fmt.Errorf("run %d: %w", runIndex, err)
		}
		result.Runs = append(result.Runs, checkpointRun.Record)
		runIndex++

		isSustainable := sustainable(checkpointRun.Record.Result, offered, cfg.successRatio)
		switch checkpointRun.Acceptance {
		case checkpointSuccess:
			if !isSustainable {
				return 0, 0, false, nil, fmt.Errorf("run %d is marked successful but is not sustainable", runIndex-1)
			}
			maximum = max(maximum, min(offered, checkpointRun.Record.Result.WorkflowsPerSec))
			if offered > cfg.maxRPS/cfg.growthFactor {
				return runIndex, maximum, true, nil, nil
			}
		case checkpointBoundary:
			if maximum <= 0 || isSustainable {
				return 0, 0, false, nil, fmt.Errorf("run %d is not a valid calibration boundary", runIndex-1)
			}
			return runIndex, maximum, false, nil, nil
		default:
			return 0, 0, false, nil, fmt.Errorf("run %d has invalid acceptance %q", runIndex-1, checkpointRun.Acceptance)
		}
	}
	return 0, 0, false, nil, errors.New("calibration checkpoint has no next rate or terminal state")
}

func reconstructCheckpointSteady(
	cfg config,
	checkpointCase checkpointCase,
	result *caseResult,
	runIndex int,
	maximum float64,
) (int, *runRequest, error) {
	for _, ratio := range []float64{0.50, 0.80, 0.90} {
		target := maximum * ratio
		trials := make([]loadResult, 0, cfg.trials)
		for trial := 1; trial <= cfg.trials; trial++ {
			request := runRequest{
				profile: checkpointCase.Profile, payloadBytes: checkpointCase.PayloadBytes,
				stage: "steady", targetRatio: ratio, targetRPS: target,
				workflows: workflowsForDuration(target, cfg.measurement), duration: cfg.measurement, trial: trial,
			}
			if runIndex == len(checkpointCase.Runs) {
				return runIndex, &request, nil
			}
			checkpointRun := checkpointCase.Runs[runIndex]
			if checkpointRun.Acceptance != checkpointSuccess {
				return 0, nil, fmt.Errorf("run %d is a non-calibration boundary", runIndex)
			}
			if err := validateCheckpointRun(cfg, request, checkpointRun); err != nil {
				return 0, nil, fmt.Errorf("run %d: %w", runIndex, err)
			}
			result.Runs = append(result.Runs, checkpointRun.Record)
			trials = append(trials, checkpointRun.Record.Result)
			runIndex++
		}
		result.Targets = append(result.Targets, summarizeTarget(ratio, target, trials))
	}
	return runIndex, nil, nil
}

func reconstructCheckpointBatch(
	cfg config,
	checkpointCase checkpointCase,
	result caseResult,
	runIndex int,
	maximum float64,
) (resumableCaseState, error) {
	request := runRequest{
		profile: checkpointCase.Profile, payloadBytes: checkpointCase.PayloadBytes,
		stage: "batch", workflows: cfg.batchWorkflows, trial: 1,
	}
	if runIndex == len(checkpointCase.Runs) {
		return resumableCaseState{Result: result, Next: &request, Maximum: maximum}, nil
	}
	checkpointRun := checkpointCase.Runs[runIndex]
	if checkpointRun.Acceptance != checkpointSuccess {
		return resumableCaseState{}, fmt.Errorf("run %d is a batch boundary", runIndex)
	}
	if err := validateCheckpointRun(cfg, request, checkpointRun); err != nil {
		return resumableCaseState{}, fmt.Errorf("run %d: %w", runIndex, err)
	}
	result.Runs = append(result.Runs, checkpointRun.Record)
	runIndex++
	if runIndex != len(checkpointCase.Runs) {
		return resumableCaseState{}, fmt.Errorf("checkpoint contains %d unexpected runs", len(checkpointCase.Runs)-runIndex)
	}
	return resumableCaseState{Result: result, Maximum: maximum, Done: true}, nil
}

func makeCheckpointRun(
	cfg config,
	request runRequest,
	record runRecord,
	acceptance string,
) (checkpointRun, error) {
	if err := validateRunRecord(cfg, request, record, acceptance); err != nil {
		return checkpointRun{}, err
	}
	artifacts, err := collectCheckpointArtifacts(cfg, request, record, acceptance)
	if err != nil {
		return checkpointRun{}, err
	}
	return checkpointRun{Record: record, Acceptance: acceptance, Artifacts: artifacts}, nil
}

func validateCheckpointRun(cfg config, request runRequest, run checkpointRun) error {
	if err := validateRunRecord(cfg, request, run.Record, run.Acceptance); err != nil {
		return err
	}
	artifacts, err := collectCheckpointArtifacts(cfg, request, run.Record, run.Acceptance)
	if err != nil {
		return err
	}
	if !slices.Equal(run.Artifacts, artifacts) {
		return errors.New("checkpoint artifact checksums changed")
	}
	return nil
}

func validateRunRecord(cfg config, request runRequest, record runRecord, acceptance string) error {
	if record.Profile != request.profile || record.PayloadBytes != request.payloadBytes ||
		record.Stage != request.stage || record.TargetRatio != request.targetRatio ||
		record.TargetRPS != request.targetRPS || record.Trial != request.trial {
		return errors.New("checkpoint run identity does not match the expected sample")
	}
	if record.Result.TargetWorkflowsPerSec != request.targetRPS {
		return errors.New("checkpoint result target does not match the expected sample")
	}
	expectedWorkflows, err := checkpointRunWorkflows(cfg, request, record, acceptance)
	if err != nil {
		return err
	}
	if acceptance == checkpointSuccess {
		if !completeCheckpointResult(record.Result, expectedWorkflows) {
			return errors.New("checkpoint success result is incomplete")
		}
		if (request.stage == "calibration" || request.stage == "steady") &&
			!sustainable(record.Result, request.targetRPS, cfg.successRatio) {
			return errors.New("checkpoint success result is below its sustainable target")
		}
	} else if acceptance == checkpointBoundary && !validCheckpointBoundary(record.Result, expectedWorkflows) {
		return errors.New("checkpoint calibration boundary result is invalid")
	}
	runDir := filepath.Join(cfg.outputDir, runArtifactName(request))
	if _, err := secureArtifactPath(runDir, filepath.Base(record.ResultFile)); err != nil {
		return err
	}
	if _, err := secureArtifactPath(runDir, filepath.Base(record.MetadataFile)); err != nil {
		return err
	}
	var result loadResult
	if err := readJSON(record.ResultFile, &result); err != nil {
		return fmt.Errorf("read checkpoint result: %w", err)
	}
	if result != record.Result {
		return errors.New("checkpoint record differs from its result file")
	}
	return nil
}

func checkpointRunWorkflows(
	cfg config,
	request runRequest,
	record runRecord,
	acceptance string,
) (int, error) {
	runDir := filepath.Join(cfg.outputDir, runArtifactName(request))
	expectedResultFile := filepath.Join(runDir, "measure.result.json")
	expectedMetadataFile := filepath.Join(runDir, "measure.metadata.json")
	if acceptance == checkpointBoundary && filepath.Clean(record.ResultFile) == filepath.Join(runDir, "warmup.result.json") {
		expectedResultFile = filepath.Join(runDir, "warmup.result.json")
		expectedMetadataFile = filepath.Join(runDir, "warmup.metadata.json")
	} else if acceptance != checkpointSuccess && acceptance != checkpointBoundary {
		return 0, fmt.Errorf("invalid checkpoint acceptance %q", acceptance)
	}
	if !filepath.IsAbs(record.ResultFile) || filepath.Clean(record.ResultFile) != expectedResultFile ||
		!filepath.IsAbs(record.MetadataFile) || filepath.Clean(record.MetadataFile) != expectedMetadataFile {
		return 0, errors.New("checkpoint result paths do not match the expected sample directory")
	}
	if filepath.Base(expectedResultFile) == "warmup.result.json" {
		return workflowsForDuration(request.targetRPS, cfg.warmup), nil
	}
	return request.workflows, nil
}

func collectCheckpointArtifacts(
	cfg config,
	request runRequest,
	record runRecord,
	acceptance string,
) ([]checkpointArtifact, error) {
	runName := runArtifactName(request)
	runDir := filepath.Join(cfg.outputDir, runName)
	markerPath := filepath.Join(runDir, "sample-clean.json")
	if _, err := secureArtifactPath(runDir, "sample-clean.json"); err != nil {
		return nil, err
	}
	var marker sampleCleanMarker
	if err := readJSON(markerPath, &marker); err != nil {
		return nil, fmt.Errorf("read clean-sample marker: %w", err)
	}
	if marker.Version != sampleCleanMarkerVersion || marker.ResumeID != cfg.resumeID ||
		marker.RunName != runName || !marker.Clean {
		return nil, errors.New("clean-sample marker does not match the checkpointed sample")
	}

	directPaths := []string{
		"cleanup.log",
		"reset.log",
		"sample-capture-state.json",
		"sample-clean.json",
		"warmup.log",
		"warmup.metadata.json",
		"warmup.result.json",
	}
	if filepath.Base(record.ResultFile) == "measure.result.json" {
		directPaths = append(directPaths, "measure.log", "measure.metadata.json", "measure.result.json")
	} else if acceptance != checkpointBoundary {
		return nil, errors.New("only a calibration boundary may omit measurement artifacts")
	}
	if cfg.serverConfigFile != "" {
		directPaths = append(directPaths, "server.log")
	}
	directPaths = append(directPaths, observabilityManifest)

	seen := make(map[string]struct{}, len(directPaths))
	artifacts := make([]checkpointArtifact, 0, len(directPaths))
	for _, relativePath := range directPaths {
		artifact, err := checkpointArtifactForPath(runDir, relativePath)
		if err != nil {
			return nil, err
		}
		seen[relativePath] = struct{}{}
		artifacts = append(artifacts, artifact)
	}
	observabilityArtifacts, err := readObservabilityManifest(runDir, seen)
	if err != nil {
		return nil, err
	}
	artifacts = append(artifacts, observabilityArtifacts...)
	slices.SortFunc(artifacts, func(a checkpointArtifact, b checkpointArtifact) int {
		return strings.Compare(a.Path, b.Path)
	})
	return artifacts, nil
}

func readObservabilityManifest(
	runDir string,
	seen map[string]struct{},
) ([]checkpointArtifact, error) {
	manifestPath, err := secureArtifactPath(runDir, observabilityManifest)
	if err != nil {
		return nil, err
	}
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("read observability manifest: %w", err)
	}

	var artifacts []checkpointArtifact
	previousPath := ""
	scanner := bufio.NewScanner(bytes.NewReader(manifest))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) != 2 || len(parts[0]) != sha256.Size*2 {
			return nil, fmt.Errorf("invalid observability checksum line %q", line)
		}
		if _, err := hex.DecodeString(parts[0]); err != nil || parts[0] != strings.ToLower(parts[0]) {
			return nil, fmt.Errorf("invalid observability checksum %q", parts[0])
		}
		relativePath := parts[1]
		if !strings.HasPrefix(relativePath, "observability/") {
			return nil, fmt.Errorf("observability artifact %q is outside observability/", relativePath)
		}
		if relativePath <= previousPath {
			return nil, errors.New("observability manifest paths are not strictly sorted")
		}
		if _, duplicate := seen[relativePath]; duplicate {
			return nil, fmt.Errorf("duplicate checkpoint artifact %q", relativePath)
		}
		artifact, err := checkpointArtifactForPath(runDir, relativePath)
		if err != nil {
			return nil, err
		}
		if artifact.SHA256 != parts[0] {
			return nil, fmt.Errorf("observability artifact checksum mismatch for %q", relativePath)
		}
		seen[relativePath] = struct{}{}
		artifacts = append(artifacts, artifact)
		previousPath = relativePath
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read observability manifest: %w", err)
	}
	if len(artifacts) == 0 {
		return nil, errors.New("observability manifest is empty")
	}
	return artifacts, nil
}

func checkpointArtifactForPath(runDir string, relativePath string) (checkpointArtifact, error) {
	path, err := secureArtifactPath(runDir, relativePath)
	if err != nil {
		return checkpointArtifact{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return checkpointArtifact{}, fmt.Errorf("open checkpoint artifact %q: %w", relativePath, err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return checkpointArtifact{}, fmt.Errorf("hash checkpoint artifact %q: %w", relativePath, err)
	}
	return checkpointArtifact{Path: relativePath, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func secureArtifactPath(runDir string, relativePath string) (string, error) {
	if relativePath == "" || filepath.IsAbs(relativePath) || filepath.ToSlash(filepath.Clean(relativePath)) != relativePath ||
		relativePath == "." || relativePath == ".." || strings.HasPrefix(relativePath, "../") {
		return "", fmt.Errorf("invalid checkpoint artifact path %q", relativePath)
	}
	path := filepath.Join(runDir, filepath.FromSlash(relativePath))
	resolvedRunDir, err := filepath.EvalSymlinks(runDir)
	if err != nil {
		return "", fmt.Errorf("resolve sample artifact directory: %w", err)
	}
	if filepath.Clean(resolvedRunDir) != filepath.Clean(runDir) {
		return "", errors.New("sample artifact directory contains a symbolic link")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("inspect checkpoint artifact %q: %w", relativePath, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("checkpoint artifact %q is not a regular file", relativePath)
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve checkpoint artifact %q: %w", relativePath, err)
	}
	containedPath, err := filepath.Rel(resolvedRunDir, resolvedPath)
	if err != nil || containedPath == ".." || strings.HasPrefix(containedPath, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("checkpoint artifact %q escapes its sample directory", relativePath)
	}
	return path, nil
}

func archiveUncommittedRunDir(outputDir string, request runRequest) error {
	runName := runArtifactName(request)
	runDir := filepath.Join(outputDir, runName)
	info, err := os.Lstat(runDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect uncommitted sample directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("uncommitted sample path is not a directory: %s", runDir)
	}
	rejectedDir := filepath.Join(outputDir, "rejected-samples")
	if err := os.MkdirAll(rejectedDir, 0o755); err != nil {
		return fmt.Errorf("create rejected-sample directory: %w", err)
	}
	for attempt := 1; ; attempt++ {
		destination := filepath.Join(rejectedDir, fmt.Sprintf("%s-%d", runName, attempt))
		if _, err := os.Lstat(destination); errors.Is(err, os.ErrNotExist) {
			if err := os.Rename(runDir, destination); err != nil {
				return fmt.Errorf("archive uncommitted sample directory: %w", err)
			}
			return nil
		} else if err != nil {
			return fmt.Errorf("inspect rejected-sample destination: %w", err)
		}
	}
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
		dir, err := filepath.Abs(filepath.Join(cfg.outputDir, name))
		if err != nil {
			return record, fmt.Errorf("resolve sample artifact directory: %w", err)
		}
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
	runName := runArtifactName(request)
	return append(os.Environ(),
		"TEMPORALPERF_ADDRESS="+cfg.address,
		"TEMPORALPERF_NAMESPACE="+cfg.namespace,
		"TEMPORALPERF_CONFIG_FILE="+cfg.serverConfigFile,
		"TEMPORALPERF_RESUME_ID="+cfg.resumeID,
		"TEMPORALPERF_RUN_NAME="+runName,
		"TEMPORALPERF_PROFILE="+request.profile.Name,
		"TEMPORALPERF_PAYLOAD_BYTES="+strconv.Itoa(request.payloadBytes),
		"TEMPORALPERF_STAGE="+request.stage,
		"TEMPORALPERF_TRIAL="+strconv.Itoa(request.trial),
		"TEMPORALPERF_TARGET_RPS="+strconv.FormatFloat(request.targetRPS, 'g', -1, 64),
		"TEMPORALPERF_TARGET_RATIO="+strconv.FormatFloat(request.targetRatio, 'g', -1, 64),
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
