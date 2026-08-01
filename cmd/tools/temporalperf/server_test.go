package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	clusterspb "go.temporal.io/server/api/cluster/v1"
	serverconfig "go.temporal.io/server/common/config"
)

func TestPrepareConfigLoadsTemporalServerConfigAndDerivesAddress(t *testing.T) {
	configFile, err := filepath.Abs("../../../config/development-sqlite.yaml")
	require.NoError(t, err)
	cfg, err := parseFlags([]string{"-config-file", configFile, "-reset-command", ":"})
	require.NoError(t, err)

	require.NoError(t, prepareConfig(&cfg))
	require.Equal(t, configFile, cfg.serverConfigFile)
	require.Equal(t, "127.0.0.1:7233", cfg.address)
}

func TestPrepareConfigPreservesExplicitAddress(t *testing.T) {
	configFile, err := filepath.Abs("../../../config/development-sqlite.yaml")
	require.NoError(t, err)
	cfg, err := parseFlags([]string{
		"-config-file", configFile,
		"-address", "frontend.example:8233",
		"-reset-command", ":",
	})
	require.NoError(t, err)

	require.NoError(t, prepareConfig(&cfg))
	require.Equal(t, "frontend.example:8233", cfg.address)
}

func TestFrontendAddressUsesReachableLoopbackForWildcard(t *testing.T) {
	for name, bindIP := range map[string]string{
		"IPv4": "0.0.0.0",
		"IPv6": "::",
	} {
		t.Run(name, func(t *testing.T) {
			cfg := &serverconfig.Config{Services: map[string]serverconfig.Service{
				"frontend": {RPC: serverconfig.RPC{GRPCPort: 7233, BindOnIP: bindIP}},
			}}

			address, err := frontendAddress(cfg)

			require.NoError(t, err)
			if bindIP == "::" {
				require.Equal(t, "[::1]:7233", address)
			} else {
				require.Equal(t, "127.0.0.1:7233", address)
			}
		})
	}
}

func TestFrontendAddressRejectsMissingFrontend(t *testing.T) {
	_, err := frontendAddress(&serverconfig.Config{})
	require.ErrorContains(t, err, "frontend service")
}

func TestManagedServerCommandUsesUnchangedTemporalConfig(t *testing.T) {
	cfg := config{
		serverBinary:     "/opt/temporal-server",
		serverConfigFile: "/etc/temporal/benchmark.yaml",
	}

	command := managedServerCommand(t.Context(), cfg)

	require.Equal(t, []string{
		"/opt/temporal-server",
		"--config-file", "/etc/temporal/benchmark.yaml",
		"--allow-no-auth",
		"start",
	}, command.Args)
}

func TestHookEnvironmentIncludesTemporalConfigFile(t *testing.T) {
	env := hookEnvironment(
		config{serverConfigFile: "/etc/temporal/benchmark.yaml"},
		runRequest{profile: profileCatalog["tiny"]},
		t.TempDir(),
	)
	require.Contains(t, env, "TEMPORALPERF_CONFIG_FILE=/etc/temporal/benchmark.yaml")
}

func TestValidateConfigRejectsInvalidManagedServerSettings(t *testing.T) {
	base, err := parseFlags(nil)
	require.NoError(t, err)
	base.serverConfigFile = "/etc/temporal/benchmark.yaml"
	require.NoError(t, validateConfig(base))

	missingBinary := base
	missingBinary.serverBinary = ""
	require.ErrorContains(t, validateConfig(missingBinary), "-server-binary")

	invalidStartTimeout := base
	invalidStartTimeout.serverStartTimeout = 0
	require.ErrorContains(t, validateConfig(invalidStartTimeout), "-server-start-timeout")

	invalidStopTimeout := base
	invalidStopTimeout.serverStopTimeout = 0
	require.ErrorContains(t, validateConfig(invalidStopTimeout), "-server-stop-timeout")
}

func TestCommandExecutorManagesServerBetweenResetAndCleanup(t *testing.T) {
	dir := t.TempDir()
	lifecycle := filepath.Join(dir, "lifecycle.log")
	appendLifecycle := func(event string) error {
		file, err := os.OpenFile(lifecycle, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		_, writeErr := fmt.Fprintln(file, event)
		return errors.Join(writeErr, file.Close())
	}
	cfg := config{
		address: "127.0.0.1:7233", namespace: "temporal-perf", outputDir: dir,
		resetCommand:      fmt.Sprintf("printf 'reset\\n' >> %q", lifecycle),
		cleanupCommand:    fmt.Sprintf("printf 'cleanup\\n' >> %q", lifecycle),
		serverConfigFile:  "/etc/temporal/benchmark.yaml",
		serverStopTimeout: time.Second,
		taskQueues:        1, workersPerTaskQueue: 1, concurrency: 1,
		warmup: time.Second, initialRPS: 1, timeout: time.Minute,
	}
	startServer := func(_ context.Context, got config, _ string) (serverStopper, error) {
		require.Equal(t, cfg.serverConfigFile, got.serverConfigFile)
		require.NoError(t, appendLifecycle("start"))
		return func(context.Context) error {
			return appendLifecycle("stop")
		}, nil
	}
	executeLoad := func(
		ctx context.Context,
		cfg config,
		warmup runRequest,
		measure runRequest,
		artifactDir string,
	) (runRecord, error) {
		require.NoError(t, appendLifecycle("load"))
		return fakeLoadExecutor(ctx, cfg, warmup, measure, artifactDir)
	}
	request := runRequest{
		profile: profileCatalog["tiny"], payloadBytes: 128, stage: "steady",
		targetRPS: 1, workflows: 1, trial: 1,
	}

	_, err := commandExecutorWithLoadAndServer(cfg, executeLoad, startServer)(t.Context(), request)

	require.NoError(t, err)
	contents, err := os.ReadFile(lifecycle)
	require.NoError(t, err)
	require.Equal(t, "reset\nstart\nload\nstop\ncleanup\n", string(contents))
}

func TestWaitForManagedServerReportsEarlyExit(t *testing.T) {
	done := make(chan error, 1)
	exitErr := errors.New("server startup failed")
	done <- exitErr

	err := waitForManagedServer(t.Context(), "127.0.0.1:7233", done, func(context.Context, string) error {
		t.Fatal("health check must not run after the server exits")
		return nil
	})

	require.ErrorIs(t, err, exitErr)
	require.ErrorIs(t, <-done, exitErr)
}

func TestWaitForManagedServerAcceptsHealthyFrontend(t *testing.T) {
	done := make(chan error, 1)
	healthChecks := 0

	err := waitForManagedServer(t.Context(), "127.0.0.1:7233", done, func(_ context.Context, address string) error {
		healthChecks++
		require.Equal(t, "127.0.0.1:7233", address)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 1, healthChecks)
}

func TestValidateManagedServerMembershipRequiresEveryService(t *testing.T) {
	response := &adminservice.DescribeClusterResponse{MembershipInfo: &clusterspb.MembershipInfo{}}
	for _, service := range managedServerServices {
		response.MembershipInfo.Rings = append(response.MembershipInfo.Rings, &clusterspb.RingInfo{
			Role: service, MemberCount: 1,
		})
	}
	require.NoError(t, validateManagedServerMembership(response))

	response.MembershipInfo.Rings[2].MemberCount = 0
	require.ErrorContains(t, validateManagedServerMembership(response), "matching service")
}

func TestStartManagedServerReportsMissingBinary(t *testing.T) {
	dir := t.TempDir()
	cfg := config{
		address:            "127.0.0.1:7233",
		serverBinary:       filepath.Join(dir, "missing-temporal-server"),
		serverConfigFile:   filepath.Join(dir, "config.yaml"),
		serverStartTimeout: time.Second,
		serverStopTimeout:  time.Second,
	}

	stop, err := startManagedServer(t.Context(), cfg, dir)

	require.ErrorContains(t, err, "missing-temporal-server")
	require.Nil(t, stop)
	_, statErr := os.Stat(filepath.Join(dir, "server.log"))
	require.NoError(t, statErr)
}
