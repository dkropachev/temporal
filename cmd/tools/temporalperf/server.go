package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	serverconfig "go.temporal.io/server/common/config"
	"go.temporal.io/server/temporal/environment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	managedServerHealthInterval = 200 * time.Millisecond
	workflowServiceHealthName   = "temporal.api.workflowservice.v1.WorkflowService"
)

var managedServerServices = []string{"frontend", "history", "matching", "worker"}

type (
	serverStopper func(context.Context) error
	serverStarter func(context.Context, config, string) (serverStopper, error)
	healthChecker func(context.Context, string) error

	managedServerProcess struct {
		command *exec.Cmd
		done    chan error
		logFile *os.File
	}
)

func prepareConfig(cfg *config) error {
	if cfg.serverConfigFile == "" {
		return nil
	}

	configFile, err := filepath.Abs(cfg.serverConfigFile)
	if err != nil {
		return fmt.Errorf("resolve Temporal server config file: %w", err)
	}
	serverCfg, err := serverconfig.Load(serverconfig.WithConfigFile(configFile))
	if err != nil {
		return fmt.Errorf("load Temporal server config: %w", err)
	}
	if cfg.requireTargetOnly {
		if err := validateCassandraTargetOnly(serverCfg); err != nil {
			return err
		}
	}
	cfg.serverConfigFile = configFile
	if !cfg.addressExplicit {
		cfg.address, err = frontendAddress(serverCfg)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateCassandraTargetOnly(cfg *serverconfig.Config) error {
	store, ok := cfg.Persistence.DataStores[cfg.Persistence.DefaultStore]
	if !ok {
		return fmt.Errorf("default persistence store %q is not configured", cfg.Persistence.DefaultStore)
	}
	if store.Cassandra == nil {
		return errors.New("-require-cassandra-target-only requires a Cassandra default store")
	}
	cassandra := store.Cassandra
	checks := []struct {
		name string
		got  string
		want string
	}{
		{name: "historyNodeMigrationMode", got: string(cassandra.HistoryNodeMigrationMode), want: string(serverconfig.CassandraHistoryNodeMigrationModeV2Only)},
		{name: "historyTreeMigrationMode", got: string(cassandra.HistoryTreeMigrationMode), want: "target-only"},
		{name: "executionMigrationMode", got: string(cassandra.ExecutionMigrationMode), want: string(serverconfig.CassandraExecutionMigrationModeTargetOnly)},
		{name: "queueV2MigrationMode", got: string(cassandra.QueueV2MigrationMode), want: "target-only"},
		{name: "legacyQueueMigrationMode", got: string(cassandra.LegacyQueueMigrationMode), want: "target-only"},
		{name: "matchingTaskMigrationMode", got: string(cassandra.MatchingTaskMigrationMode), want: "target-only"},
		{name: "taskQueueUserDataMigrationMode", got: string(cassandra.TaskQueueUserDataMigrationMode), want: "target-only"},
	}
	for _, check := range checks {
		if check.got != check.want {
			return fmt.Errorf("-require-cassandra-target-only requires %s=%q, got %q", check.name, check.want, check.got)
		}
	}
	if cassandra.ExecutionStorageBuckets <= 0 {
		return errors.New("-require-cassandra-target-only requires positive executionStorageBuckets")
	}
	if cassandra.TaskQueueUserDataBucketCount <= 0 {
		return errors.New("-require-cassandra-target-only requires positive taskQueueUserDataBucketCount")
	}
	if cassandra.LegacyQueueMessageBucketSize <= 0 {
		return errors.New("-require-cassandra-target-only requires positive legacyQueueMessageBucketSize")
	}
	if cassandra.MatchingTaskStorageBucketCount <= 0 {
		return errors.New("-require-cassandra-target-only requires positive matchingTaskStorageBucketCount")
	}
	if cassandra.QueueV2MessageBucketSpan <= 0 {
		return errors.New("-require-cassandra-target-only requires positive queueV2MessageBucketSpan")
	}
	return nil
}

func validateManagedServerConfig(cfg config) error {
	if cfg.requireTargetOnly && cfg.serverConfigFile == "" {
		return errors.New("-require-cassandra-target-only requires -config-file")
	}
	if cfg.serverConfigFile == "" {
		return nil
	}
	if cfg.serverBinary == "" {
		return errors.New("-server-binary must be set with -config-file")
	}
	if cfg.serverStartTimeout <= 0 || cfg.serverStopTimeout <= 0 {
		return errors.New("-server-start-timeout and -server-stop-timeout must be positive")
	}
	return nil
}

func frontendAddress(cfg *serverconfig.Config) (string, error) {
	frontend, ok := cfg.Services["frontend"]
	if !ok {
		return "", errors.New("temporal server config does not define the frontend service")
	}
	if frontend.RPC.GRPCPort <= 0 {
		return "", errors.New("temporal frontend gRPC port must be positive")
	}
	if frontend.RPC.BindOnLocalHost && frontend.RPC.BindOnIP != "" {
		return "", errors.New("temporal frontend bindOnLocalHost and bindOnIP are mutually exclusive")
	}

	host := frontend.RPC.BindOnIP
	switch host {
	case "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	case "":
		if frontend.RPC.BindOnLocalHost {
			host = environment.GetLocalhostIP()
		} else {
			listenIP, err := serverconfig.ListenIP()
			if err != nil {
				return "", fmt.Errorf("resolve Temporal frontend listen address: %w", err)
			}
			host = listenIP.String()
		}
	default:
		if net.ParseIP(host) == nil {
			return "", fmt.Errorf("temporal frontend bindOnIP %q is not an IP address", host)
		}
	}
	return net.JoinHostPort(host, fmt.Sprint(frontend.RPC.GRPCPort)), nil
}

func startManagedServer(ctx context.Context, cfg config, artifactDir string) (serverStopper, error) {
	if cfg.serverConfigFile == "" {
		return nil, nil
	}

	logFile, err := os.Create(filepath.Join(artifactDir, "server.log"))
	if err != nil {
		return nil, fmt.Errorf("create server log: %w", err)
	}
	command := managedServerCommand(ctx, cfg)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		return nil, errors.Join(fmt.Errorf("execute %s: %w", cfg.serverBinary, err), logFile.Close())
	}

	process := &managedServerProcess{
		command: command,
		done:    make(chan error, 1),
		logFile: logFile,
	}
	go func() {
		process.done <- command.Wait()
	}()

	startupCtx, cancel := context.WithTimeout(ctx, cfg.serverStartTimeout)
	defer cancel()
	if err := waitForManagedServer(startupCtx, cfg.address, process.done, checkTemporalHealth); err != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), cfg.serverStopTimeout)
		defer stopCancel()
		return nil, errors.Join(err, process.stop(stopCtx))
	}
	return process.stop, nil
}

func managedServerCommand(ctx context.Context, cfg config) *exec.Cmd {
	return exec.CommandContext(
		ctx,
		cfg.serverBinary,
		"--config-file", cfg.serverConfigFile,
		"--allow-no-auth",
		"start",
	)
}

func waitForManagedServer(
	ctx context.Context,
	address string,
	done chan error,
	checkHealth healthChecker,
) error {
	var lastHealthErr error
	for {
		select {
		case err := <-done:
			done <- err
			if err == nil {
				return errors.New("temporal server stopped before becoming healthy")
			}
			return fmt.Errorf("temporal server stopped before becoming healthy: %w", err)
		default:
		}

		attemptCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := checkHealth(attemptCtx, address)
		cancel()
		if err == nil {
			return nil
		}
		lastHealthErr = err

		timer := time.NewTimer(managedServerHealthInterval)
		select {
		case err := <-done:
			timer.Stop()
			done <- err
			if err == nil {
				return errors.New("temporal server stopped before becoming healthy")
			}
			return fmt.Errorf("temporal server stopped before becoming healthy: %w", err)
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(
				fmt.Errorf("wait for managed Temporal server health at %s: %w", address, ctx.Err()),
				lastHealthErr,
			)
		case <-timer.C:
		}
	}
}

func checkTemporalHealth(ctx context.Context, address string) (retErr error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, connection.Close())
	}()
	health, err := healthpb.NewHealthClient(connection).Check(ctx, &healthpb.HealthCheckRequest{
		Service: workflowServiceHealthName,
	})
	if err != nil {
		return fmt.Errorf("frontend health check: %w", err)
	}
	if health.Status != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("frontend health status is %s", health.Status)
	}
	adminClient := adminservice.NewAdminServiceClient(connection)
	membership, err := adminClient.DescribeCluster(
		ctx,
		&adminservice.DescribeClusterRequest{},
	)
	if err != nil {
		return fmt.Errorf("describe Temporal cluster: %w", err)
	}
	if err := validateManagedServerMembership(membership); err != nil {
		return err
	}
	deepHealth, err := adminClient.DeepHealthCheck(ctx, &adminservice.DeepHealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("deep health check: %w", err)
	}
	if deepHealth.GetState() != enumsspb.HEALTH_STATE_SERVING {
		return fmt.Errorf("deep health state is %s", deepHealth.GetState())
	}
	return nil
}

func validateManagedServerMembership(response *adminservice.DescribeClusterResponse) error {
	counts := make(map[string]int32)
	for _, ring := range response.GetMembershipInfo().GetRings() {
		counts[ring.GetRole()] = ring.GetMemberCount()
	}
	for _, service := range managedServerServices {
		if counts[service] < 1 {
			return fmt.Errorf("temporal %s service has no reachable members", service)
		}
	}
	return nil
}

func (p *managedServerProcess) stop(ctx context.Context) error {
	select {
	case err := <-p.done:
		return errors.Join(serverExitError(err), p.logFile.Close())
	default:
	}

	signalErr := p.command.Process.Signal(os.Interrupt)
	if signalErr != nil && !errors.Is(signalErr, os.ErrProcessDone) {
		killErr := p.command.Process.Kill()
		waitErr := <-p.done
		return errors.Join(signalErr, killErr, serverExitError(waitErr), p.logFile.Close())
	}
	select {
	case err := <-p.done:
		if errors.Is(signalErr, os.ErrProcessDone) {
			signalErr = nil
		}
		return errors.Join(signalErr, serverExitError(err), p.logFile.Close())
	case <-ctx.Done():
		killErr := p.command.Process.Kill()
		waitErr := <-p.done
		return errors.Join(
			fmt.Errorf("graceful server shutdown: %w", ctx.Err()),
			signalErr,
			killErr,
			serverExitError(waitErr),
			p.logFile.Close(),
		)
	}
}

func serverExitError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("temporal server process: %w", err)
}
