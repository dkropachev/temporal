package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	_ "time/tzdata"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/temporal"
)

type commandConfig struct {
	configFile  string
	allowNoAuth bool
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Temporal performance server failed: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	commandConfig, err := parseCommand(args)
	if err != nil {
		return err
	}

	cfg, err := config.Load(config.WithConfigFile(commandConfig.configFile))
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	if err := configureNoopVisibility(&cfg.Persistence); err != nil {
		return err
	}
	logger := log.NewZapLogger(log.BuildZapLogger(cfg.Log))
	authorizer, err := authorization.GetAuthorizerFromConfig(&cfg.Global.Authorization)
	if err != nil {
		return fmt.Errorf("create authorizer: %w", err)
	}
	if authorization.IsNoopAuthorizer(authorizer) && !commandConfig.allowNoAuth {
		return errors.New("--allow-no-auth is required with the configured no-op authorizer")
	}
	claimMapper, err := authorization.GetClaimMapperFromConfig(&cfg.Global.Authorization, logger)
	if err != nil {
		return fmt.Errorf("create claim mapper: %w", err)
	}
	audienceMapper, err := authorization.GetAudienceMapperFromConfig(&cfg.Global.Authorization)
	if err != nil {
		return fmt.Errorf("create audience mapper: %w", err)
	}

	visibilityFactory := &noopVisibilityFactory{}
	server, err := temporal.NewServer(
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithConfig(cfg),
		temporal.WithLogger(logger),
		temporal.InterruptOn(temporal.InterruptCh()),
		temporal.WithAuthorizer(authorizer),
		temporal.WithClaimMapper(func(*config.Config) authorization.ClaimMapper {
			return claimMapper
		}),
		temporal.WithAudienceGetter(func(*config.Config) authorization.JWTAudienceMapper {
			return audienceMapper
		}),
		temporal.WithCustomVisibilityStoreFactory(visibilityFactory),
	)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}
	if err := server.Start(); err != nil {
		return fmt.Errorf("run server: %w", err)
	}

	summary := visibilityFactory.summary()
	_, _ = fmt.Fprintf(
		os.Stdout,
		"BENCHMARK_NOOP_VISIBILITY writes=%d reads=%d validations=%d admin=%d stores=%d\n",
		summary.writes,
		summary.reads,
		summary.validations,
		summary.admin,
		summary.stores,
	)
	if summary.reads != 0 || summary.admin != 0 {
		return fmt.Errorf(
			"visibility isolation violated: reads=%d admin=%d",
			summary.reads,
			summary.admin,
		)
	}
	return nil
}

func configureNoopVisibility(persistence *config.Persistence) error {
	if persistence.DefaultStore == noopVisibilityStoreName {
		return fmt.Errorf("default datastore name %q is reserved", noopVisibilityStoreName)
	}
	if persistence.DataStores == nil {
		return errors.New("temporal persistence datastores are required")
	}
	if _, exists := persistence.DataStores[noopVisibilityStoreName]; exists {
		return fmt.Errorf("datastore name %q is reserved", noopVisibilityStoreName)
	}
	persistence.DataStores[noopVisibilityStoreName] = config.DataStore{
		CustomDataStoreConfig: &config.CustomDatastoreConfig{
			Name:      noopVisibilityStoreName,
			IndexName: noopVisibilityStoreName,
		},
	}
	persistence.VisibilityStore = noopVisibilityStoreName
	persistence.SecondaryVisibilityStore = ""
	return nil
}

func parseCommand(args []string) (commandConfig, error) {
	var commandConfig commandConfig
	flags := flag.NewFlagSet("temporalperf-visibility-server", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var configFileSet bool
	flags.Func("config-file", "Temporal server configuration file", func(value string) error {
		if configFileSet {
			return errors.New("--config-file may only be specified once")
		}
		configFileSet = true
		commandConfig.configFile = value
		return nil
	})
	var allowNoAuthSet bool
	flags.BoolFunc("allow-no-auth", "allow the configured no-op authorizer", func(value string) error {
		if allowNoAuthSet {
			return errors.New("--allow-no-auth may only be specified once")
		}
		allowNoAuthSet = true
		allowNoAuth, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		commandConfig.allowNoAuth = allowNoAuth
		return nil
	})
	if err := flags.Parse(args); err != nil {
		return commandConfig, err
	}
	if commandConfig.configFile == "" {
		return commandConfig, errors.New("--config-file is required")
	}
	if flags.NArg() != 1 || flags.Arg(0) != "start" {
		return commandConfig, errors.New("exactly one start command is required")
	}
	return commandConfig, nil
}
