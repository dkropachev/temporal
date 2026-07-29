package cassandra

import (
	"context"
	"fmt"
	"strings"

	"github.com/urfave/cli"
	"go.temporal.io/server/common/auth"
	c "go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	persistencecassandra "go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/temporal/environment"
	"go.temporal.io/server/tools/common/schema"
)

const (
	defaultNumReplicas                    = 1
	defaultHistoryNodeBackfillPageSize    = 1000
	defaultHistoryNodeBackfillConcurrency = 16
	historyNodeBackfillPageSizeFlag       = "page-size"
	historyNodeBackfillConcurrencyFlag    = "concurrency"
	confirmHistoryNodeSourceRebuildFlag   = "confirm-source-rebuild"
	confirmHistoryNodeV1RebuildFlag       = "confirm-v1-rebuild"
)

// SetupSchemaConfig contains the configuration params needed to setup schema tables
type SetupSchemaConfig struct {
	CQLClientConfig
	schema.SetupConfig
}

// setupSchema executes the setupSchemaTask
// using the given command line arguments
// as input
func setupSchema(cli *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	client, err := newCQLClient(config, logger)
	if err != nil {
		logger.Error("Unable to establish CQL session.", tag.Error(err))
		return err
	}
	defer client.Close()
	if err := schema.Setup(cli, client, logger); err != nil {
		logger.Error("Unable to setup CQL schema.", tag.Error(err))
		return err
	}
	return nil
}

// updateSchema executes the updateSchemaTask
// using the given command line args as input
func updateSchema(cli *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	client, err := newCQLClient(config, logger)
	if err != nil {
		logger.Error("Unable to establish CQL session.", tag.Error(err))
		return err
	}
	defer client.Close()
	if err := schema.Update(cli, client, logger); err != nil {
		logger.Error("Unable to update CQL schema.", tag.Error(err))
		return err
	}
	return nil
}

func createKeyspace(cli *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	keyspace := cli.String(schema.CLIOptKeyspace)
	if keyspace == "" {
		err := fmt.Errorf("missing %s argument", flag(schema.CLIOptKeyspace))
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	err = doCreateKeyspace(config, keyspace, logger)
	if err != nil {
		logger.Error("Unable to create keyspace.", tag.Error(err))
		return err
	}
	return nil
}

func dropKeyspace(cli *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	keyspace := cli.String(schema.CLIOptKeyspace)
	if keyspace == "" {
		err := fmt.Errorf("missing %s argument", flag(schema.CLIOptKeyspace))
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	err = doDropKeyspace(config, keyspace, logger)
	if err != nil {
		logger.Error("Unable to drop keyspace.", tag.Error(err))
		return err
	}
	return nil
}

func validateHealth(cli *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}

	config.Keyspace = systemKeyspace

	client, err := newCQLClient(config, logger)
	if err != nil {
		logger.Error("Unable to establish CQL session.", tag.Error(err))
		return err
	}

	defer client.Close()
	return nil
}

func backfillHistoryNodeV2(ctx *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(ctx)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	client, err := newCQLClient(config, logger)
	if err != nil {
		logger.Error("Unable to establish CQL session.", tag.Error(err))
		return err
	}
	defer client.Close()

	if err := persistencecassandra.ValidateHistoryNodeV2BackfillSchema(
		context.Background(),
		client.session,
		client.keyspace,
	); err != nil {
		logger.Error("History node schema is not valid for V2 backfill.", tag.Error(err))
		return err
	}
	copied, err := persistencecassandra.BackfillHistoryNodeV2(
		context.Background(),
		client.session,
		persistencecassandra.HistoryNodeBackfillOptions{
			PageSize:    ctx.Int(historyNodeBackfillPageSizeFlag),
			Concurrency: ctx.Int(historyNodeBackfillConcurrencyFlag),
		},
	)
	if err != nil {
		logger.Error("Unable to backfill history_node_v2.", tag.Error(err))
		return err
	}
	logger.Info(fmt.Sprintf("Backfilled %d history_node_v2 rows.", copied))
	return nil
}

func backfillHistoryNodeV1(ctx *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(ctx)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	client, err := newCQLClient(config, logger)
	if err != nil {
		logger.Error("Unable to establish CQL session.", tag.Error(err))
		return err
	}
	defer client.Close()

	if err := persistencecassandra.ValidateHistoryNodeV1BackfillSchema(
		context.Background(),
		client.session,
		client.keyspace,
	); err != nil {
		logger.Error("History node schema is not valid for V1 backfill.", tag.Error(err))
		return err
	}
	copied, err := persistencecassandra.BackfillHistoryNodeV1(
		context.Background(),
		client.session,
		persistencecassandra.HistoryNodeBackfillOptions{
			PageSize:    ctx.Int(historyNodeBackfillPageSizeFlag),
			Concurrency: ctx.Int(historyNodeBackfillConcurrencyFlag),
		},
	)
	if err != nil {
		logger.Error("Unable to backfill history_node.", tag.Error(err))
		return err
	}
	logger.Info(fmt.Sprintf("Backfilled %d history_node rows.", copied))
	return nil
}

func recreateHistoryNodeV1(ctx *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(ctx)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	client, err := newCQLClient(config, logger)
	if err != nil {
		logger.Error("Unable to establish CQL session.", tag.Error(err))
		return err
	}
	defer client.Close()

	err = persistencecassandra.RecreateHistoryNodeV1(
		context.Background(),
		client.session,
		client.keyspace,
		ctx.Bool(confirmHistoryNodeV1RebuildFlag),
	)
	if err != nil {
		logger.Error("Unable to recreate history_node with the V1 layout.", tag.Error(err))
		return err
	}
	logger.Info("history_node has the V1 layout.")
	return nil
}

func recreateHistoryNodeV2(ctx *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(ctx)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	client, err := newCQLClient(config, logger)
	if err != nil {
		logger.Error("Unable to establish CQL session.", tag.Error(err))
		return err
	}
	defer client.Close()

	err = persistencecassandra.RecreateHistoryNodeV2(
		context.Background(),
		client.session,
		client.keyspace,
		ctx.Bool(confirmHistoryNodeSourceRebuildFlag),
	)
	if err != nil {
		logger.Error("Unable to recreate history_node_v2.", tag.Error(err))
		return err
	}
	logger.Info("history_node_v2 has been cleared and recreated.")
	return nil
}

func doCreateKeyspace(cfg *CQLClientConfig, name string, logger log.Logger) error {
	cfg.Keyspace = systemKeyspace
	client, err := newCQLClient(cfg, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.createKeyspace(name)
}

func doDropKeyspace(cfg *CQLClientConfig, name string, logger log.Logger) error {
	cfg.Keyspace = systemKeyspace
	client, err := newCQLClient(cfg, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.dropKeyspace(name)
}

func newCQLClientConfig(cli *cli.Context) (*CQLClientConfig, error) {
	config := &CQLClientConfig{
		Hosts:                    cli.GlobalString(schema.CLIOptEndpoint),
		Port:                     cli.GlobalInt(schema.CLIOptPort),
		User:                     cli.GlobalString(schema.CLIOptUser),
		Password:                 cli.GlobalString(schema.CLIOptPassword),
		AllowedAuthenticators:    cli.GlobalStringSlice(schema.CLIOptAllowedAuthenticators),
		Timeout:                  cli.GlobalInt(schema.CLIOptTimeout),
		Keyspace:                 cli.GlobalString(schema.CLIOptKeyspace),
		numReplicas:              cli.Int(schema.CLIOptReplicationFactor),
		Datacenter:               cli.String(schema.CLIOptDatacenter),
		Consistency:              cli.String(schema.CLIOptConsistency),
		DisableInitialHostLookup: cli.GlobalBool(schema.CLIFlagDisableInitialHostLookup),
	}

	if cli.GlobalBool(schema.CLIFlagEnableTLS) {
		config.TLS = &auth.TLS{
			Enabled:                true,
			CertFile:               cli.GlobalString(schema.CLIFlagTLSCertFile),
			KeyFile:                cli.GlobalString(schema.CLIFlagTLSKeyFile),
			CaFile:                 cli.GlobalString(schema.CLIFlagTLSCaFile),
			ServerName:             cli.GlobalString(schema.CLIFlagTLSHostName),
			EnableHostVerification: !cli.GlobalBool(schema.CLIFlagTLSDisableHostVerification),
		}
	}

	config.AddressTranslator = &c.CassandraAddressTranslator{
		Translator: cli.GlobalString(schema.CLIOptAddressTranslator),
		Options:    parseOptionsMap(cli.GlobalString(schema.CLIOptAddressTranslatorOptions)),
	}

	if err := validateCQLClientConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func parseOptionsMap(value string) map[string]string {
	if len(value) == 0 {
		return make(map[string]string)
	}

	parsedMap := make(map[string]string)

	for pair := range strings.SplitSeq(value, ",") {
		trimmedPair := strings.ReplaceAll(pair, " ", "")
		if len(trimmedPair) == 0 {
			continue
		}
		splitPair := strings.Split(trimmedPair, "=")
		if len(splitPair) != 2 {
			continue
		}
		if len(splitPair[0]) == 0 || len(splitPair[1]) == 0 {
			continue
		}
		parsedMap[splitPair[0]] = splitPair[1]
	}

	return parsedMap
}

func validateCQLClientConfig(config *CQLClientConfig) error {
	if len(config.Hosts) == 0 {
		return schema.NewConfigError("missing cassandra endpoint argument " + flag(schema.CLIOptEndpoint))
	}
	if config.Keyspace == "" {
		return schema.NewConfigError("missing " + flag(schema.CLIOptKeyspace) + " argument ")
	}
	if config.Port == 0 {
		config.Port = environment.GetCassandraPort()
	}
	if config.numReplicas == 0 {
		config.numReplicas = defaultNumReplicas
	}

	if config.AddressTranslator != nil && len(config.AddressTranslator.Options) != 0 {
		if len(config.AddressTranslator.Translator) == 0 {
			return schema.NewConfigError("missing address translator argument " + flag(schema.CLIOptAddressTranslator))
		}
	}

	return nil
}

func flag(opt string) string {
	return "(-" + opt + ")"
}
