package cassandra

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/urfave/cli"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	persistencecassandra "go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
)

const (
	defaultSchemaMigrationPageSize            = 16
	defaultSchemaMigrationConcurrency         = 16
	defaultExecutionStorageBuckets            = 16
	defaultSchemaMigrationMaxMismatches       = 20
	schemaMigrationPageSizeFlag               = "page-size"
	schemaMigrationConcurrencyFlag            = "concurrency"
	schemaMigrationTokenRangesFlag            = "token-ranges"
	schemaMigrationCheckpointFileFlag         = "checkpoint-file"
	schemaMigrationBucketCountFlag            = "buckets"
	schemaMigrationQueueTypeFlag              = "queue-type"
	schemaMigrationNamespaceIDFlag            = "namespace-id"
	schemaMigrationTaskQueueFlag              = "task-queue"
	schemaMigrationTaskQueueTypeFlag          = "task-queue-type"
	schemaMigrationFairFlag                   = "fair"
	schemaMigrationRangeSizeFlag              = "range-size"
	schemaMigrationMessageBucketSizeFlag      = "message-bucket-size"
	schemaMigrationMaximumMismatchesFlag      = "max-mismatches"
	confirmSourceAuthoritativeFlag            = "confirm-source-authoritative"
	confirmLegacyQueueSourceDualFlag          = "confirm-source-dual-rollout"
	confirmExecutionSourceRebuildFlag         = "confirm-source-rebuild"
	confirmTaskQueueUserDataWriterBarrierFlag = "confirm-no-source-only-writers"
	confirmMatchingTaskWriterBarrierFlag      = "confirm-no-pre-fence-matching-writers"
	executionsV2SourceTable                   = "executions"
	executionsV2TargetTable                   = "executions_v2"
	queueV2MetadataSourceTable                = "queues"
	queueV2MetadataTargetTable                = "queues_v2"
	taskQueueUserDataV2SourceTable            = "task_queue_user_data"
	taskQueueUserDataV2TargetTable            = "task_queue_user_data_v2"
	taskQueueUserDataV2TransactionTable       = "task_queue_user_data_v2_txn"
	matchingTaskSourceTable                   = "tasks"
	matchingTaskFairSourceTable               = "tasks_v2"
	matchingTaskV3TargetTable                 = "tasks_v3"
	matchingTaskV3FairTargetTable             = "tasks_v3_fair"
)

func migrationCLICommands(logger log.Logger) []cli.Command {
	commands := []cli.Command{
		{
			Name:  "prepare-executions-v2-backfill",
			Usage: "truncate executions_v2 after every writer enters source-rebuild mode",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  schemaMigrationBucketCountFlag,
					Value: defaultExecutionStorageBuckets,
					Usage: "immutable number of executions_v2 storage buckets per history shard",
				},
				cli.BoolFlag{
					Name:  confirmExecutionSourceRebuildFlag,
					Usage: "confirm every Temporal writer uses executionMigrationMode source-rebuild",
				},
			},
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, prepareExecutionsV2Backfill, logger)
			},
		},
		{
			Name:  "backfill-executions-v2",
			Usage: "copy executions into bucketed executions_v2 with a resumable token-range checkpoint",
			Flags: append(tokenRangeMigrationCLIFlags(
				persistencecassandra.DefaultExecutionBackfillTokenRangeCount,
			), cli.IntFlag{
				Name:  schemaMigrationBucketCountFlag,
				Value: defaultExecutionStorageBuckets,
				Usage: "immutable number of executions_v2 storage buckets per history shard",
			}),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, backfillExecutionsV2, logger)
			},
		},
		{
			Name:  "validate-executions-v2",
			Usage: "compare executions and executions_v2 in both directions before target cutover",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  schemaMigrationPageSizeFlag,
					Value: defaultSchemaMigrationPageSize,
					Usage: "number of execution rows fetched per page",
				},
				cli.IntFlag{
					Name:  schemaMigrationTokenRangesFlag,
					Value: persistencecassandra.DefaultExecutionBackfillTokenRangeCount,
					Usage: "number of contiguous Murmur3 token ranges in each table scan",
				},
				cli.StringFlag{
					Name:  schemaMigrationCheckpointFileFlag,
					Usage: "required durable checkpoint file; use a new path for each complete validation pass",
				},
				cli.IntFlag{
					Name:  schemaMigrationBucketCountFlag,
					Value: defaultExecutionStorageBuckets,
					Usage: "immutable number of executions_v2 storage buckets per history shard",
				},
				cli.IntFlag{
					Name:  schemaMigrationMaximumMismatchesFlag,
					Value: defaultSchemaMigrationMaxMismatches,
					Usage: "maximum mismatch details retained; zero means unlimited",
				},
			},
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, validateExecutionsV2, logger)
			},
		},
		{
			Name:  "backfill-queue-v2-metadata",
			Usage: "copy one QueueV2 type from queues into bucketed queues_v2 with a resumable page checkpoint",
			Flags: queueV2MetadataMigrationCLIFlags(),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, backfillQueueV2Metadata, logger)
			},
		},
		{
			Name:  "validate-queue-v2-metadata",
			Usage: "compare both directions of one QueueV2 type before target cutover",
			Flags: queueV2MetadataMigrationCLIFlags(),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, validateQueueV2Metadata, logger)
			},
		},
		{
			Name:  "backfill-task-queue-user-data-v2",
			Usage: "copy task_queue_user_data into its bucketed V2 table with a resumable token-range checkpoint",
			Flags: append(tokenRangeMigrationCLIFlags(
				persistencecassandra.DefaultTaskQueueUserDataBackfillTokenRangeCount,
			), cli.IntFlag{
				Name:  schemaMigrationBucketCountFlag,
				Value: persistencecassandra.DefaultTaskQueueUserDataBucketCount,
				Usage: "immutable number of task queue user data buckets per namespace",
			}),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, backfillTaskQueueUserDataV2, logger)
			},
		},
		{
			Name:  "recover-task-queue-user-data-v2",
			Usage: "resolve pending V2 user-data transactions with a resumable token-range checkpoint",
			Flags: append(tokenRangeMigrationCLIFlags(
				persistencecassandra.DefaultTaskQueueUserDataBackfillTokenRangeCount,
			), cli.IntFlag{
				Name:  schemaMigrationBucketCountFlag,
				Value: persistencecassandra.DefaultTaskQueueUserDataBucketCount,
				Usage: "immutable number of task queue user data buckets per namespace",
			}),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, recoverTaskQueueUserDataV2, logger)
			},
		},
		{
			Name:  "validate-task-queue-user-data-v2",
			Usage: "compare V1 and V2 user data for each supplied namespace before target cutover",
			Flags: []cli.Flag{
				cli.StringSliceFlag{
					Name:  schemaMigrationNamespaceIDFlag,
					Usage: "namespace ID to validate; repeat for every namespace",
				},
				cli.IntFlag{
					Name:  schemaMigrationBucketCountFlag,
					Value: persistencecassandra.DefaultTaskQueueUserDataBucketCount,
					Usage: "immutable number of task queue user data buckets per namespace",
				},
			},
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, validateTaskQueueUserDataV2, logger)
			},
		},
		{
			Name:  "cutover-task-queue-user-data-v2",
			Usage: "fence one namespace, reconcile V2, validate both directions, and publish target authority",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schemaMigrationNamespaceIDFlag,
					Usage: "required namespace ID",
				},
				cli.IntFlag{
					Name:  schemaMigrationBucketCountFlag,
					Value: persistencecassandra.DefaultTaskQueueUserDataBucketCount,
					Usage: "immutable number of task queue user data buckets per namespace",
				},
				cli.IntFlag{
					Name:  schemaMigrationConcurrencyFlag,
					Value: defaultSchemaMigrationConcurrency,
					Usage: "maximum concurrent reconciliation operations",
				},
				cli.BoolFlag{
					Name:  confirmTaskQueueUserDataWriterBarrierFlag,
					Usage: "confirm no source-only binary can write after per-namespace fencing activates",
				},
			},
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, cutoverTaskQueueUserDataV2, logger)
			},
		},
		{
			Name:  "backfill-matching-tasks-v3",
			Usage: "copy classic or fair matching tasks into V3 with a resumable token-range checkpoint",
			Flags: append(tokenRangeMigrationCLIFlags(
				persistencecassandra.DefaultMatchingTaskBackfillTokenRangeCount,
			),
				cli.IntFlag{
					Name:  schemaMigrationBucketCountFlag,
					Value: persistencecassandra.DefaultMatchingTaskStorageBucketCount,
					Usage: "immutable number of matching task storage buckets",
				},
				cli.Int64Flag{
					Name:  schemaMigrationRangeSizeFlag,
					Value: persistencecassandra.DefaultMatchingTaskRangeSize,
					Usage: "immutable number of task IDs assigned to one range",
				},
				cli.BoolFlag{
					Name:  schemaMigrationFairFlag,
					Usage: "migrate fair tasks_v2 into tasks_v3_fair instead of classic tasks",
				},
			),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, backfillMatchingTasksV3, logger)
			},
		},
		{
			Name:  "validate-matching-tasks-v3",
			Usage: "compare one classic or fair matching queue against V3 before target cutover",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schemaMigrationNamespaceIDFlag,
					Usage: "required namespace ID",
				},
				cli.StringFlag{
					Name:  schemaMigrationTaskQueueFlag,
					Usage: "required physical task queue name",
				},
				cli.IntFlag{
					Name:  schemaMigrationTaskQueueTypeFlag,
					Usage: "required task queue type enum value",
				},
				cli.IntFlag{
					Name:  schemaMigrationBucketCountFlag,
					Value: persistencecassandra.DefaultMatchingTaskStorageBucketCount,
					Usage: "immutable number of matching task storage buckets",
				},
				cli.BoolFlag{
					Name:  schemaMigrationFairFlag,
					Usage: "validate tasks_v2 against tasks_v3_fair instead of classic tables",
				},
			},
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, validateMatchingTasksV3, logger)
			},
		},
		{
			Name:  "cutover-matching-tasks-v3",
			Usage: "fence, reconcile, validate, and publish target authority for one classic or fair physical matching queue",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schemaMigrationNamespaceIDFlag,
					Usage: "required namespace ID",
				},
				cli.StringFlag{
					Name:  schemaMigrationTaskQueueFlag,
					Usage: "required physical task queue name",
				},
				cli.IntFlag{
					Name:  schemaMigrationTaskQueueTypeFlag,
					Usage: "required task queue type enum value",
				},
				cli.IntFlag{
					Name:  schemaMigrationBucketCountFlag,
					Value: persistencecassandra.DefaultMatchingTaskStorageBucketCount,
					Usage: "immutable number of matching task storage buckets",
				},
				cli.BoolFlag{
					Name:  schemaMigrationFairFlag,
					Usage: "cut over tasks_v2 into tasks_v3_fair instead of classic tables",
				},
				cli.BoolFlag{
					Name:  confirmMatchingTaskWriterBarrierFlag,
					Usage: "confirm no pre-fence matching writer can access this source queue after cutover begins",
				},
			},
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, cutoverMatchingTasksV3, logger)
			},
		},
		{
			Name:  "activate-legacy-queue-v2-source-fence",
			Usage: "activate the source authority sentinel after every legacy queue writer runs fence-capable code",
			Flags: legacyQueueV2AuthorityCLIFlags(),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, activateLegacyQueueV2SourceFence, logger)
			},
		},
		{
			Name:  "backfill-legacy-queue-v2",
			Usage: "copy one physical legacy queue partition into bucketed V2 tables; rerun safely after interruption",
			Flags: legacyQueueV2MigrationCLIFlags(false, false),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, backfillLegacyQueueV2, logger)
			},
		},
		{
			Name:  "reconcile-legacy-queue-v2",
			Usage: "repair one physical legacy queue V2 target while the source remains authoritative",
			Flags: legacyQueueV2MigrationCLIFlags(true, true),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, reconcileLegacyQueueV2, logger)
			},
		},
		{
			Name:  "validate-legacy-queue-v2",
			Usage: "compare one physical legacy queue and its V2 target before cutover",
			Flags: legacyQueueV2MigrationCLIFlags(true, false),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, validateLegacyQueueV2, logger)
			},
		},
		{
			Name:  "cutover-legacy-queue-v2",
			Usage: "seal, exactly reconcile, activate, and publish one physical legacy queue V2 target",
			Flags: legacyQueueV2CutoverCLIFlags(),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, cutoverLegacyQueueV2, logger)
			},
		},
	}
	commands = append(commands, queueV2MessagesV3MigrationCLICommands(logger)...)
	return append(commands, schemaLayoutMigrationCLICommands(logger)...)
}

func schemaMigrationCLIHandler(
	ctx *cli.Context,
	handler func(*cli.Context, log.Logger) error,
	logger log.Logger,
) {
	cliHandler(ctx, func(handlerCtx *cli.Context, handlerLogger log.Logger) error {
		err := handler(handlerCtx, handlerLogger)
		if err != nil {
			handlerLogger.Error("Cassandra schema migration command failed.", tag.Error(err))
		}
		return err
	}, logger)
}

func tokenRangeMigrationCLIFlags(defaultTokenRanges int) []cli.Flag {
	return []cli.Flag{
		cli.IntFlag{
			Name:  schemaMigrationPageSizeFlag,
			Value: defaultSchemaMigrationPageSize,
			Usage: "number of source rows fetched per page",
		},
		cli.IntFlag{
			Name:  schemaMigrationConcurrencyFlag,
			Value: defaultSchemaMigrationConcurrency,
			Usage: "maximum concurrent target writes",
		},
		cli.IntFlag{
			Name:  schemaMigrationTokenRangesFlag,
			Value: defaultTokenRanges,
			Usage: "number of contiguous Murmur3 token ranges in the scan",
		},
		cli.StringFlag{
			Name:  schemaMigrationCheckpointFileFlag,
			Usage: "required durable checkpoint file; use a new path for each complete pass",
		},
	}
}

func queueV2MetadataMigrationCLIFlags() []cli.Flag {
	return []cli.Flag{
		cli.IntFlag{
			Name:  schemaMigrationQueueTypeFlag,
			Usage: "required QueueV2 type; run once for every queue type",
		},
		cli.IntFlag{
			Name:  schemaMigrationPageSizeFlag,
			Value: defaultSchemaMigrationPageSize,
			Usage: "number of queue metadata rows fetched per page",
		},
		cli.StringFlag{
			Name:  schemaMigrationCheckpointFileFlag,
			Usage: "required durable checkpoint file; use a new path for each complete pass",
		},
	}
}

func legacyQueueV2MigrationCLIFlags(withMismatches bool, withSourceConfirmation bool) []cli.Flag {
	flags := []cli.Flag{
		cli.IntFlag{
			Name:  schemaMigrationQueueTypeFlag,
			Usage: "required physical queue type; run separately for its negative DLQ type",
		},
		cli.IntFlag{
			Name:  schemaMigrationPageSizeFlag,
			Value: defaultSchemaMigrationPageSize,
			Usage: "number of source queue messages fetched per page",
		},
		cli.Int64Flag{
			Name:  schemaMigrationMessageBucketSizeFlag,
			Value: persistencecassandra.DefaultLegacyQueueV2MessageBucketSize,
			Usage: "immutable number of message IDs in each V2 bucket",
		},
	}
	if withMismatches {
		flags = append(flags, cli.IntFlag{
			Name:  schemaMigrationMaximumMismatchesFlag,
			Value: defaultSchemaMigrationMaxMismatches,
			Usage: "maximum mismatch details retained in the result; zero means unlimited",
		})
	}
	if withSourceConfirmation {
		flags = append(flags, cli.BoolFlag{
			Name:  confirmSourceAuthoritativeFlag,
			Usage: "confirm all writers still treat the legacy queue table as authoritative",
		})
	}
	return flags
}

func legacyQueueV2AuthorityCLIFlags() []cli.Flag {
	return []cli.Flag{
		cli.IntFlag{
			Name:  schemaMigrationQueueTypeFlag,
			Usage: "required physical queue type; run separately for its negative DLQ type",
		},
		cli.Int64Flag{
			Name:  schemaMigrationMessageBucketSizeFlag,
			Value: persistencecassandra.DefaultLegacyQueueV2MessageBucketSize,
			Usage: "immutable number of message IDs in each V2 bucket",
		},
		cli.BoolFlag{
			Name:  confirmLegacyQueueSourceDualFlag,
			Usage: "confirm every legacy queue writer understands the source authority sentinel",
		},
	}
}

func legacyQueueV2CutoverCLIFlags() []cli.Flag {
	flags := legacyQueueV2MigrationCLIFlags(true, false)
	return append(flags, cli.BoolFlag{
		Name:  confirmLegacyQueueSourceDualFlag,
		Usage: "confirm every legacy queue writer is in source-dual or target-dual mode",
	})
}

func prepareExecutionsV2Backfill(ctx *cli.Context, logger log.Logger) error {
	if !ctx.Bool(confirmExecutionSourceRebuildFlag) {
		return fmt.Errorf(
			"%s is required because preparation truncates executions_v2",
			flag(confirmExecutionSourceRebuildFlag),
		)
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := persistencecassandra.PrepareExecutionsV2Backfill(
		context.Background(),
		client.session,
		client.keyspace,
		ctx.Int(schemaMigrationBucketCountFlag),
		true,
	); err != nil {
		return err
	}
	logger.Info("executions_v2 has been cleared; start backfill with a new checkpoint file.")
	return nil
}

func backfillExecutionsV2(ctx *cli.Context, logger log.Logger) error {
	checkpointPath, err := requiredCheckpointPath(ctx)
	if err != nil {
		return err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()

	bucketCount := ctx.Int(schemaMigrationBucketCountFlag)
	tokenRangeCount := ctx.Int(schemaMigrationTokenRangesFlag)
	identity, err := newSchemaMigrationCheckpointIdentity(
		context.Background(),
		client.session,
		client.keyspace,
		"backfill-executions-v2",
		executionsV2SourceTable,
		fmt.Sprintf("storage-buckets=%d", bucketCount),
		executionsV2TargetTable,
		tokenRangeCount,
	)
	if err != nil {
		return err
	}
	options := persistencecassandra.ExecutionBackfillOptions{
		PageSize:        ctx.Int(schemaMigrationPageSizeFlag),
		Concurrency:     ctx.Int(schemaMigrationConcurrencyFlag),
		TokenRangeCount: tokenRangeCount,
		StorageBuckets:  bucketCount,
		Partitioner:     identity.Partitioner,
	}
	copied, err := runHistoryNodeBackfill(
		context.Background(),
		checkpointPath,
		identity,
		func(validateCtx context.Context) error {
			return validateHistoryNodeBackfillCheckpointIdentity(validateCtx, client.session, identity)
		},
		func(
			rangeCtx context.Context,
			tokenRange persistencecassandra.HistoryNodeBackfillTokenRange,
		) (int64, error) {
			return persistencecassandra.BackfillExecutionsV2Range(rangeCtx, client.session, options, tokenRange)
		},
	)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Backfilled %d executions_v2 rows.", copied))
	return nil
}

func validateExecutionsV2(ctx *cli.Context, logger log.Logger) error {
	checkpointPath, err := requiredCheckpointPath(ctx)
	if err != nil {
		return err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()

	bucketCount := ctx.Int(schemaMigrationBucketCountFlag)
	tokenRangeCount := ctx.Int(schemaMigrationTokenRangesFlag)
	identity, err := newSchemaMigrationCheckpointIdentity(
		context.Background(),
		client.session,
		client.keyspace,
		"validate-executions-v2",
		executionsV2SourceTable,
		fmt.Sprintf("storage-buckets=%d", bucketCount),
		executionsV2TargetTable,
		tokenRangeCount,
	)
	if err != nil {
		return err
	}
	options := persistencecassandra.ExecutionValidationOptions{
		PageSize:        ctx.Int(schemaMigrationPageSizeFlag),
		TokenRangeCount: tokenRangeCount,
		StorageBuckets:  bucketCount,
		MaxMismatches:   ctx.Int(schemaMigrationMaximumMismatchesFlag),
		Partitioner:     identity.Partitioner,
	}
	validated, err := runHistoryNodeBackfill(
		context.Background(),
		checkpointPath,
		identity,
		func(validateCtx context.Context) error {
			return validateHistoryNodeBackfillCheckpointIdentity(validateCtx, client.session, identity)
		},
		func(
			rangeCtx context.Context,
			tokenRange persistencecassandra.HistoryNodeBackfillTokenRange,
		) (int64, error) {
			result, err := persistencecassandra.ValidateExecutionsV2Range(
				rangeCtx,
				client.session,
				options,
				tokenRange,
			)
			if err != nil {
				return result.SourceRows + result.TargetRows, err
			}
			if !result.Matches() {
				return result.SourceRows + result.TargetRows, fmt.Errorf(
					"execution layouts differ: source=%d target=%d mismatches=%v",
					result.SourceRows,
					result.TargetRows,
					result.Mismatches,
				)
			}
			return result.SourceRows + result.TargetRows, nil
		},
	)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Validated %d execution rows across both layouts.", validated))
	return nil
}

func backfillQueueV2Metadata(ctx *cli.Context, logger log.Logger) error {
	return runQueueV2MetadataMigration(ctx, logger, false)
}

func validateQueueV2Metadata(ctx *cli.Context, logger log.Logger) error {
	return runQueueV2MetadataMigration(ctx, logger, true)
}

func runQueueV2MetadataMigration(ctx *cli.Context, logger log.Logger, validate bool) error {
	checkpointPath, err := requiredCheckpointPath(ctx)
	if err != nil {
		return err
	}
	queueType, err := requiredQueueV2Type(ctx)
	if err != nil {
		return err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := persistencecassandra.ValidateQueueV2MigrationModeSchema(
		context.Background(),
		client.session,
		client.keyspace,
		config.CassandraQueueV2MigrationModeSourceDual,
	); err != nil {
		return err
	}
	operation := "backfill-queue-v2-metadata"
	if validate {
		operation = "validate-queue-v2-metadata"
	}
	pageSize := ctx.Int(schemaMigrationPageSizeFlag)
	identity, err := newMigrationPageCheckpointIdentity(
		context.Background(),
		client.session,
		client.keyspace,
		operation,
		queueV2MetadataSourceTable,
		queueV2MetadataTargetTable,
		fmt.Sprintf("queue-type=%d,page-size=%d", queueType, pageSize),
	)
	if err != nil {
		return err
	}
	validateIdentity := func(validateCtx context.Context) error {
		return validateMigrationPageCheckpointIdentity(validateCtx, client.session, identity)
	}
	if !validate {
		rows, err := runMigrationPagePhase(
			context.Background(),
			checkpointPath,
			identity,
			"source-to-target",
			validateIdentity,
			func(pageCtx context.Context, pageToken []byte) (int, []byte, error) {
				page, err := persistencecassandra.BackfillQueueV2MetadataPage(
					pageCtx,
					client.session,
					queueType,
					pageSize,
					pageToken,
				)
				if err != nil {
					return 0, nil, err
				}
				return page.RowsProcessed, page.NextPageToken, nil
			},
		)
		if err != nil {
			return err
		}
		logger.Info(fmt.Sprintf("Backfilled %d QueueV2 metadata rows for queue type %d.", rows, queueType))
		return nil
	}

	sourceRows, err := runMigrationPagePhase(
		context.Background(),
		checkpointPath,
		identity,
		"source-to-target",
		validateIdentity,
		func(pageCtx context.Context, pageToken []byte) (int, []byte, error) {
			page, err := persistencecassandra.ValidateQueueV2MetadataPage(
				pageCtx,
				client.session,
				queueType,
				pageSize,
				pageToken,
			)
			if err != nil {
				return 0, nil, err
			}
			return page.RowsProcessed, page.NextPageToken, nil
		},
	)
	if err != nil {
		return err
	}
	targetRows, err := runMigrationPagePhase(
		context.Background(),
		checkpointPath,
		identity,
		"target-to-source",
		validateIdentity,
		func(pageCtx context.Context, pageToken []byte) (int, []byte, error) {
			page, err := persistencecassandra.ValidateQueueV2TargetMetadataPage(
				pageCtx,
				client.session,
				queueType,
				pageSize,
				pageToken,
			)
			if err != nil {
				return 0, nil, err
			}
			return page.RowsProcessed, page.NextPageToken, nil
		},
	)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf(
		"Validated %d source and %d target QueueV2 metadata rows for queue type %d.",
		sourceRows,
		targetRows,
		queueType,
	))
	return nil
}

func backfillTaskQueueUserDataV2(ctx *cli.Context, logger log.Logger) error {
	return runTaskQueueUserDataV2TokenRanges(ctx, logger, false)
}

func recoverTaskQueueUserDataV2(ctx *cli.Context, logger log.Logger) error {
	return runTaskQueueUserDataV2TokenRanges(ctx, logger, true)
}

func runTaskQueueUserDataV2TokenRanges(ctx *cli.Context, logger log.Logger, recoverTransactions bool) error {
	checkpointPath, err := requiredCheckpointPath(ctx)
	if err != nil {
		return err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := persistencecassandra.ValidateTaskQueueUserDataMigrationModeSchema(
		context.Background(),
		client.session,
		client.keyspace,
		persistencecassandra.TaskQueueUserDataMigrationModeTargetOnly,
	); err != nil {
		return err
	}

	bucketCount := ctx.Int(schemaMigrationBucketCountFlag)
	tokenRangeCount := ctx.Int(schemaMigrationTokenRangesFlag)
	operation := "backfill-task-queue-user-data-v2"
	sourceTable := taskQueueUserDataV2SourceTable
	targetTable := taskQueueUserDataV2TargetTable
	if recoverTransactions {
		operation = "recover-task-queue-user-data-v2"
		sourceTable = taskQueueUserDataV2TargetTable
		targetTable = taskQueueUserDataV2TransactionTable
	}
	identity, err := newSchemaMigrationCheckpointIdentity(
		context.Background(),
		client.session,
		client.keyspace,
		operation,
		sourceTable,
		fmt.Sprintf("storage-buckets=%d", bucketCount),
		targetTable,
		tokenRangeCount,
	)
	if err != nil {
		return err
	}
	if recoverTransactions {
		options := persistencecassandra.TaskQueueUserDataRecoveryOptions{
			PageSize:        ctx.Int(schemaMigrationPageSizeFlag),
			Concurrency:     ctx.Int(schemaMigrationConcurrencyFlag),
			TokenRangeCount: tokenRangeCount,
			BucketCount:     bucketCount,
			Partitioner:     identity.Partitioner,
		}
		recovered, err := runHistoryNodeBackfill(
			context.Background(),
			checkpointPath,
			identity,
			func(validateCtx context.Context) error {
				return validateHistoryNodeBackfillCheckpointIdentity(validateCtx, client.session, identity)
			},
			func(
				rangeCtx context.Context,
				tokenRange persistencecassandra.HistoryNodeBackfillTokenRange,
			) (int64, error) {
				return persistencecassandra.RecoverTaskQueueUserDataV2TransactionsRange(
					rangeCtx,
					client.session,
					options,
					persistencecassandra.TaskQueueUserDataBackfillTokenRange(tokenRange),
				)
			},
		)
		if err != nil {
			return err
		}
		logger.Info(fmt.Sprintf("Recovered %d task queue user data V2 transaction participants.", recovered))
		return nil
	}

	options := persistencecassandra.TaskQueueUserDataBackfillOptions{
		PageSize:        ctx.Int(schemaMigrationPageSizeFlag),
		Concurrency:     ctx.Int(schemaMigrationConcurrencyFlag),
		TokenRangeCount: tokenRangeCount,
		BucketCount:     bucketCount,
		Partitioner:     identity.Partitioner,
	}
	copied, err := runHistoryNodeBackfill(
		context.Background(),
		checkpointPath,
		identity,
		func(validateCtx context.Context) error {
			return validateHistoryNodeBackfillCheckpointIdentity(validateCtx, client.session, identity)
		},
		func(
			rangeCtx context.Context,
			tokenRange persistencecassandra.HistoryNodeBackfillTokenRange,
		) (int64, error) {
			return persistencecassandra.BackfillTaskQueueUserDataV2Range(
				rangeCtx,
				client.session,
				options,
				persistencecassandra.TaskQueueUserDataBackfillTokenRange(tokenRange),
			)
		},
	)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Backfilled %d task_queue_user_data_v2 rows.", copied))
	return nil
}

func validateTaskQueueUserDataV2(ctx *cli.Context, logger log.Logger) error {
	namespaceIDs := ctx.StringSlice(schemaMigrationNamespaceIDFlag)
	if len(namespaceIDs) == 0 {
		return fmt.Errorf("missing %s argument", flag(schemaMigrationNamespaceIDFlag))
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := persistencecassandra.ValidateTaskQueueUserDataMigrationModeSchema(
		context.Background(),
		client.session,
		client.keyspace,
		persistencecassandra.TaskQueueUserDataMigrationModeTargetOnly,
	); err != nil {
		return err
	}
	bucketCount := ctx.Int(schemaMigrationBucketCountFlag)
	for _, namespaceID := range namespaceIDs {
		namespaceID = strings.TrimSpace(namespaceID)
		if namespaceID == "" {
			return fmt.Errorf("%s must not be empty", flag(schemaMigrationNamespaceIDFlag))
		}
		result, err := persistencecassandra.ValidateTaskQueueUserDataV2Namespace(
			context.Background(),
			client.session,
			namespaceID,
			bucketCount,
		)
		if err != nil {
			return err
		}
		if !result.Matches() {
			return fmt.Errorf(
				"task queue user data layouts differ for namespace %s: source=%d target=%d mismatches=%v",
				namespaceID,
				result.SourceRows,
				result.TargetRows,
				result.Mismatches,
			)
		}
		logger.Info(fmt.Sprintf(
			"Validated %d task queue user data rows for namespace %s.",
			result.SourceRows,
			namespaceID,
		))
	}
	return nil
}

func cutoverTaskQueueUserDataV2(ctx *cli.Context, logger log.Logger) error {
	if !ctx.Bool(confirmTaskQueueUserDataWriterBarrierFlag) {
		return fmt.Errorf(
			"%s is required before activating source-visible task queue user data fencing",
			flag(confirmTaskQueueUserDataWriterBarrierFlag),
		)
	}
	namespaceID := strings.TrimSpace(ctx.String(schemaMigrationNamespaceIDFlag))
	if namespaceID == "" {
		return fmt.Errorf("missing %s argument", flag(schemaMigrationNamespaceIDFlag))
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := persistencecassandra.ValidateTaskQueueUserDataMigrationModeSchema(
		context.Background(),
		client.session,
		client.keyspace,
		persistencecassandra.TaskQueueUserDataMigrationModeTargetDual,
	); err != nil {
		return err
	}
	bucketCount := ctx.Int(schemaMigrationBucketCountFlag)
	spec, err := persistencecassandra.SchemaLayoutSpecForTable(
		context.Background(),
		client.session,
		client.keyspace,
		persistencecassandra.SchemaLayoutTaskQueueUserData,
		taskQueueUserDataV2TargetTable,
		int64(bucketCount),
	)
	if err != nil {
		return err
	}
	if _, err := persistencecassandra.NewSchemaLayoutMetadataStore(client.session).RequireTargetReady(
		context.Background(),
		spec,
	); err != nil {
		return fmt.Errorf("task queue user data layout is not target-ready: %w", err)
	}
	validation, err := persistencecassandra.CutoverTaskQueueUserDataV2Namespace(
		context.Background(),
		client.session,
		persistencecassandra.TaskQueueUserDataCutoverOptions{
			NamespaceID: namespaceID,
			BucketCount: bucketCount,
			Concurrency: ctx.Int(schemaMigrationConcurrencyFlag),
			Generation:  spec.Generation,
		},
	)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf(
		"Cut over task queue user data namespace %s to generation %s with %d validated rows.",
		namespaceID,
		spec.Generation,
		validation.TargetRows,
	))
	return nil
}

func backfillMatchingTasksV3(ctx *cli.Context, logger log.Logger) error {
	checkpointPath, err := requiredCheckpointPath(ctx)
	if err != nil {
		return err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()

	fair := ctx.Bool(schemaMigrationFairFlag)
	if err := persistencecassandra.ValidateMatchingTaskMigrationModeSchema(
		context.Background(),
		client.session,
		client.keyspace,
		persistencecassandra.MatchingTaskMigrationModeSourceDual,
		fair,
	); err != nil {
		return err
	}
	sourceTable := matchingTaskSourceTable
	targetTable := matchingTaskV3TargetTable
	if fair {
		sourceTable = matchingTaskFairSourceTable
		targetTable = matchingTaskV3FairTargetTable
	}
	bucketCount := ctx.Int(schemaMigrationBucketCountFlag)
	rangeSize := ctx.Int64(schemaMigrationRangeSizeFlag)
	tokenRangeCount := ctx.Int(schemaMigrationTokenRangesFlag)
	identity, err := newSchemaMigrationCheckpointIdentity(
		context.Background(),
		client.session,
		client.keyspace,
		"backfill-matching-tasks-v3",
		sourceTable,
		fmt.Sprintf("fair=%t,storage-buckets=%d,range-size=%d", fair, bucketCount, rangeSize),
		targetTable,
		tokenRangeCount,
	)
	if err != nil {
		return err
	}
	options := persistencecassandra.MatchingTaskBackfillOptions{
		PageSize:        ctx.Int(schemaMigrationPageSizeFlag),
		Concurrency:     ctx.Int(schemaMigrationConcurrencyFlag),
		TokenRangeCount: tokenRangeCount,
		BucketCount:     bucketCount,
		RangeSize:       rangeSize,
		Fair:            fair,
		Partitioner:     identity.Partitioner,
	}
	copied, err := runHistoryNodeBackfill(
		context.Background(),
		checkpointPath,
		identity,
		func(validateCtx context.Context) error {
			return validateHistoryNodeBackfillCheckpointIdentity(validateCtx, client.session, identity)
		},
		func(
			rangeCtx context.Context,
			tokenRange persistencecassandra.HistoryNodeBackfillTokenRange,
		) (int64, error) {
			return persistencecassandra.BackfillMatchingTasksV3Range(
				rangeCtx,
				client.session,
				options,
				tokenRange,
			)
		},
	)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Backfilled %d matching task V3 rows (fair=%t).", copied, fair))
	return nil
}

func validateMatchingTasksV3(ctx *cli.Context, logger log.Logger) error {
	namespaceID := strings.TrimSpace(ctx.String(schemaMigrationNamespaceIDFlag))
	if namespaceID == "" {
		return fmt.Errorf("missing %s argument", flag(schemaMigrationNamespaceIDFlag))
	}
	taskQueue := strings.TrimSpace(ctx.String(schemaMigrationTaskQueueFlag))
	if taskQueue == "" {
		return fmt.Errorf("missing %s argument", flag(schemaMigrationTaskQueueFlag))
	}
	taskQueueType, err := requiredTaskQueueType(ctx)
	if err != nil {
		return err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	fair := ctx.Bool(schemaMigrationFairFlag)
	if err := persistencecassandra.ValidateMatchingTaskMigrationModeSchema(
		context.Background(),
		client.session,
		client.keyspace,
		persistencecassandra.MatchingTaskMigrationModeSourceDual,
		fair,
	); err != nil {
		return err
	}
	result, err := persistencecassandra.ValidateMatchingTasksV3Queue(
		context.Background(),
		client.session,
		namespaceID,
		taskQueue,
		taskQueueType,
		fair,
		ctx.Int(schemaMigrationBucketCountFlag),
	)
	if err != nil {
		return err
	}
	if !result.Matches() {
		return fmt.Errorf(
			"matching task layouts differ for namespace=%s queue=%q type=%s: source=%d target=%d mismatches=%v",
			namespaceID,
			taskQueue,
			taskQueueType,
			result.SourceTasks,
			result.TargetTasks,
			result.Mismatches,
		)
	}
	logger.Info(fmt.Sprintf(
		"Validated %d matching task V3 rows for namespace=%s queue=%q type=%s (fair=%t).",
		result.SourceTasks,
		namespaceID,
		taskQueue,
		taskQueueType,
		fair,
	))
	return nil
}

func cutoverMatchingTasksV3(ctx *cli.Context, logger log.Logger) error {
	if !ctx.Bool(confirmMatchingTaskWriterBarrierFlag) {
		return fmt.Errorf(
			"%s is required because cutover activates a source-visible matching queue fence",
			flag(confirmMatchingTaskWriterBarrierFlag),
		)
	}
	namespaceID := strings.TrimSpace(ctx.String(schemaMigrationNamespaceIDFlag))
	if namespaceID == "" {
		return fmt.Errorf("missing %s argument", flag(schemaMigrationNamespaceIDFlag))
	}
	taskQueue := strings.TrimSpace(ctx.String(schemaMigrationTaskQueueFlag))
	if taskQueue == "" {
		return fmt.Errorf("missing %s argument", flag(schemaMigrationTaskQueueFlag))
	}
	taskQueueType, err := requiredTaskQueueType(ctx)
	if err != nil {
		return err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()

	fair := ctx.Bool(schemaMigrationFairFlag)
	bucketCount := ctx.Int(schemaMigrationBucketCountFlag)
	if err := persistencecassandra.ValidateMatchingTaskMigrationModeSchema(
		context.Background(),
		client.session,
		client.keyspace,
		persistencecassandra.MatchingTaskMigrationModeTargetDual,
		fair,
	); err != nil {
		return err
	}
	layoutName := persistencecassandra.SchemaLayoutMatchingTasks
	targetTable := matchingTaskV3TargetTable
	if fair {
		layoutName = persistencecassandra.SchemaLayoutMatchingTasksFair
		targetTable = matchingTaskV3FairTargetTable
	}
	spec, err := persistencecassandra.SchemaLayoutSpecForTable(
		context.Background(),
		client.session,
		client.keyspace,
		layoutName,
		targetTable,
		int64(bucketCount),
	)
	if err != nil {
		return err
	}
	if _, err := persistencecassandra.NewSchemaLayoutMetadataStore(client.session).RequireTargetReady(
		context.Background(),
		spec,
	); err != nil {
		return fmt.Errorf("matching task layout is not target-ready: %w", err)
	}
	factory := persistencecassandra.NewFactoryFromSession(
		config.Cassandra{Keyspace: client.keyspace},
		"",
		logger,
		client.session,
		serialization.NewSerializer(),
	)
	if err := factory.CutoverMatchingTasksV3Queue(
		context.Background(),
		namespaceID,
		taskQueue,
		taskQueueType,
		fair,
		bucketCount,
	); err != nil {
		return err
	}
	result, err := persistencecassandra.ValidateMatchingTasksV3Queue(
		context.Background(),
		client.session,
		namespaceID,
		taskQueue,
		taskQueueType,
		fair,
		bucketCount,
	)
	if err != nil {
		return err
	}
	if !result.CanEnterTargetOnly() {
		return fmt.Errorf(
			"matching task cutover verification failed for namespace=%s queue=%q type=%s: source=%d target=%d target-only-ready=%t mismatches=%v",
			namespaceID,
			taskQueue,
			taskQueueType,
			result.SourceTasks,
			result.TargetTasks,
			result.TargetOnlyReady,
			result.Mismatches,
		)
	}
	logger.Info(fmt.Sprintf(
		"Cut over matching task queue namespace=%s queue=%q type=%s (fair=%t) after validating %d target rows.",
		namespaceID,
		taskQueue,
		taskQueueType,
		fair,
		result.TargetTasks,
	))
	return nil
}

func activateLegacyQueueV2SourceFence(ctx *cli.Context, logger log.Logger) error {
	if !ctx.Bool(confirmLegacyQueueSourceDualFlag) {
		return fmt.Errorf(
			"%s is required because pre-fence legacy queue binaries cannot run after activation",
			flag(confirmLegacyQueueSourceDualFlag),
		)
	}
	queueType, err := requiredLegacyQueueType(ctx)
	if err != nil {
		return err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := persistencecassandra.ValidateLegacyQueueMigrationModeSchema(
		context.Background(),
		client.session,
		client.keyspace,
		config.CassandraLegacyQueueMigrationModeSourceDual,
	); err != nil {
		return err
	}
	if err := persistencecassandra.InitializeLegacyQueueV2SourceAuthority(
		context.Background(),
		client.session,
		queueType,
		ctx.Int64(schemaMigrationMessageBucketSizeFlag),
	); err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Activated legacy queue V2 source fence for physical queue type %d.", queueType))
	return nil
}

func backfillLegacyQueueV2(ctx *cli.Context, logger log.Logger) error {
	queueType, options, client, err := legacyQueueV2MigrationConfig(ctx, logger, false)
	if err != nil {
		return err
	}
	defer client.Close()
	copied, err := persistencecassandra.BackfillLegacyQueueV2(
		context.Background(),
		client.session,
		queueType,
		options,
	)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Backfilled %d messages for legacy queue type %d.", copied, queueType))
	return nil
}

func reconcileLegacyQueueV2(ctx *cli.Context, logger log.Logger) error {
	if !ctx.Bool(confirmSourceAuthoritativeFlag) {
		return fmt.Errorf("%s is required because reconciliation deletes divergent target rows", flag(confirmSourceAuthoritativeFlag))
	}
	queueType, options, client, err := legacyQueueV2MigrationConfig(ctx, logger, true)
	if err != nil {
		return err
	}
	defer client.Close()
	result, err := persistencecassandra.ReconcileLegacyQueueV2(
		context.Background(),
		client.session,
		queueType,
		options,
	)
	if err != nil {
		return err
	}
	if !result.Matches() {
		return legacyQueueV2MismatchError(queueType, result)
	}
	logger.Info(fmt.Sprintf(
		"Reconciled %d source and %d target messages for legacy queue type %d.",
		result.SourceRows,
		result.TargetRows,
		queueType,
	))
	return nil
}

func validateLegacyQueueV2(ctx *cli.Context, logger log.Logger) error {
	queueType, options, client, err := legacyQueueV2MigrationConfig(ctx, logger, true)
	if err != nil {
		return err
	}
	defer client.Close()
	result, err := persistencecassandra.ValidateLegacyQueueV2(
		context.Background(),
		client.session,
		queueType,
		options,
	)
	if err != nil {
		return err
	}
	if !result.Matches() {
		return legacyQueueV2MismatchError(queueType, result)
	}
	logger.Info(fmt.Sprintf(
		"Validated %d source and %d target messages for legacy queue type %d.",
		result.SourceRows,
		result.TargetRows,
		queueType,
	))
	return nil
}

func cutoverLegacyQueueV2(ctx *cli.Context, logger log.Logger) error {
	if !ctx.Bool(confirmLegacyQueueSourceDualFlag) {
		return fmt.Errorf(
			"%s is required because cutover seals source writes for this physical queue",
			flag(confirmLegacyQueueSourceDualFlag),
		)
	}
	queueType, options, client, err := legacyQueueV2MigrationConfig(ctx, logger, true)
	if err != nil {
		return err
	}
	defer client.Close()
	result, err := persistencecassandra.CutoverLegacyQueueV2(
		context.Background(),
		client.session,
		queueType,
		options,
	)
	if err != nil {
		return err
	}
	if result.AlreadyTarget {
		logger.Info(fmt.Sprintf("Legacy queue type %d was already target-authoritative.", queueType))
		return nil
	}
	logger.Info(fmt.Sprintf(
		"Cut over legacy queue type %d after validating %d source and %d target messages.",
		queueType,
		result.Validation.SourceRows,
		result.Validation.TargetRows,
	))
	return nil
}

func legacyQueueV2MigrationConfig(
	ctx *cli.Context,
	logger log.Logger,
	withMismatches bool,
) (persistence.QueueType, persistencecassandra.LegacyQueueV2MigrationOptions, *cqlClient, error) {
	queueType, err := requiredLegacyQueueType(ctx)
	if err != nil {
		return 0, persistencecassandra.LegacyQueueV2MigrationOptions{}, nil, err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return 0, persistencecassandra.LegacyQueueV2MigrationOptions{}, nil, err
	}
	if err := persistencecassandra.ValidateLegacyQueueMigrationModeSchema(
		context.Background(),
		client.session,
		client.keyspace,
		config.CassandraLegacyQueueMigrationModeSourceDual,
	); err != nil {
		client.Close()
		return 0, persistencecassandra.LegacyQueueV2MigrationOptions{}, nil, err
	}
	maxMismatches := 0
	if withMismatches {
		maxMismatches = ctx.Int(schemaMigrationMaximumMismatchesFlag)
	}
	return queueType, persistencecassandra.LegacyQueueV2MigrationOptions{
		PageSize:          ctx.Int(schemaMigrationPageSizeFlag),
		MessageBucketSize: ctx.Int64(schemaMigrationMessageBucketSizeFlag),
		MaxMismatches:     maxMismatches,
	}, client, nil
}

func legacyQueueV2MismatchError(
	queueType persistence.QueueType,
	result persistencecassandra.LegacyQueueV2ValidationResult,
) error {
	return fmt.Errorf(
		"legacy queue layouts differ for queue type %d: source=%d target=%d mismatches=%v",
		queueType,
		result.SourceRows,
		result.TargetRows,
		result.Mismatches,
	)
}

func newSchemaMigrationCQLClient(ctx *cli.Context, logger log.Logger) (*cqlClient, error) {
	clientConfig, err := newCQLClientConfig(ctx)
	if err != nil {
		return nil, err
	}
	return newCQLClient(clientConfig, logger)
}

func requiredCheckpointPath(ctx *cli.Context) (string, error) {
	checkpointPath := strings.TrimSpace(ctx.String(schemaMigrationCheckpointFileFlag))
	if checkpointPath == "" {
		return "", fmt.Errorf("missing %s argument", flag(schemaMigrationCheckpointFileFlag))
	}
	return checkpointPath, nil
}

func requiredQueueV2Type(ctx *cli.Context) (persistence.QueueV2Type, error) {
	if !ctx.IsSet(schemaMigrationQueueTypeFlag) {
		return 0, fmt.Errorf("missing %s argument", flag(schemaMigrationQueueTypeFlag))
	}
	return persistence.QueueV2Type(ctx.Int(schemaMigrationQueueTypeFlag)), nil
}

func requiredLegacyQueueType(ctx *cli.Context) (persistence.QueueType, error) {
	if !ctx.IsSet(schemaMigrationQueueTypeFlag) {
		return 0, fmt.Errorf("missing %s argument", flag(schemaMigrationQueueTypeFlag))
	}
	queueType := ctx.Int(schemaMigrationQueueTypeFlag)
	if queueType < math.MinInt32 || queueType > math.MaxInt32 || queueType == 0 {
		return 0, fmt.Errorf("%s must be a non-zero int32", flag(schemaMigrationQueueTypeFlag))
	}
	return persistence.QueueType(queueType), nil
}

func requiredTaskQueueType(ctx *cli.Context) (enumspb.TaskQueueType, error) {
	if !ctx.IsSet(schemaMigrationTaskQueueTypeFlag) {
		return enumspb.TASK_QUEUE_TYPE_UNSPECIFIED, fmt.Errorf("missing %s argument", flag(schemaMigrationTaskQueueTypeFlag))
	}
	value := ctx.Int(schemaMigrationTaskQueueTypeFlag)
	if value < math.MinInt32 || value > math.MaxInt32 {
		return enumspb.TASK_QUEUE_TYPE_UNSPECIFIED, fmt.Errorf("%s must be an int32", flag(schemaMigrationTaskQueueTypeFlag))
	}
	taskQueueType := enumspb.TaskQueueType(value)
	if taskQueueType == enumspb.TASK_QUEUE_TYPE_UNSPECIFIED {
		return enumspb.TASK_QUEUE_TYPE_UNSPECIFIED, fmt.Errorf("%s must not be unspecified", flag(schemaMigrationTaskQueueTypeFlag))
	}
	if _, ok := enumspb.TaskQueueType_name[int32(taskQueueType)]; !ok {
		return enumspb.TASK_QUEUE_TYPE_UNSPECIFIED, fmt.Errorf("unsupported %s value %d", flag(schemaMigrationTaskQueueTypeFlag), value)
	}
	return taskQueueType, nil
}

func newSchemaMigrationCheckpointIdentity(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	direction string,
	sourceTable string,
	sourceLayout string,
	targetTable string,
	tokenRangeCount int,
) (historyNodeBackfillCheckpointIdentity, error) {
	sourceTableID, err := readHistoryNodeBackfillTableID(ctx, session, keyspace, sourceTable)
	if err != nil {
		return historyNodeBackfillCheckpointIdentity{}, err
	}
	targetTableID, err := readHistoryNodeBackfillTableID(ctx, session, keyspace, targetTable)
	if err != nil {
		return historyNodeBackfillCheckpointIdentity{}, err
	}
	partitioner, err := persistencecassandra.GetHistoryNodeBackfillPartitioner(ctx, session)
	if err != nil {
		return historyNodeBackfillCheckpointIdentity{}, err
	}
	if partitioner != persistencecassandra.HistoryNodeBackfillMurmur3Partitioner {
		return historyNodeBackfillCheckpointIdentity{}, fmt.Errorf(
			"schema migration requires Cassandra partitioner %q, got %q",
			persistencecassandra.HistoryNodeBackfillMurmur3Partitioner,
			partitioner,
		)
	}
	return historyNodeBackfillCheckpointIdentity{
		Direction:       direction,
		Keyspace:        keyspace,
		SourceTable:     sourceTable,
		SourceTableID:   sourceTableID,
		SourceLayout:    sourceLayout,
		TargetTable:     targetTable,
		TargetTableID:   targetTableID,
		Partitioner:     partitioner,
		TokenRangeCount: tokenRangeCount,
	}, nil
}
