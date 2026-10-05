package cassandra

import (
	"context"
	"fmt"
	"strings"

	"github.com/urfave/cli"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	persistencecassandra "go.temporal.io/server/common/persistence/cassandra"
)

const (
	schemaMigrationQueueNameFlag         = "queue-name"
	schemaMigrationMessageBucketSpanFlag = "message-bucket-span"
	confirmQueueV2SourceFencingFlag      = "confirm-source-writers-fenced"
	queueV2MessagesSourceTable           = "queue_messages"
	queueV2MessagesV3TargetTable         = "queue_messages_v3"
)

type queueV2MessagesV3MigrationOptions struct {
	queueType         persistence.QueueV2Type
	queueName         string
	pageSize          int
	messageBucketSpan int64
	checkpointPath    string
}

func queueV2MessagesV3MigrationCLICommands(logger log.Logger) []cli.Command {
	return []cli.Command{
		{
			Name:  "backfill-queue-v2-messages-v3",
			Usage: "copy one QueueV2 message stream into queue_messages_v3 with a resumable checkpoint",
			Flags: queueV2MessagesV3MigrationCLIFlags(true, true),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, backfillQueueV2MessagesV3, logger)
			},
		},
		{
			Name:  "reconcile-queue-v2-messages-v3",
			Usage: "repair and validate one target message stream with a resumable checkpoint while its source remains authoritative",
			Flags: queueV2MessagesV3MigrationCLIFlags(true, true),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, reconcileQueueV2MessagesV3, logger)
			},
		},
		{
			Name:  "validate-queue-v2-messages-v3",
			Usage: "compare one stable QueueV2 message stream in both directions before target cutover",
			Flags: queueV2MessagesV3MigrationCLIFlags(false, false),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, validateQueueV2MessagesV3, logger)
			},
		},
		{
			Name:  "cutover-queue-v2",
			Usage: "seal, exactly reconcile, and atomically publish one physical QueueV2 queue to the target layout",
			Flags: append(queueV2MessagesV3MigrationCLIFlags(false, false), cli.BoolFlag{
				Name:  confirmQueueV2SourceFencingFlag,
				Usage: "confirm every QueueV2 writer is running source-dual, target-shadow, or target-dual fencing code",
			}),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, cutoverQueueV2, logger)
			},
		},
	}
}

func cutoverQueueV2(ctx *cli.Context, logger log.Logger) error {
	if !ctx.Bool(confirmQueueV2SourceFencingFlag) {
		return fmt.Errorf(
			"%s is required before sealing a QueueV2 source queue",
			flag(confirmQueueV2SourceFencingFlag),
		)
	}
	options, err := requiredQueueV2MessagesV3MigrationOptions(ctx, false, false)
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
		config.CassandraQueueV2MigrationModeTargetDual,
	); err != nil {
		return err
	}
	if _, err := requireSchemaLayoutAuthority(
		context.Background(),
		client.session,
		client.keyspace,
		schemaLayoutDescriptor{
			name:        persistencecassandra.SchemaLayoutQueueV2Metadata,
			targetTable: queueV2MetadataTargetTable,
		},
		64,
		persistencecassandra.SchemaLayoutAuthorityTargetReady,
	); err != nil {
		return fmt.Errorf("require QueueV2 metadata target-ready authority: %w", err)
	}
	messageLayout, err := requireSchemaLayoutAuthority(
		context.Background(),
		client.session,
		client.keyspace,
		schemaLayoutDescriptor{
			name:        persistencecassandra.SchemaLayoutQueueV2Messages,
			targetTable: queueV2MessagesV3TargetTable,
		},
		options.messageBucketSpan,
		persistencecassandra.SchemaLayoutAuthorityTargetReady,
	)
	if err != nil {
		return fmt.Errorf("require QueueV2 messages target-ready authority: %w", err)
	}
	result, err := persistencecassandra.CutoverQueueV2ToTarget(
		context.Background(),
		client.session,
		persistencecassandra.QueueV2CutoverOptions{
			QueueType:         options.queueType,
			QueueName:         options.queueName,
			PageSize:          options.pageSize,
			MessageBucketSpan: options.messageBucketSpan,
			Generation:        messageLayout.Generation,
		},
	)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf(
		"Cut over QueueV2 queue type %d and name %q to generation %s at epoch %d; validated %d source and %d target messages.",
		options.queueType,
		options.queueName,
		result.Generation,
		result.Epoch,
		result.Validation.SourceRows,
		result.Validation.TargetRows,
	))
	return nil
}

func queueV2MessagesV3MigrationCLIFlags(withCheckpoint bool, withSourceConfirmation bool) []cli.Flag {
	flags := []cli.Flag{
		cli.IntFlag{
			Name:  schemaMigrationQueueTypeFlag,
			Usage: "required QueueV2 type",
		},
		cli.StringFlag{
			Name:  schemaMigrationQueueNameFlag,
			Usage: "required QueueV2 name; run once for every physical queue",
		},
		cli.IntFlag{
			Name:  schemaMigrationPageSizeFlag,
			Value: defaultSchemaMigrationPageSize,
			Usage: "number of messages copied or compared per page",
		},
		cli.Int64Flag{
			Name:  schemaMigrationMessageBucketSpanFlag,
			Value: persistencecassandra.DefaultQueueV2MessageBucketSpan,
			Usage: "immutable number of message IDs in each queue_messages_v3 bucket",
		},
	}
	if withCheckpoint {
		flags = append(flags, cli.StringFlag{
			Name:  schemaMigrationCheckpointFileFlag,
			Usage: "required durable checkpoint file; use a new path for each complete reconciliation pass",
		})
	}
	if withSourceConfirmation {
		flags = append(flags, cli.BoolFlag{
			Name:  confirmSourceAuthoritativeFlag,
			Usage: "confirm queue_messages remains authoritative for this stream",
		})
	}
	return flags
}

func backfillQueueV2MessagesV3(ctx *cli.Context, logger log.Logger) error {
	return runQueueV2MessagesV3Copy(ctx, logger, false)
}

func reconcileQueueV2MessagesV3(ctx *cli.Context, logger log.Logger) error {
	return runQueueV2MessagesV3Copy(ctx, logger, true)
}

func runQueueV2MessagesV3Copy(ctx *cli.Context, logger log.Logger, validate bool) error {
	options, err := requiredQueueV2MessagesV3MigrationOptions(ctx, true, true)
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
	if _, err := requireSchemaLayoutAuthority(
		context.Background(),
		client.session,
		client.keyspace,
		schemaLayoutDescriptor{
			name:        persistencecassandra.SchemaLayoutQueueV2Messages,
			targetTable: queueV2MessagesV3TargetTable,
		},
		options.messageBucketSpan,
		persistencecassandra.SchemaLayoutAuthorityPreparing,
	); err != nil {
		return fmt.Errorf("require QueueV2 message layout preparing authority: %w", err)
	}

	operation := "backfill-queue-v2-messages-v3"
	if validate {
		operation = "reconcile-queue-v2-messages-v3"
	}
	identity, err := newMigrationPageCheckpointIdentity(
		context.Background(),
		client.session,
		client.keyspace,
		operation,
		queueV2MessagesSourceTable,
		queueV2MessagesV3TargetTable,
		fmt.Sprintf(
			"queue-type=%d,queue-name=%q,page-size=%d,message-bucket-span=%d",
			options.queueType,
			options.queueName,
			options.pageSize,
			options.messageBucketSpan,
		),
	)
	if err != nil {
		return err
	}
	validateIdentity := func(validateCtx context.Context) error {
		return validateMigrationPageCheckpointIdentity(validateCtx, client.session, identity)
	}
	rows, err := runMigrationPagePhase(
		context.Background(),
		options.checkpointPath,
		identity,
		"source-to-target",
		validateIdentity,
		func(pageCtx context.Context, pageToken []byte) (int, []byte, error) {
			page, err := persistencecassandra.BackfillQueueV2MessagesPage(
				pageCtx,
				client.session,
				options.queueType,
				options.queueName,
				options.pageSize,
				pageToken,
				options.messageBucketSpan,
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
	if !validate {
		logger.Info(fmt.Sprintf(
			"Backfilled %d QueueV2 messages for queue type %d and name %q.",
			rows,
			options.queueType,
			options.queueName,
		))
		return nil
	}
	result, err := persistencecassandra.ValidateQueueV2Messages(
		context.Background(),
		client.session,
		options.queueType,
		options.queueName,
		options.pageSize,
		options.messageBucketSpan,
	)
	if err != nil {
		return err
	}
	if !result.Matches() {
		return queueV2MessagesV3MismatchError(options, result)
	}
	logger.Info(fmt.Sprintf(
		"Reconciled and validated %d source and %d target QueueV2 messages for queue type %d and name %q.",
		result.SourceRows,
		result.TargetRows,
		options.queueType,
		options.queueName,
	))
	return nil
}

func validateQueueV2MessagesV3(ctx *cli.Context, logger log.Logger) error {
	options, err := requiredQueueV2MessagesV3MigrationOptions(ctx, false, false)
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
	if _, err := requireSchemaLayoutAuthority(
		context.Background(),
		client.session,
		client.keyspace,
		schemaLayoutDescriptor{
			name:        persistencecassandra.SchemaLayoutQueueV2Messages,
			targetTable: queueV2MessagesV3TargetTable,
		},
		options.messageBucketSpan,
		persistencecassandra.SchemaLayoutAuthorityPreparing,
		persistencecassandra.SchemaLayoutAuthorityTargetReady,
	); err != nil {
		return fmt.Errorf("require QueueV2 message layout pre-cutover authority: %w", err)
	}
	result, err := persistencecassandra.ValidateQueueV2Messages(
		context.Background(),
		client.session,
		options.queueType,
		options.queueName,
		options.pageSize,
		options.messageBucketSpan,
	)
	if err != nil {
		return err
	}
	if !result.Matches() {
		return queueV2MessagesV3MismatchError(options, result)
	}
	logger.Info(fmt.Sprintf(
		"Validated %d source and %d target QueueV2 messages for queue type %d and name %q.",
		result.SourceRows,
		result.TargetRows,
		options.queueType,
		options.queueName,
	))
	return nil
}

func requiredQueueV2MessagesV3MigrationOptions(
	ctx *cli.Context,
	withCheckpoint bool,
	withSourceConfirmation bool,
) (queueV2MessagesV3MigrationOptions, error) {
	queueType, err := requiredQueueV2Type(ctx)
	if err != nil {
		return queueV2MessagesV3MigrationOptions{}, err
	}
	queueName := strings.TrimSpace(ctx.String(schemaMigrationQueueNameFlag))
	if queueName == "" {
		return queueV2MessagesV3MigrationOptions{}, fmt.Errorf(
			"missing %s argument",
			flag(schemaMigrationQueueNameFlag),
		)
	}
	pageSize := ctx.Int(schemaMigrationPageSizeFlag)
	if pageSize <= 0 {
		return queueV2MessagesV3MigrationOptions{}, fmt.Errorf(
			"%s must be positive",
			flag(schemaMigrationPageSizeFlag),
		)
	}
	messageBucketSpan := ctx.Int64(schemaMigrationMessageBucketSpanFlag)
	if messageBucketSpan <= 0 {
		return queueV2MessagesV3MigrationOptions{}, fmt.Errorf(
			"%s must be positive",
			flag(schemaMigrationMessageBucketSpanFlag),
		)
	}
	checkpointPath := ""
	if withCheckpoint {
		checkpointPath, err = requiredCheckpointPath(ctx)
		if err != nil {
			return queueV2MessagesV3MigrationOptions{}, err
		}
	}
	if withSourceConfirmation && !ctx.Bool(confirmSourceAuthoritativeFlag) {
		return queueV2MessagesV3MigrationOptions{}, fmt.Errorf(
			"%s is required before copying source-authoritative QueueV2 messages",
			flag(confirmSourceAuthoritativeFlag),
		)
	}
	return queueV2MessagesV3MigrationOptions{
		queueType:         queueType,
		queueName:         queueName,
		pageSize:          pageSize,
		messageBucketSpan: messageBucketSpan,
		checkpointPath:    checkpointPath,
	}, nil
}

func queueV2MessagesV3MismatchError(
	options queueV2MessagesV3MigrationOptions,
	result persistencecassandra.QueueV2MessageValidationResult,
) error {
	return fmt.Errorf(
		"QueueV2 message layouts differ for queue type %d and name %q: source=%d target=%d mismatches=%v",
		options.queueType,
		options.queueName,
		result.SourceRows,
		result.TargetRows,
		result.Mismatches,
	)
}
