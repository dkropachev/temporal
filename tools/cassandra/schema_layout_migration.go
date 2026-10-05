package cassandra

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/urfave/cli"
	"go.temporal.io/server/common/log"
	persistencecassandra "go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	schemaLayoutNameFlag               = "layout-name"
	schemaLayoutImmutableParameterFlag = "immutable-parameter"
	schemaLayoutExpectedEpochFlag      = "expected-epoch"
	schemaLayoutInventoryPageSizeFlag  = "inventory-page-size"
	schemaLayoutExpectedShardsFlag     = "expected-execution-shards"
	confirmSchemaLayoutTargetReadyFlag = "confirm-target-validated"
	confirmSchemaLayoutTargetOnlyFlag  = "confirm-target-only-cutover"
)

type schemaLayoutDescriptor struct {
	name        persistencecassandra.SchemaLayoutName
	targetTable string
}

type schemaLayoutMetadataOperator interface {
	InitializePreparing(
		context.Context,
		persistencecassandra.SchemaLayoutSpec,
	) (persistencecassandra.SchemaLayoutMetadata, error)
	LoadAndValidate(
		context.Context,
		persistencecassandra.SchemaLayoutSpec,
	) (persistencecassandra.SchemaLayoutMetadata, error)
	CompareAndSwapAuthority(
		context.Context,
		persistencecassandra.SchemaLayoutMetadata,
		persistencecassandra.SchemaLayoutAuthorityState,
	) (persistencecassandra.SchemaLayoutMetadata, error)
}

func schemaLayoutMigrationCLICommands(logger log.Logger) []cli.Command {
	return []cli.Command{
		{
			Name:  "initialize-schema-layout",
			Usage: "record the live target table generation and initialize one layout in preparing state",
			Flags: schemaLayoutIdentityCLIFlags(),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, initializeSchemaLayout, logger)
			},
		},
		{
			Name:  "inspect-schema-layout",
			Usage: "inspect one layout after checking its live table generation and immutable parameter",
			Flags: schemaLayoutIdentityCLIFlags(),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, inspectSchemaLayout, logger)
			},
		},
		{
			Name:  "mark-schema-layout-target-ready",
			Usage: "atomically move one validated layout from preparing to target-ready without changing service config",
			Flags: schemaLayoutTransitionCLIFlags(cli.BoolFlag{
				Name:  confirmSchemaLayoutTargetReadyFlag,
				Usage: "confirm a stable, bidirectional validation completed for the entire layout generation",
			}),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, markSchemaLayoutTargetReady, logger)
			},
		},
		{
			Name:  "mark-schema-layout-target-only",
			Usage: "atomically move one layout from target-ready to target-only without changing service config",
			Flags: schemaLayoutTransitionCLIFlags(cli.BoolFlag{
				Name:  confirmSchemaLayoutTargetOnlyFlag,
				Usage: "confirm source writers are fenced and every active process can honor target-only authority",
			}),
			Action: func(c *cli.Context) {
				schemaMigrationCLIHandler(c, markSchemaLayoutTargetOnly, logger)
			},
		},
	}
}

func schemaLayoutIdentityCLIFlags() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name: schemaLayoutNameFlag,
			Usage: "required schema layout family name: executions, history_node, history_tree, " +
				"queue_v2_metadata, queue_v2_messages, legacy_queue, matching_tasks, " +
				"matching_tasks_fair, or task_queue_user_data",
		},
		cli.Int64Flag{
			Name:  schemaLayoutImmutableParameterFlag,
			Usage: "required immutable bucket count or span for this layout generation; use zero when unused",
		},
	}
}

func schemaLayoutTransitionCLIFlags(confirmation cli.BoolFlag) []cli.Flag {
	return append(schemaLayoutIdentityCLIFlags(),
		cli.Int64Flag{
			Name:  schemaLayoutExpectedEpochFlag,
			Usage: "required exact authority epoch reported by inspect-schema-layout",
		},
		cli.IntFlag{
			Name:  schemaLayoutInventoryPageSizeFlag,
			Value: defaultSchemaMigrationPageSize,
			Usage: "maximum source entity keys fetched in each target-only inventory page",
		},
		cli.IntFlag{
			Name:  schemaLayoutExpectedShardsFlag,
			Usage: "required fixed Temporal history shard count when cutting executions to target-only",
		},
		confirmation,
	)
}

func initializeSchemaLayout(ctx *cli.Context, logger log.Logger) error {
	descriptor, immutableParameter, err := requiredSchemaLayoutIdentity(ctx)
	if err != nil {
		return err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	spec, err := schemaLayoutSpecForLiveTable(
		context.Background(),
		client.session,
		client.keyspace,
		descriptor,
		immutableParameter,
	)
	if err != nil {
		return err
	}
	metadata, err := initializeSchemaLayoutAuthority(
		context.Background(),
		persistencecassandra.NewSchemaLayoutMetadataStore(client.session),
		spec,
	)
	if err != nil {
		return err
	}
	logSchemaLayoutMetadata(logger, descriptor, metadata)
	return nil
}

func inspectSchemaLayout(ctx *cli.Context, logger log.Logger) error {
	descriptor, immutableParameter, err := requiredSchemaLayoutIdentity(ctx)
	if err != nil {
		return err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	spec, err := schemaLayoutSpecForLiveTable(
		context.Background(),
		client.session,
		client.keyspace,
		descriptor,
		immutableParameter,
	)
	if err != nil {
		return err
	}
	metadata, err := inspectSchemaLayoutAuthority(
		context.Background(),
		persistencecassandra.NewSchemaLayoutMetadataStore(client.session),
		spec,
	)
	if err != nil {
		return err
	}
	logSchemaLayoutMetadata(logger, descriptor, metadata)
	return nil
}

func initializeSchemaLayoutAuthority(
	ctx context.Context,
	store schemaLayoutMetadataOperator,
	spec persistencecassandra.SchemaLayoutSpec,
) (persistencecassandra.SchemaLayoutMetadata, error) {
	return store.InitializePreparing(ctx, spec)
}

func inspectSchemaLayoutAuthority(
	ctx context.Context,
	store schemaLayoutMetadataOperator,
	spec persistencecassandra.SchemaLayoutSpec,
) (persistencecassandra.SchemaLayoutMetadata, error) {
	return store.LoadAndValidate(ctx, spec)
}

func markSchemaLayoutTargetReady(ctx *cli.Context, logger log.Logger) error {
	if !ctx.Bool(confirmSchemaLayoutTargetReadyFlag) {
		return fmt.Errorf(
			"%s is required before marking a schema layout target-ready",
			flag(confirmSchemaLayoutTargetReadyFlag),
		)
	}
	return transitionSchemaLayoutFromCLI(
		ctx,
		logger,
		persistencecassandra.SchemaLayoutAuthorityPreparing,
		persistencecassandra.SchemaLayoutAuthorityTargetReady,
	)
}

func markSchemaLayoutTargetOnly(ctx *cli.Context, logger log.Logger) error {
	if !ctx.Bool(confirmSchemaLayoutTargetOnlyFlag) {
		return fmt.Errorf(
			"%s is required before marking a schema layout target-only",
			flag(confirmSchemaLayoutTargetOnlyFlag),
		)
	}
	return transitionSchemaLayoutFromCLI(
		ctx,
		logger,
		persistencecassandra.SchemaLayoutAuthorityTargetReady,
		persistencecassandra.SchemaLayoutAuthorityTargetOnly,
	)
}

//nolint:revive // The CLI keeps identity, audit, and CAS gates together so no transition can bypass a prerequisite.
func transitionSchemaLayoutFromCLI(
	ctx *cli.Context,
	logger log.Logger,
	expectedState persistencecassandra.SchemaLayoutAuthorityState,
	targetState persistencecassandra.SchemaLayoutAuthorityState,
) error {
	descriptor, immutableParameter, err := requiredSchemaLayoutIdentity(ctx)
	if err != nil {
		return err
	}
	expectedEpoch, err := requiredSchemaLayoutExpectedEpoch(ctx)
	if err != nil {
		return err
	}
	pageSize, err := requiredSchemaLayoutInventoryPageSize(ctx)
	if err != nil {
		return err
	}
	client, err := newSchemaMigrationCQLClient(ctx, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	spec, err := schemaLayoutSpecForLiveTable(
		context.Background(),
		client.session,
		client.keyspace,
		descriptor,
		immutableParameter,
	)
	if err != nil {
		return err
	}
	if targetState == persistencecassandra.SchemaLayoutAuthorityTargetOnly &&
		isQueueV2SchemaLayout(descriptor.name) {
		metadataSpec, messagesSpec, err := queueV2SchemaLayoutPairForTransition(
			context.Background(),
			client.session,
			client.keyspace,
			spec,
		)
		if err != nil {
			return err
		}
		audit := func() error {
			result, err := persistencecassandra.AuditSchemaLayoutTargetOnlyInventory(
				context.Background(),
				client.session,
				spec,
				persistencecassandra.SchemaLayoutInventoryAuditOptions{
					PageSize:            pageSize,
					QueueV2MetadataSpec: metadataSpec,
					QueueV2MessagesSpec: messagesSpec,
				},
			)
			if err != nil {
				return fmt.Errorf("QueueV2 target-only inventory audit failed: %w", err)
			}
			logger.Info(fmt.Sprintf(
				"Audited %d source and %d target QueueV2 entities before paired target-only cutover.",
				result.SourceEntities,
				result.TargetEntities,
			))
			return nil
		}
		metadata, messages, err := transitionQueueV2SchemaLayoutsTargetOnly(
			context.Background(),
			persistencecassandra.NewSchemaLayoutMetadataStore(client.session),
			metadataSpec,
			messagesSpec,
			descriptor.name,
			expectedEpoch,
			audit,
		)
		if err != nil {
			return err
		}
		metadataDescriptor, _ := schemaLayoutDescriptorForName(metadata.Name)
		messagesDescriptor, _ := schemaLayoutDescriptorForName(messages.Name)
		logSchemaLayoutMetadata(logger, metadataDescriptor, metadata)
		logSchemaLayoutMetadata(logger, messagesDescriptor, messages)
		return nil
	}
	var beforeCompareAndSwap func() error
	if targetState == persistencecassandra.SchemaLayoutAuthorityTargetOnly &&
		schemaLayoutHasTargetOnlyInventoryAudit(descriptor.name) {
		beforeCompareAndSwap = func() error {
			expectedExecutionShards := 0
			if descriptor.name == persistencecassandra.SchemaLayoutExecutions {
				expectedExecutionShards = ctx.Int(schemaLayoutExpectedShardsFlag)
				if expectedExecutionShards <= 0 {
					return fmt.Errorf(
						"%s must be positive for the executions target-only inventory audit",
						flag(schemaLayoutExpectedShardsFlag),
					)
				}
			}
			result, err := persistencecassandra.AuditSchemaLayoutTargetOnlyInventory(
				context.Background(),
				client.session,
				spec,
				persistencecassandra.SchemaLayoutInventoryAuditOptions{
					PageSize:                pageSize,
					ExpectedExecutionShards: expectedExecutionShards,
				},
			)
			if err != nil {
				return fmt.Errorf("schema layout %q target-only inventory audit failed: %w", descriptor.name, err)
			}
			logger.Info(fmt.Sprintf(
				"Audited %d source and %d target entities before schema layout %s target-only cutover.",
				result.SourceEntities,
				result.TargetEntities,
				descriptor.name,
			))
			return nil
		}
	}
	metadata, err := transitionSchemaLayoutAuthorityWithGate(
		context.Background(),
		persistencecassandra.NewSchemaLayoutMetadataStore(client.session),
		spec,
		expectedState,
		targetState,
		expectedEpoch,
		beforeCompareAndSwap,
	)
	if err != nil {
		return err
	}
	logSchemaLayoutMetadata(logger, descriptor, metadata)
	return nil
}

func transitionSchemaLayoutAuthority(
	ctx context.Context,
	store schemaLayoutMetadataOperator,
	spec persistencecassandra.SchemaLayoutSpec,
	expectedState persistencecassandra.SchemaLayoutAuthorityState,
	targetState persistencecassandra.SchemaLayoutAuthorityState,
	expectedEpoch int64,
) (persistencecassandra.SchemaLayoutMetadata, error) {
	return transitionSchemaLayoutAuthorityWithGate(
		ctx,
		store,
		spec,
		expectedState,
		targetState,
		expectedEpoch,
		nil,
	)
}

func transitionSchemaLayoutAuthorityWithGate(
	ctx context.Context,
	store schemaLayoutMetadataOperator,
	spec persistencecassandra.SchemaLayoutSpec,
	expectedState persistencecassandra.SchemaLayoutAuthorityState,
	targetState persistencecassandra.SchemaLayoutAuthorityState,
	expectedEpoch int64,
	beforeCompareAndSwap func() error,
) (persistencecassandra.SchemaLayoutMetadata, error) {
	metadata, err := store.LoadAndValidate(ctx, spec)
	if err != nil {
		return persistencecassandra.SchemaLayoutMetadata{}, err
	}
	if metadata.AuthorityState == targetState {
		if expectedEpoch < math.MaxInt64 && metadata.Epoch == expectedEpoch+1 {
			return metadata, nil
		}
		return persistencecassandra.SchemaLayoutMetadata{}, fmt.Errorf(
			"schema layout %q is already %q at epoch %d, not the resumable result of expected epoch %d",
			spec.Name,
			targetState,
			metadata.Epoch,
			expectedEpoch,
		)
	}
	if metadata.AuthorityState != expectedState || metadata.Epoch != expectedEpoch {
		return persistencecassandra.SchemaLayoutMetadata{}, fmt.Errorf(
			"schema layout %q is %q at epoch %d; expected %q at epoch %d",
			spec.Name,
			metadata.AuthorityState,
			metadata.Epoch,
			expectedState,
			expectedEpoch,
		)
	}
	if beforeCompareAndSwap != nil {
		if err := beforeCompareAndSwap(); err != nil {
			return persistencecassandra.SchemaLayoutMetadata{}, err
		}
	}
	return store.CompareAndSwapAuthority(ctx, metadata, targetState)
}

func schemaLayoutHasTargetOnlyInventoryAudit(name persistencecassandra.SchemaLayoutName) bool {
	switch name {
	case persistencecassandra.SchemaLayoutExecutions,
		persistencecassandra.SchemaLayoutHistoryNode,
		persistencecassandra.SchemaLayoutHistoryTree,
		persistencecassandra.SchemaLayoutQueueV2Metadata,
		persistencecassandra.SchemaLayoutQueueV2Messages,
		persistencecassandra.SchemaLayoutMatchingTasks,
		persistencecassandra.SchemaLayoutMatchingTasksFair,
		persistencecassandra.SchemaLayoutTaskQueueUserData,
		persistencecassandra.SchemaLayoutLegacyQueue:
		return true
	default:
		return false
	}
}

func isQueueV2SchemaLayout(name persistencecassandra.SchemaLayoutName) bool {
	return name == persistencecassandra.SchemaLayoutQueueV2Metadata ||
		name == persistencecassandra.SchemaLayoutQueueV2Messages
}

func queueV2SchemaLayoutPairForTransition(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	requested persistencecassandra.SchemaLayoutSpec,
) (metadataSpec, messagesSpec persistencecassandra.SchemaLayoutSpec, err error) {
	if !isQueueV2SchemaLayout(requested.Name) {
		return persistencecassandra.SchemaLayoutSpec{}, persistencecassandra.SchemaLayoutSpec{}, fmt.Errorf(
			"schema layout %q is not a QueueV2 layout",
			requested.Name,
		)
	}
	counterpartName := persistencecassandra.SchemaLayoutQueueV2Metadata
	if requested.Name == persistencecassandra.SchemaLayoutQueueV2Metadata {
		counterpartName = persistencecassandra.SchemaLayoutQueueV2Messages
	}
	store := persistencecassandra.NewSchemaLayoutMetadataStore(session)
	counterpartMetadata, err := store.Load(ctx, counterpartName)
	if err != nil {
		return persistencecassandra.SchemaLayoutSpec{}, persistencecassandra.SchemaLayoutSpec{}, fmt.Errorf(
			"load paired QueueV2 schema layout %q: %w",
			counterpartName,
			err,
		)
	}
	counterpartDescriptor, err := schemaLayoutDescriptorForName(counterpartName)
	if err != nil {
		return persistencecassandra.SchemaLayoutSpec{}, persistencecassandra.SchemaLayoutSpec{}, err
	}
	counterpart, err := schemaLayoutSpecForLiveTable(
		ctx,
		session,
		keyspace,
		counterpartDescriptor,
		counterpartMetadata.ImmutableParameter,
	)
	if err != nil {
		return persistencecassandra.SchemaLayoutSpec{}, persistencecassandra.SchemaLayoutSpec{}, err
	}
	metadataSpec = requested
	messagesSpec = counterpart
	if requested.Name == persistencecassandra.SchemaLayoutQueueV2Messages {
		metadataSpec = counterpart
		messagesSpec = requested
	}
	return metadataSpec, messagesSpec, nil
}

//nolint:revive // This is a crash-resumable two-record state machine with strict ordering and epoch validation.
func transitionQueueV2SchemaLayoutsTargetOnly(
	ctx context.Context,
	store schemaLayoutMetadataOperator,
	metadataSpec persistencecassandra.SchemaLayoutSpec,
	messagesSpec persistencecassandra.SchemaLayoutSpec,
	requestedName persistencecassandra.SchemaLayoutName,
	expectedEpoch int64,
	beforeCompareAndSwap func() error,
) (metadata, messages persistencecassandra.SchemaLayoutMetadata, err error) {
	if metadataSpec.Name != persistencecassandra.SchemaLayoutQueueV2Metadata ||
		messagesSpec.Name != persistencecassandra.SchemaLayoutQueueV2Messages {
		return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, errors.New(
			"paired QueueV2 transition requires metadata and message layout identities",
		)
	}
	if !isQueueV2SchemaLayout(requestedName) {
		return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, fmt.Errorf(
			"schema layout %q is not a QueueV2 layout",
			requestedName,
		)
	}
	metadata, err = store.LoadAndValidate(ctx, metadataSpec)
	if err != nil {
		return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, err
	}
	messages, err = store.LoadAndValidate(ctx, messagesSpec)
	if err != nil {
		return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, err
	}
	requested := metadata
	if requestedName == persistencecassandra.SchemaLayoutQueueV2Messages {
		requested = messages
	}
	if requested.AuthorityState == persistencecassandra.SchemaLayoutAuthorityTargetReady {
		if requested.Epoch != expectedEpoch {
			return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, fmt.Errorf(
				"schema layout %q is %q at epoch %d; expected %q at epoch %d",
				requested.Name,
				requested.AuthorityState,
				requested.Epoch,
				persistencecassandra.SchemaLayoutAuthorityTargetReady,
				expectedEpoch,
			)
		}
	} else if requested.AuthorityState != persistencecassandra.SchemaLayoutAuthorityTargetOnly ||
		expectedEpoch == math.MaxInt64 || requested.Epoch != expectedEpoch+1 {
		return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, fmt.Errorf(
			"schema layout %q is %q at epoch %d, not the requested or resumable target-only transition from epoch %d",
			requested.Name,
			requested.AuthorityState,
			requested.Epoch,
			expectedEpoch,
		)
	}
	if metadata.AuthorityState == persistencecassandra.SchemaLayoutAuthorityTargetOnly &&
		messages.AuthorityState == persistencecassandra.SchemaLayoutAuthorityTargetReady {
		return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, errors.New(
			"QueueV2 schema layouts are in an invalid partial transition: metadata is target-only before messages",
		)
	}
	if metadata.AuthorityState == messages.AuthorityState {
		if metadata.Epoch != messages.Epoch {
			return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, fmt.Errorf(
				"QueueV2 schema layout epochs differ in state %q: metadata=%d messages=%d",
				metadata.AuthorityState,
				metadata.Epoch,
				messages.Epoch,
			)
		}
	} else if messages.AuthorityState != persistencecassandra.SchemaLayoutAuthorityTargetOnly ||
		messages.Epoch != metadata.Epoch+1 {
		return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, fmt.Errorf(
			"QueueV2 schema layout transition is inconsistent: metadata=%s/%d messages=%s/%d",
			metadata.AuthorityState,
			metadata.Epoch,
			messages.AuthorityState,
			messages.Epoch,
		)
	}
	for _, layout := range []persistencecassandra.SchemaLayoutMetadata{metadata, messages} {
		if layout.AuthorityState != persistencecassandra.SchemaLayoutAuthorityTargetReady &&
			layout.AuthorityState != persistencecassandra.SchemaLayoutAuthorityTargetOnly {
			return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, fmt.Errorf(
				"QueueV2 schema layout %q is %q at epoch %d; expected target-ready or target-only",
				layout.Name,
				layout.AuthorityState,
				layout.Epoch,
			)
		}
	}
	if metadata.AuthorityState == persistencecassandra.SchemaLayoutAuthorityTargetOnly &&
		messages.AuthorityState == persistencecassandra.SchemaLayoutAuthorityTargetOnly {
		return metadata, messages, nil
	}
	if beforeCompareAndSwap != nil {
		if err := beforeCompareAndSwap(); err != nil {
			return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, err
		}
	}
	if messages.AuthorityState == persistencecassandra.SchemaLayoutAuthorityTargetReady {
		messages, err = store.CompareAndSwapAuthority(
			ctx,
			messages,
			persistencecassandra.SchemaLayoutAuthorityTargetOnly,
		)
		if err != nil {
			return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, err
		}
	}
	metadata, err = store.CompareAndSwapAuthority(
		ctx,
		metadata,
		persistencecassandra.SchemaLayoutAuthorityTargetOnly,
	)
	if err != nil {
		return persistencecassandra.SchemaLayoutMetadata{}, persistencecassandra.SchemaLayoutMetadata{}, err
	}
	return metadata, messages, nil
}

func requireSchemaLayoutAuthority(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	descriptor schemaLayoutDescriptor,
	immutableParameter int64,
	allowed ...persistencecassandra.SchemaLayoutAuthorityState,
) (persistencecassandra.SchemaLayoutMetadata, error) {
	spec, err := schemaLayoutSpecForLiveTable(
		ctx,
		session,
		keyspace,
		descriptor,
		immutableParameter,
	)
	if err != nil {
		return persistencecassandra.SchemaLayoutMetadata{}, err
	}
	metadata, err := persistencecassandra.NewSchemaLayoutMetadataStore(session).LoadAndValidate(ctx, spec)
	if err != nil {
		return persistencecassandra.SchemaLayoutMetadata{}, err
	}
	for _, state := range allowed {
		if metadata.AuthorityState == state {
			return metadata, nil
		}
	}
	return persistencecassandra.SchemaLayoutMetadata{}, fmt.Errorf(
		"schema layout %q is %q at epoch %d; expected one of %v",
		descriptor.name,
		metadata.AuthorityState,
		metadata.Epoch,
		allowed,
	)
}

func schemaLayoutSpecForLiveTable(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	descriptor schemaLayoutDescriptor,
	immutableParameter int64,
) (persistencecassandra.SchemaLayoutSpec, error) {
	return persistencecassandra.SchemaLayoutSpecForTable(
		ctx,
		session,
		keyspace,
		descriptor.name,
		descriptor.targetTable,
		immutableParameter,
	)
}

func requiredSchemaLayoutIdentity(ctx *cli.Context) (schemaLayoutDescriptor, int64, error) {
	name := strings.TrimSpace(ctx.String(schemaLayoutNameFlag))
	if name == "" {
		return schemaLayoutDescriptor{}, 0, fmt.Errorf("missing %s argument", flag(schemaLayoutNameFlag))
	}
	descriptor, err := schemaLayoutDescriptorForName(persistencecassandra.SchemaLayoutName(name))
	if err != nil {
		return schemaLayoutDescriptor{}, 0, err
	}
	if !ctx.IsSet(schemaLayoutImmutableParameterFlag) {
		return schemaLayoutDescriptor{}, 0, fmt.Errorf(
			"missing %s argument",
			flag(schemaLayoutImmutableParameterFlag),
		)
	}
	immutableParameter := ctx.Int64(schemaLayoutImmutableParameterFlag)
	if immutableParameter < 0 {
		return schemaLayoutDescriptor{}, 0, fmt.Errorf(
			"%s must not be negative",
			flag(schemaLayoutImmutableParameterFlag),
		)
	}
	return descriptor, immutableParameter, nil
}

func requiredSchemaLayoutExpectedEpoch(ctx *cli.Context) (int64, error) {
	if !ctx.IsSet(schemaLayoutExpectedEpochFlag) {
		return 0, fmt.Errorf("missing %s argument", flag(schemaLayoutExpectedEpochFlag))
	}
	epoch := ctx.Int64(schemaLayoutExpectedEpochFlag)
	if epoch < 0 || epoch == math.MaxInt64 {
		return 0, fmt.Errorf("%s must be between zero and %d", flag(schemaLayoutExpectedEpochFlag), int64(math.MaxInt64-1))
	}
	return epoch, nil
}

func requiredSchemaLayoutInventoryPageSize(ctx *cli.Context) (int, error) {
	pageSize := ctx.Int(schemaLayoutInventoryPageSizeFlag)
	if pageSize <= 0 {
		return 0, fmt.Errorf("%s must be positive", flag(schemaLayoutInventoryPageSizeFlag))
	}
	return pageSize, nil
}

func schemaLayoutDescriptorForName(
	name persistencecassandra.SchemaLayoutName,
) (schemaLayoutDescriptor, error) {
	var targetTable string
	switch name {
	case persistencecassandra.SchemaLayoutExecutions:
		targetTable = executionsV2TargetTable
	case persistencecassandra.SchemaLayoutHistoryNode:
		targetTable = "history_node_v2"
	case persistencecassandra.SchemaLayoutHistoryTree:
		targetTable = "history_tree_v2"
	case persistencecassandra.SchemaLayoutQueueV2Metadata:
		targetTable = queueV2MetadataTargetTable
	case persistencecassandra.SchemaLayoutQueueV2Messages:
		targetTable = "queue_messages_v3"
	case persistencecassandra.SchemaLayoutLegacyQueue:
		targetTable = "legacy_queue_v2_messages"
	case persistencecassandra.SchemaLayoutMatchingTasks:
		targetTable = matchingTaskV3TargetTable
	case persistencecassandra.SchemaLayoutMatchingTasksFair:
		targetTable = matchingTaskV3FairTargetTable
	case persistencecassandra.SchemaLayoutTaskQueueUserData:
		targetTable = taskQueueUserDataV2TargetTable
	default:
		return schemaLayoutDescriptor{}, fmt.Errorf("unsupported schema layout name %q", name)
	}
	return schemaLayoutDescriptor{name: name, targetTable: targetTable}, nil
}

func logSchemaLayoutMetadata(
	logger log.Logger,
	descriptor schemaLayoutDescriptor,
	metadata persistencecassandra.SchemaLayoutMetadata,
) {
	logger.Info(fmt.Sprintf(
		"Schema layout %s: target=%s generation=%s version=%d immutable-parameter=%d authority=%s epoch=%d updated-at=%s.",
		descriptor.name,
		descriptor.targetTable,
		metadata.Generation,
		metadata.Version,
		metadata.ImmutableParameter,
		metadata.AuthorityState,
		metadata.Epoch,
		metadata.UpdatedAt.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"),
	))
}
