package cassandra

import (
	"context"
	"fmt"
	"sync"

	"github.com/gocql/gocql"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
)

type (
	// Factory vends datastore implementations backed by cassandra
	Factory struct {
		sync.RWMutex
		cfg         config.Cassandra
		clusterName string
		logger      log.Logger
		session     commongocql.Session
		serializer  serialization.Serializer
	}
)

// NewFactory returns an instance of a factory object which can be used to create
// data stores that are backed by cassandra
func NewFactory(
	cfg config.Cassandra,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
	serializer serialization.Serializer,
) *Factory {
	session, err := commongocql.NewSession(
		func() (*gocql.ClusterConfig, error) {
			return commongocql.NewCassandraCluster(cfg, r)
		},
		logger,
		metricsHandler,
	)
	if err != nil {
		logger.Fatal("unable to initialize cassandra session", tag.Error(err))
	}
	return NewFactoryFromSession(
		cfg,
		clusterName,
		logger,
		session,
		serializer,
	)
}

// NewFactoryFromSession returns an instance of a factory object from the given session.
func NewFactoryFromSession(
	cfg config.Cassandra,
	clusterName string,
	logger log.Logger,
	session commongocql.Session,
	serializer serialization.Serializer,
) *Factory {
	return &Factory{
		cfg:         cfg,
		clusterName: clusterName,
		logger:      logger,
		session:     session,
		serializer:  serializer,
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	return f.newMatchingTaskStore(false)
}

// NewTaskStore returns a new task store
func (f *Factory) NewFairTaskStore() (p.TaskStore, error) {
	return f.newMatchingTaskStore(true)
}

//nolint:revive // Startup validation intentionally keeps the coupled matching and user-data migration gates together.
func (f *Factory) newMatchingTaskStore(enableFairness bool) (p.TaskStore, error) {
	mode := normalizeTaskQueueUserDataMigrationMode(f.cfg.TaskQueueUserDataMigrationMode)
	userDataBucketCount := f.cfg.TaskQueueUserDataBucketCount
	if userDataBucketCount == 0 {
		userDataBucketCount = DefaultTaskQueueUserDataBucketCount
	}
	if err := ValidateTaskQueueUserDataMigrationModeSchema(
		context.TODO(),
		f.session,
		f.cfg.Keyspace,
		mode,
	); err != nil {
		return nil, fmt.Errorf("validate Cassandra task queue user data migration schema: %w", err)
	}
	if mode == TaskQueueUserDataMigrationModeSourceOnly {
		if err := RequireSchemaLayoutSourceCompatible(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutTaskQueueUserData,
			taskQueueUserDataV2TableName, int64(userDataBucketCount),
		); err != nil {
			return nil, err
		}
	}
	var userDataGeneration gocql.UUID
	if mode != TaskQueueUserDataMigrationModeSourceOnly {
		spec, err := SchemaLayoutSpecForTable(
			context.TODO(),
			f.session,
			f.cfg.Keyspace,
			SchemaLayoutTaskQueueUserData,
			taskQueueUserDataV2TableName,
			int64(userDataBucketCount),
		)
		if err != nil {
			return nil, err
		}
		metadataStore := NewSchemaLayoutMetadataStore(f.session)
		switch mode {
		case TaskQueueUserDataMigrationModeTargetOnly:
			_, err = metadataStore.RequireTargetOnly(context.TODO(), spec)
		case TaskQueueUserDataMigrationModeTargetDual:
			_, err = metadataStore.RequireTargetReady(context.TODO(), spec)
		default:
			var metadata SchemaLayoutMetadata
			metadata, err = metadataStore.LoadAndValidate(context.TODO(), spec)
			if err == nil && (metadata.AuthorityState == SchemaLayoutAuthorityTargetOnly ||
				metadata.AuthorityState == SchemaLayoutAuthorityRetired) {
				err = &SchemaLayoutAuthorityError{
					Name:     spec.Name,
					Expected: []SchemaLayoutAuthorityState{SchemaLayoutAuthorityPreparing, SchemaLayoutAuthorityTargetReady},
					Actual:   metadata.AuthorityState,
				}
			}
		}
		if err != nil {
			return nil, fmt.Errorf("validate Cassandra task queue user data layout authority: %w", err)
		}
		userDataGeneration = spec.Generation
	}
	if err := ValidateMatchingTaskMigrationModeSchema(
		context.TODO(),
		f.session,
		f.cfg.Keyspace,
		f.cfg.MatchingTaskMigrationMode,
		enableFairness,
	); err != nil {
		return nil, fmt.Errorf("validate Cassandra matching task migration schema: %w", err)
	}
	taskMode := normalizeMatchingTaskMigrationMode(f.cfg.MatchingTaskMigrationMode)
	layoutName := SchemaLayoutMatchingTasks
	tableName := matchingTaskV3TableName
	if enableFairness {
		layoutName = SchemaLayoutMatchingTasksFair
		tableName = matchingTaskV3FairTableName
	}
	bucketCount := f.cfg.MatchingTaskStorageBucketCount
	if bucketCount == 0 {
		bucketCount = DefaultMatchingTaskStorageBucketCount
	}
	if taskMode == MatchingTaskMigrationModeTargetOnly || taskMode == MatchingTaskMigrationModeTargetDual {
		var err error
		if taskMode == MatchingTaskMigrationModeTargetOnly {
			err = RequireSchemaLayoutTargetOnly(
				context.TODO(), f.session, f.cfg.Keyspace, layoutName, tableName, int64(bucketCount),
			)
		} else {
			err = RequireSchemaLayoutTargetReady(
				context.TODO(), f.session, f.cfg.Keyspace, layoutName, tableName, int64(bucketCount),
			)
		}
		if err != nil {
			return nil, err
		}
	} else {
		if err := RequireSchemaLayoutSourceCompatible(
			context.TODO(), f.session, f.cfg.Keyspace, layoutName, tableName, int64(bucketCount),
		); err != nil {
			return nil, err
		}
	}
	return NewMatchingTaskStoreWithMigrations(
		f.session,
		f.logger,
		enableFairness,
		mode,
		userDataBucketCount,
		f.cfg.MatchingTaskMigrationMode,
		f.cfg.MatchingTaskStorageBucketCount,
		userDataGeneration,
	)
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	layout, err := newExecutionLayout(f.cfg.ExecutionMigrationMode, f.cfg.ExecutionStorageBuckets)
	if err != nil {
		return nil, err
	}
	if err := validateExecutionLayoutSchema(context.TODO(), f.session, f.cfg.Keyspace, layout); err != nil {
		return nil, err
	}
	if err := f.validateExecutionLayoutAuthority(layout); err != nil {
		return nil, err
	}
	return newShardStore(f.clusterName, f.session, f.logger, layout), nil
}

func (f *Factory) validateExecutionLayoutAuthority(layout executionLayout) error {
	switch layout.mode {
	case config.CassandraExecutionMigrationModeTargetOnly:
		return RequireSchemaLayoutTargetOnly(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutExecutions,
			executionsV2TableName, int64(layout.buckets),
		)
	case config.CassandraExecutionMigrationModeTargetDual:
		return RequireSchemaLayoutTargetReady(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutExecutions,
			executionsV2TableName, int64(layout.buckets),
		)
	default:
		return RequireSchemaLayoutSourceCompatible(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutExecutions,
			executionsV2TableName, int64(layout.buckets),
		)
	}
}

// NewMetadataStore returns a metadata store
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	return NewMetadataStore(f.clusterName, f.session, f.logger)
}

// NewClusterMetadataStore returns a metadata store
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return NewClusterMetadataStore(f.session, f.logger)
}

// NewExecutionStore returns a new ExecutionStore.
func (f *Factory) NewExecutionStore() (p.ExecutionStore, error) {
	layout, err := newExecutionLayout(f.cfg.ExecutionMigrationMode, f.cfg.ExecutionStorageBuckets)
	if err != nil {
		return nil, err
	}
	mode := normalizeHistoryNodeMigrationMode(f.cfg.HistoryNodeMigrationMode)
	if err := ValidateHistoryNodeMigrationMode(mode); err != nil {
		return nil, err
	}
	if err := validateExecutionLayoutSchema(context.TODO(), f.session, f.cfg.Keyspace, layout); err != nil {
		return nil, err
	}
	if err := f.validateExecutionLayoutAuthority(layout); err != nil {
		return nil, err
	}
	if err := ValidateHistoryNodeMigrationModeSchema(
		context.TODO(),
		f.session,
		f.cfg.Keyspace,
		mode,
	); err != nil {
		return nil, fmt.Errorf("validate Cassandra history node migration schema: %w", err)
	}
	switch {
	case mode == config.CassandraHistoryNodeMigrationModeV2Only:
		if err := RequireSchemaLayoutTargetOnly(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutHistoryNode,
			historyNodeV2TableName, 0,
		); err != nil {
			return nil, err
		}
	case historyNodeUsesCanonicalTarget(mode):
		if err := RequireSchemaLayoutTargetReady(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutHistoryNode,
			historyNodeV2TableName, 0,
		); err != nil {
			return nil, err
		}
	default:
		if err := RequireSchemaLayoutSourceCompatible(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutHistoryNode,
			historyNodeV2TableName, 0,
		); err != nil {
			return nil, err
		}
	}
	treeMode := normalizeHistoryTreeMigrationMode(f.cfg.HistoryTreeMigrationMode)
	if err := ValidateHistoryTreeMigrationModeSchema(
		context.TODO(),
		f.session,
		f.cfg.Keyspace,
		treeMode,
	); err != nil {
		return nil, fmt.Errorf("validate Cassandra history tree migration schema: %w", err)
	}
	switch treeMode {
	case config.CassandraHistoryTreeMigrationModeTargetOnly:
		if err := RequireSchemaLayoutTargetOnly(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutHistoryTree,
			historyTreeV2TableName, int64(historyTreeV2BucketCount),
		); err != nil {
			return nil, err
		}
	case config.CassandraHistoryTreeMigrationModeTargetDual:
		if err := RequireSchemaLayoutTargetReady(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutHistoryTree,
			historyTreeV2TableName, int64(historyTreeV2BucketCount),
		); err != nil {
			return nil, err
		}
	default:
		if err := RequireSchemaLayoutSourceCompatible(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutHistoryTree,
			historyTreeV2TableName, int64(historyTreeV2BucketCount),
		); err != nil {
			return nil, err
		}
	}
	generations, err := getHistoryNodeTableGenerations(
		context.TODO(),
		f.session,
		f.cfg.Keyspace,
	)
	if err != nil {
		return nil, fmt.Errorf("read Cassandra history node table generations: %w", err)
	}
	return newExecutionStore(
		f.session,
		f.serializer,
		f.logger,
		mode,
		treeMode,
		generations,
		layout,
	), nil
}

// NewQueue returns a new queue backed by cassandra
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	if err := ValidateLegacyQueueMigrationModeSchema(
		context.TODO(),
		f.session,
		f.cfg.Keyspace,
		f.cfg.LegacyQueueMigrationMode,
	); err != nil {
		return nil, fmt.Errorf("validate Cassandra legacy queue migration schema: %w", err)
	}
	bucketSize := f.cfg.LegacyQueueMessageBucketSize
	if bucketSize == 0 {
		bucketSize = DefaultLegacyQueueV2MessageBucketSize
	}
	legacyQueueMode := normalizeLegacyQueueMigrationMode(f.cfg.LegacyQueueMigrationMode)
	switch legacyQueueMode {
	case config.CassandraLegacyQueueMigrationModeTargetOnly:
		if err := RequireSchemaLayoutTargetOnly(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutLegacyQueue,
			"legacy_queue_v2_messages", bucketSize,
		); err != nil {
			return nil, err
		}
	case config.CassandraLegacyQueueMigrationModeTargetDual:
		if err := RequireSchemaLayoutTargetReady(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutLegacyQueue,
			"legacy_queue_v2_messages", bucketSize,
		); err != nil {
			return nil, err
		}
	default:
		if err := RequireSchemaLayoutSourceCompatible(
			context.TODO(), f.session, f.cfg.Keyspace, SchemaLayoutLegacyQueue,
			"legacy_queue_v2_messages", bucketSize,
		); err != nil {
			return nil, err
		}
	}
	return NewQueueStoreWithMigrationMode(
		queueType,
		f.session,
		f.logger,
		f.cfg.LegacyQueueMigrationMode,
		bucketSize,
	)
}

// NewQueueV2 returns a new data-access object for queues and messages stored in Cassandra. It will never return an
// error.
func (f *Factory) NewQueueV2() (p.QueueV2, error) {
	if err := ValidateQueueV2MigrationModeSchema(
		context.TODO(),
		f.session,
		f.cfg.Keyspace,
		f.cfg.QueueV2MigrationMode,
	); err != nil {
		return nil, fmt.Errorf("validate Cassandra QueueV2 migration schema: %w", err)
	}
	span := f.cfg.QueueV2MessageBucketSpan
	if span == 0 {
		span = DefaultQueueV2MessageBucketSpan
	}
	queueV2Mode := normalizeQueueV2MigrationMode(f.cfg.QueueV2MigrationMode)
	var queueV2Generation gocql.UUID
	if queueV2Mode == config.CassandraQueueV2MigrationModeSourceOnly {
		for _, layout := range []struct {
			name      SchemaLayoutName
			table     string
			parameter int64
		}{
			{name: SchemaLayoutQueueV2Metadata, table: "queues_v2", parameter: int64(queueV2MetadataBucketCount)},
			{name: SchemaLayoutQueueV2Messages, table: "queue_messages_v3", parameter: span},
		} {
			if err := RequireSchemaLayoutSourceCompatible(
				context.TODO(), f.session, f.cfg.Keyspace, layout.name, layout.table, layout.parameter,
			); err != nil {
				return nil, err
			}
		}
	} else {
		metadataSpec, err := SchemaLayoutSpecForTable(
			context.TODO(),
			f.session,
			f.cfg.Keyspace,
			SchemaLayoutQueueV2Metadata,
			"queues_v2",
			int64(queueV2MetadataBucketCount),
		)
		if err != nil {
			return nil, err
		}
		messageSpec, err := SchemaLayoutSpecForTable(
			context.TODO(),
			f.session,
			f.cfg.Keyspace,
			SchemaLayoutQueueV2Messages,
			"queue_messages_v3",
			span,
		)
		if err != nil {
			return nil, err
		}
		metadataStore := NewSchemaLayoutMetadataStore(f.session)
		for _, spec := range []SchemaLayoutSpec{metadataSpec, messageSpec} {
			switch queueV2Mode {
			case config.CassandraQueueV2MigrationModeTargetOnly:
				_, err = metadataStore.RequireTargetOnly(context.TODO(), spec)
			case config.CassandraQueueV2MigrationModeTargetDual:
				_, err = metadataStore.RequireTargetReady(context.TODO(), spec)
			default:
				var metadata SchemaLayoutMetadata
				metadata, err = metadataStore.LoadAndValidate(context.TODO(), spec)
				if err == nil && (metadata.AuthorityState == SchemaLayoutAuthorityTargetOnly ||
					metadata.AuthorityState == SchemaLayoutAuthorityRetired) {
					err = &SchemaLayoutAuthorityError{
						Name:     spec.Name,
						Expected: []SchemaLayoutAuthorityState{SchemaLayoutAuthorityPreparing, SchemaLayoutAuthorityTargetReady},
						Actual:   metadata.AuthorityState,
					}
				}
			}
			if err != nil {
				return nil, fmt.Errorf("validate Cassandra QueueV2 layout authority: %w", err)
			}
		}
		queueV2Generation = messageSpec.Generation
	}
	return newQueueV2StoreWithMigrationIdentity(
		f.session,
		f.logger,
		f.cfg.QueueV2MigrationMode,
		span,
		queueV2Generation,
		queueV2Mode == config.CassandraQueueV2MigrationModeTargetDual ||
			queueV2Mode == config.CassandraQueueV2MigrationModeTargetOnly,
	)
}

// NewNexusEndpointStore returns a new NexusEndpointStore
func (f *Factory) NewNexusEndpointStore() (p.NexusEndpointStore, error) {
	return NewNexusEndpointStore(f.session, f.logger), nil
}

// Close closes the factory
func (f *Factory) Close() {
	f.Lock()
	defer f.Unlock()
	f.session.Close()
}
