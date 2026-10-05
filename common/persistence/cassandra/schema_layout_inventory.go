package cassandra

import (
	"context"
	"errors"
	"fmt"
	"math"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	templateScanExecutionShardInventory          = `SELECT DISTINCT shard_id FROM executions`
	templateScanExecutionTargetShardInventory    = `SELECT DISTINCT shard_id FROM executions_v2`
	templateScanHistoryTreeInventory             = `SELECT DISTINCT tree_id FROM history_tree`
	templateScanMatchingQueueInventory           = `SELECT DISTINCT namespace_id, task_queue_name, task_queue_type FROM %s`
	templateScanMatchingTargetQueueInventory     = `SELECT DISTINCT namespace_id, task_queue_name, task_queue_type, storage_bucket FROM %s`
	templateScanUserDataInventory                = `SELECT DISTINCT namespace_id FROM task_queue_user_data`
	templateScanUserDataTargetInventory          = `SELECT DISTINCT namespace_id, bucket_id FROM task_queue_user_data_v2`
	templateScanUserDataTargetTxnInventory       = `SELECT DISTINCT namespace_id FROM task_queue_user_data_v2_txn`
	templateScanLegacyQueueInventory             = `SELECT DISTINCT queue_type FROM queue`
	templateScanLegacyQueueTargetStateInventory  = `SELECT DISTINCT queue_type FROM legacy_queue_v2_state`
	templateScanLegacyQueueTargetBucketInventory = `SELECT DISTINCT queue_type, bucket_id FROM legacy_queue_v2_messages`
	templateScanLegacyQueueTargetDeleteInventory = `SELECT DISTINCT queue_type FROM legacy_queue_v2_delete_ranges`
	templateScanQueueV2Inventory                 = `SELECT queue_type, queue_name FROM queues`
	templateScanQueueV2TargetInventory           = `SELECT queue_type, metadata_bucket, queue_name FROM queues_v2`
	templateScanQueueV2MessageBucketInventory    = `SELECT DISTINCT queue_type, queue_name, message_bucket ` +
		`FROM queue_messages_v3 WHERE queue_type = ? AND queue_name = ? ALLOW FILTERING`
)

// SchemaLayoutInventoryAuditResult summarizes a bounded-memory source inventory audit.
type SchemaLayoutInventoryAuditResult struct {
	SourceEntities int64
	TargetEntities int64
}

// SchemaLayoutInventoryAuditOptions bounds scans and supplies fixed inventory cardinality where available.
type SchemaLayoutInventoryAuditOptions struct {
	PageSize                int
	ExpectedExecutionShards int
	QueueV2MetadataSpec     SchemaLayoutSpec
	QueueV2MessagesSpec     SchemaLayoutSpec
}

// AuditSchemaLayoutTargetOnlyInventory verifies that every source entity has published target authority.
func AuditSchemaLayoutTargetOnlyInventory(
	ctx context.Context,
	session gocql.Session,
	spec SchemaLayoutSpec,
	options SchemaLayoutInventoryAuditOptions,
) (SchemaLayoutInventoryAuditResult, error) {
	if err := validateSchemaLayoutSpec(spec); err != nil {
		return SchemaLayoutInventoryAuditResult{}, err
	}
	if options.PageSize <= 0 {
		return SchemaLayoutInventoryAuditResult{}, fmt.Errorf("schema layout inventory page size must be positive: %d", options.PageSize)
	}

	switch spec.Name {
	case SchemaLayoutExecutions:
		return auditExecutionTargetOnlyInventory(
			ctx,
			session,
			spec.ImmutableParameter,
			options.PageSize,
			options.ExpectedExecutionShards,
		)
	case SchemaLayoutHistoryNode:
		return auditHistoryNodeTargetOnlyInventory(ctx, session, options.PageSize)
	case SchemaLayoutHistoryTree:
		return auditHistoryTreeTargetOnlyInventory(ctx, session, options.PageSize)
	case SchemaLayoutQueueV2Metadata, SchemaLayoutQueueV2Messages:
		return auditQueueV2TargetOnlyInventory(ctx, session, spec, options)
	case SchemaLayoutMatchingTasks:
		return auditMatchingTaskTargetOnlyInventory(ctx, session, false, spec.ImmutableParameter, options.PageSize)
	case SchemaLayoutMatchingTasksFair:
		return auditMatchingTaskTargetOnlyInventory(ctx, session, true, spec.ImmutableParameter, options.PageSize)
	case SchemaLayoutTaskQueueUserData:
		return auditTaskQueueUserDataTargetOnlyInventory(ctx, session, spec, options.PageSize)
	case SchemaLayoutLegacyQueue:
		return auditLegacyQueueTargetOnlyInventory(ctx, session, spec.ImmutableParameter, options.PageSize)
	default:
		return SchemaLayoutInventoryAuditResult{}, fmt.Errorf(
			"schema layout %q has no per-entity target-only inventory audit",
			spec.Name,
		)
	}
}

func auditHistoryNodeTargetOnlyInventory(
	ctx context.Context,
	session gocql.Session,
	pageSize int,
) (SchemaLayoutInventoryAuditResult, error) {
	validation, err := ValidateHistoryNodeV2(ctx, session, pageSize, 1)
	result := SchemaLayoutInventoryAuditResult{
		SourceEntities: validation.LegacyRows,
		TargetEntities: validation.V2Rows,
	}
	if err != nil {
		return result, err
	}
	if !validation.Matches() {
		return result, fmt.Errorf(
			"history node target-only inventory differs: legacy=%d target=%d missing-target=%d mismatched-target=%d missing-source=%d mismatched-source=%d",
			validation.LegacyRows,
			validation.V2Rows,
			validation.MissingInV2,
			validation.MismatchedInV2,
			validation.MissingInLegacy,
			validation.MismatchedLegacy,
		)
	}
	return result, nil
}

func auditHistoryTreeTargetOnlyInventory(
	ctx context.Context,
	session gocql.Session,
	pageSize int,
) (SchemaLayoutInventoryAuditResult, error) {
	result := SchemaLayoutInventoryAuditResult{}
	iter := session.Query(templateScanHistoryTreeInventory).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var treeID string
		if !iter.Scan(&treeID) {
			break
		}
		result.SourceEntities++
		authority, err := readHistoryTreeAuthority(ctx, session, treeID)
		if err != nil {
			_ = iter.Close()
			return result, fmt.Errorf("read history tree %s source authority: %w", treeID, err)
		}
		if authority.authority != historyTreeMigrationAuthorityTarget || authority.timestamp <= 0 {
			_ = iter.Close()
			return result, fmt.Errorf(
				"history tree %s is not target-authoritative: authority=%s timestamp=%d",
				treeID,
				authority.authority,
				authority.timestamp,
			)
		}
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanHistoryTreeTargetOnlyInventory", err)
	}

	validation, err := ValidateHistoryTreeV2(ctx, session, pageSize)
	if err != nil {
		return result, err
	}
	if !validation.Matches() {
		return result, fmt.Errorf(
			"history tree target-only inventory differs: legacy=%d target=%d missing-target=%d mismatched-target=%d missing-source=%d mismatched-source=%d",
			validation.LegacyRows,
			validation.V2Rows,
			validation.MissingInV2,
			validation.MismatchedInV2,
			validation.MissingInLegacy,
			validation.MismatchedLegacy,
		)
	}
	result.TargetEntities = result.SourceEntities
	return result, nil
}

//nolint:revive // The audit keeps pair-state and exhaustive source/target checks in one fail-closed operation.
func auditQueueV2TargetOnlyInventory(
	ctx context.Context,
	session gocql.Session,
	requestedSpec SchemaLayoutSpec,
	options SchemaLayoutInventoryAuditOptions,
) (SchemaLayoutInventoryAuditResult, error) {
	metadataSpec := options.QueueV2MetadataSpec
	messagesSpec := options.QueueV2MessagesSpec
	if err := validateQueueV2InventorySpecs(requestedSpec, metadataSpec, messagesSpec); err != nil {
		return SchemaLayoutInventoryAuditResult{}, err
	}
	metadataStore := NewSchemaLayoutMetadataStore(session)
	globalAuthorities := make([]SchemaLayoutMetadata, 0, 2)
	for _, spec := range []SchemaLayoutSpec{metadataSpec, messagesSpec} {
		metadata, err := metadataStore.LoadAndValidate(ctx, spec)
		if err != nil {
			return SchemaLayoutInventoryAuditResult{}, err
		}
		if metadata.AuthorityState != SchemaLayoutAuthorityTargetReady &&
			metadata.AuthorityState != SchemaLayoutAuthorityTargetOnly {
			return SchemaLayoutInventoryAuditResult{}, &SchemaLayoutAuthorityError{
				Name: spec.Name,
				Expected: []SchemaLayoutAuthorityState{
					SchemaLayoutAuthorityTargetReady,
					SchemaLayoutAuthorityTargetOnly,
				},
				Actual: metadata.AuthorityState,
			}
		}
		globalAuthorities = append(globalAuthorities, metadata)
	}
	metadataAuthority := globalAuthorities[0]
	messagesAuthority := globalAuthorities[1]
	if metadataAuthority.AuthorityState == SchemaLayoutAuthorityTargetOnly &&
		messagesAuthority.AuthorityState != SchemaLayoutAuthorityTargetOnly {
		return SchemaLayoutInventoryAuditResult{}, errors.New(
			"QueueV2 global layout authority is inconsistent: metadata is target-only before messages",
		)
	}
	if metadataAuthority.AuthorityState == messagesAuthority.AuthorityState {
		if metadataAuthority.Epoch != messagesAuthority.Epoch {
			return SchemaLayoutInventoryAuditResult{}, fmt.Errorf(
				"QueueV2 global layout epochs differ in state %q: metadata=%d messages=%d",
				metadataAuthority.AuthorityState,
				metadataAuthority.Epoch,
				messagesAuthority.Epoch,
			)
		}
	} else if messagesAuthority.AuthorityState != SchemaLayoutAuthorityTargetOnly ||
		messagesAuthority.Epoch != metadataAuthority.Epoch+1 {
		return SchemaLayoutInventoryAuditResult{}, fmt.Errorf(
			"QueueV2 global layout transition is inconsistent: metadata=%s/%d messages=%s/%d",
			metadataAuthority.AuthorityState,
			metadataAuthority.Epoch,
			messagesAuthority.AuthorityState,
			messagesAuthority.Epoch,
		)
	}

	if err := validateQueueV2MessageBucketSpan(messagesSpec.ImmutableParameter); err != nil {
		return SchemaLayoutInventoryAuditResult{}, err
	}
	queueStore := &queueV2Store{
		session:          session,
		migrationMode:    config.CassandraQueueV2MigrationModeTargetOnly,
		messageSpan:      messagesSpec.ImmutableParameter,
		layoutGeneration: messagesSpec.Generation,
	}
	result := SchemaLayoutInventoryAuditResult{}
	iter := session.Query(templateScanQueueV2Inventory).WithContext(ctx).PageSize(options.PageSize).Iter()
	for {
		var queueTypeValue int
		var queueName string
		if !iter.Scan(&queueTypeValue, &queueName) {
			break
		}
		queueType := persistence.QueueV2Type(queueTypeValue)
		result.SourceEntities++
		if err := auditQueueV2TargetOnlyQueue(ctx, queueStore, queueType, queueName, options.PageSize); err != nil {
			_ = iter.Close()
			return result, err
		}
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanQueueV2TargetOnlyInventory", err)
	}
	targetIter := session.Query(templateScanQueueV2TargetInventory).
		WithContext(ctx).
		PageSize(options.PageSize).
		Iter()
	for {
		var queueTypeValue int
		var metadataBucket int
		var queueName string
		if !targetIter.Scan(&queueTypeValue, &metadataBucket, &queueName) {
			break
		}
		queueType := persistence.QueueV2Type(queueTypeValue)
		expectedBucket := queueV2MetadataBucket(queueType, queueName)
		if metadataBucket != expectedBucket {
			_ = targetIter.Close()
			return result, fmt.Errorf(
				"QueueV2 target metadata type=%d name=%q is in bucket %d; expected %d",
				queueType,
				queueName,
				metadataBucket,
				expectedBucket,
			)
		}
		result.TargetEntities++
	}
	if err := targetIter.Close(); err != nil {
		return result, gocql.ConvertError("ScanQueueV2TargetMetadataInventory", err)
	}
	if result.SourceEntities != result.TargetEntities {
		return result, fmt.Errorf(
			"QueueV2 metadata inventory cardinality differs: source=%d target=%d",
			result.SourceEntities,
			result.TargetEntities,
		)
	}
	return result, nil
}

func validateQueueV2InventorySpecs(
	requestedSpec SchemaLayoutSpec,
	metadataSpec SchemaLayoutSpec,
	messagesSpec SchemaLayoutSpec,
) error {
	if err := validateSchemaLayoutSpec(metadataSpec); err != nil {
		return fmt.Errorf("validate QueueV2 metadata layout identity: %w", err)
	}
	if err := validateSchemaLayoutSpec(messagesSpec); err != nil {
		return fmt.Errorf("validate QueueV2 message layout identity: %w", err)
	}
	if metadataSpec.Name != SchemaLayoutQueueV2Metadata ||
		metadataSpec.ImmutableParameter != queueV2MetadataBucketCount {
		return fmt.Errorf(
			"QueueV2 metadata inventory requires layout %q with %d buckets",
			SchemaLayoutQueueV2Metadata,
			queueV2MetadataBucketCount,
		)
	}
	if messagesSpec.Name != SchemaLayoutQueueV2Messages {
		return fmt.Errorf("QueueV2 message inventory requires layout %q", SchemaLayoutQueueV2Messages)
	}
	if err := validateQueueV2MessageBucketSpan(messagesSpec.ImmutableParameter); err != nil {
		return err
	}
	expectedRequested := metadataSpec
	if requestedSpec.Name == SchemaLayoutQueueV2Messages {
		expectedRequested = messagesSpec
	}
	if requestedSpec != expectedRequested {
		return fmt.Errorf(
			"QueueV2 requested layout identity does not match paired inventory identity: requested=%+v expected=%+v",
			requestedSpec,
			expectedRequested,
		)
	}
	return nil
}

func auditQueueV2TargetOnlyQueue(
	ctx context.Context,
	store *queueV2Store,
	queueType persistence.QueueV2Type,
	queueName string,
	pageSize int,
) error {
	sourceMetadata, err := readQueueV2MetadataAuthority(
		ctx,
		store.session,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
	)
	if err != nil {
		return fmt.Errorf("read QueueV2 source metadata authority for type=%d name=%q: %w", queueType, queueName, err)
	}
	sourceMessages, err := readQueueV2MessageAuthority(
		ctx,
		store.session,
		queueType,
		queueName,
		queueV2MetadataLayoutSource,
		0,
	)
	if err != nil {
		return fmt.Errorf("read QueueV2 source message authority for type=%d name=%q: %w", queueType, queueName, err)
	}
	if err := validateQueueV2InventoryAuthority(store, queueType, queueName, sourceMetadata); err != nil {
		return fmt.Errorf("QueueV2 source metadata inventory is incomplete: %w", err)
	}
	if err := validateQueueV2AuthorityPair(queueType, queueName, sourceMetadata, sourceMessages); err != nil {
		return err
	}
	sourceQueue, err := store.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutSource)
	if err != nil {
		return fmt.Errorf("read QueueV2 source metadata for type=%d name=%q: %w", queueType, queueName, err)
	}

	targetMetadata, err := readQueueV2MetadataAuthority(
		ctx,
		store.session,
		queueType,
		queueName,
		queueV2MetadataLayoutTarget,
	)
	if err != nil {
		return fmt.Errorf("read QueueV2 target metadata authority for type=%d name=%q: %w", queueType, queueName, err)
	}
	if err := validateQueueV2AuthorityPair(queueType, queueName, sourceMetadata, targetMetadata); err != nil {
		return err
	}
	if err := auditQueueV2TargetMetadataPlacement(ctx, store, queueType, queueName, pageSize); err != nil {
		return err
	}
	targetQueue, err := store.getQueueFromLayout(ctx, queueName, queueType, queueV2MetadataLayoutTarget)
	if err != nil {
		return fmt.Errorf("read QueueV2 target metadata for type=%d name=%q: %w", queueType, queueName, err)
	}
	if !queueV2MetadataEqual(sourceQueue, targetQueue) {
		return fmt.Errorf("QueueV2 metadata differs for type=%d name=%q", queueType, queueName)
	}
	minimumMessageID, err := queueV2MinimumMessageID(queueType, queueName, targetQueue)
	if err != nil {
		return err
	}
	minimumBucket, err := queueV2MessageBucketForID(minimumMessageID, store.messageBucketSpan())
	if err != nil {
		return err
	}
	directory, err := store.getQueueV2MessageDirectory(ctx, queueType, queueName)
	if err != nil {
		return fmt.Errorf("read QueueV2 target message directory for type=%d name=%q: %w", queueType, queueName, err)
	}
	if directory.activeBucket < 0 {
		return fmt.Errorf(
			"QueueV2 target message directory for type=%d name=%q has invalid active bucket %d",
			queueType,
			queueName,
			directory.activeBucket,
		)
	}
	if err := auditQueueV2TargetMessagePartitions(
		ctx,
		store,
		queueType,
		queueName,
		targetMetadata,
		minimumBucket,
		directory.activeBucket,
		pageSize,
	); err != nil {
		return err
	}
	validation, err := ValidateQueueV2Messages(
		ctx,
		store.session,
		queueType,
		queueName,
		pageSize,
		store.messageBucketSpan(),
	)
	if err != nil {
		return fmt.Errorf("validate QueueV2 messages for type=%d name=%q: %w", queueType, queueName, err)
	}
	if !validation.Matches() {
		return fmt.Errorf(
			"QueueV2 messages differ for type=%d name=%q: source=%d target=%d mismatches=%v",
			queueType,
			queueName,
			validation.SourceRows,
			validation.TargetRows,
			validation.Mismatches,
		)
	}
	return nil
}

func auditQueueV2TargetMetadataPlacement(
	ctx context.Context,
	store *queueV2Store,
	queueType persistence.QueueV2Type,
	queueName string,
	pageSize int,
) error {
	expectedBucket := queueV2MetadataBucket(queueType, queueName)
	iter := store.session.Query(
		templateScanQueueV2TargetMetadataBuckets,
		queueType,
		queueName,
	).WithContext(ctx).PageSize(pageSize).Iter()
	rows := 0
	for {
		var bucket int
		if !iter.Scan(&bucket) {
			break
		}
		rows++
		if bucket != expectedBucket {
			_ = iter.Close()
			return fmt.Errorf(
				"QueueV2 target metadata type=%d name=%q is in bucket %d; expected %d",
				queueType,
				queueName,
				bucket,
				expectedBucket,
			)
		}
	}
	if err := iter.Close(); err != nil {
		return gocql.ConvertError("ScanQueueV2TargetMetadataPlacement", err)
	}
	if rows != 1 {
		return fmt.Errorf(
			"QueueV2 target metadata type=%d name=%q has %d rows; expected exactly one",
			queueType,
			queueName,
			rows,
		)
	}
	return nil
}

//nolint:revive // Every physical partition must be checked without retaining an unbounded bucket set.
func auditQueueV2TargetMessagePartitions(
	ctx context.Context,
	store *queueV2Store,
	queueType persistence.QueueV2Type,
	queueName string,
	expectedAuthority queueV2AuthorityRecord,
	minimumBucket int64,
	activeBucket int64,
	pageSize int,
) error {
	if minimumBucket < 0 || activeBucket < minimumBucket {
		return fmt.Errorf(
			"QueueV2 target message bucket range is invalid for type=%d name=%q: minimum=%d active=%d",
			queueType,
			queueName,
			minimumBucket,
			activeBucket,
		)
	}
	iter := store.session.Query(
		templateScanQueueV2MessageBucketInventory,
		queueType,
		queueName,
	).WithContext(ctx).PageSize(pageSize).Iter()
	directoryFound := false
	requiredBucketCount := uint64(activeBucket) - uint64(minimumBucket) + 1
	var foundRequiredBuckets uint64
	minimumFoundBucket := int64(math.MaxInt64)
	maximumFoundBucket := int64(-1)
	for {
		var (
			actualQueueType int
			actualQueueName string
			bucket          int64
		)
		if !iter.Scan(&actualQueueType, &actualQueueName, &bucket) {
			break
		}
		if actualQueueType != int(queueType) || actualQueueName != queueName {
			_ = iter.Close()
			return fmt.Errorf(
				"QueueV2 target message inventory returned unexpected partition type=%d name=%q",
				actualQueueType,
				actualQueueName,
			)
		}
		authority, err := readQueueV2MessageAuthority(
			ctx,
			store.session,
			queueType,
			queueName,
			queueV2MetadataLayoutTarget,
			bucket,
		)
		if err != nil {
			_ = iter.Close()
			return fmt.Errorf(
				"read QueueV2 target message authority for type=%d name=%q bucket=%d: %w",
				queueType,
				queueName,
				bucket,
				err,
			)
		}
		if err := validateQueueV2AuthorityPair(queueType, queueName, expectedAuthority, authority); err != nil {
			_ = iter.Close()
			return fmt.Errorf("QueueV2 target message bucket %d inventory is incomplete: %w", bucket, err)
		}
		if bucket == queueV2MessageDirectoryBucket {
			directory, err := store.getQueueV2MessageDirectory(ctx, queueType, queueName)
			if err != nil {
				_ = iter.Close()
				return fmt.Errorf(
					"read QueueV2 target message directory state for type=%d name=%q: %w",
					queueType,
					queueName,
					err,
				)
			}
			if directory.activeBucket != activeBucket {
				_ = iter.Close()
				return fmt.Errorf(
					"QueueV2 target message directory changed active bucket during inventory: before=%d after=%d",
					activeBucket,
					directory.activeBucket,
				)
			}
			directoryFound = true
			continue
		}
		if bucket < 0 {
			_ = iter.Close()
			return fmt.Errorf(
				"QueueV2 target message inventory contains invalid bucket %d for type=%d name=%q",
				bucket,
				queueType,
				queueName,
			)
		}
		if bucket > activeBucket {
			_ = iter.Close()
			return fmt.Errorf(
				"QueueV2 target message inventory contains future bucket %d beyond active bucket %d for type=%d name=%q",
				bucket,
				activeBucket,
				queueType,
				queueName,
			)
		}
		state, err := store.getQueueV2MessageBucketState(ctx, queueType, queueName, bucket)
		if err != nil {
			_ = iter.Close()
			return fmt.Errorf(
				"read QueueV2 target message bucket state for type=%d name=%q bucket=%d: %w",
				queueType,
				queueName,
				bucket,
				err,
			)
		}
		firstMessageID, err := queueV2MessageBucketFirstID(bucket, store.messageBucketSpan())
		if err != nil {
			_ = iter.Close()
			return err
		}
		lastMessageID, err := queueV2MessageBucketLastID(bucket, store.messageBucketSpan())
		if err != nil {
			_ = iter.Close()
			return err
		}
		if state.lastMessageID < firstMessageID-1 || state.lastMessageID > lastMessageID {
			_ = iter.Close()
			return fmt.Errorf(
				"QueueV2 target message bucket %d has invalid tail %d; expected [%d,%d]",
				bucket,
				state.lastMessageID,
				firstMessageID-1,
				lastMessageID,
			)
		}
		if bucket != activeBucket && state.lastMessageID != lastMessageID {
			_ = iter.Close()
			return fmt.Errorf(
				"QueueV2 inactive target message bucket %d has tail %d; expected %d",
				bucket,
				state.lastMessageID,
				lastMessageID,
			)
		}
		if bucket >= minimumBucket && bucket <= activeBucket {
			foundRequiredBuckets++
			minimumFoundBucket = min(minimumFoundBucket, bucket)
			maximumFoundBucket = max(maximumFoundBucket, bucket)
		}
	}
	if err := iter.Close(); err != nil {
		return gocql.ConvertError("ScanQueueV2TargetMessageBucketInventory", err)
	}
	if !directoryFound {
		return fmt.Errorf("QueueV2 target message directory partition is missing for type=%d name=%q", queueType, queueName)
	}
	if foundRequiredBuckets != requiredBucketCount ||
		minimumFoundBucket != minimumBucket || maximumFoundBucket != activeBucket {
		return fmt.Errorf(
			"QueueV2 required target message bucket inventory is incomplete for type=%d name=%q: "+
				"range=[%d,%d] found=%d expected=%d min-found=%d max-found=%d",
			queueType,
			queueName,
			minimumBucket,
			activeBucket,
			foundRequiredBuckets,
			requiredBucketCount,
			minimumFoundBucket,
			maximumFoundBucket,
		)
	}
	return nil
}

func validateQueueV2InventoryAuthority(
	store *queueV2Store,
	queueType persistence.QueueV2Type,
	queueName string,
	record queueV2AuthorityRecord,
) error {
	if err := store.validateQueueV2Authority(
		queueType,
		queueName,
		record,
		queueV2MigrationAuthorityTarget,
	); err != nil {
		return err
	}
	if record.epoch <= 0 {
		return fmt.Errorf("QueueV2 queue type %d and name %q has invalid epoch %d", queueType, queueName, record.epoch)
	}
	return nil
}

//nolint:revive // Every shard and physical bucket must be checked before the one-way global cutover.
func auditExecutionTargetOnlyInventory(
	ctx context.Context,
	session gocql.Session,
	immutableParameter int64,
	pageSize int,
	expectedShards int,
) (SchemaLayoutInventoryAuditResult, error) {
	if expectedShards <= 0 {
		return SchemaLayoutInventoryAuditResult{}, fmt.Errorf(
			"execution target-only inventory requires a positive expected shard count: %d",
			expectedShards,
		)
	}
	if expectedShards > math.MaxInt32 {
		return SchemaLayoutInventoryAuditResult{}, fmt.Errorf(
			"execution expected shard count exceeds int32: %d",
			expectedShards,
		)
	}
	if immutableParameter > math.MaxInt {
		return SchemaLayoutInventoryAuditResult{}, fmt.Errorf(
			"execution storage bucket count overflows int: %d",
			immutableParameter,
		)
	}
	targetLayout, err := newExecutionLayout(
		config.CassandraExecutionMigrationModeTargetOnly,
		int(immutableParameter),
	)
	if err != nil {
		return SchemaLayoutInventoryAuditResult{}, err
	}
	sourceLayout := executionLayout{
		mode:    config.CassandraExecutionMigrationModeLegacy,
		buckets: targetLayout.buckets,
	}
	if _, err := targetLayout.partition(int32(expectedShards), targetLayout.buckets-1); err != nil {
		return SchemaLayoutInventoryAuditResult{}, err
	}
	result := SchemaLayoutInventoryAuditResult{}
	iter := session.Query(templateScanExecutionShardInventory).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var shardID int
		if !iter.Scan(&shardID) {
			break
		}
		if shardID < 1 || shardID > expectedShards {
			_ = iter.Close()
			return result, fmt.Errorf(
				"execution source inventory contains shard ID %d outside fixed range [1,%d]",
				shardID,
				expectedShards,
			)
		}
		logicalShardID := int32(shardID)
		result.SourceEntities++
		source, err := readExecutionShardAuthority(ctx, session, sourceLayout, logicalShardID)
		if err != nil {
			_ = iter.Close()
			return result, fmt.Errorf("read execution source shard %d authority: %w", logicalShardID, err)
		}
		if source.authority != executionShardAuthorityTarget || source.bucketCount != targetLayout.buckets {
			_ = iter.Close()
			return result, fmt.Errorf(
				"execution source shard %d is not target-authoritative for %d buckets: authority=%s buckets=%d",
				logicalShardID,
				targetLayout.buckets,
				source.authority,
				source.bucketCount,
			)
		}
		partitions, err := targetLayout.partitions(logicalShardID)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		for _, partition := range partitions {
			target, err := readExecutionShardAuthority(ctx, session, targetLayout, partition)
			if err != nil {
				_ = iter.Close()
				return result, fmt.Errorf(
					"read execution target shard %d partition %d authority: %w",
					logicalShardID,
					partition,
					err,
				)
			}
			if target.authority != executionShardAuthorityTarget ||
				target.bucketCount != targetLayout.buckets ||
				target.rangeID != source.rangeID {
				_ = iter.Close()
				return result, fmt.Errorf(
					"execution target shard %d partition %d is incomplete: source range=%d target range=%d authority=%s buckets=%d",
					logicalShardID,
					partition,
					source.rangeID,
					target.rangeID,
					target.authority,
					target.bucketCount,
				)
			}
		}
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanExecutionShardTargetOnlyInventory", err)
	}
	if result.SourceEntities != int64(expectedShards) {
		return result, fmt.Errorf(
			"execution source inventory is incomplete: contains %d shards in fixed range [1,%d]; expected exactly %d",
			result.SourceEntities,
			expectedShards,
			expectedShards,
		)
	}
	return auditExecutionTargetPartitions(
		ctx,
		session,
		sourceLayout,
		targetLayout,
		expectedShards,
		pageSize,
		result,
	)
}

func auditExecutionTargetPartitions(
	ctx context.Context,
	session gocql.Session,
	sourceLayout executionLayout,
	targetLayout executionLayout,
	expectedShards int,
	pageSize int,
	result SchemaLayoutInventoryAuditResult,
) (SchemaLayoutInventoryAuditResult, error) {
	expectedPartitions := int64(expectedShards) * int64(targetLayout.buckets)
	iter := session.Query(templateScanExecutionTargetShardInventory).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var partitionValue int
		if !iter.Scan(&partitionValue) {
			break
		}
		partition := int64(partitionValue)
		if partition < 1 || partition > expectedPartitions || partition > math.MaxInt32 {
			_ = iter.Close()
			return result, fmt.Errorf(
				"execution target inventory contains orphan partition %d outside fixed range [1,%d]",
				partition,
				expectedPartitions,
			)
		}
		logicalShardID := int32((partition-1)/int64(targetLayout.buckets)) + 1
		source, err := readExecutionShardAuthority(ctx, session, sourceLayout, logicalShardID)
		if err != nil {
			_ = iter.Close()
			return result, fmt.Errorf(
				"execution target partition %d has no source shard %d: %w",
				partition,
				logicalShardID,
				err,
			)
		}
		if source.authority != executionShardAuthorityTarget || source.bucketCount != targetLayout.buckets {
			_ = iter.Close()
			return result, fmt.Errorf(
				"execution target partition %d maps to invalid source shard %d authority=%s buckets=%d",
				partition,
				logicalShardID,
				source.authority,
				source.bucketCount,
			)
		}
		target, err := readExecutionShardAuthority(ctx, session, targetLayout, int32(partition))
		if err != nil {
			_ = iter.Close()
			return result, fmt.Errorf("read execution target partition %d authority: %w", partition, err)
		}
		if target.authority != executionShardAuthorityTarget ||
			target.bucketCount != targetLayout.buckets ||
			target.rangeID != source.rangeID {
			_ = iter.Close()
			return result, fmt.Errorf(
				"execution target partition %d is incomplete for source shard %d: source range=%d target range=%d authority=%s buckets=%d",
				partition,
				logicalShardID,
				source.rangeID,
				target.rangeID,
				target.authority,
				target.bucketCount,
			)
		}
		result.TargetEntities++
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanExecutionTargetShardInventory", err)
	}
	if result.TargetEntities != expectedPartitions {
		return result, fmt.Errorf(
			"execution target inventory contains %d partitions; expected exactly %d",
			result.TargetEntities,
			expectedPartitions,
		)
	}
	return result, nil
}

func auditMatchingTaskTargetOnlyInventory(
	ctx context.Context,
	session gocql.Session,
	fair bool,
	immutableParameter int64,
	pageSize int,
) (SchemaLayoutInventoryAuditResult, error) {
	if immutableParameter > math.MaxInt {
		return SchemaLayoutInventoryAuditResult{}, fmt.Errorf(
			"matching task storage bucket count overflows int: %d",
			immutableParameter,
		)
	}
	bucketCount := int(immutableParameter)
	if err := validateMatchingTaskStorageBucketCount(bucketCount); err != nil {
		return SchemaLayoutInventoryAuditResult{}, err
	}
	version := matchingTaskVersion1
	sourceTable := "tasks"
	if fair {
		version = matchingTaskVersion2
		sourceTable = "tasks_v2"
	}
	sourceStore := &taskQueueStore{Session: session, version: version}
	targetStore := newMatchingTaskStoreV3(session, fair, bucketCount)
	result := SchemaLayoutInventoryAuditResult{}
	iter := session.Query(fmt.Sprintf(templateScanMatchingQueueInventory, sourceTable)).
		WithContext(ctx).
		PageSize(pageSize).
		Iter()
	for {
		var namespaceID string
		var taskQueue string
		var taskTypeValue int
		if !iter.Scan(&namespaceID, &taskQueue, &taskTypeValue) {
			break
		}
		taskType := enumspb.TaskQueueType(taskTypeValue)
		result.SourceEntities++
		source, err := sourceStore.readMigrationAuthority(ctx, namespaceID, taskQueue, taskType)
		if err != nil {
			_ = iter.Close()
			return result, fmt.Errorf(
				"read matching source authority for namespace=%s queue=%q type=%d: %w",
				namespaceID,
				taskQueue,
				taskTypeValue,
				err,
			)
		}
		if err := validateMatchingTaskSourceInventoryAuthority(
			namespaceID,
			taskQueue,
			taskTypeValue,
			bucketCount,
			source,
		); err != nil {
			_ = iter.Close()
			return result, err
		}
		target, err := readMatchingTaskTargetInventoryAuthority(
			ctx,
			targetStore,
			namespaceID,
			taskQueue,
			taskType,
		)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		if target.authority != matchingTaskAuthorityTarget ||
			target.bucketCount != int16(bucketCount) ||
			target.rangeID != source.rangeID {
			_ = iter.Close()
			return result, fmt.Errorf(
				"matching target namespace=%s queue=%q type=%d is incomplete: source range=%d target range=%d authority=%d buckets=%d",
				namespaceID,
				taskQueue,
				taskTypeValue,
				source.rangeID,
				target.rangeID,
				target.authority,
				target.bucketCount,
			)
		}
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanMatchingTaskTargetOnlyInventory", err)
	}
	return auditMatchingTaskTargetPartitions(
		ctx,
		session,
		sourceStore,
		targetStore,
		pageSize,
		result,
	)
}

func validateMatchingTaskSourceInventoryAuthority(
	namespaceID string,
	taskQueue string,
	taskTypeValue int,
	bucketCount int,
	source matchingTaskSourceAuthorityRecord,
) error {
	if source.authority == matchingTaskAuthorityTarget && source.bucketCount == int16(bucketCount) {
		return nil
	}
	return fmt.Errorf(
		"matching source namespace=%s queue=%q type=%d is not target-authoritative for %d buckets: authority=%d buckets=%d",
		namespaceID,
		taskQueue,
		taskTypeValue,
		bucketCount,
		source.authority,
		source.bucketCount,
	)
}

//nolint:revive // Every physical target partition is checked against its source identity and transition metadata.
func auditMatchingTaskTargetPartitions(
	ctx context.Context,
	session gocql.Session,
	sourceStore *taskQueueStore,
	targetStore *matchingTaskStoreV3,
	pageSize int,
	result SchemaLayoutInventoryAuditResult,
) (SchemaLayoutInventoryAuditResult, error) {
	iter := session.Query(fmt.Sprintf(templateScanMatchingTargetQueueInventory, targetStore.table)).
		WithContext(ctx).
		PageSize(pageSize).
		Iter()
	for {
		var namespaceID string
		var taskQueue string
		var taskTypeValue int
		var bucketValue int
		if !iter.Scan(&namespaceID, &taskQueue, &taskTypeValue, &bucketValue) {
			break
		}
		if bucketValue < 0 || bucketValue >= targetStore.bucketCount {
			_ = iter.Close()
			return result, fmt.Errorf(
				"matching target inventory contains invalid bucket %d for namespace=%s queue=%q type=%d; expected [0,%d)",
				bucketValue,
				namespaceID,
				taskQueue,
				taskTypeValue,
				targetStore.bucketCount,
			)
		}
		taskType := enumspb.TaskQueueType(taskTypeValue)
		source, err := sourceStore.readMigrationAuthority(ctx, namespaceID, taskQueue, taskType)
		if err != nil {
			_ = iter.Close()
			return result, fmt.Errorf(
				"matching target partition namespace=%s queue=%q type=%d bucket=%d has no valid source: %w",
				namespaceID,
				taskQueue,
				taskTypeValue,
				bucketValue,
				err,
			)
		}
		if err := validateMatchingTaskSourceInventoryAuthority(
			namespaceID,
			taskQueue,
			taskTypeValue,
			targetStore.bucketCount,
			source,
		); err != nil {
			_ = iter.Close()
			return result, fmt.Errorf("matching target partition has no valid source: %w", err)
		}
		metadata, err := targetStore.getMetadataAtBucket(
			ctx,
			namespaceID,
			taskQueue,
			taskType,
			int16(bucketValue),
		)
		if gocql.IsNotFoundError(err) {
			continue
		}
		if err != nil {
			_ = iter.Close()
			return result, fmt.Errorf(
				"matching target partition namespace=%s queue=%q type=%d bucket=%d has no valid metadata: %w",
				namespaceID,
				taskQueue,
				taskTypeValue,
				bucketValue,
				err,
			)
		}
		active, err := validateMatchingTaskTargetInventoryMetadata(
			namespaceID,
			taskQueue,
			taskTypeValue,
			bucketValue,
			source,
			metadata,
			targetStore.bucketCount,
		)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		if active {
			result.TargetEntities++
		}
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanMatchingTaskTargetPartitionInventory", err)
	}
	if result.TargetEntities != result.SourceEntities {
		return result, fmt.Errorf(
			"matching target inventory contains %d active queues; source contains %d queues",
			result.TargetEntities,
			result.SourceEntities,
		)
	}
	return result, nil
}

func validateMatchingTaskTargetInventoryMetadata(
	namespaceID string,
	taskQueue string,
	taskTypeValue int,
	bucketValue int,
	source matchingTaskSourceAuthorityRecord,
	metadata *matchingTaskV3Metadata,
	bucketCount int,
) (bool, error) {
	if metadata.authority != matchingTaskAuthorityTarget || metadata.bucketCount != int16(bucketCount) {
		return false, fmt.Errorf(
			"matching target metadata namespace=%s queue=%q type=%d bucket=%d has authority=%d buckets=%d; expected authority=%d buckets=%d",
			namespaceID,
			taskQueue,
			taskTypeValue,
			bucketValue,
			metadata.authority,
			metadata.bucketCount,
			matchingTaskAuthorityTarget,
			bucketCount,
		)
	}
	expectedBucket, err := matchingTaskStorageBucket(metadata.rangeID, bucketCount)
	if err != nil || int(expectedBucket) != bucketValue {
		return false, fmt.Errorf(
			"matching target metadata namespace=%s queue=%q type=%d bucket=%d has invalid range ID %d",
			namespaceID,
			taskQueue,
			taskTypeValue,
			bucketValue,
			metadata.rangeID,
		)
	}
	switch metadata.state {
	case matchingTaskMetadataStateActive:
		if metadata.rangeID != source.rangeID {
			return false, fmt.Errorf(
				"matching target active metadata namespace=%s queue=%q type=%d bucket=%d has range ID %d; source has %d",
				namespaceID,
				taskQueue,
				taskTypeValue,
				bucketValue,
				metadata.rangeID,
				source.rangeID,
			)
		}
		return true, nil
	case matchingTaskMetadataStateFenced:
		if !metadata.nextRangeIDValid || metadata.nextRangeID <= metadata.rangeID || metadata.nextRangeID > source.rangeID {
			return false, fmt.Errorf(
				"matching target fenced metadata namespace=%s queue=%q type=%d bucket=%d has invalid transition %d -> %d for source range %d",
				namespaceID,
				taskQueue,
				taskTypeValue,
				bucketValue,
				metadata.rangeID,
				metadata.nextRangeID,
				source.rangeID,
			)
		}
		return false, nil
	default:
		return false, fmt.Errorf(
			"matching target metadata namespace=%s queue=%q type=%d bucket=%d has invalid state %d",
			namespaceID,
			taskQueue,
			taskTypeValue,
			bucketValue,
			metadata.state,
		)
	}
}

func readMatchingTaskTargetInventoryAuthority(
	ctx context.Context,
	store *matchingTaskStoreV3,
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
) (*matchingTaskV3Metadata, error) {
	var active *matchingTaskV3Metadata
	for bucket := range store.bucketCount {
		metadata, err := store.getMetadataAtBucket(ctx, namespaceID, taskQueue, taskType, int16(bucket))
		if gocql.IsNotFoundError(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf(
				"read matching target authority for namespace=%s queue=%q type=%d bucket=%d: %w",
				namespaceID,
				taskQueue,
				taskType,
				bucket,
				err,
			)
		}
		if metadata.bucketCount != int16(store.bucketCount) {
			return nil, fmt.Errorf(
				"matching target namespace=%s queue=%q type=%d bucket=%d uses %d buckets; expected %d",
				namespaceID,
				taskQueue,
				taskType,
				bucket,
				metadata.bucketCount,
				store.bucketCount,
			)
		}
		if metadata.state != matchingTaskMetadataStateActive {
			continue
		}
		if active != nil {
			return nil, fmt.Errorf(
				"matching target namespace=%s queue=%q type=%d has multiple active authority rows",
				namespaceID,
				taskQueue,
				taskType,
			)
		}
		active = metadata
	}
	if active == nil {
		return nil, fmt.Errorf(
			"matching target authority is missing for namespace=%s queue=%q type=%d",
			namespaceID,
			taskQueue,
			taskType,
		)
	}
	return active, nil
}

func auditTaskQueueUserDataTargetOnlyInventory(
	ctx context.Context,
	session gocql.Session,
	spec SchemaLayoutSpec,
	pageSize int,
) (SchemaLayoutInventoryAuditResult, error) {
	if spec.ImmutableParameter > math.MaxInt {
		return SchemaLayoutInventoryAuditResult{}, fmt.Errorf(
			"task queue user data bucket count overflows int: %d",
			spec.ImmutableParameter,
		)
	}
	bucketCount := int(spec.ImmutableParameter)
	if _, err := taskQueueUserDataBucket("", bucketCount); err != nil {
		return SchemaLayoutInventoryAuditResult{}, err
	}
	identity := taskQueueUserDataAuthorityRecord{
		bucketCount: bucketCount,
		generation:  spec.Generation,
	}
	result := SchemaLayoutInventoryAuditResult{}
	iter := session.Query(templateScanUserDataInventory).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var namespaceID string
		if !iter.Scan(&namespaceID) {
			break
		}
		result.SourceEntities++
		source, err := readTaskQueueUserDataSourceAuthority(ctx, session, namespaceID)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		if err := validateTaskQueueUserDataAuthority(
			namespaceID,
			source,
			identity,
			taskQueueUserDataAuthorityTarget,
		); err != nil {
			_ = iter.Close()
			return result, fmt.Errorf("task queue user data source inventory is incomplete: %w", err)
		}
		target, err := readTaskQueueUserDataTargetAuthority(ctx, session, namespaceID)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		if err := validateTaskQueueUserDataAuthority(
			namespaceID,
			target,
			identity,
			taskQueueUserDataAuthorityTarget,
		); err != nil {
			_ = iter.Close()
			return result, fmt.Errorf("task queue user data target inventory is incomplete: %w", err)
		}
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanTaskQueueUserDataTargetOnlyInventory", err)
	}
	return auditTaskQueueUserDataTargetPartitions(
		ctx,
		session,
		identity,
		pageSize,
		result,
	)
}

func validateTaskQueueUserDataInventorySource(
	namespaceID string,
	record taskQueueUserDataAuthorityRecord,
	identity taskQueueUserDataAuthorityRecord,
) error {
	if err := validateTaskQueueUserDataAuthority(
		namespaceID,
		record,
		identity,
		taskQueueUserDataAuthorityTarget,
	); err != nil {
		return fmt.Errorf("task queue user data target namespace %s has no valid source: %w", namespaceID, err)
	}
	return nil
}

func validateTaskQueueUserDataTargetPartitionIdentity(
	ctx context.Context,
	session gocql.Session,
	namespaceID string,
	identity taskQueueUserDataAuthorityRecord,
) error {
	source, err := readTaskQueueUserDataSourceAuthority(ctx, session, namespaceID)
	if err != nil {
		return fmt.Errorf("read task queue user data source authority for namespace %s: %w", namespaceID, err)
	}
	if err := validateTaskQueueUserDataInventorySource(namespaceID, source, identity); err != nil {
		return err
	}
	target, err := readTaskQueueUserDataTargetAuthority(ctx, session, namespaceID)
	if err != nil {
		return fmt.Errorf("read task queue user data target authority for namespace %s: %w", namespaceID, err)
	}
	if err := validateTaskQueueUserDataAuthority(
		namespaceID,
		target,
		identity,
		taskQueueUserDataAuthorityTarget,
	); err != nil {
		return fmt.Errorf("task queue user data target inventory is incomplete: %w", err)
	}
	return nil
}

func auditTaskQueueUserDataTargetPartitions(
	ctx context.Context,
	session gocql.Session,
	identity taskQueueUserDataAuthorityRecord,
	pageSize int,
	result SchemaLayoutInventoryAuditResult,
) (SchemaLayoutInventoryAuditResult, error) {
	iter := session.Query(templateScanUserDataTargetInventory).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var namespaceID string
		var bucketID int
		if !iter.Scan(&namespaceID, &bucketID) {
			break
		}
		if bucketID < 0 || bucketID >= identity.bucketCount {
			_ = iter.Close()
			return result, fmt.Errorf(
				"task queue user data target namespace %s contains invalid bucket %d; expected [0,%d)",
				namespaceID,
				bucketID,
				identity.bucketCount,
			)
		}
		if err := validateTaskQueueUserDataTargetPartitionIdentity(
			ctx,
			session,
			namespaceID,
			identity,
		); err != nil {
			_ = iter.Close()
			return result, err
		}
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanTaskQueueUserDataTargetPartitionInventory", err)
	}

	iter = session.Query(templateScanUserDataTargetTxnInventory).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var namespaceID string
		if !iter.Scan(&namespaceID) {
			break
		}
		if err := validateTaskQueueUserDataTargetPartitionIdentity(
			ctx,
			session,
			namespaceID,
			identity,
		); err != nil {
			_ = iter.Close()
			return result, err
		}
		result.TargetEntities++
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanTaskQueueUserDataTargetTransactionInventory", err)
	}
	if result.TargetEntities != result.SourceEntities {
		return result, fmt.Errorf(
			"task queue user data target inventory contains %d namespaces; source contains %d namespaces",
			result.TargetEntities,
			result.SourceEntities,
		)
	}
	return result, nil
}

func auditLegacyQueueTargetOnlyInventory(
	ctx context.Context,
	session gocql.Session,
	messageBucketSize int64,
	pageSize int,
) (SchemaLayoutInventoryAuditResult, error) {
	if messageBucketSize <= 0 {
		return SchemaLayoutInventoryAuditResult{}, fmt.Errorf(
			"legacy queue v2 message bucket size must be positive: %d",
			messageBucketSize,
		)
	}
	store := newLegacyQueueV2MigrationStore(session, messageBucketSize)
	result := SchemaLayoutInventoryAuditResult{}
	iter := session.Query(templateScanLegacyQueueInventory).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var queueTypeValue int
		if !iter.Scan(&queueTypeValue) {
			break
		}
		if queueTypeValue < math.MinInt32 || queueTypeValue > math.MaxInt32 {
			_ = iter.Close()
			return result, fmt.Errorf("legacy queue source inventory contains invalid queue type %d", queueTypeValue)
		}
		queueType := persistence.QueueType(queueTypeValue)
		result.SourceEntities++
		source, err := readValidLegacyQueueInventorySource(ctx, session, queueType, messageBucketSize)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		target, err := store.getLegacyQueueV2State(ctx, queueType)
		if err != nil {
			_ = iter.Close()
			return result, fmt.Errorf("read legacy queue type %d target authority: %w", queueType, err)
		}
		if err := validateLegacyQueueInventoryState(store, queueType, target); err != nil {
			_ = iter.Close()
			return result, err
		}
		if err := validateLegacyQueueAuthorityRecord(queueType, "target state", source, target.authorityRecord()); err != nil {
			_ = iter.Close()
			return result, err
		}
		if err := validateLegacyQueueRequiredTargetPartitions(ctx, store, queueType, target); err != nil {
			_ = iter.Close()
			return result, fmt.Errorf("validate legacy queue type %d target partitions: %w", queueType, err)
		}
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanLegacyQueueTargetOnlyInventory", err)
	}
	return auditLegacyQueueTargetPartitions(
		ctx,
		session,
		store,
		messageBucketSize,
		pageSize,
		result,
	)
}

func readValidLegacyQueueInventorySource(
	ctx context.Context,
	session gocql.Session,
	queueType persistence.QueueType,
	messageBucketSize int64,
) (legacyQueueAuthorityRecord, error) {
	source, err := readLegacyQueueSourceAuthority(ctx, session, queueType)
	if err != nil {
		return legacyQueueAuthorityRecord{}, fmt.Errorf(
			"legacy queue target type %d has no source authority: %w",
			queueType,
			err,
		)
	}
	if err := validateLegacyQueueAuthority(
		queueType,
		messageBucketSize,
		legacyQueueMigrationAuthorityTarget,
		source,
	); err != nil {
		return legacyQueueAuthorityRecord{}, err
	}
	return source, nil
}

func validateLegacyQueueInventoryState(
	store *QueueStore,
	queueType persistence.QueueType,
	state legacyQueueV2State,
) error {
	if err := store.validateLegacyQueueV2State(queueType, state); err != nil {
		return err
	}
	if state.activeBucket < 0 {
		return fmt.Errorf("legacy queue type %d target state has invalid active bucket %d", queueType, state.activeBucket)
	}
	if state.minimumMessageID < persistence.FirstQueueMessageID ||
		state.cleanupMessageID < persistence.FirstQueueMessageID ||
		state.cleanupMessageID > state.minimumMessageID {
		return fmt.Errorf(
			"legacy queue type %d target state has invalid message bounds: minimum=%d cleanup=%d",
			queueType,
			state.minimumMessageID,
			state.cleanupMessageID,
		)
	}
	if state.version < 0 {
		return fmt.Errorf("legacy queue type %d target state has invalid version %d", queueType, state.version)
	}
	activeLast, err := legacyQueueV2BucketLastMessageID(state.activeBucket, state.messageBucketSize)
	if err != nil {
		return fmt.Errorf("legacy queue type %d target state has invalid active bucket: %w", queueType, err)
	}
	if activeLast < math.MaxInt64 && state.minimumMessageID > activeLast+1 {
		return fmt.Errorf(
			"legacy queue type %d target state minimum message ID %d exceeds active bucket %d boundary %d",
			queueType,
			state.minimumMessageID,
			state.activeBucket,
			activeLast+1,
		)
	}
	return nil
}

func validateLegacyQueueRequiredTargetPartitions(
	ctx context.Context,
	store *QueueStore,
	queueType persistence.QueueType,
	state legacyQueueV2State,
) error {
	expected := state.authorityRecord()
	bucketAuthority, err := readLegacyQueueTargetBucketAuthority(ctx, store.session, queueType, state.activeBucket)
	if err != nil {
		return err
	}
	if err := validateLegacyQueueAuthorityRecord(
		queueType,
		fmt.Sprintf("target bucket %d", state.activeBucket),
		expected,
		bucketAuthority,
	); err != nil {
		return err
	}
	if err := validateLegacyQueueInventoryBucketState(ctx, store, queueType, state.activeBucket); err != nil {
		return err
	}
	deleteAuthority, err := readLegacyQueueTargetDeleteAuthority(ctx, store.session, queueType)
	if err != nil {
		return err
	}
	return validateLegacyQueueAuthorityRecord(
		queueType,
		"target delete-range partition",
		expected,
		deleteAuthority,
	)
}

func validateLegacyQueueInventoryBucketState(
	ctx context.Context,
	store *QueueStore,
	queueType persistence.QueueType,
	bucket int64,
) error {
	state, err := store.getLegacyQueueV2BucketState(ctx, queueType, bucket)
	if err != nil {
		return fmt.Errorf("read legacy queue type %d target bucket %d state: %w", queueType, bucket, err)
	}
	firstMessageID, err := legacyQueueV2BucketFirstMessageID(bucket, store.messageBucketSize)
	if err != nil {
		return err
	}
	lastMessageID, err := legacyQueueV2BucketLastMessageID(bucket, store.messageBucketSize)
	if err != nil {
		return err
	}
	if state.lastMessageID < firstMessageID-1 || state.lastMessageID > lastMessageID || state.version < 0 {
		return fmt.Errorf(
			"legacy queue type %d target bucket %d has invalid state: last message ID=%d version=%d",
			queueType,
			bucket,
			state.lastMessageID,
			state.version,
		)
	}
	return nil
}

func readValidLegacyQueueInventoryState(
	ctx context.Context,
	store *QueueStore,
	queueType persistence.QueueType,
	source legacyQueueAuthorityRecord,
) (legacyQueueV2State, error) {
	state, err := store.getLegacyQueueV2State(ctx, queueType)
	if err != nil {
		return legacyQueueV2State{}, fmt.Errorf("read legacy queue type %d target state: %w", queueType, err)
	}
	if err := validateLegacyQueueInventoryState(store, queueType, state); err != nil {
		return legacyQueueV2State{}, err
	}
	if err := validateLegacyQueueAuthorityRecord(queueType, "target state", source, state.authorityRecord()); err != nil {
		return legacyQueueV2State{}, err
	}
	return state, nil
}

//nolint:revive // Three physical target tables must be exhaustively streamed before irreversible cutover.
func auditLegacyQueueTargetPartitions(
	ctx context.Context,
	session gocql.Session,
	store *QueueStore,
	messageBucketSize int64,
	pageSize int,
	result SchemaLayoutInventoryAuditResult,
) (SchemaLayoutInventoryAuditResult, error) {
	iter := session.Query(templateScanLegacyQueueTargetStateInventory).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var queueTypeValue int
		if !iter.Scan(&queueTypeValue) {
			break
		}
		queueType := persistence.QueueType(queueTypeValue)
		source, err := readValidLegacyQueueInventorySource(ctx, session, queueType, messageBucketSize)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		if _, err := readValidLegacyQueueInventoryState(ctx, store, queueType, source); err != nil {
			_ = iter.Close()
			return result, err
		}
		result.TargetEntities++
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanLegacyQueueTargetStateInventory", err)
	}
	if result.TargetEntities != result.SourceEntities {
		return result, fmt.Errorf(
			"legacy queue target inventory contains %d state entities; source contains %d queue types",
			result.TargetEntities,
			result.SourceEntities,
		)
	}

	iter = session.Query(templateScanLegacyQueueTargetBucketInventory).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var queueTypeValue int
		var bucket int64
		if !iter.Scan(&queueTypeValue, &bucket) {
			break
		}
		queueType := persistence.QueueType(queueTypeValue)
		source, err := readValidLegacyQueueInventorySource(ctx, session, queueType, messageBucketSize)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		state, err := readValidLegacyQueueInventoryState(ctx, store, queueType, source)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		if bucket < 0 || bucket > state.activeBucket {
			_ = iter.Close()
			return result, fmt.Errorf(
				"legacy queue target type %d contains invalid bucket %d with active bucket %d",
				queueType,
				bucket,
				state.activeBucket,
			)
		}
		authority, err := readLegacyQueueTargetBucketAuthority(ctx, session, queueType, bucket)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		if err := validateLegacyQueueAuthorityRecord(
			queueType,
			fmt.Sprintf("target bucket %d", bucket),
			source,
			authority,
		); err != nil {
			_ = iter.Close()
			return result, err
		}
		if err := validateLegacyQueueInventoryBucketState(ctx, store, queueType, bucket); err != nil {
			_ = iter.Close()
			return result, err
		}
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanLegacyQueueTargetBucketInventory", err)
	}

	iter = session.Query(templateScanLegacyQueueTargetDeleteInventory).WithContext(ctx).PageSize(pageSize).Iter()
	for {
		var queueTypeValue int
		if !iter.Scan(&queueTypeValue) {
			break
		}
		queueType := persistence.QueueType(queueTypeValue)
		source, err := readValidLegacyQueueInventorySource(ctx, session, queueType, messageBucketSize)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		if _, err := readValidLegacyQueueInventoryState(ctx, store, queueType, source); err != nil {
			_ = iter.Close()
			return result, err
		}
		authority, err := readLegacyQueueTargetDeleteAuthority(ctx, session, queueType)
		if err != nil {
			_ = iter.Close()
			return result, err
		}
		if err := validateLegacyQueueAuthorityRecord(
			queueType,
			"target delete-range partition",
			source,
			authority,
		); err != nil {
			_ = iter.Close()
			return result, err
		}
	}
	if err := iter.Close(); err != nil {
		return result, gocql.ConvertError("ScanLegacyQueueTargetDeleteInventory", err)
	}
	return result, nil
}
