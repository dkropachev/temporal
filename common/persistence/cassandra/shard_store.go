package cassandra

import (
	"context"
	"fmt"
	"strings"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	templateCreateShardQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id, shard, shard_encoding, range_id)` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateCreateShardWithAuthorityQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id, shard, shard_encoding, ` +
		`range_id, migration_authority, storage_bucket_count)` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateGetShardQuery = `SELECT shard, shard_encoding, range_id ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`
	templateGetShardWithAuthorityQuery = `SELECT shard, shard_encoding, range_id, migration_authority, storage_bucket_count ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateGetShardRangeIDQuery = `SELECT range_id ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateUpdateShardQuery = `UPDATE executions ` +
		`SET shard = ?, shard_encoding = ?, range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`
	templateUpdateShardWithAuthorityQuery = `UPDATE executions ` +
		`SET shard = ?, shard_encoding = ?, range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF range_id = ? AND migration_authority = ? AND storage_bucket_count = ?`
)

type executionShardRow struct {
	data        []byte
	encoding    string
	rangeID     int64
	authority   executionShardAuthority
	bucketCount int32
}

type (
	ShardStore struct {
		ClusterName string
		Session     gocql.Session
		Logger      log.Logger
		layout      executionLayout
	}
)

func NewShardStore(
	clusterName string,
	session gocql.Session,
	logger log.Logger,
) *ShardStore {
	return newShardStore(
		clusterName,
		session,
		logger,
		executionLayout{mode: config.CassandraExecutionMigrationModeLegacy, buckets: 1},
	)
}

func newShardStore(
	clusterName string,
	session gocql.Session,
	logger log.Logger,
	layout executionLayout,
) *ShardStore {
	return &ShardStore{
		ClusterName: clusterName,
		Session:     session,
		Logger:      logger,
		layout:      layout,
	}
}

func (d *ShardStore) GetOrCreateShard(
	ctx context.Context,
	request *p.InternalGetOrCreateShardRequest,
) (*p.InternalGetOrCreateShardResponse, error) {
	if d.layout.mode == config.CassandraExecutionMigrationModeTargetDual {
		return d.getOrCreateTargetDualShard(ctx, request)
	}
	if primary, mirror, ok := d.migrationStores(); ok {
		response, err := primary.GetOrCreateShard(ctx, request)
		if err != nil {
			return nil, err
		}
		rangeID, err := primary.getShardRangeID(ctx, request.ShardID)
		if err != nil {
			return nil, err
		}
		partitions, err := mirror.layout.partitions(request.ShardID)
		if err != nil {
			return nil, err
		}
		if err := mirror.ensureShardPartitions(
			ctx,
			request.ShardID,
			partitions,
			rangeID,
			response.ShardInfo.Data,
			response.ShardInfo.EncodingType.String(),
		); err != nil {
			if mirrorErr := d.handleMirrorError("GetOrCreateShard", err); mirrorErr != nil {
				return nil, mirrorErr
			}
		}
		return response, nil
	}
	partitions, err := d.layout.partitions(request.ShardID)
	if err != nil {
		return nil, err
	}
	row, err := d.getShardRow(ctx, partitions[0], d.layout.requiresAuthority())
	if err == nil {
		if err := ensureExecutionShardAuthority(
			ctx,
			d.Session,
			d.layout,
			request.ShardID,
			partitions[0],
			executionShardAuthorityRecord{
				rangeID:     row.rangeID,
				authority:   row.authority,
				bucketCount: row.bucketCount,
			},
		); err != nil {
			return nil, err
		}
		if err := d.ensureShardPartitions(ctx, request.ShardID, partitions[1:], row.rangeID, row.data, row.encoding); err != nil {
			return nil, err
		}
		return &p.InternalGetOrCreateShardResponse{
			ShardInfo: p.NewDataBlob(row.data, row.encoding),
		}, nil
	} else if !gocql.IsNotFoundError(err) || request.CreateShardInfo == nil {
		return nil, gocql.ConvertError("GetOrCreateShard", err)
	}

	// shard was not found and we should create it
	rangeID, shardInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, err
	}

	createQuery := templateCreateShardQuery
	args := []any{
		partitions[0], rowTypeShard, rowTypeShardNamespaceID, rowTypeShardWorkflowID,
		rowTypeShardRunID, defaultVisibilityTimestamp, rowTypeShardTaskID,
		shardInfo.Data, shardInfo.EncodingType.String(), rangeID,
	}
	if d.layout.requiresAuthority() {
		createQuery = templateCreateShardWithAuthorityQuery
		args = append(args, int(d.layout.authority), int(d.layout.buckets))
	}
	query := d.Session.Query(d.layout.query(createQuery), args...).WithContext(ctx)

	previous := make(map[string]any)
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return nil, gocql.ConvertError("GetOrCreateShard", err)
	}
	if !applied {
		// conflict, try again
		request.CreateShardInfo = nil // prevent loop
		return d.GetOrCreateShard(ctx, request)
	}
	if err := d.ensureShardPartitions(
		ctx,
		request.ShardID,
		partitions[1:],
		rangeID,
		shardInfo.Data,
		shardInfo.EncodingType.String(),
	); err != nil {
		return nil, err
	}
	return &p.InternalGetOrCreateShardResponse{
		ShardInfo: shardInfo,
	}, nil
}

//nolint:revive // Shard creation validates and resumes every source/target authority state.
func (d *ShardStore) getOrCreateTargetDualShard(
	ctx context.Context,
	request *p.InternalGetOrCreateShardRequest,
) (*p.InternalGetOrCreateShardResponse, error) {
	sourceLayout := executionLayout{
		mode:      config.CassandraExecutionMigrationModeLegacy,
		buckets:   d.layout.buckets,
		authority: executionShardAuthoritySource,
	}
	source := *d
	source.layout = sourceLayout
	row, err := source.getShardRow(ctx, request.ShardID, true)
	if gocql.IsNotFoundError(err) {
		response, createErr := source.GetOrCreateShard(ctx, request)
		if createErr != nil {
			return nil, createErr
		}
		targetLayout := executionLayout{
			mode:      config.CassandraExecutionMigrationModeTargetOnly,
			buckets:   d.layout.buckets,
			authority: executionShardAuthoritySource,
		}
		target := *d
		target.layout = targetLayout
		partitions, partitionErr := targetLayout.partitions(request.ShardID)
		if partitionErr != nil {
			return nil, partitionErr
		}
		rangeID, rangeErr := source.getShardRangeID(ctx, request.ShardID)
		if rangeErr != nil {
			return nil, rangeErr
		}
		if ensureErr := target.ensureShardPartitions(
			ctx,
			request.ShardID,
			partitions,
			rangeID,
			response.ShardInfo.Data,
			response.ShardInfo.EncodingType.String(),
		); ensureErr != nil {
			return nil, ensureErr
		}
		return response, nil
	}
	if err != nil {
		return nil, gocql.ConvertError("GetTargetDualSourceShard", err)
	}
	if row.bucketCount != d.layout.buckets && row.authority != executionShardAuthorityUnspecified {
		return nil, fmt.Errorf(
			"execution shard %d has persisted bucket count %d; configured %d",
			request.ShardID,
			row.bucketCount,
			d.layout.buckets,
		)
	}
	if row.authority == executionShardAuthorityUnspecified {
		if err := initializeExecutionShardAuthority(
			ctx, d.Session, sourceLayout, request.ShardID, request.ShardID,
		); err != nil {
			return nil, err
		}
		row.authority = executionShardAuthoritySource
		row.bucketCount = d.layout.buckets
	}
	if row.authority == executionShardAuthoritySource || row.authority == executionShardAuthoritySealing {
		if row.authority == executionShardAuthoritySource {
			targetLayout := executionLayout{
				mode:      config.CassandraExecutionMigrationModeTargetOnly,
				buckets:   d.layout.buckets,
				authority: executionShardAuthoritySource,
			}
			target := *d
			target.layout = targetLayout
			partitions, partitionErr := targetLayout.partitions(request.ShardID)
			if partitionErr != nil {
				return nil, partitionErr
			}
			if ensureErr := target.ensureShardPartitions(
				ctx,
				request.ShardID,
				partitions,
				row.rangeID,
				row.data,
				row.encoding,
			); ensureErr != nil {
				return nil, ensureErr
			}
		}
		return &p.InternalGetOrCreateShardResponse{ShardInfo: p.NewDataBlob(row.data, row.encoding)}, nil
	}
	if row.authority != executionShardAuthorityTarget {
		return nil, fmt.Errorf("execution shard %d has invalid authority %s", request.ShardID, row.authority)
	}
	target := *d
	target.layout = d.layout.authoritativeLayout()
	return target.GetOrCreateShard(ctx, request)
}

func (d *ShardStore) getShardRow(
	ctx context.Context,
	partition int32,
	withAuthority bool,
) (executionShardRow, error) {
	query := templateGetShardQuery
	if withAuthority {
		query = templateGetShardWithAuthorityQuery
	}
	row := executionShardRow{}
	if !withAuthority {
		err := d.Session.Query(
			d.layout.query(query), executionShardRowKey(partition)...,
		).WithContext(ctx).Scan(&row.data, &row.encoding, &row.rangeID)
		return row, err
	}
	var authority, bucketCount *int
	err := d.Session.Query(
		d.layout.query(query), executionShardRowKey(partition)...,
	).WithContext(ctx).Scan(&row.data, &row.encoding, &row.rangeID, &authority, &bucketCount)
	if authority != nil {
		row.authority = executionShardAuthority(*authority)
	}
	if bucketCount != nil {
		row.bucketCount = int32(*bucketCount)
	}
	return row, err
}

func (d *ShardStore) UpdateShard(
	ctx context.Context,
	request *p.InternalUpdateShardRequest,
) error {
	if d.layout.mode == config.CassandraExecutionMigrationModeTargetDual {
		return d.updateTargetDualShard(ctx, request)
	}
	if primary, mirror, ok := d.migrationStores(); ok {
		if err := primary.UpdateShard(ctx, request); err != nil {
			return err
		}
		return d.handleMirrorError("UpdateShard", mirror.UpdateShard(ctx, request))
	}
	partitions, err := d.layout.partitions(request.ShardID)
	if err != nil {
		return err
	}
	if len(partitions) > 1 {
		partitions = append(partitions[1:], partitions[0])
	}
	for _, partition := range partitions {
		queryTemplate := templateUpdateShardQuery
		args := []any{
			request.ShardInfo.Data,
			request.ShardInfo.EncodingType.String(),
			request.RangeID,
			partition,
			rowTypeShard,
			rowTypeShardNamespaceID,
			rowTypeShardWorkflowID,
			rowTypeShardRunID,
			defaultVisibilityTimestamp,
			rowTypeShardTaskID,
			request.PreviousRangeID,
		}
		if d.layout.requiresAuthority() {
			queryTemplate = templateUpdateShardWithAuthorityQuery
			args = append(args, int(d.layout.authority), int(d.layout.buckets))
		}
		query := d.Session.Query(d.layout.query(queryTemplate), args...).WithContext(ctx)

		previous := make(map[string]any)
		applied, queryErr := query.MapScanCAS(previous)
		if queryErr != nil {
			return gocql.ConvertError("UpdateShard", queryErr)
		}
		if applied {
			continue
		}
		if d.layout.requiresAuthority() {
			record, readErr := readExecutionShardAuthority(ctx, d.Session, d.layout, partition)
			if readErr != nil {
				return gocql.ConvertError("ReadExecutionShardAfterUpdateConflict", readErr)
			}
			if record.rangeID == request.RangeID &&
				record.authority == d.layout.authority &&
				record.bucketCount == d.layout.buckets {
				continue
			}
		} else if previous["range_id"] == request.RangeID {
			continue
		}

		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}
		return &p.ShardOwnershipLostError{
			ShardID: request.ShardID,
			Msg: fmt.Sprintf("Failed to update shard storage partition %d. previous_range_id: %v, columns: (%v)",
				partition, request.PreviousRangeID, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *ShardStore) updateTargetDualShard(
	ctx context.Context,
	request *p.InternalUpdateShardRequest,
) error {
	sourceLayout := executionLayout{
		mode:      config.CassandraExecutionMigrationModeLegacy,
		buckets:   d.layout.buckets,
		authority: executionShardAuthorityTarget,
	}
	sourceRecord, err := readExecutionShardAuthority(
		ctx,
		d.Session,
		sourceLayout,
		request.ShardID,
	)
	if err != nil {
		return gocql.ConvertError("ReadExecutionSourceAuthority", err)
	}
	if sourceRecord.bucketCount != d.layout.buckets {
		return fmt.Errorf(
			"execution shard %d has persisted bucket count %d; configured %d",
			request.ShardID,
			sourceRecord.bucketCount,
			d.layout.buckets,
		)
	}

	switch sourceRecord.authority {
	case executionShardAuthoritySource, executionShardAuthoritySealing:
		return d.cutOverExecutionShard(ctx, request, sourceRecord)
	case executionShardAuthorityTarget:
		primary, mirror, _ := d.migrationStores()
		if err := primary.UpdateShard(ctx, request); err != nil {
			return err
		}
		if err := mirror.UpdateShard(ctx, request); err != nil {
			return fmt.Errorf("mirror UpdateShard: %w", err)
		}
		return nil
	default:
		return fmt.Errorf(
			"execution shard %d has invalid authority %s",
			request.ShardID,
			sourceRecord.authority,
		)
	}
}

//nolint:revive // Cutover seals, reconciles, and activates every physical shard partition atomically by authority.
func (d *ShardStore) cutOverExecutionShard(
	ctx context.Context,
	request *p.InternalUpdateShardRequest,
	sourceRecord executionShardAuthorityRecord,
) error {
	targetSourceLayout := executionLayout{
		mode:      config.CassandraExecutionMigrationModeTargetOnly,
		buckets:   d.layout.buckets,
		authority: executionShardAuthoritySource,
	}
	target := *d
	target.layout = targetSourceLayout
	partitions, err := targetSourceLayout.partitions(request.ShardID)
	if err != nil {
		return err
	}
	if sourceRecord.authority == executionShardAuthoritySource {
		if err := target.ensureShardPartitions(
			ctx,
			request.ShardID,
			partitions,
			sourceRecord.rangeID,
			request.ShardInfo.Data,
			request.ShardInfo.EncodingType.String(),
		); err != nil {
			return fmt.Errorf("prepare target execution shard partitions: %w", err)
		}
		args := []any{request.RangeID, int(executionShardAuthoritySealing)}
		args = append(args, executionShardRowKey(request.ShardID)...)
		args = append(
			args,
			request.PreviousRangeID,
			int(executionShardAuthoritySource),
			int(d.layout.buckets),
		)
		applied, queryErr := d.Session.Query(
			templateSealExecutionSourceShard,
			args...,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if queryErr != nil {
			return gocql.ConvertError("SealExecutionSourceShard", queryErr)
		}
		if !applied {
			current, readErr := readExecutionShardAuthority(
				ctx,
				d.Session,
				executionLayout{mode: config.CassandraExecutionMigrationModeLegacy},
				request.ShardID,
			)
			if readErr != nil {
				return gocql.ConvertError("ReadExecutionSourceShardAfterSealConflict", readErr)
			}
			if current.authority == executionShardAuthorityTarget && current.rangeID == request.RangeID {
				return nil
			}
			if current.authority != executionShardAuthoritySealing || current.rangeID != request.RangeID {
				return executionShardTransitionConflict(request.ShardID, request, current)
			}
		}
	} else if sourceRecord.rangeID != request.RangeID {
		return executionShardTransitionConflict(request.ShardID, request, sourceRecord)
	}

	for _, partition := range partitions {
		if err := d.sealExecutionTargetPartition(ctx, request.ShardID, partition); err != nil {
			return err
		}
	}
	if err := ReconcileExecutionShardV2(
		ctx,
		d.Session,
		request.ShardID,
		int(d.layout.buckets),
	); err != nil {
		return fmt.Errorf("reconcile sealed execution shard: %w", err)
	}
	for _, partition := range partitions {
		if err := d.activateExecutionShardPartition(
			ctx,
			targetSourceLayout,
			request.ShardID,
			partition,
			request,
		); err != nil {
			return fmt.Errorf("activate target execution shard partition %d: %w", partition, err)
		}
	}
	if err := d.activateExecutionShardPartition(
		ctx,
		executionLayout{mode: config.CassandraExecutionMigrationModeLegacy, buckets: d.layout.buckets},
		request.ShardID,
		request.ShardID,
		request,
	); err != nil {
		return fmt.Errorf("publish target execution shard authority: %w", err)
	}
	return nil
}

func (d *ShardStore) sealExecutionTargetPartition(
	ctx context.Context,
	logicalShardID int32,
	partition int32,
) error {
	layout := executionLayout{
		mode:    config.CassandraExecutionMigrationModeTargetOnly,
		buckets: d.layout.buckets,
	}
	record, err := readExecutionShardAuthority(ctx, d.Session, layout, partition)
	if err != nil {
		return gocql.ConvertError("ReadExecutionTargetAuthorityBeforeSeal", err)
	}
	switch record.authority {
	case executionShardAuthoritySealing, executionShardAuthorityTarget:
		if record.bucketCount != d.layout.buckets {
			return fmt.Errorf(
				"execution shard %d target partition %d has bucket count %d; configured %d",
				logicalShardID,
				partition,
				record.bucketCount,
				d.layout.buckets,
			)
		}
		return nil
	case executionShardAuthoritySource:
	default:
		return fmt.Errorf(
			"execution shard %d target partition %d cannot seal from authority %s",
			logicalShardID,
			partition,
			record.authority,
		)
	}

	args := []any{int(executionShardAuthoritySealing)}
	args = append(args, executionShardRowKey(partition)...)
	args = append(args, int(executionShardAuthoritySource), int(d.layout.buckets))
	applied, err := d.Session.Query(
		layout.query(templateSealExecutionTargetShard),
		args...,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("SealExecutionTargetShard", err)
	}
	if applied {
		return nil
	}
	record, err = readExecutionShardAuthority(ctx, d.Session, layout, partition)
	if err != nil {
		return gocql.ConvertError("ReadExecutionTargetAuthorityAfterSealConflict", err)
	}
	if (record.authority == executionShardAuthoritySealing || record.authority == executionShardAuthorityTarget) &&
		record.bucketCount == d.layout.buckets {
		return nil
	}
	return fmt.Errorf(
		"execution shard %d target partition %d seal conflicted with authority %s and %d buckets",
		logicalShardID,
		partition,
		record.authority,
		record.bucketCount,
	)
}

func (d *ShardStore) activateExecutionShardPartition(
	ctx context.Context,
	layout executionLayout,
	logicalShardID int32,
	partition int32,
	request *p.InternalUpdateShardRequest,
) error {
	record, err := readExecutionShardAuthority(ctx, d.Session, layout, partition)
	if err != nil {
		return gocql.ConvertError("ReadExecutionShardAuthorityBeforeActivation", err)
	}
	if record.authority == executionShardAuthorityTarget &&
		record.bucketCount == d.layout.buckets &&
		record.rangeID == request.RangeID {
		return nil
	}
	if record.authority != executionShardAuthoritySealing ||
		record.bucketCount != d.layout.buckets ||
		record.rangeID != request.RangeID {
		return executionShardTransitionConflict(logicalShardID, request, record)
	}

	args := []any{
		request.ShardInfo.Data,
		request.ShardInfo.EncodingType.String(),
		request.RangeID,
		int(executionShardAuthorityTarget),
	}
	args = append(args, executionShardRowKey(partition)...)
	args = append(
		args,
		request.RangeID,
		int(executionShardAuthoritySealing),
		int(d.layout.buckets),
	)
	applied, err := d.Session.Query(
		layout.query(templateActivateExecutionShard),
		args...,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("ActivateExecutionShard", err)
	}
	if applied {
		return nil
	}
	record, err = readExecutionShardAuthority(ctx, d.Session, layout, partition)
	if err != nil {
		return gocql.ConvertError("ReadExecutionShardAuthorityAfterActivationConflict", err)
	}
	if record.authority == executionShardAuthorityTarget &&
		record.bucketCount == d.layout.buckets &&
		record.rangeID == request.RangeID {
		return nil
	}
	return executionShardTransitionConflict(logicalShardID, request, record)
}

func executionShardTransitionConflict(
	logicalShardID int32,
	request *p.InternalUpdateShardRequest,
	record executionShardAuthorityRecord,
) error {
	return &p.ShardOwnershipLostError{
		ShardID: logicalShardID,
		Msg: fmt.Sprintf(
			"execution shard transition expected previous range %d or sealed range %d; persisted range %d, authority %s, buckets %d",
			request.PreviousRangeID,
			request.RangeID,
			record.rangeID,
			record.authority,
			record.bucketCount,
		),
	}
}

func (d *ShardStore) AssertShardOwnership(
	ctx context.Context,
	request *p.AssertShardOwnershipRequest,
) error {
	if !d.layout.requiresAuthority() {
		return nil
	}
	layout := d.layout.authoritativeLayout()
	partitions, err := layout.partitions(request.ShardID)
	if err != nil {
		return err
	}
	for _, partition := range partitions {
		record, err := readExecutionShardAuthority(ctx, d.Session, layout, partition)
		if err != nil {
			return gocql.ConvertError("AssertShardOwnership", err)
		}
		if record.rangeID != request.RangeID ||
			record.authority != layout.authority ||
			record.bucketCount != layout.buckets {
			return &p.ShardOwnershipLostError{
				ShardID: request.ShardID,
				Msg: fmt.Sprintf(
					"Failed to assert shard storage partition %d ownership. expected range_id %d, authority %s, buckets %d; got range_id %d, authority %s, buckets %d",
					partition,
					request.RangeID,
					layout.authority,
					layout.buckets,
					record.rangeID,
					record.authority,
					record.bucketCount,
				),
			}
		}
	}
	return nil
}

func (d *ShardStore) ensureShardPartitions(
	ctx context.Context,
	logicalShardID int32,
	partitions []int32,
	rangeID int64,
	data []byte,
	encoding string,
) error {
	for _, partition := range partitions {
		query := templateCreateShardQuery
		args := []any{
			partition,
			rowTypeShard,
			rowTypeShardNamespaceID,
			rowTypeShardWorkflowID,
			rowTypeShardRunID,
			defaultVisibilityTimestamp,
			rowTypeShardTaskID,
			data,
			encoding,
			rangeID,
		}
		if d.layout.requiresAuthority() {
			query = templateCreateShardWithAuthorityQuery
			args = append(args, int(d.layout.authority), int(d.layout.buckets))
		}
		applied, err := d.Session.Query(d.layout.query(query), args...).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return gocql.ConvertError("InitializeShardStoragePartition", err)
		}
		if !applied && d.layout.requiresAuthority() {
			row, readErr := d.getShardRow(ctx, partition, true)
			if readErr != nil {
				return gocql.ConvertError("ReadShardStoragePartition", readErr)
			}
			if err := ensureExecutionShardAuthority(
				ctx,
				d.Session,
				d.layout,
				logicalShardID,
				partition,
				executionShardAuthorityRecord{
					rangeID:     row.rangeID,
					authority:   row.authority,
					bucketCount: row.bucketCount,
				},
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *ShardStore) getShardRangeID(ctx context.Context, shardID int32) (int64, error) {
	partitions, err := d.layout.partitions(shardID)
	if err != nil {
		return 0, err
	}
	var rangeID int64
	err = d.Session.Query(
		d.layout.query(templateGetShardRangeIDQuery),
		partitions[0],
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
	).WithContext(ctx).Scan(&rangeID)
	if err != nil {
		return 0, gocql.ConvertError("GetShardRangeID", err)
	}
	return rangeID, nil
}

func (d *ShardStore) migrationStores() (primaryStore *ShardStore, mirrorStore *ShardStore, hasMirror bool) {
	mirrorLayout, ok := d.layout.mirrorLayout()
	if !ok {
		return nil, nil, false
	}
	primary := *d
	primary.layout = d.layout.authoritativeLayout()
	mirror := *d
	mirror.layout = mirrorLayout
	return &primary, &mirror, true
}

func (d *ShardStore) handleMirrorError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if d.layout.mirrorRequired() {
		return fmt.Errorf("mirror %s: %w", operation, err)
	}
	if d.Logger != nil {
		d.Logger.Warn("Cassandra executions source-rebuild shard mirror failed", tag.Operation(operation), tag.Error(err))
	}
	return nil
}

func (d *ShardStore) GetName() string {
	return cassandraPersistenceName
}

func (d *ShardStore) GetClusterName() string {
	return d.ClusterName
}

func (d *ShardStore) Close() {
	if d.Session != nil {
		d.Session.Close()
	}
}
