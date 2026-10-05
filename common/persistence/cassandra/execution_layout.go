package cassandra

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"strings"

	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const defaultExecutionStorageBuckets = 16

const executionsV2TableName = "executions_v2"

var executionTableReplacer = strings.NewReplacer(
	"INSERT INTO executions ", "INSERT INTO executions_v2 ",
	"UPDATE executions ", "UPDATE executions_v2 ",
	"FROM executions ", "FROM executions_v2 ",
)

type executionLayout struct {
	mode      config.CassandraExecutionMigrationMode
	buckets   int32
	authority executionShardAuthority
}

type executionBatch struct {
	*gocql.Batch
	layout executionLayout
}

func newExecutionBatch(batch *gocql.Batch, layout executionLayout) *executionBatch {
	return &executionBatch{Batch: batch, layout: layout}
}

func (b *executionBatch) Query(query string, args ...any) {
	b.Batch.Query(b.layout.query(query), args...)
}

func newExecutionLayout(mode config.CassandraExecutionMigrationMode, buckets int) (executionLayout, error) {
	if mode == "" {
		mode = config.CassandraExecutionMigrationModeLegacy
	}
	switch mode {
	case config.CassandraExecutionMigrationModeLegacy:
		return executionLayout{mode: mode, buckets: 1}, nil
	case config.CassandraExecutionMigrationModeSourceDual,
		config.CassandraExecutionMigrationModeSourceRebuild:
		if buckets == 0 {
			buckets = defaultExecutionStorageBuckets
		}
		if buckets < 1 || buckets > 1024 || buckets&(buckets-1) != 0 {
			return executionLayout{}, fmt.Errorf("execution storage buckets must be a power of two between 1 and 1024, got %d", buckets)
		}
		return executionLayout{
			mode:      mode,
			buckets:   int32(buckets),
			authority: executionShardAuthoritySource,
		}, nil
	case config.CassandraExecutionMigrationModeTargetDual,
		config.CassandraExecutionMigrationModeTargetOnly:
		if buckets == 0 {
			buckets = defaultExecutionStorageBuckets
		}
		if buckets < 1 || buckets > 1024 || buckets&(buckets-1) != 0 {
			return executionLayout{}, fmt.Errorf("execution storage buckets must be a power of two between 1 and 1024, got %d", buckets)
		}
		return executionLayout{
			mode:      mode,
			buckets:   int32(buckets),
			authority: executionShardAuthorityTarget,
		}, nil
	default:
		return executionLayout{}, fmt.Errorf("unknown Cassandra execution migration mode %q", mode)
	}
}

func (l executionLayout) authoritativeLayout() executionLayout {
	switch l.mode {
	case config.CassandraExecutionMigrationModeSourceRebuild,
		config.CassandraExecutionMigrationModeSourceDual:
		return executionLayout{
			mode:      config.CassandraExecutionMigrationModeLegacy,
			buckets:   l.buckets,
			authority: executionShardAuthoritySource,
		}
	case config.CassandraExecutionMigrationModeTargetDual:
		return executionLayout{
			mode:      config.CassandraExecutionMigrationModeTargetOnly,
			buckets:   l.buckets,
			authority: executionShardAuthorityTarget,
		}
	default:
		return l
	}
}

func (l executionLayout) mirrorLayout() (executionLayout, bool) {
	switch l.mode {
	case config.CassandraExecutionMigrationModeSourceRebuild,
		config.CassandraExecutionMigrationModeSourceDual:
		return executionLayout{
			mode:      config.CassandraExecutionMigrationModeTargetOnly,
			buckets:   l.buckets,
			authority: executionShardAuthoritySource,
		}, true
	case config.CassandraExecutionMigrationModeTargetDual:
		return executionLayout{
			mode:      config.CassandraExecutionMigrationModeLegacy,
			buckets:   l.buckets,
			authority: executionShardAuthorityTarget,
		}, true
	default:
		return executionLayout{}, false
	}
}

func (l executionLayout) mirrorRequired() bool {
	return l.mode != config.CassandraExecutionMigrationModeSourceRebuild
}

func validateExecutionLayoutSchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	layout executionLayout,
) error {
	mirror, hasMirror := layout.mirrorLayout()
	if !layout.isTarget() && (!hasMirror || !mirror.isTarget()) {
		return nil
	}
	var tableID [16]byte
	err := session.Query(templateGetHistoryNodeTableID, keyspace, executionsV2TableName).WithContext(ctx).Scan(&tableID)
	if gocql.IsNotFoundError(err) {
		return fmt.Errorf("cassandra execution migration mode %q requires table %s.%s", layout.mode, keyspace, executionsV2TableName)
	}
	if err != nil {
		return fmt.Errorf("validate cassandra execution layout: %w", err)
	}
	return nil
}

func (l executionLayout) isTarget() bool {
	return l.mode == config.CassandraExecutionMigrationModeTargetOnly ||
		l.mode == config.CassandraExecutionMigrationModeTargetDual
}

func (l executionLayout) requiresAuthority() bool {
	return l.authority != executionShardAuthorityUnspecified
}

func (l executionLayout) query(query string) string {
	if !l.isTarget() {
		return query
	}
	return executionTableReplacer.Replace(query)
}

func (l executionLayout) workflowPartition(
	shardID int32,
	namespaceID string,
	workflowID string,
) (int32, error) {
	if !l.isTarget() {
		return shardID, nil
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(namespaceID))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write([]byte(workflowID))
	bucket := int32(hasher.Sum32() & uint32(l.buckets-1))
	return l.partition(shardID, bucket)
}

func (l executionLayout) partition(shardID int32, bucket int32) (int32, error) {
	if !l.isTarget() {
		return shardID, nil
	}
	if shardID < 1 {
		return 0, fmt.Errorf("history shard ID must be positive, got %d", shardID)
	}
	if bucket < 0 || bucket >= l.buckets {
		return 0, fmt.Errorf("execution storage bucket %d outside [0,%d)", bucket, l.buckets)
	}
	partition := (int64(shardID)-1)*int64(l.buckets) + int64(bucket) + 1
	if partition > math.MaxInt32 {
		return 0, fmt.Errorf("execution storage partition overflows int32: shard %d, buckets %d", shardID, l.buckets)
	}
	return int32(partition), nil
}

func (l executionLayout) partitions(shardID int32) ([]int32, error) {
	if !l.isTarget() {
		return []int32{shardID}, nil
	}
	partitions := make([]int32, l.buckets)
	for bucket := range l.buckets {
		partition, err := l.partition(shardID, bucket)
		if err != nil {
			return nil, err
		}
		partitions[bucket] = partition
	}
	return partitions, nil
}

func workflowMutationNamespaceID(workflow *p.InternalWorkflowMutation) string {
	if workflow == nil {
		return ""
	}
	return workflow.NamespaceID
}

func workflowMutationWorkflowID(workflow *p.InternalWorkflowMutation) string {
	if workflow == nil {
		return ""
	}
	return workflow.WorkflowID
}

func workflowSnapshotNamespaceID(workflow *p.InternalWorkflowSnapshot) string {
	if workflow == nil {
		return ""
	}
	return workflow.NamespaceID
}

func workflowSnapshotWorkflowID(workflow *p.InternalWorkflowSnapshot) string {
	if workflow == nil {
		return ""
	}
	return workflow.WorkflowID
}
