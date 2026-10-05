package config

// CassandraQueueV2MigrationMode selects the metadata table used by the Cassandra QueueV2 store.
type CassandraQueueV2MigrationMode string

const (
	CassandraQueueV2MigrationModeSourceOnly   CassandraQueueV2MigrationMode = "source-only"
	CassandraQueueV2MigrationModeSourceDual   CassandraQueueV2MigrationMode = "source-dual"
	CassandraQueueV2MigrationModeTargetShadow CassandraQueueV2MigrationMode = "target-shadow"
	CassandraQueueV2MigrationModeTargetDual   CassandraQueueV2MigrationMode = "target-dual"
	CassandraQueueV2MigrationModeTargetOnly   CassandraQueueV2MigrationMode = "target-only"
)
