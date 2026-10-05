package config

// CassandraMatchingTaskMigrationMode selects the Cassandra matching task table layout.
type CassandraMatchingTaskMigrationMode string

const (
	CassandraMatchingTaskMigrationModeSourceOnly CassandraMatchingTaskMigrationMode = "source-only"
	CassandraMatchingTaskMigrationModeSourceDual CassandraMatchingTaskMigrationMode = "source-dual"
	CassandraMatchingTaskMigrationModeTargetDual CassandraMatchingTaskMigrationMode = "target-dual"
	CassandraMatchingTaskMigrationModeTargetOnly CassandraMatchingTaskMigrationMode = "target-only"
)
