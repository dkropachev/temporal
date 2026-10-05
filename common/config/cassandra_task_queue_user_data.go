package config

// CassandraTaskQueueUserDataMigrationMode selects the task queue user data table used during online migration.
type CassandraTaskQueueUserDataMigrationMode string

const (
	CassandraTaskQueueUserDataMigrationModeSourceOnly   CassandraTaskQueueUserDataMigrationMode = "source-only"
	CassandraTaskQueueUserDataMigrationModeSourceDual   CassandraTaskQueueUserDataMigrationMode = "source-dual"
	CassandraTaskQueueUserDataMigrationModeTargetShadow CassandraTaskQueueUserDataMigrationMode = "target-shadow"
	CassandraTaskQueueUserDataMigrationModeTargetDual   CassandraTaskQueueUserDataMigrationMode = "target-dual"
	CassandraTaskQueueUserDataMigrationModeTargetOnly   CassandraTaskQueueUserDataMigrationMode = "target-only"
)
