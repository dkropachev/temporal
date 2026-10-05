package config

// CassandraLegacyQueueMigrationMode selects the message table used by the Cassandra legacy queue store.
type CassandraLegacyQueueMigrationMode string

const (
	CassandraLegacyQueueMigrationModeSourceOnly CassandraLegacyQueueMigrationMode = "source-only"
	CassandraLegacyQueueMigrationModeSourceDual CassandraLegacyQueueMigrationMode = "source-dual"
	CassandraLegacyQueueMigrationModeTargetDual CassandraLegacyQueueMigrationMode = "target-dual"
	CassandraLegacyQueueMigrationModeTargetOnly CassandraLegacyQueueMigrationMode = "target-only"
)
