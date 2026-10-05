package config

// CassandraHistoryTreeMigrationMode selects the history_tree table used during online migration.
type CassandraHistoryTreeMigrationMode string

const (
	CassandraHistoryTreeMigrationModeSourceOnly    CassandraHistoryTreeMigrationMode = "source-only"
	CassandraHistoryTreeMigrationModeSourceRebuild CassandraHistoryTreeMigrationMode = "source-rebuild"
	CassandraHistoryTreeMigrationModeSourceDual    CassandraHistoryTreeMigrationMode = "source-dual"
	CassandraHistoryTreeMigrationModeTargetPrepare CassandraHistoryTreeMigrationMode = "target-prepare"
	CassandraHistoryTreeMigrationModeTargetDual    CassandraHistoryTreeMigrationMode = "target-dual"
	CassandraHistoryTreeMigrationModeTargetOnly    CassandraHistoryTreeMigrationMode = "target-only"
)
