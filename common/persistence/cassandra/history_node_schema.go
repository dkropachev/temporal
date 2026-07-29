package cassandra

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	historyNodeTableName   = "history_node"
	historyNodeV2TableName = "history_node_v2"

	templateGetHistoryNodeSchemaColumns = `SELECT column_name, kind, position, clustering_order FROM system_schema.columns ` +
		`WHERE keyspace_name = ? AND table_name = ?`

	templateDropHistoryNodeTable   = `DROP TABLE IF EXISTS %s.history_node`
	templateDropHistoryNodeV2Table = `DROP TABLE IF EXISTS %s.history_node_v2`
	templateCreateHistoryNodeV1    = `CREATE TABLE IF NOT EXISTS %s.history_node (` +
		`tree_id uuid, branch_id uuid, node_id bigint, txn_id bigint, prev_txn_id bigint, ` +
		`data blob, data_encoding text, PRIMARY KEY ((tree_id), branch_id, node_id, txn_id)) ` +
		`WITH CLUSTERING ORDER BY (branch_id ASC, node_id ASC, txn_id DESC) ` +
		`AND COMPACTION = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}`
	templateCreateHistoryNodeV2 = `CREATE TABLE IF NOT EXISTS %s.history_node_v2 (` +
		`tree_id uuid, branch_id uuid, node_id bigint, txn_id bigint, prev_txn_id bigint, ` +
		`data blob, data_encoding text, PRIMARY KEY ((tree_id, branch_id), node_id, txn_id)) ` +
		`WITH CLUSTERING ORDER BY (node_id ASC, txn_id DESC) ` +
		`AND COMPACTION = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}`
)

// HistoryNodeTableLayout identifies the primary-key layout of a history node table.
type HistoryNodeTableLayout int

const (
	HistoryNodeTableLayoutMissing HistoryNodeTableLayout = iota
	HistoryNodeTableLayoutLegacyV1
	HistoryNodeTableLayoutBranchV2
	HistoryNodeTableLayoutUnknown
)

type historyNodeKeyColumn struct {
	name            string
	position        int
	clusteringOrder string
}

type historyNodeModeLayouts struct {
	historyNode   []HistoryNodeTableLayout
	historyNodeV2 []HistoryNodeTableLayout
}

func (l HistoryNodeTableLayout) String() string {
	switch l {
	case HistoryNodeTableLayoutMissing:
		return "missing"
	case HistoryNodeTableLayoutLegacyV1:
		return "legacy-v1"
	case HistoryNodeTableLayoutBranchV2:
		return "branch-v2"
	case HistoryNodeTableLayoutUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("unknown(%d)", l)
	}
}

func normalizeHistoryNodeMigrationMode(
	mode config.CassandraHistoryNodeMigrationMode,
) config.CassandraHistoryNodeMigrationMode {
	if mode == "" {
		return config.CassandraHistoryNodeMigrationModeLegacyV1Dual
	}
	return mode
}

// ValidateHistoryNodeMigrationMode checks whether a configured migration mode is supported.
func ValidateHistoryNodeMigrationMode(mode config.CassandraHistoryNodeMigrationMode) error {
	switch normalizeHistoryNodeMigrationMode(mode) {
	case config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2,
		config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
		config.CassandraHistoryNodeMigrationModeOldV2RebuildV2,
		config.CassandraHistoryNodeMigrationModeOldV2Dual,
		config.CassandraHistoryNodeMigrationModeOldV2CutoverDual,
		config.CassandraHistoryNodeMigrationModeV1RebuildDual,
		config.CassandraHistoryNodeMigrationModeV2Only,
		config.CassandraHistoryNodeMigrationModeCanonicalDual:
		return nil
	default:
		return fmt.Errorf("unsupported Cassandra history node migration mode %q", mode)
	}
}

// GetHistoryNodeTableLayout reads the table primary key from Cassandra system schema.
func GetHistoryNodeTableLayout(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	table string,
) (HistoryNodeTableLayout, error) {
	if keyspace == "" {
		return HistoryNodeTableLayoutUnknown, errors.New("history node schema validation requires a keyspace")
	}

	iter := session.Query(
		templateGetHistoryNodeSchemaColumns,
		keyspace,
		table,
	).WithContext(ctx).Iter()

	var partitionKeys []historyNodeKeyColumn
	var clusteringKeys []historyNodeKeyColumn
	for {
		var (
			columnName string
			kind       string
			position   int
			order      string
		)
		if !iter.Scan(&columnName, &kind, &position, &order) {
			break
		}
		column := historyNodeKeyColumn{
			name:            columnName,
			position:        position,
			clusteringOrder: strings.ToLower(order),
		}
		switch kind {
		case "partition_key":
			partitionKeys = append(partitionKeys, column)
		case "clustering":
			clusteringKeys = append(clusteringKeys, column)
		default:
			continue
		}
	}
	if err := iter.Close(); err != nil {
		return HistoryNodeTableLayoutUnknown, fmt.Errorf(
			"read schema for %s.%s: %w",
			keyspace,
			table,
			err,
		)
	}
	if len(partitionKeys) == 0 && len(clusteringKeys) == 0 {
		return HistoryNodeTableLayoutMissing, nil
	}

	sort.Slice(partitionKeys, func(i, j int) bool {
		return partitionKeys[i].position < partitionKeys[j].position
	})
	sort.Slice(clusteringKeys, func(i, j int) bool {
		return clusteringKeys[i].position < clusteringKeys[j].position
	})

	switch {
	case historyNodeKeyColumnsEqual(partitionKeys, "tree_id") &&
		historyNodeKeyColumnsEqual(clusteringKeys, "branch_id", "node_id", "txn_id") &&
		historyNodeClusteringOrderEqual(clusteringKeys, "asc", "asc", "desc"):
		return HistoryNodeTableLayoutLegacyV1, nil
	case historyNodeKeyColumnsEqual(partitionKeys, "tree_id", "branch_id") &&
		historyNodeKeyColumnsEqual(clusteringKeys, "node_id", "txn_id") &&
		historyNodeClusteringOrderEqual(clusteringKeys, "asc", "desc"):
		return HistoryNodeTableLayoutBranchV2, nil
	default:
		return HistoryNodeTableLayoutUnknown, nil
	}
}

func historyNodeKeyColumnsEqual(columns []historyNodeKeyColumn, names ...string) bool {
	if len(columns) != len(names) {
		return false
	}
	for i, name := range names {
		if columns[i].name != name || columns[i].position != i {
			return false
		}
	}
	return true
}

func historyNodeClusteringOrderEqual(columns []historyNodeKeyColumn, orders ...string) bool {
	if len(columns) != len(orders) {
		return false
	}
	for i, order := range orders {
		if columns[i].clusteringOrder != order {
			return false
		}
	}
	return true
}

// ValidateHistoryNodeMigrationModeSchema verifies that both history tables match the configured mode.
func ValidateHistoryNodeMigrationModeSchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	mode config.CassandraHistoryNodeMigrationMode,
) error {
	mode = normalizeHistoryNodeMigrationMode(mode)
	if err := ValidateHistoryNodeMigrationMode(mode); err != nil {
		return err
	}

	historyNodeLayout, err := GetHistoryNodeTableLayout(ctx, session, keyspace, historyNodeTableName)
	if err != nil {
		return err
	}
	v2Layout, err := GetHistoryNodeTableLayout(ctx, session, keyspace, historyNodeV2TableName)
	if err != nil {
		return err
	}
	allowed, err := allowedHistoryNodeModeLayouts(mode)
	if err != nil {
		return err
	}
	if !slices.Contains(allowed.historyNode, historyNodeLayout) {
		return historyNodeModeLayoutError(
			keyspace,
			historyNodeTableName,
			historyNodeLayout,
			mode,
			allowed.historyNode,
		)
	}
	if !slices.Contains(allowed.historyNodeV2, v2Layout) {
		return historyNodeModeLayoutError(
			keyspace,
			historyNodeV2TableName,
			v2Layout,
			mode,
			allowed.historyNodeV2,
		)
	}
	return nil
}

func allowedHistoryNodeModeLayouts(
	mode config.CassandraHistoryNodeMigrationMode,
) (historyNodeModeLayouts, error) {
	switch mode {
	case config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2:
		return historyNodeModeLayouts{
			historyNode:   []HistoryNodeTableLayout{HistoryNodeTableLayoutLegacyV1},
			historyNodeV2: []HistoryNodeTableLayout{HistoryNodeTableLayoutBranchV2, HistoryNodeTableLayoutMissing},
		}, nil
	case config.CassandraHistoryNodeMigrationModeOldV2RebuildV2:
		return historyNodeModeLayouts{
			historyNode:   []HistoryNodeTableLayout{HistoryNodeTableLayoutBranchV2},
			historyNodeV2: []HistoryNodeTableLayout{HistoryNodeTableLayoutBranchV2, HistoryNodeTableLayoutMissing},
		}, nil
	case config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
		config.CassandraHistoryNodeMigrationModeCanonicalDual:
		return historyNodeModeLayouts{
			historyNode:   []HistoryNodeTableLayout{HistoryNodeTableLayoutLegacyV1},
			historyNodeV2: []HistoryNodeTableLayout{HistoryNodeTableLayoutBranchV2},
		}, nil
	case config.CassandraHistoryNodeMigrationModeOldV2Dual,
		config.CassandraHistoryNodeMigrationModeOldV2CutoverDual:
		return historyNodeModeLayouts{
			historyNode:   []HistoryNodeTableLayout{HistoryNodeTableLayoutBranchV2},
			historyNodeV2: []HistoryNodeTableLayout{HistoryNodeTableLayoutBranchV2},
		}, nil
	case config.CassandraHistoryNodeMigrationModeV1RebuildDual,
		config.CassandraHistoryNodeMigrationModeV2Only:
		return historyNodeModeLayouts{
			historyNode: []HistoryNodeTableLayout{
				HistoryNodeTableLayoutMissing,
				HistoryNodeTableLayoutLegacyV1,
				HistoryNodeTableLayoutBranchV2,
			},
			historyNodeV2: []HistoryNodeTableLayout{HistoryNodeTableLayoutBranchV2},
		}, nil
	default:
		return historyNodeModeLayouts{}, fmt.Errorf(
			"unsupported Cassandra history node migration mode %q",
			mode,
		)
	}
}

func historyNodeModeLayoutError(
	keyspace string,
	table string,
	layout HistoryNodeTableLayout,
	mode config.CassandraHistoryNodeMigrationMode,
	expected []HistoryNodeTableLayout,
) error {
	expectedNames := make([]string, len(expected))
	for i, expectedLayout := range expected {
		expectedNames[i] = expectedLayout.String()
	}
	return fmt.Errorf(
		"%s.%s has %s layout, mode %q requires %s",
		keyspace,
		table,
		layout,
		mode,
		strings.Join(expectedNames, " or "),
	)
}

// ValidateHistoryNodeV2BackfillSchema checks the source and destination layouts for a forward backfill.
func ValidateHistoryNodeV2BackfillSchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
) error {
	sourceLayout, err := GetHistoryNodeTableLayout(ctx, session, keyspace, historyNodeTableName)
	if err != nil {
		return err
	}
	if sourceLayout != HistoryNodeTableLayoutLegacyV1 &&
		sourceLayout != HistoryNodeTableLayoutBranchV2 {
		return fmt.Errorf(
			"%s.%s has %s layout, forward backfill requires %s or %s",
			keyspace,
			historyNodeTableName,
			sourceLayout,
			HistoryNodeTableLayoutLegacyV1,
			HistoryNodeTableLayoutBranchV2,
		)
	}
	return validateHistoryNodeTableLayout(
		ctx,
		session,
		keyspace,
		historyNodeV2TableName,
		HistoryNodeTableLayoutBranchV2,
	)
}

// ValidateHistoryNodeV1BackfillSchema checks the source and destination layouts for a reverse backfill.
func ValidateHistoryNodeV1BackfillSchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
) error {
	if err := validateHistoryNodeTableLayout(
		ctx,
		session,
		keyspace,
		historyNodeV2TableName,
		HistoryNodeTableLayoutBranchV2,
	); err != nil {
		return err
	}
	return validateHistoryNodeTableLayout(
		ctx,
		session,
		keyspace,
		historyNodeTableName,
		HistoryNodeTableLayoutLegacyV1,
	)
}

// RecreateHistoryNodeV2 clears the inactive V2 table before its authoritative-source backfill.
// Callers must first move every Temporal process to a source-rebuild mode.
func RecreateHistoryNodeV2(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	confirmSourceRebuild bool,
) error {
	if !confirmSourceRebuild {
		return errors.New(
			"recreating history_node_v2 requires confirmation that every Temporal writer is in a source-rebuild mode",
		)
	}

	sourceLayout, err := GetHistoryNodeTableLayout(ctx, session, keyspace, historyNodeTableName)
	if err != nil {
		return err
	}
	if sourceLayout != HistoryNodeTableLayoutLegacyV1 &&
		sourceLayout != HistoryNodeTableLayoutBranchV2 {
		return fmt.Errorf(
			"refusing to rebuild %s.%s from %s.%s with %s layout",
			keyspace,
			historyNodeV2TableName,
			keyspace,
			historyNodeTableName,
			sourceLayout,
		)
	}

	currentLayout, err := GetHistoryNodeTableLayout(ctx, session, keyspace, historyNodeV2TableName)
	if err != nil {
		return err
	}
	switch currentLayout {
	case HistoryNodeTableLayoutBranchV2:
		if err := session.Query(
			fmt.Sprintf(templateDropHistoryNodeV2Table, quoteCQLIdentifier(keyspace)),
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("drop history_node_v2 before rebuild: %w", err)
		}
		if err := session.AwaitSchemaAgreement(ctx); err != nil {
			return fmt.Errorf("wait for history_node_v2 drop schema agreement: %w", err)
		}
	case HistoryNodeTableLayoutMissing:
	default:
		return fmt.Errorf(
			"refusing to recreate %s.%s with %s layout",
			keyspace,
			historyNodeV2TableName,
			currentLayout,
		)
	}

	if err := session.Query(
		fmt.Sprintf(templateCreateHistoryNodeV2, quoteCQLIdentifier(keyspace)),
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("create history_node_v2: %w", err)
	}
	if err := session.AwaitSchemaAgreement(ctx); err != nil {
		return fmt.Errorf("wait for history_node_v2 create schema agreement: %w", err)
	}
	return validateHistoryNodeTableLayout(
		ctx,
		session,
		keyspace,
		historyNodeV2TableName,
		HistoryNodeTableLayoutBranchV2,
	)
}

// RecreateHistoryNodeV1 replaces the old branch-partitioned table with the legacy layout.
// Callers must first move every Temporal process to V1-rebuild mode.
func RecreateHistoryNodeV1(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	confirmV1Rebuild bool,
) error {
	if !confirmV1Rebuild {
		return errors.New(
			"recreating history_node requires confirmation that every Temporal writer is in v1-rebuild mode",
		)
	}
	if err := validateHistoryNodeTableLayout(
		ctx,
		session,
		keyspace,
		historyNodeV2TableName,
		HistoryNodeTableLayoutBranchV2,
	); err != nil {
		return err
	}

	currentLayout, err := GetHistoryNodeTableLayout(ctx, session, keyspace, historyNodeTableName)
	if err != nil {
		return err
	}
	switch currentLayout {
	case HistoryNodeTableLayoutLegacyV1:
		return nil
	case HistoryNodeTableLayoutBranchV2:
		if err := session.Query(
			fmt.Sprintf(templateDropHistoryNodeTable, quoteCQLIdentifier(keyspace)),
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("drop old branch-partitioned history_node: %w", err)
		}
		if err := session.AwaitSchemaAgreement(ctx); err != nil {
			return fmt.Errorf("wait for history_node drop schema agreement: %w", err)
		}
	case HistoryNodeTableLayoutMissing:
	default:
		return fmt.Errorf(
			"refusing to recreate %s.%s with %s layout",
			keyspace,
			historyNodeTableName,
			currentLayout,
		)
	}

	if err := session.Query(
		fmt.Sprintf(templateCreateHistoryNodeV1, quoteCQLIdentifier(keyspace)),
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("create legacy history_node: %w", err)
	}
	if err := session.AwaitSchemaAgreement(ctx); err != nil {
		return fmt.Errorf("wait for history_node create schema agreement: %w", err)
	}
	return validateHistoryNodeTableLayout(
		ctx,
		session,
		keyspace,
		historyNodeTableName,
		HistoryNodeTableLayoutLegacyV1,
	)
}

func validateHistoryNodeTableLayout(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	table string,
	expected HistoryNodeTableLayout,
) error {
	layout, err := GetHistoryNodeTableLayout(ctx, session, keyspace, table)
	if err != nil {
		return err
	}
	if layout != expected {
		return fmt.Errorf(
			"%s.%s has %s layout, expected %s",
			keyspace,
			table,
			layout,
			expected,
		)
	}
	return nil
}

func quoteCQLIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
