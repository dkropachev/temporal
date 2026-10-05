package cassandra

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/primitives"
)

const (
	historyTreeTableName   = "history_tree"
	historyTreeV2TableName = "history_tree_v2"

	historyTreeV2BucketCount = 16

	historyTreePageTokenPrefix       = "\xff\xff\xff\xff\xff\xff\xff\xff\xffTEMPORAL-HISTORY-TREE-PAGE"
	historyTreePageTokenVersion      = byte(1)
	historyTreePageTokenHeaderLength = len(historyTreePageTokenPrefix) + 1 + 1 + 16 + 4
	historyTreeFNVOffset32           = uint32(2166136261)
	historyTreeFNVPrime32            = uint32(16777619)
	templateDropHistoryTreeV2Table   = `DROP TABLE IF EXISTS %s.history_tree_v2`
	templateCreateHistoryTreeV2      = `CREATE TABLE IF NOT EXISTS %s.history_tree_v2 (` +
		`tree_id uuid, branch_bucket int, branch_id uuid, branch blob, branch_encoding text, ` +
		`PRIMARY KEY ((tree_id, branch_bucket), branch_id)) ` +
		`WITH CLUSTERING ORDER BY (branch_id ASC) ` +
		`AND COMPACTION = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}`
)

type historyTreeReadLayout byte

const (
	historyTreeReadLayoutLegacyV1 historyTreeReadLayout = iota + 1
	historyTreeReadLayoutBucketV2
)

// HistoryTreeTableLayout identifies the primary-key layout of a history tree table.
type HistoryTreeTableLayout int

const (
	HistoryTreeTableLayoutMissing HistoryTreeTableLayout = iota
	HistoryTreeTableLayoutLegacyV1
	HistoryTreeTableLayoutBucketV2
	HistoryTreeTableLayoutUnknown
)

type historyTreeMutationPlan struct {
	primaryQuery        string
	mirrorQuery         string
	optionalMirrorTable string
}

type historyTreeModeLayouts struct {
	historyTree   []HistoryTreeTableLayout
	historyTreeV2 []HistoryTreeTableLayout
}

func (l HistoryTreeTableLayout) String() string {
	switch l {
	case HistoryTreeTableLayoutMissing:
		return "missing"
	case HistoryTreeTableLayoutLegacyV1:
		return "legacy-v1"
	case HistoryTreeTableLayoutBucketV2:
		return "bucket-v2"
	case HistoryTreeTableLayoutUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("unknown(%d)", l)
	}
}

func historyTreeBranchBucket(branchID string) (int, error) {
	uuid, err := primitives.ParseUUID(branchID)
	if err != nil {
		return 0, fmt.Errorf("parse history branch ID: %w", err)
	}
	if len(uuid) != 16 {
		return 0, errors.New("history branch ID must be a non-empty UUID")
	}
	hash := historyTreeFNVOffset32
	for _, value := range uuid {
		hash ^= uint32(value)
		hash *= historyTreeFNVPrime32
	}
	return int(hash % historyTreeV2BucketCount), nil
}

func normalizeHistoryTreeMigrationMode(
	mode config.CassandraHistoryTreeMigrationMode,
) config.CassandraHistoryTreeMigrationMode {
	if mode == "" {
		return config.CassandraHistoryTreeMigrationModeSourceOnly
	}
	return mode
}

// ValidateHistoryTreeMigrationMode checks whether a configured tree migration mode is supported.
func ValidateHistoryTreeMigrationMode(mode config.CassandraHistoryTreeMigrationMode) error {
	switch normalizeHistoryTreeMigrationMode(mode) {
	case config.CassandraHistoryTreeMigrationModeSourceOnly,
		config.CassandraHistoryTreeMigrationModeSourceRebuild,
		config.CassandraHistoryTreeMigrationModeSourceDual,
		config.CassandraHistoryTreeMigrationModeTargetPrepare,
		config.CassandraHistoryTreeMigrationModeTargetDual,
		config.CassandraHistoryTreeMigrationModeTargetOnly:
		return nil
	default:
		return fmt.Errorf("unsupported Cassandra history tree migration mode %q", mode)
	}
}

func (h *HistoryStore) historyTreeMutationPlan(
	legacyQuery string,
	v2Query string,
) (historyTreeMutationPlan, error) {
	switch h.historyTreeMigrationMode {
	case config.CassandraHistoryTreeMigrationModeSourceOnly:
		return historyTreeMutationPlan{primaryQuery: legacyQuery}, nil
	case config.CassandraHistoryTreeMigrationModeSourceRebuild:
		return historyTreeMutationPlan{
			primaryQuery:        legacyQuery,
			mirrorQuery:         v2Query,
			optionalMirrorTable: historyTreeV2TableName,
		}, nil
	case config.CassandraHistoryTreeMigrationModeSourceDual:
		return historyTreeMutationPlan{primaryQuery: legacyQuery, mirrorQuery: v2Query}, nil
	case config.CassandraHistoryTreeMigrationModeTargetPrepare,
		config.CassandraHistoryTreeMigrationModeTargetDual:
		return historyTreeMutationPlan{primaryQuery: v2Query, mirrorQuery: legacyQuery}, nil
	case config.CassandraHistoryTreeMigrationModeTargetOnly:
		return historyTreeMutationPlan{primaryQuery: v2Query}, nil
	default:
		return historyTreeMutationPlan{}, fmt.Errorf(
			"unsupported Cassandra history tree migration mode %q",
			h.historyTreeMigrationMode,
		)
	}
}

func (h *HistoryStore) historyTreeReadLayout() (historyTreeReadLayout, error) {
	switch h.historyTreeMigrationMode {
	case config.CassandraHistoryTreeMigrationModeSourceOnly,
		config.CassandraHistoryTreeMigrationModeSourceRebuild,
		config.CassandraHistoryTreeMigrationModeSourceDual,
		config.CassandraHistoryTreeMigrationModeTargetPrepare:
		return historyTreeReadLayoutLegacyV1, nil
	case config.CassandraHistoryTreeMigrationModeTargetDual,
		config.CassandraHistoryTreeMigrationModeTargetOnly:
		return historyTreeReadLayoutBucketV2, nil
	default:
		return 0, fmt.Errorf(
			"unsupported Cassandra history tree migration mode %q",
			h.historyTreeMigrationMode,
		)
	}
}

func (h *HistoryStore) validateHistoryTreeContinuationLayout(layout historyTreeReadLayout) error {
	configuredLayout, err := h.historyTreeReadLayout()
	if err != nil {
		return err
	}
	if layout == configuredLayout {
		return nil
	}
	if h.historyTreeMigrationMode == config.CassandraHistoryTreeMigrationModeSourceOnly ||
		h.historyTreeMigrationMode == config.CassandraHistoryTreeMigrationModeTargetOnly ||
		h.historyTreeMigrationMode == config.CassandraHistoryTreeMigrationModeSourceRebuild {
		return &p.InvalidPersistenceRequestError{
			Msg: "history tree page token uses a retired layout; restart pagination",
		}
	}
	if layout != historyTreeReadLayoutLegacyV1 && layout != historyTreeReadLayoutBucketV2 {
		return &p.InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("history tree page token has invalid layout %d", layout),
		}
	}
	return nil
}

func (h *HistoryStore) historyTreeGenerationForLayout(layout historyTreeReadLayout) [16]byte {
	switch layout {
	case historyTreeReadLayoutLegacyV1:
		return h.historyNodeGenerations.historyTree
	case historyTreeReadLayoutBucketV2:
		return h.historyNodeGenerations.historyTreeV2
	default:
		return [16]byte{}
	}
}

func (h *HistoryStore) encodeHistoryTreePageState(
	pageState []byte,
	layout historyTreeReadLayout,
) []byte {
	if len(pageState) == 0 || layout == historyTreeReadLayoutLegacyV1 {
		return pageState
	}
	token := make([]byte, historyTreePageTokenHeaderLength+len(pageState))
	offset := copy(token, historyTreePageTokenPrefix)
	token[offset] = historyTreePageTokenVersion
	offset++
	token[offset] = byte(layout)
	offset++
	generation := h.historyTreeGenerationForLayout(layout)
	copy(token[offset:], generation[:])
	offset += len(generation)
	binary.BigEndian.PutUint32(token[offset:], uint32(len(pageState)))
	offset += 4
	copy(token[offset:], pageState)
	return token
}

func (h *HistoryStore) decodeHistoryTreePageState(
	pageToken []byte,
) (historyTreeReadLayout, []byte, error) {
	configuredLayout, err := h.historyTreeReadLayout()
	if err != nil {
		return 0, nil, err
	}
	if len(pageToken) == 0 {
		return configuredLayout, nil, nil
	}
	prefix := []byte(historyTreePageTokenPrefix)
	if !bytes.HasPrefix(pageToken, prefix) {
		if h.historyTreeMigrationMode == config.CassandraHistoryTreeMigrationModeTargetOnly {
			return 0, nil, &p.InvalidPersistenceRequestError{
				Msg: "history tree page token has no source-layout metadata; restart pagination",
			}
		}
		return historyTreeReadLayoutLegacyV1, pageToken, nil
	}
	if len(pageToken) < historyTreePageTokenHeaderLength {
		return 0, nil, &p.InvalidPersistenceRequestError{
			Msg: "history tree page token is truncated; restart pagination",
		}
	}
	offset := len(prefix)
	if pageToken[offset] != historyTreePageTokenVersion {
		return 0, nil, &p.InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("history tree page token version %d is invalid", pageToken[offset]),
		}
	}
	offset++
	layout := historyTreeReadLayout(pageToken[offset])
	offset++
	if layout != historyTreeReadLayoutLegacyV1 && layout != historyTreeReadLayoutBucketV2 {
		return 0, nil, &p.InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("history tree page token layout %d is invalid", layout),
		}
	}
	expectedGeneration := h.historyTreeGenerationForLayout(layout)
	if !slices.Equal(pageToken[offset:offset+16], expectedGeneration[:]) {
		return 0, nil, &p.InvalidPersistenceRequestError{
			Msg: "history tree page token refers to a different table generation; restart pagination",
		}
	}
	offset += 16
	pageStateLength := int(binary.BigEndian.Uint32(pageToken[offset:]))
	offset += 4
	if pageStateLength == 0 || pageStateLength != len(pageToken)-offset {
		return 0, nil, &p.InvalidPersistenceRequestError{
			Msg: "history tree page token length is invalid; restart pagination",
		}
	}
	if err := h.validateHistoryTreeContinuationLayout(layout); err != nil {
		return 0, nil, err
	}
	return layout, pageToken[offset:], nil
}

// GetHistoryTreeTableLayout reads a history tree table's primary-key layout.
func GetHistoryTreeTableLayout(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	table string,
) (HistoryTreeTableLayout, error) {
	if keyspace == "" {
		return HistoryTreeTableLayoutUnknown, errors.New("history tree schema validation requires a keyspace")
	}
	iter := session.Query(
		templateGetHistoryNodeSchemaColumns,
		keyspace,
		table,
	).WithContext(ctx).Iter()
	var partitionKeys []historyNodeKeyColumn
	var clusteringKeys []historyNodeKeyColumn
	for {
		var columnName, kind, order string
		var position int
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
		return HistoryTreeTableLayoutUnknown, fmt.Errorf("read schema for %s.%s: %w", keyspace, table, err)
	}
	if len(partitionKeys) == 0 && len(clusteringKeys) == 0 {
		return HistoryTreeTableLayoutMissing, nil
	}
	sort.Slice(partitionKeys, func(i, j int) bool { return partitionKeys[i].position < partitionKeys[j].position })
	sort.Slice(clusteringKeys, func(i, j int) bool { return clusteringKeys[i].position < clusteringKeys[j].position })
	switch {
	case historyNodeKeyColumnsEqual(partitionKeys, "tree_id") &&
		historyNodeKeyColumnsEqual(clusteringKeys, "branch_id") &&
		historyNodeClusteringOrderEqual(clusteringKeys, "asc"):
		return HistoryTreeTableLayoutLegacyV1, nil
	case historyNodeKeyColumnsEqual(partitionKeys, "tree_id", "branch_bucket") &&
		historyNodeKeyColumnsEqual(clusteringKeys, "branch_id") &&
		historyNodeClusteringOrderEqual(clusteringKeys, "asc"):
		return HistoryTreeTableLayoutBucketV2, nil
	default:
		return HistoryTreeTableLayoutUnknown, nil
	}
}

// ValidateHistoryTreeMigrationModeSchema verifies layouts required by the configured history mode.
func ValidateHistoryTreeMigrationModeSchema(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	mode config.CassandraHistoryTreeMigrationMode,
) error {
	mode = normalizeHistoryTreeMigrationMode(mode)
	if err := ValidateHistoryTreeMigrationMode(mode); err != nil {
		return err
	}
	legacyLayout, err := GetHistoryTreeTableLayout(ctx, session, keyspace, historyTreeTableName)
	if err != nil {
		return err
	}
	v2Layout, err := GetHistoryTreeTableLayout(ctx, session, keyspace, historyTreeV2TableName)
	if err != nil {
		return err
	}
	allowed, err := allowedHistoryTreeModeLayouts(mode)
	if err != nil {
		return err
	}
	if !slices.Contains(allowed.historyTree, legacyLayout) {
		return historyTreeModeLayoutError(keyspace, historyTreeTableName, legacyLayout, mode, allowed.historyTree)
	}
	if !slices.Contains(allowed.historyTreeV2, v2Layout) {
		return historyTreeModeLayoutError(keyspace, historyTreeV2TableName, v2Layout, mode, allowed.historyTreeV2)
	}
	if mode == config.CassandraHistoryTreeMigrationModeSourceDual ||
		mode == config.CassandraHistoryTreeMigrationModeTargetPrepare ||
		mode == config.CassandraHistoryTreeMigrationModeTargetDual {
		return validateHistoryTreeSourceAuthorityColumns(ctx, session, keyspace)
	}
	return nil
}

func validateHistoryTreeSourceAuthorityColumns(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
) error {
	iter := session.Query(
		templateGetHistoryNodeSchemaColumns,
		keyspace,
		historyTreeTableName,
	).WithContext(ctx).Iter()
	columns := make(map[string]struct{})
	for {
		var name, kind, order string
		var position int
		if !iter.Scan(&name, &kind, &position, &order) {
			break
		}
		if kind == "regular" || kind == "static" {
			columns[name] = struct{}{}
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("read history tree authority schema: %w", err)
	}
	for _, required := range []string{"migration_authority", "migration_timestamp"} {
		if _, ok := columns[required]; !ok {
			return fmt.Errorf("%s.%s is missing required migration column %q", keyspace, historyTreeTableName, required)
		}
	}
	return nil
}

func allowedHistoryTreeModeLayouts(
	mode config.CassandraHistoryTreeMigrationMode,
) (historyTreeModeLayouts, error) {
	switch mode {
	case config.CassandraHistoryTreeMigrationModeSourceOnly,
		config.CassandraHistoryTreeMigrationModeSourceRebuild:
		return historyTreeModeLayouts{
			historyTree:   []HistoryTreeTableLayout{HistoryTreeTableLayoutLegacyV1},
			historyTreeV2: []HistoryTreeTableLayout{HistoryTreeTableLayoutBucketV2, HistoryTreeTableLayoutMissing},
		}, nil
	case config.CassandraHistoryTreeMigrationModeSourceDual,
		config.CassandraHistoryTreeMigrationModeTargetPrepare,
		config.CassandraHistoryTreeMigrationModeTargetDual:
		return historyTreeModeLayouts{
			historyTree:   []HistoryTreeTableLayout{HistoryTreeTableLayoutLegacyV1},
			historyTreeV2: []HistoryTreeTableLayout{HistoryTreeTableLayoutBucketV2},
		}, nil
	case config.CassandraHistoryTreeMigrationModeTargetOnly:
		return historyTreeModeLayouts{
			historyTree: []HistoryTreeTableLayout{
				HistoryTreeTableLayoutMissing,
				HistoryTreeTableLayoutLegacyV1,
			},
			historyTreeV2: []HistoryTreeTableLayout{HistoryTreeTableLayoutBucketV2},
		}, nil
	default:
		return historyTreeModeLayouts{}, fmt.Errorf("unsupported cassandra history tree migration mode %q", mode)
	}
}

func historyTreeModeLayoutError(
	keyspace string,
	table string,
	layout HistoryTreeTableLayout,
	mode config.CassandraHistoryTreeMigrationMode,
	expected []HistoryTreeTableLayout,
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

// RecreateHistoryTreeV2 clears the inactive bucketed table before a source-authoritative backfill.
func RecreateHistoryTreeV2(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	confirmSourceRebuild bool,
) error {
	if !confirmSourceRebuild {
		return errors.New(
			"recreating history_tree_v2 requires confirmation that every Temporal writer is in a source-rebuild mode",
		)
	}
	if layout, err := GetHistoryTreeTableLayout(ctx, session, keyspace, historyTreeTableName); err != nil {
		return err
	} else if layout != HistoryTreeTableLayoutLegacyV1 {
		return fmt.Errorf("refusing to rebuild %s.%s from %s layout", keyspace, historyTreeV2TableName, layout)
	}
	currentLayout, err := GetHistoryTreeTableLayout(ctx, session, keyspace, historyTreeV2TableName)
	if err != nil {
		return err
	}
	switch currentLayout {
	case HistoryTreeTableLayoutBucketV2:
		if err := session.Query(
			fmt.Sprintf(templateDropHistoryTreeV2Table, quoteCQLIdentifier(keyspace)),
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("drop history_tree_v2 before rebuild: %w", err)
		}
		if err := session.AwaitSchemaAgreement(ctx); err != nil {
			return fmt.Errorf("wait for history_tree_v2 drop schema agreement: %w", err)
		}
	case HistoryTreeTableLayoutMissing:
	default:
		return fmt.Errorf("refusing to recreate %s.%s with %s layout", keyspace, historyTreeV2TableName, currentLayout)
	}
	if err := session.Query(
		fmt.Sprintf(templateCreateHistoryTreeV2, quoteCQLIdentifier(keyspace)),
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("create history_tree_v2: %w", err)
	}
	if err := session.AwaitSchemaAgreement(ctx); err != nil {
		return fmt.Errorf("wait for history_tree_v2 create schema agreement: %w", err)
	}
	layout, err := GetHistoryTreeTableLayout(ctx, session, keyspace, historyTreeV2TableName)
	if err != nil {
		return err
	}
	if layout != HistoryTreeTableLayoutBucketV2 {
		return fmt.Errorf("%s.%s has %s layout, expected %s", keyspace, historyTreeV2TableName, layout, HistoryTreeTableLayoutBucketV2)
	}
	return nil
}
