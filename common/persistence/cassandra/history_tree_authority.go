package cassandra

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type historyTreeMigrationAuthority int

const (
	historyTreeMigrationAuthorityUnspecified historyTreeMigrationAuthority = iota
	historyTreeMigrationAuthoritySource
	historyTreeMigrationAuthoritySealing
	historyTreeMigrationAuthorityTarget
)

const (
	historyTreeAuthorityBranchID = "00000000-0000-0000-0000-000000000000"
)

const (
	templateGetHistoryTreeAuthority = `SELECT migration_authority, migration_timestamp FROM history_tree ` +
		`WHERE tree_id = ? AND branch_id = ?`
	templateInitializeHistoryTreeAuthority = `INSERT INTO history_tree ` +
		`(tree_id, branch_id, migration_authority, migration_timestamp) VALUES (?, ?, ?, ?) IF NOT EXISTS`
	templateReserveHistoryTreeMutationTimestamp = `UPDATE history_tree SET migration_timestamp = ? ` +
		`WHERE tree_id = ? AND branch_id = ? IF migration_authority = ? AND migration_timestamp = ?`
	templateGuardHistoryTreeAuthority = `UPDATE history_tree SET migration_authority = ? ` +
		`WHERE tree_id = ? AND branch_id = ? IF migration_authority = ? AND migration_timestamp = ?`
	templateTransitionHistoryTreeAuthority = `UPDATE history_tree ` +
		`SET migration_authority = ?, migration_timestamp = ? WHERE tree_id = ? AND branch_id = ? ` +
		`IF migration_authority = ? AND migration_timestamp = ?`
	templateReadAnyLegacyHistoryTreeBranch = `SELECT branch_id FROM history_tree WHERE tree_id = ?`
)

type historyTreeAuthorityRecord struct {
	authority historyTreeMigrationAuthority
	timestamp int64
}

type historyTreeMutation struct {
	authority historyTreeMigrationAuthority
	timestamp int64
}

func (a historyTreeMigrationAuthority) String() string {
	switch a {
	case historyTreeMigrationAuthoritySource:
		return "source"
	case historyTreeMigrationAuthoritySealing:
		return "sealing"
	case historyTreeMigrationAuthorityTarget:
		return "target"
	default:
		return fmt.Sprintf("unknown(%d)", a)
	}
}

func readHistoryTreeAuthority(
	ctx context.Context,
	session gocql.Session,
	treeID string,
) (historyTreeAuthorityRecord, error) {
	var authority *int
	var timestamp *int64
	err := session.Query(
		templateGetHistoryTreeAuthority,
		treeID,
		historyTreeAuthorityBranchID,
	).WithContext(ctx).Scan(&authority, &timestamp)
	if err != nil {
		return historyTreeAuthorityRecord{}, err
	}
	record := historyTreeAuthorityRecord{}
	if authority != nil {
		record.authority = historyTreeMigrationAuthority(*authority)
	}
	if timestamp != nil {
		record.timestamp = *timestamp
	}
	return record, nil
}

func initializeHistoryTreeAuthority(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	authority historyTreeMigrationAuthority,
) (historyTreeAuthorityRecord, error) {
	timestamp := time.Now().UTC().UnixMicro()
	applied, err := session.Query(
		templateInitializeHistoryTreeAuthority,
		treeID,
		historyTreeAuthorityBranchID,
		int(authority),
		timestamp,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return historyTreeAuthorityRecord{}, gocql.ConvertError("InitializeHistoryTreeAuthority", err)
	}
	if applied {
		return historyTreeAuthorityRecord{authority: authority, timestamp: timestamp}, nil
	}
	record, err := readHistoryTreeAuthority(ctx, session, treeID)
	if err != nil {
		return historyTreeAuthorityRecord{}, gocql.ConvertError("ReadHistoryTreeAuthorityAfterInitialize", err)
	}
	return record, nil
}

func historyTreeHasLegacyBranches(
	ctx context.Context,
	session gocql.Session,
	treeID string,
) (bool, error) {
	iter := session.Query(templateReadAnyLegacyHistoryTreeBranch, treeID).WithContext(ctx).Iter()
	for {
		var branchID string
		if !iter.Scan(&branchID) {
			break
		}
		if branchID != historyTreeAuthorityBranchID {
			_ = iter.Close()
			return true, nil
		}
	}
	if err := iter.Close(); err != nil {
		return false, err
	}
	return false, nil
}

func historyTreeHasV2Branches(
	ctx context.Context,
	session gocql.Session,
	treeID string,
) (bool, error) {
	for bucket := range historyTreeV2BucketCount {
		var branchID string
		err := session.Query(
			`SELECT branch_id FROM history_tree_v2 WHERE tree_id = ? AND branch_bucket = ? LIMIT 1`,
			treeID,
			bucket,
		).WithContext(ctx).Scan(&branchID)
		if err == nil {
			return true, nil
		}
		if !gocql.IsNotFoundError(err) {
			return false, err
		}
	}
	return false, nil
}

func initialHistoryTreeAuthority(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	preferTargetForEmpty bool,
) (historyTreeMigrationAuthority, error) {
	if !preferTargetForEmpty {
		return historyTreeMigrationAuthoritySource, nil
	}
	hasLegacy, err := historyTreeHasLegacyBranches(ctx, session, treeID)
	if err != nil {
		return historyTreeMigrationAuthorityUnspecified, err
	}
	if hasLegacy {
		return historyTreeMigrationAuthoritySource, nil
	}
	hasV2, err := historyTreeHasV2Branches(ctx, session, treeID)
	if err != nil {
		return historyTreeMigrationAuthorityUnspecified, err
	}
	if hasV2 {
		return historyTreeMigrationAuthoritySource, nil
	}
	return historyTreeMigrationAuthorityTarget, nil
}

//nolint:revive // Reserving a timestamp is a retrying authority state machine with explicit conflict handling.
func reserveHistoryTreeMutationTimestamp(
	ctx context.Context,
	session gocql.Session,
	treeID string,
	allowed ...historyTreeMigrationAuthority,
) (historyTreeMutation, error) {
	for {
		if err := ctx.Err(); err != nil {
			return historyTreeMutation{}, err
		}
		record, err := readHistoryTreeAuthority(ctx, session, treeID)
		if gocql.IsNotFoundError(err) || (err == nil && record.authority == historyTreeMigrationAuthorityUnspecified) {
			preferTargetForEmpty := false
			for _, authority := range allowed {
				preferTargetForEmpty = preferTargetForEmpty || authority == historyTreeMigrationAuthorityTarget
			}
			initial, initialErr := initialHistoryTreeAuthority(
				ctx,
				session,
				treeID,
				preferTargetForEmpty,
			)
			if initialErr != nil {
				return historyTreeMutation{}, gocql.ConvertError("InspectHistoryTreeForAuthority", initialErr)
			}
			record, err = initializeHistoryTreeAuthority(ctx, session, treeID, initial)
		}
		if err != nil {
			return historyTreeMutation{}, gocql.ConvertError("ReadHistoryTreeAuthority", err)
		}
		allowedAuthority := false
		for _, authority := range allowed {
			allowedAuthority = allowedAuthority || record.authority == authority
		}
		if !allowedAuthority {
			return historyTreeMutation{}, historyTreeAuthorityConflict(treeID, allowed, record)
		}
		if record.timestamp == math.MaxInt64 {
			return historyTreeMutation{}, errors.New("history tree migration timestamp overflow")
		}
		timestamp := max(time.Now().UTC().UnixMicro(), record.timestamp+1)
		applied, err := session.Query(
			templateReserveHistoryTreeMutationTimestamp,
			timestamp,
			treeID,
			historyTreeAuthorityBranchID,
			int(record.authority),
			record.timestamp,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return historyTreeMutation{}, gocql.ConvertError("ReserveHistoryTreeMutationTimestamp", err)
		}
		if applied {
			return historyTreeMutation{authority: record.authority, timestamp: timestamp}, nil
		}
	}
}

func historyTreeAuthorityConflict(
	treeID string,
	expected []historyTreeMigrationAuthority,
	actual historyTreeAuthorityRecord,
) error {
	expectedNames := make([]string, len(expected))
	for index, authority := range expected {
		expectedNames[index] = authority.String()
	}
	return &p.ConditionFailedError{Msg: fmt.Sprintf(
		"history tree %s requires authority %v; persisted authority is %s at timestamp %d",
		treeID,
		expectedNames,
		actual.authority,
		actual.timestamp,
	)}
}

func (h *HistoryStore) prepareHistoryTreeMutation(
	ctx context.Context,
	treeID string,
	branchID string,
) (historyTreeMutation, error) {
	if branchID == historyTreeAuthorityBranchID &&
		h.historyTreeMigrationMode != "" {
		return historyTreeMutation{}, errors.New("history branch ID is reserved for Cassandra migration metadata")
	}
	switch h.historyTreeMigrationMode {
	case "", // Retained for stores constructed directly in older tests.
		config.CassandraHistoryTreeMigrationModeSourceOnly,
		config.CassandraHistoryTreeMigrationModeSourceRebuild,
		config.CassandraHistoryTreeMigrationModeTargetOnly:
		return historyTreeMutation{timestamp: time.Now().UTC().UnixMicro()}, nil
	case config.CassandraHistoryTreeMigrationModeSourceDual:
		return reserveHistoryTreeMutationTimestamp(
			ctx,
			h.Session,
			treeID,
			historyTreeMigrationAuthoritySource,
		)
	case config.CassandraHistoryTreeMigrationModeTargetPrepare:
		return reserveHistoryTreeMutationTimestamp(
			ctx,
			h.Session,
			treeID,
			historyTreeMigrationAuthoritySource,
			historyTreeMigrationAuthorityTarget,
		)
	case config.CassandraHistoryTreeMigrationModeTargetDual:
		return reserveHistoryTreeMutationTimestamp(
			ctx,
			h.Session,
			treeID,
			historyTreeMigrationAuthorityTarget,
		)
	default:
		return historyTreeMutation{}, fmt.Errorf(
			"unsupported Cassandra history tree migration mode %q",
			h.historyTreeMigrationMode,
		)
	}
}

func (h *HistoryStore) executeGuardedHistoryTreeSourceMutation(
	ctx context.Context,
	mutation historyTreeMutation,
	treeID string,
	operation string,
	query string,
	args ...any,
) error {
	batch := h.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx).WithTimestamp(mutation.timestamp)
	batch.Query(query, args...)
	batch.Query(
		templateGuardHistoryTreeAuthority,
		int(mutation.authority),
		treeID,
		historyTreeAuthorityBranchID,
		int(mutation.authority),
		mutation.timestamp,
	)
	applied, iter, err := h.Session.MapExecuteBatchCAS(batch, make(map[string]any))
	if iter != nil {
		defer func() { _ = iter.Close() }()
	}
	if err != nil {
		return gocql.ConvertError(operation, err)
	}
	if applied {
		return nil
	}
	record, err := readHistoryTreeAuthority(ctx, h.Session, treeID)
	if err != nil {
		return gocql.ConvertError("ReadHistoryTreeAuthorityAfterConflict", err)
	}
	return historyTreeAuthorityConflict(treeID, []historyTreeMigrationAuthority{mutation.authority}, record)
}
