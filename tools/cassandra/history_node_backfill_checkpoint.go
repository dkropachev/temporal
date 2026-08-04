package cassandra

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	persistencecassandra "go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	historyNodeBackfillCheckpointVersion = 2

	getHistoryNodeBackfillTableIDQuery = `SELECT id FROM system_schema.tables ` +
		`WHERE keyspace_name = ? AND table_name = ?`
)

type historyNodeBackfillCheckpointIdentity struct {
	Direction       string `json:"direction"`
	Keyspace        string `json:"keyspace"`
	SourceTable     string `json:"source_table"`
	SourceTableID   string `json:"source_table_id"`
	SourceLayout    string `json:"source_layout"`
	TargetTable     string `json:"target_table"`
	TargetTableID   string `json:"target_table_id"`
	Partitioner     string `json:"partitioner"`
	TokenRangeCount int    `json:"token_range_count"`
}

type historyNodeBackfillCheckpoint struct {
	Version         int                                   `json:"version"`
	Identity        historyNodeBackfillCheckpointIdentity `json:"identity"`
	CompletedRanges []int                                 `json:"completed_ranges"`
}

type historyNodeBackfillRangeFunc func(
	context.Context,
	persistencecassandra.HistoryNodeBackfillTokenRange,
) (int64, error)

type historyNodeBackfillIdentityValidator func(context.Context) error

func newHistoryNodeBackfillCheckpointIdentity(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	direction string,
	sourceTable string,
	sourceLayout persistencecassandra.HistoryNodeTableLayout,
	targetTable string,
	tokenRangeCount int,
) (historyNodeBackfillCheckpointIdentity, error) {
	sourceTableID, err := readHistoryNodeBackfillTableID(ctx, session, keyspace, sourceTable)
	if err != nil {
		return historyNodeBackfillCheckpointIdentity{}, err
	}
	targetTableID, err := readHistoryNodeBackfillTableID(ctx, session, keyspace, targetTable)
	if err != nil {
		return historyNodeBackfillCheckpointIdentity{}, err
	}
	partitioner, err := persistencecassandra.GetHistoryNodeBackfillPartitioner(ctx, session)
	if err != nil {
		return historyNodeBackfillCheckpointIdentity{}, err
	}
	if partitioner != persistencecassandra.HistoryNodeBackfillMurmur3Partitioner {
		return historyNodeBackfillCheckpointIdentity{}, fmt.Errorf(
			"history node backfill requires Cassandra partitioner %q, got %q",
			persistencecassandra.HistoryNodeBackfillMurmur3Partitioner,
			partitioner,
		)
	}
	return historyNodeBackfillCheckpointIdentity{
		Direction:       direction,
		Keyspace:        keyspace,
		SourceTable:     sourceTable,
		SourceTableID:   sourceTableID,
		SourceLayout:    sourceLayout.String(),
		TargetTable:     targetTable,
		TargetTableID:   targetTableID,
		Partitioner:     partitioner,
		TokenRangeCount: tokenRangeCount,
	}, nil
}

func readHistoryNodeBackfillTableID(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	table string,
) (string, error) {
	var id [16]byte
	if err := session.Query(
		getHistoryNodeBackfillTableIDQuery,
		keyspace,
		table,
	).WithContext(ctx).Scan(&id); err != nil {
		return "", fmt.Errorf("read table ID for %s.%s: %w", keyspace, table, err)
	}
	return hex.EncodeToString(id[:]), nil
}

func runHistoryNodeBackfill(
	ctx context.Context,
	checkpointPath string,
	identity historyNodeBackfillCheckpointIdentity,
	validateIdentity historyNodeBackfillIdentityValidator,
	backfillRange historyNodeBackfillRangeFunc,
) (int64, error) {
	if checkpointPath == "" {
		return 0, errors.New("history node backfill checkpoint file is required")
	}
	if validateIdentity == nil {
		return 0, errors.New("history node backfill identity validator is required")
	}
	if err := validateIdentity(ctx); err != nil {
		return 0, fmt.Errorf("validate history node backfill tables: %w", err)
	}
	tokenRanges, err := persistencecassandra.HistoryNodeBackfillTokenRanges(identity.TokenRangeCount)
	if err != nil {
		return 0, err
	}

	checkpoint, found, err := loadHistoryNodeBackfillCheckpoint(checkpointPath, identity)
	if err != nil {
		return 0, err
	}
	if !found {
		checkpoint = historyNodeBackfillCheckpoint{
			Version:  historyNodeBackfillCheckpointVersion,
			Identity: identity,
		}
		if err := saveHistoryNodeBackfillCheckpoint(checkpointPath, checkpoint); err != nil {
			return 0, err
		}
	}

	completed := make(map[int]struct{}, len(checkpoint.CompletedRanges))
	for _, index := range checkpoint.CompletedRanges {
		completed[index] = struct{}{}
	}

	var copied int64
	for _, tokenRange := range tokenRanges {
		if _, ok := completed[tokenRange.Index]; ok {
			continue
		}
		if err := ctx.Err(); err != nil {
			return copied, err
		}
		rangeCopied, err := backfillRange(ctx, tokenRange)
		copied += rangeCopied
		if err != nil {
			return copied, fmt.Errorf(
				"backfill token range %d [%d, %d]: %w",
				tokenRange.Index,
				tokenRange.StartToken,
				tokenRange.EndToken,
				err,
			)
		}

		checkpoint.CompletedRanges = append(checkpoint.CompletedRanges, tokenRange.Index)
		if err := saveHistoryNodeBackfillCheckpoint(checkpointPath, checkpoint); err != nil {
			return copied, fmt.Errorf(
				"save checkpoint after token range %d: %w",
				tokenRange.Index,
				err,
			)
		}
	}
	if err := validateIdentity(ctx); err != nil {
		return copied, fmt.Errorf("validate history node backfill tables after copy: %w", err)
	}
	return copied, nil
}

func validateHistoryNodeBackfillCheckpointIdentity(
	ctx context.Context,
	session gocql.Session,
	identity historyNodeBackfillCheckpointIdentity,
) error {
	sourceTableID, err := readHistoryNodeBackfillTableID(
		ctx,
		session,
		identity.Keyspace,
		identity.SourceTable,
	)
	if err != nil {
		return err
	}
	targetTableID, err := readHistoryNodeBackfillTableID(
		ctx,
		session,
		identity.Keyspace,
		identity.TargetTable,
	)
	if err != nil {
		return err
	}
	partitioner, err := persistencecassandra.GetHistoryNodeBackfillPartitioner(ctx, session)
	if err != nil {
		return err
	}
	if sourceTableID != identity.SourceTableID ||
		targetTableID != identity.TargetTableID ||
		partitioner != identity.Partitioner {
		return errors.New("history node backfill source or target changed during the run")
	}
	return nil
}

func loadHistoryNodeBackfillCheckpoint(
	path string,
	identity historyNodeBackfillCheckpointIdentity,
) (historyNodeBackfillCheckpoint, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return historyNodeBackfillCheckpoint{}, false, nil
	}
	if err != nil {
		return historyNodeBackfillCheckpoint{}, false, fmt.Errorf(
			"read history node backfill checkpoint %q: %w",
			path,
			err,
		)
	}

	var checkpoint historyNodeBackfillCheckpoint
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return historyNodeBackfillCheckpoint{}, false, fmt.Errorf(
			"decode history node backfill checkpoint %q: %w",
			path,
			err,
		)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return historyNodeBackfillCheckpoint{}, false, fmt.Errorf(
			"decode history node backfill checkpoint %q: %w",
			path,
			err,
		)
	}
	if checkpoint.Version != historyNodeBackfillCheckpointVersion {
		return historyNodeBackfillCheckpoint{}, false, fmt.Errorf(
			"history node backfill checkpoint %q has version %d, expected %d",
			path,
			checkpoint.Version,
			historyNodeBackfillCheckpointVersion,
		)
	}
	if checkpoint.Identity != identity {
		return historyNodeBackfillCheckpoint{}, false, fmt.Errorf(
			"history node backfill checkpoint %q does not match this backfill",
			path,
		)
	}

	completed := make(map[int]struct{}, len(checkpoint.CompletedRanges))
	for _, index := range checkpoint.CompletedRanges {
		if index < 0 || index >= identity.TokenRangeCount {
			return historyNodeBackfillCheckpoint{}, false, fmt.Errorf(
				"history node backfill checkpoint %q contains invalid token range %d",
				path,
				index,
			)
		}
		if _, ok := completed[index]; ok {
			return historyNodeBackfillCheckpoint{}, false, fmt.Errorf(
				"history node backfill checkpoint %q contains duplicate token range %d",
				path,
				index,
			)
		}
		completed[index] = struct{}{}
	}
	return checkpoint, true, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("unexpected data after checkpoint")
	}
	return err
}

func saveHistoryNodeBackfillCheckpoint(
	path string,
	checkpoint historyNodeBackfillCheckpoint,
) error {
	checkpoint.CompletedRanges = append([]int(nil), checkpoint.CompletedRanges...)
	sort.Ints(checkpoint.CompletedRanges)
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("encode history node backfill checkpoint %q: %w", path, err)
	}
	data = append(data, '\n')

	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create history node backfill checkpoint %q: %w", path, err)
	}
	tempPath := file.Name()
	defer func() {
		_ = os.Remove(tempPath)
	}()

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write history node backfill checkpoint %q: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync history node backfill checkpoint %q: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close history node backfill checkpoint %q: %w", path, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace history node backfill checkpoint %q: %w", path, err)
	}
	if err := syncHistoryNodeBackfillCheckpointDirectory(directory); err != nil {
		return fmt.Errorf("sync history node backfill checkpoint directory %q: %w", directory, err)
	}
	return nil
}
