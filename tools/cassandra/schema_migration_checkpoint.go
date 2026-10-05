package cassandra

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const migrationPageCheckpointVersion = 1

type migrationPageCheckpointIdentity struct {
	Operation     string `json:"operation"`
	Keyspace      string `json:"keyspace"`
	SourceTable   string `json:"source_table"`
	SourceTableID string `json:"source_table_id"`
	TargetTable   string `json:"target_table"`
	TargetTableID string `json:"target_table_id"`
	Scope         string `json:"scope"`
}

type migrationPageProgress struct {
	NextPageToken []byte `json:"next_page_token,omitempty"`
	RowsProcessed int64  `json:"rows_processed"`
	Complete      bool   `json:"complete"`
}

type migrationPageCheckpoint struct {
	Version  int                              `json:"version"`
	Identity migrationPageCheckpointIdentity  `json:"identity"`
	Phases   map[string]migrationPageProgress `json:"phases"`
}

type migrationPageFunc func(context.Context, []byte) (int, []byte, error)

func newMigrationPageCheckpointIdentity(
	ctx context.Context,
	session gocql.Session,
	keyspace string,
	operation string,
	sourceTable string,
	targetTable string,
	scope string,
) (migrationPageCheckpointIdentity, error) {
	sourceTableID, err := readHistoryNodeBackfillTableID(ctx, session, keyspace, sourceTable)
	if err != nil {
		return migrationPageCheckpointIdentity{}, err
	}
	targetTableID, err := readHistoryNodeBackfillTableID(ctx, session, keyspace, targetTable)
	if err != nil {
		return migrationPageCheckpointIdentity{}, err
	}
	return migrationPageCheckpointIdentity{
		Operation:     operation,
		Keyspace:      keyspace,
		SourceTable:   sourceTable,
		SourceTableID: sourceTableID,
		TargetTable:   targetTable,
		TargetTableID: targetTableID,
		Scope:         scope,
	}, nil
}

func validateMigrationPageCheckpointIdentity(
	ctx context.Context,
	session gocql.Session,
	identity migrationPageCheckpointIdentity,
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
	if sourceTableID != identity.SourceTableID || targetTableID != identity.TargetTableID {
		return errors.New("schema migration source or target table changed during the run")
	}
	return nil
}

func runMigrationPagePhase(
	ctx context.Context,
	checkpointPath string,
	identity migrationPageCheckpointIdentity,
	phase string,
	validateIdentity historyNodeBackfillIdentityValidator,
	processPage migrationPageFunc,
) (int64, error) {
	if checkpointPath == "" {
		return 0, errors.New("schema migration checkpoint file is required")
	}
	if phase == "" {
		return 0, errors.New("schema migration phase is required")
	}
	if validateIdentity == nil {
		return 0, errors.New("schema migration identity validator is required")
	}
	if processPage == nil {
		return 0, errors.New("schema migration page function is required")
	}
	if err := validateIdentity(ctx); err != nil {
		return 0, fmt.Errorf("validate schema migration tables: %w", err)
	}

	checkpoint, found, err := loadMigrationPageCheckpoint(checkpointPath, identity)
	if err != nil {
		return 0, err
	}
	if !found {
		checkpoint = migrationPageCheckpoint{
			Version:  migrationPageCheckpointVersion,
			Identity: identity,
			Phases:   make(map[string]migrationPageProgress),
		}
		if err := saveMigrationPageCheckpoint(checkpointPath, checkpoint); err != nil {
			return 0, err
		}
	}

	progress := checkpoint.Phases[phase]
	if progress.Complete {
		return progress.RowsProcessed, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return progress.RowsProcessed, err
		}
		previousToken := bytes.Clone(progress.NextPageToken)
		rows, nextPageToken, err := processPage(ctx, progress.NextPageToken)
		if err != nil {
			return progress.RowsProcessed, fmt.Errorf("process schema migration phase %q page: %w", phase, err)
		}
		if rows < 0 {
			return progress.RowsProcessed, fmt.Errorf("schema migration phase %q returned negative row count %d", phase, rows)
		}
		if len(nextPageToken) != 0 && bytes.Equal(previousToken, nextPageToken) {
			return progress.RowsProcessed, fmt.Errorf("schema migration phase %q did not advance its page token", phase)
		}
		progress.RowsProcessed += int64(rows)
		progress.NextPageToken = bytes.Clone(nextPageToken)
		progress.Complete = len(nextPageToken) == 0
		checkpoint.Phases[phase] = progress
		if err := saveMigrationPageCheckpoint(checkpointPath, checkpoint); err != nil {
			return progress.RowsProcessed, err
		}
		if progress.Complete {
			break
		}
	}
	if err := validateIdentity(ctx); err != nil {
		return progress.RowsProcessed, fmt.Errorf("validate schema migration tables after scan: %w", err)
	}
	return progress.RowsProcessed, nil
}

func loadMigrationPageCheckpoint(
	path string,
	identity migrationPageCheckpointIdentity,
) (migrationPageCheckpoint, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return migrationPageCheckpoint{}, false, nil
	}
	if err != nil {
		return migrationPageCheckpoint{}, false, fmt.Errorf("read schema migration checkpoint %q: %w", path, err)
	}

	var checkpoint migrationPageCheckpoint
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return migrationPageCheckpoint{}, false, fmt.Errorf("decode schema migration checkpoint %q: %w", path, err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return migrationPageCheckpoint{}, false, fmt.Errorf("decode schema migration checkpoint %q: %w", path, err)
	}
	if checkpoint.Version != migrationPageCheckpointVersion {
		return migrationPageCheckpoint{}, false, fmt.Errorf(
			"schema migration checkpoint %q has version %d, expected %d",
			path,
			checkpoint.Version,
			migrationPageCheckpointVersion,
		)
	}
	if checkpoint.Identity != identity {
		return migrationPageCheckpoint{}, false, fmt.Errorf("schema migration checkpoint %q does not match this migration", path)
	}
	if checkpoint.Phases == nil {
		checkpoint.Phases = make(map[string]migrationPageProgress)
	}
	return checkpoint, true, nil
}

func saveMigrationPageCheckpoint(path string, checkpoint migrationPageCheckpoint) error {
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("encode schema migration checkpoint %q: %w", path, err)
	}
	data = append(data, '\n')
	return writeMigrationCheckpoint(path, data)
}

func writeMigrationCheckpoint(path string, data []byte) error {
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create migration checkpoint %q: %w", path, err)
	}
	tempPath := file.Name()
	defer func() {
		_ = os.Remove(tempPath)
	}()

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write migration checkpoint %q: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync migration checkpoint %q: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close migration checkpoint %q: %w", path, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace migration checkpoint %q: %w", path, err)
	}
	if err := syncHistoryNodeBackfillCheckpointDirectory(directory); err != nil {
		return fmt.Errorf("sync migration checkpoint directory %q: %w", directory, err)
	}
	return nil
}
