package cassandra

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunMigrationPagePhaseResumes(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "migration.json")
	identity := testMigrationPageCheckpointIdentity()
	pageErr := errors.New("page failed")
	var firstTokens [][]byte

	rows, err := runMigrationPagePhase(
		t.Context(),
		checkpointPath,
		identity,
		"source-to-target",
		func(context.Context) error { return nil },
		func(_ context.Context, token []byte) (int, []byte, error) {
			firstTokens = append(firstTokens, bytes.Clone(token))
			if len(token) == 0 {
				return 2, []byte("page-2"), nil
			}
			return 0, nil, pageErr
		},
	)

	require.Equal(t, int64(2), rows)
	require.ErrorIs(t, err, pageErr)
	require.Equal(t, [][]byte{nil, []byte("page-2")}, firstTokens)

	var resumedTokens [][]byte
	rows, err = runMigrationPagePhase(
		t.Context(),
		checkpointPath,
		identity,
		"source-to-target",
		func(context.Context) error { return nil },
		func(_ context.Context, token []byte) (int, []byte, error) {
			resumedTokens = append(resumedTokens, bytes.Clone(token))
			return 3, nil, nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, int64(5), rows)
	require.Equal(t, [][]byte{[]byte("page-2")}, resumedTokens)
	checkpoint, found, err := loadMigrationPageCheckpoint(checkpointPath, identity)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, migrationPageProgress{RowsProcessed: 5, Complete: true}, checkpoint.Phases["source-to-target"])
}

func TestRunMigrationPagePhaseKeepsIndependentPhases(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "migration.json")
	identity := testMigrationPageCheckpointIdentity()
	validateIdentity := func(context.Context) error { return nil }

	rows, err := runMigrationPagePhase(
		t.Context(),
		checkpointPath,
		identity,
		"source-to-target",
		validateIdentity,
		func(context.Context, []byte) (int, []byte, error) { return 4, nil, nil },
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), rows)

	rows, err = runMigrationPagePhase(
		t.Context(),
		checkpointPath,
		identity,
		"target-to-source",
		validateIdentity,
		func(context.Context, []byte) (int, []byte, error) { return 6, nil, nil },
	)
	require.NoError(t, err)
	require.Equal(t, int64(6), rows)

	checkpoint, found, err := loadMigrationPageCheckpoint(checkpointPath, identity)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, migrationPageProgress{RowsProcessed: 4, Complete: true}, checkpoint.Phases["source-to-target"])
	require.Equal(t, migrationPageProgress{RowsProcessed: 6, Complete: true}, checkpoint.Phases["target-to-source"])
}

func TestRunMigrationPagePhaseRejectsDifferentIdentity(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "migration.json")
	identity := testMigrationPageCheckpointIdentity()
	_, err := runMigrationPagePhase(
		t.Context(),
		checkpointPath,
		identity,
		"source-to-target",
		func(context.Context) error { return nil },
		func(context.Context, []byte) (int, []byte, error) { return 1, nil, nil },
	)
	require.NoError(t, err)

	identity.Scope = "queue-type=2"
	called := false
	_, err = runMigrationPagePhase(
		t.Context(),
		checkpointPath,
		identity,
		"source-to-target",
		func(context.Context) error { return nil },
		func(context.Context, []byte) (int, []byte, error) {
			called = true
			return 0, nil, nil
		},
	)

	require.ErrorContains(t, err, "does not match")
	require.False(t, called)
}

func TestRunMigrationPagePhaseRejectsUnchangedPageToken(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "migration.json")
	identity := testMigrationPageCheckpointIdentity()
	calls := 0

	_, err := runMigrationPagePhase(
		t.Context(),
		checkpointPath,
		identity,
		"source-to-target",
		func(context.Context) error { return nil },
		func(_ context.Context, token []byte) (int, []byte, error) {
			calls++
			if len(token) == 0 {
				return 1, []byte("same"), nil
			}
			return 1, []byte("same"), nil
		},
	)

	require.ErrorContains(t, err, "did not advance")
	require.Equal(t, 2, calls)
}

func testMigrationPageCheckpointIdentity() migrationPageCheckpointIdentity {
	return migrationPageCheckpointIdentity{
		Operation:     "validate-queue-v2-metadata",
		Keyspace:      "temporal",
		SourceTable:   "queues",
		SourceTableID: "source-id",
		TargetTable:   "queues_v2",
		TargetTableID: "target-id",
		Scope:         "queue-type=1",
	}
}
