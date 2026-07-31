package cassandra

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	persistencecassandra "go.temporal.io/server/common/persistence/cassandra"
)

func TestRunHistoryNodeBackfillResumesCompletedRanges(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "backfill.json")
	identity := historyNodeBackfillCheckpointIdentity{
		Direction:       "history-node-v2",
		Keyspace:        "temporal",
		SourceTable:     "history_node",
		SourceTableID:   "source-id",
		SourceLayout:    "legacy-v1",
		TargetTable:     "history_node_v2",
		TargetTableID:   "target-id",
		Partitioner:     persistencecassandra.HistoryNodeBackfillMurmur3Partitioner,
		TokenRangeCount: 4,
	}
	validateIdentity := func(context.Context) error { return nil }
	backfillErr := errors.New("write failed")
	var firstAttempt []int
	copied, err := runHistoryNodeBackfill(
		t.Context(),
		checkpointPath,
		identity,
		validateIdentity,
		func(_ context.Context, tokenRange persistencecassandra.HistoryNodeBackfillTokenRange) (int64, error) {
			firstAttempt = append(firstAttempt, tokenRange.Index)
			if tokenRange.Index == 2 {
				return 1, backfillErr
			}
			return 1, nil
		},
	)

	require.Equal(t, int64(3), copied)
	require.ErrorIs(t, err, backfillErr)
	require.Equal(t, []int{0, 1, 2}, firstAttempt)
	checkpoint, found, err := loadHistoryNodeBackfillCheckpoint(checkpointPath, identity)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []int{0, 1}, checkpoint.CompletedRanges)

	var secondAttempt []int
	copied, err = runHistoryNodeBackfill(
		t.Context(),
		checkpointPath,
		identity,
		validateIdentity,
		func(_ context.Context, tokenRange persistencecassandra.HistoryNodeBackfillTokenRange) (int64, error) {
			secondAttempt = append(secondAttempt, tokenRange.Index)
			return 1, nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, int64(2), copied)
	require.Equal(t, []int{2, 3}, secondAttempt)
	checkpoint, found, err = loadHistoryNodeBackfillCheckpoint(checkpointPath, identity)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []int{0, 1, 2, 3}, checkpoint.CompletedRanges)
}

func TestRunHistoryNodeBackfillRejectsCheckpointForRecreatedTable(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "backfill.json")
	identity := historyNodeBackfillCheckpointIdentity{
		Direction:       "history-node-v2",
		Keyspace:        "temporal",
		SourceTable:     "history_node",
		SourceTableID:   "source-id",
		SourceLayout:    "legacy-v1",
		TargetTable:     "history_node_v2",
		TargetTableID:   "target-id",
		Partitioner:     persistencecassandra.HistoryNodeBackfillMurmur3Partitioner,
		TokenRangeCount: 1,
	}
	validateIdentity := func(context.Context) error { return nil }
	_, err := runHistoryNodeBackfill(
		t.Context(),
		checkpointPath,
		identity,
		validateIdentity,
		func(context.Context, persistencecassandra.HistoryNodeBackfillTokenRange) (int64, error) {
			return 0, nil
		},
	)
	require.NoError(t, err)

	identity.TargetTableID = "recreated-target-id"
	called := false
	_, err = runHistoryNodeBackfill(
		t.Context(),
		checkpointPath,
		identity,
		validateIdentity,
		func(context.Context, persistencecassandra.HistoryNodeBackfillTokenRange) (int64, error) {
			called = true
			return 0, nil
		},
	)

	require.ErrorContains(t, err, "does not match")
	require.False(t, called)
}

func TestRunHistoryNodeBackfillFailsIfTableChangesDuringRun(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "backfill.json")
	identity := historyNodeBackfillCheckpointIdentity{
		Direction:       "history-node-v2",
		Keyspace:        "temporal",
		SourceTable:     "history_node",
		SourceTableID:   "source-id",
		SourceLayout:    "legacy-v1",
		TargetTable:     "history_node_v2",
		TargetTableID:   "target-id",
		Partitioner:     persistencecassandra.HistoryNodeBackfillMurmur3Partitioner,
		TokenRangeCount: 1,
	}
	tableChanged := errors.New("table generation changed")
	validationCalls := 0

	copied, err := runHistoryNodeBackfill(
		t.Context(),
		checkpointPath,
		identity,
		func(context.Context) error {
			validationCalls++
			if validationCalls > 1 {
				return tableChanged
			}
			return nil
		},
		func(context.Context, persistencecassandra.HistoryNodeBackfillTokenRange) (int64, error) {
			return 1, nil
		},
	)

	require.Equal(t, int64(1), copied)
	require.ErrorIs(t, err, tableChanged)
	require.ErrorContains(t, err, "after copy")
}
