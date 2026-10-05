package cassandra

import (
	flagset "flag"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
	enumspb "go.temporal.io/api/enums/v1"
	persistencecassandra "go.temporal.io/server/common/persistence/cassandra"
)

func TestSchemaMigrationCommands(t *testing.T) {
	app := buildCLIOptions()
	commands := make(map[string]cli.Command, len(app.Commands))
	for _, command := range app.Commands {
		commands[command.Name] = command
	}

	require.Len(t, commands["prepare-executions-v2-backfill"].Flags, 2)
	require.Len(t, commands["backfill-executions-v2"].Flags, 5)
	require.Len(t, commands["validate-executions-v2"].Flags, 5)
	require.Len(t, commands["backfill-queue-v2-metadata"].Flags, 3)
	require.Len(t, commands["validate-queue-v2-metadata"].Flags, 3)
	require.Len(t, commands["backfill-task-queue-user-data-v2"].Flags, 5)
	require.Len(t, commands["recover-task-queue-user-data-v2"].Flags, 5)
	require.Len(t, commands["validate-task-queue-user-data-v2"].Flags, 2)
	require.Len(t, commands["backfill-matching-tasks-v3"].Flags, 7)
	require.Len(t, commands["validate-matching-tasks-v3"].Flags, 5)
	require.Len(t, commands["cutover-matching-tasks-v3"].Flags, 6)
	require.Len(t, commands["activate-legacy-queue-v2-source-fence"].Flags, 3)
	require.Len(t, commands["backfill-legacy-queue-v2"].Flags, 3)
	require.Len(t, commands["reconcile-legacy-queue-v2"].Flags, 5)
	require.Len(t, commands["validate-legacy-queue-v2"].Flags, 4)
	require.Len(t, commands["cutover-legacy-queue-v2"].Flags, 5)
	require.Len(t, commands["backfill-queue-v2-messages-v3"].Flags, 6)
	require.Len(t, commands["reconcile-queue-v2-messages-v3"].Flags, 6)
	require.Len(t, commands["validate-queue-v2-messages-v3"].Flags, 4)
	require.Len(t, commands["initialize-schema-layout"].Flags, 2)
	require.Len(t, commands["inspect-schema-layout"].Flags, 2)
	require.Len(t, commands["mark-schema-layout-target-ready"].Flags, 6)
	require.Len(t, commands["mark-schema-layout-target-only"].Flags, 6)

	require.Equal(t, 16, defaultSchemaMigrationPageSize)
	require.Equal(t, 16, defaultSchemaMigrationConcurrency)
	require.Equal(t, 16, defaultExecutionStorageBuckets)
	require.Equal(t, 20, defaultSchemaMigrationMaxMismatches)
	require.Equal(t, 4096, persistencecassandra.DefaultExecutionBackfillTokenRangeCount)
	require.Equal(t, 4096, persistencecassandra.DefaultTaskQueueUserDataBackfillTokenRangeCount)
}

func TestCutoverMatchingTasksV3RequiresWriterBarrierConfirmation(t *testing.T) {
	commands := migrationCLICommands(nil)
	var flags []cli.Flag
	for _, command := range commands {
		if command.Name == "cutover-matching-tasks-v3" {
			flags = command.Flags
			break
		}
	}
	require.NotEmpty(t, flags)
	ctx := newSchemaMigrationTestContext(t, flags, []string{
		"--namespace-id", "11111111-1111-1111-1111-111111111111",
		"--task-queue", "queue",
		"--task-queue-type", "1",
	})
	require.ErrorContains(
		t,
		cutoverMatchingTasksV3(ctx, nil),
		confirmMatchingTaskWriterBarrierFlag,
	)
}

func TestRequiredSchemaMigrationArguments(t *testing.T) {
	ctx := newSchemaMigrationTestContext(t, queueV2MetadataMigrationCLIFlags(), nil)
	_, err := requiredCheckpointPath(ctx)
	require.ErrorContains(t, err, schemaMigrationCheckpointFileFlag)
	_, err = requiredQueueV2Type(ctx)
	require.ErrorContains(t, err, schemaMigrationQueueTypeFlag)

	ctx = newSchemaMigrationTestContext(
		t,
		queueV2MetadataMigrationCLIFlags(),
		[]string{"--checkpoint-file", " /tmp/progress.json ", "--queue-type", "2"},
	)
	checkpointPath, err := requiredCheckpointPath(ctx)
	require.NoError(t, err)
	require.Equal(t, "/tmp/progress.json", checkpointPath)
	queueType, err := requiredQueueV2Type(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, int(queueType))
}

func TestRequiredLegacyQueueType(t *testing.T) {
	ctx := newSchemaMigrationTestContext(t, legacyQueueV2MigrationCLIFlags(false, false), nil)
	_, err := requiredLegacyQueueType(ctx)
	require.ErrorContains(t, err, "missing")

	ctx = newSchemaMigrationTestContext(t, legacyQueueV2MigrationCLIFlags(false, false), []string{"--queue-type=-1"})
	queueType, err := requiredLegacyQueueType(ctx)
	require.NoError(t, err)
	require.Equal(t, -1, int(queueType))

	ctx = newSchemaMigrationTestContext(t, legacyQueueV2MigrationCLIFlags(false, false), []string{"--queue-type", "0"})
	_, err = requiredLegacyQueueType(ctx)
	require.ErrorContains(t, err, "non-zero int32")
}

func TestRequiredTaskQueueType(t *testing.T) {
	flags := []cli.Flag{cli.IntFlag{Name: schemaMigrationTaskQueueTypeFlag}}
	ctx := newSchemaMigrationTestContext(t, flags, nil)
	_, err := requiredTaskQueueType(ctx)
	require.ErrorContains(t, err, "missing")

	ctx = newSchemaMigrationTestContext(t, flags, []string{"--task-queue-type", "1"})
	taskQueueType, err := requiredTaskQueueType(ctx)
	require.NoError(t, err)
	require.Equal(t, enumspb.TASK_QUEUE_TYPE_WORKFLOW, taskQueueType)

	ctx = newSchemaMigrationTestContext(t, flags, []string{"--task-queue-type", "99"})
	_, err = requiredTaskQueueType(ctx)
	require.ErrorContains(t, err, "unsupported")
}

func newSchemaMigrationTestContext(t *testing.T, flags []cli.Flag, args []string) *cli.Context {
	t.Helper()
	set := flagset.NewFlagSet("schema-migration", flagset.ContinueOnError)
	for _, currentFlag := range flags {
		currentFlag.Apply(set)
	}
	require.NoError(t, set.Parse(args))
	return cli.NewContext(nil, set, nil)
}
