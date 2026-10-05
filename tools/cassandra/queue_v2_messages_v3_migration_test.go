package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence"
	persistencecassandra "go.temporal.io/server/common/persistence/cassandra"
)

func TestRequiredQueueV2MessagesV3MigrationOptions(t *testing.T) {
	flags := queueV2MessagesV3MigrationCLIFlags(true, true)
	ctx := newSchemaMigrationTestContext(t, flags, []string{
		"--queue-type", "1",
		"--queue-name", " queue-a ",
		"--page-size", "32",
		"--message-bucket-span", "2048",
		"--checkpoint-file", " /tmp/messages.json ",
		"--confirm-source-authoritative",
	})

	options, err := requiredQueueV2MessagesV3MigrationOptions(ctx, true, true)
	require.NoError(t, err)
	require.Equal(t, persistence.QueueV2Type(1), options.queueType)
	require.Equal(t, "queue-a", options.queueName)
	require.Equal(t, 32, options.pageSize)
	require.Equal(t, int64(2048), options.messageBucketSpan)
	require.Equal(t, "/tmp/messages.json", options.checkpointPath)
}

func TestRequiredQueueV2MessagesV3MigrationOptionsFailsClosed(t *testing.T) {
	flags := queueV2MessagesV3MigrationCLIFlags(true, true)
	tests := []struct {
		name          string
		args          []string
		errorContains string
	}{
		{
			name:          "queue type",
			args:          []string{"--queue-name", "queue-a", "--checkpoint-file", "/tmp/messages.json", "--confirm-source-authoritative"},
			errorContains: schemaMigrationQueueTypeFlag,
		},
		{
			name:          "queue name",
			args:          []string{"--queue-type", "1", "--checkpoint-file", "/tmp/messages.json", "--confirm-source-authoritative"},
			errorContains: schemaMigrationQueueNameFlag,
		},
		{
			name:          "page size",
			args:          []string{"--queue-type", "1", "--queue-name", "queue-a", "--page-size", "0", "--checkpoint-file", "/tmp/messages.json", "--confirm-source-authoritative"},
			errorContains: "must be positive",
		},
		{
			name:          "message span",
			args:          []string{"--queue-type", "1", "--queue-name", "queue-a", "--message-bucket-span", "0", "--checkpoint-file", "/tmp/messages.json", "--confirm-source-authoritative"},
			errorContains: "must be positive",
		},
		{
			name:          "checkpoint",
			args:          []string{"--queue-type", "1", "--queue-name", "queue-a", "--confirm-source-authoritative"},
			errorContains: schemaMigrationCheckpointFileFlag,
		},
		{
			name:          "source authority confirmation",
			args:          []string{"--queue-type", "1", "--queue-name", "queue-a", "--checkpoint-file", "/tmp/messages.json"},
			errorContains: confirmSourceAuthoritativeFlag,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newSchemaMigrationTestContext(t, flags, test.args)
			_, err := requiredQueueV2MessagesV3MigrationOptions(ctx, true, true)
			require.ErrorContains(t, err, test.errorContains)
		})
	}
}

func TestQueueV2MessagesV3ValidationOptionsNeedNoMutationFlags(t *testing.T) {
	flags := queueV2MessagesV3MigrationCLIFlags(false, false)
	ctx := newSchemaMigrationTestContext(t, flags, []string{
		"--queue-type", "2",
		"--queue-name", "queue-b",
	})

	options, err := requiredQueueV2MessagesV3MigrationOptions(ctx, false, false)
	require.NoError(t, err)
	require.Empty(t, options.checkpointPath)
	require.Equal(t, persistencecassandra.DefaultQueueV2MessageBucketSpan, options.messageBucketSpan)
}

func TestQueueV2MessagesV3MismatchError(t *testing.T) {
	err := queueV2MessagesV3MismatchError(
		queueV2MessagesV3MigrationOptions{
			queueType: persistence.QueueV2Type(1),
			queueName: "queue-a",
		},
		persistencecassandra.QueueV2MessageValidationResult{
			SourceRows: 2,
			TargetRows: 1,
			Mismatches: []string{"message 2 differs"},
		},
	)

	require.ErrorContains(t, err, "queue-a")
	require.ErrorContains(t, err, "source=2 target=1")
}
