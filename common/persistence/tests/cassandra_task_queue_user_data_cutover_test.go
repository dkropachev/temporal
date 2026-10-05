//go:build integration

package tests

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func TestCassandraTaskQueueUserDataCutoverFencesResolvedSourceWrite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	t.Cleanup(tearDown)
	ctx := t.Context()
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	t.Cleanup(session.Close)

	spec, err := cassandra.SchemaLayoutSpecForTable(
		ctx,
		session,
		testData.Cfg.Keyspace,
		cassandra.SchemaLayoutTaskQueueUserData,
		"task_queue_user_data_v2",
		cassandra.DefaultTaskQueueUserDataBucketCount,
	)
	require.NoError(t, err)
	metadataStore := cassandra.NewSchemaLayoutMetadataStore(session)
	_, err = metadataStore.InitializePreparing(ctx, spec)
	require.NoError(t, err)
	_, err = metadataStore.MarkTargetReady(ctx, spec)
	require.NoError(t, err)

	const (
		namespaceID = "11111111-1111-1111-1111-111111111111"
		taskQueue   = "cutover-task-queue"
	)
	options := cassandra.TaskQueueUserDataCutoverOptions{
		NamespaceID: namespaceID,
		BucketCount: cassandra.DefaultTaskQueueUserDataBucketCount,
		Concurrency: 4,
		Generation:  spec.Generation,
	}
	sourceStore, err := cassandra.NewMatchingTaskStoreWithUserDataMigration(
		session,
		testData.Logger,
		false,
		config.CassandraTaskQueueUserDataMigrationModeSourceDual,
		options.BucketCount,
		spec.Generation,
	)
	require.NoError(t, err)
	require.NoError(t, updateCutoverTaskQueueUserData(ctx, sourceStore, namespaceID, taskQueue, 0, "before"))
	require.NoError(t, cassandra.ActivateTaskQueueUserDataV2NamespaceFencing(ctx, session, options))

	blocking := &blockNextUserDataBatchSession{
		Session: session,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	blocking.armed.Store(true)
	racingSourceStore, err := cassandra.NewMatchingTaskStoreWithUserDataMigration(
		blocking,
		testData.Logger,
		false,
		config.CassandraTaskQueueUserDataMigrationModeSourceDual,
		options.BucketCount,
		spec.Generation,
	)
	require.NoError(t, err)
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- updateCutoverTaskQueueUserData(ctx, racingSourceStore, namespaceID, taskQueue, 1, "must-not-commit")
	}()
	<-blocking.started

	validation, err := cassandra.CutoverTaskQueueUserDataV2Namespace(ctx, session, options)
	require.NoError(t, err)
	require.True(t, validation.Matches())
	close(blocking.release)
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, <-writeErr, &unavailable)

	targetStore, err := cassandra.NewMatchingTaskStoreWithUserDataMigration(
		session,
		testData.Logger,
		false,
		config.CassandraTaskQueueUserDataMigrationModeTargetDual,
		options.BucketCount,
		spec.Generation,
	)
	require.NoError(t, err)
	response, err := targetStore.GetTaskQueueUserData(ctx, &p.GetTaskQueueUserDataRequest{
		NamespaceID: namespaceID,
		TaskQueue:   taskQueue,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), response.Version)
	require.Equal(t, []byte("before"), response.UserData.Data)

	validation, err = cassandra.CutoverTaskQueueUserDataV2Namespace(ctx, session, options)
	require.NoError(t, err)
	require.True(t, validation.Matches())
}

type blockNextUserDataBatchSession struct {
	commongocql.Session
	armed   atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (s *blockNextUserDataBatchSession) MapExecuteBatchCAS(
	batch *commongocql.Batch,
	previous map[string]any,
) (bool, commongocql.Iter, error) {
	if s.armed.CompareAndSwap(true, false) {
		close(s.started)
		<-s.release
	}
	return s.Session.MapExecuteBatchCAS(batch, previous)
}

func updateCutoverTaskQueueUserData(
	ctx context.Context,
	store p.TaskStore,
	namespaceID string,
	taskQueue string,
	version int64,
	data string,
) error {
	return store.UpdateTaskQueueUserData(ctx, &p.InternalUpdateTaskQueueUserDataRequest{
		NamespaceID: namespaceID,
		Updates: map[string]*p.InternalSingleTaskQueueUserDataUpdate{
			taskQueue: {
				Version:  version,
				UserData: p.NewDataBlob([]byte(data), enumspb.ENCODING_TYPE_PROTO3.String()),
			},
		},
	})
}
