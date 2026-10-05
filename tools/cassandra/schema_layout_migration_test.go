package cassandra

import (
	"context"
	"errors"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
	"go.temporal.io/server/common/log"
	persistencecassandra "go.temporal.io/server/common/persistence/cassandra"
)

func TestSchemaLayoutDescriptorForName(t *testing.T) {
	expected := map[persistencecassandra.SchemaLayoutName]string{
		persistencecassandra.SchemaLayoutExecutions:        "executions_v2",
		persistencecassandra.SchemaLayoutHistoryNode:       "history_node_v2",
		persistencecassandra.SchemaLayoutHistoryTree:       "history_tree_v2",
		persistencecassandra.SchemaLayoutQueueV2Metadata:   "queues_v2",
		persistencecassandra.SchemaLayoutQueueV2Messages:   "queue_messages_v3",
		persistencecassandra.SchemaLayoutLegacyQueue:       "legacy_queue_v2_messages",
		persistencecassandra.SchemaLayoutMatchingTasks:     "tasks_v3",
		persistencecassandra.SchemaLayoutMatchingTasksFair: "tasks_v3_fair",
		persistencecassandra.SchemaLayoutTaskQueueUserData: "task_queue_user_data_v2",
	}
	require.Len(t, persistencecassandra.SchemaLayoutNames(), len(expected))
	for name, targetTable := range expected {
		descriptor, err := schemaLayoutDescriptorForName(name)
		require.NoError(t, err)
		require.Equal(t, schemaLayoutDescriptor{name: name, targetTable: targetTable}, descriptor)
	}

	_, err := schemaLayoutDescriptorForName("unknown")
	require.ErrorContains(t, err, "unsupported")
}

func TestRequiredSchemaLayoutIdentity(t *testing.T) {
	flags := schemaLayoutIdentityCLIFlags()

	ctx := newSchemaMigrationTestContext(t, flags, nil)
	_, _, err := requiredSchemaLayoutIdentity(ctx)
	require.ErrorContains(t, err, schemaLayoutNameFlag)

	ctx = newSchemaMigrationTestContext(t, flags, []string{"--layout-name", "executions"})
	_, _, err = requiredSchemaLayoutIdentity(ctx)
	require.ErrorContains(t, err, schemaLayoutImmutableParameterFlag)

	ctx = newSchemaMigrationTestContext(t, flags, []string{
		"--layout-name", " executions ",
		"--immutable-parameter", "0",
	})
	descriptor, parameter, err := requiredSchemaLayoutIdentity(ctx)
	require.NoError(t, err)
	require.Equal(t, persistencecassandra.SchemaLayoutExecutions, descriptor.name)
	require.Equal(t, int64(0), parameter)

	ctx = newSchemaMigrationTestContext(t, flags, []string{
		"--layout-name", "executions",
		"--immutable-parameter=-1",
	})
	_, _, err = requiredSchemaLayoutIdentity(ctx)
	require.ErrorContains(t, err, "must not be negative")
}

func TestRequiredSchemaLayoutExpectedEpoch(t *testing.T) {
	flags := schemaLayoutTransitionCLIFlags(cli.BoolFlag{Name: "confirm"})

	ctx := newSchemaMigrationTestContext(t, flags, nil)
	_, err := requiredSchemaLayoutExpectedEpoch(ctx)
	require.ErrorContains(t, err, schemaLayoutExpectedEpochFlag)

	ctx = newSchemaMigrationTestContext(t, flags, []string{"--expected-epoch", "0"})
	epoch, err := requiredSchemaLayoutExpectedEpoch(ctx)
	require.NoError(t, err)
	require.Zero(t, epoch)

	ctx = newSchemaMigrationTestContext(t, flags, []string{"--expected-epoch=-1"})
	_, err = requiredSchemaLayoutExpectedEpoch(ctx)
	require.ErrorContains(t, err, "between zero")
}

func TestRequiredSchemaLayoutInventoryPageSize(t *testing.T) {
	flags := schemaLayoutTransitionCLIFlags(cli.BoolFlag{Name: "confirm"})

	ctx := newSchemaMigrationTestContext(t, flags, nil)
	pageSize, err := requiredSchemaLayoutInventoryPageSize(ctx)
	require.NoError(t, err)
	require.Equal(t, defaultSchemaMigrationPageSize, pageSize)

	ctx = newSchemaMigrationTestContext(t, flags, []string{"--inventory-page-size", "0"})
	_, err = requiredSchemaLayoutInventoryPageSize(ctx)
	require.ErrorContains(t, err, "must be positive")
}

func TestTransitionSchemaLayoutAuthority(t *testing.T) {
	spec := testSchemaLayoutSpec()
	store := &testSchemaLayoutMetadataOperator{
		metadata: persistencecassandra.SchemaLayoutMetadata{
			SchemaLayoutSpec: spec,
			AuthorityState:   persistencecassandra.SchemaLayoutAuthorityPreparing,
			Epoch:            7,
		},
	}

	metadata, err := transitionSchemaLayoutAuthority(
		t.Context(),
		store,
		spec,
		persistencecassandra.SchemaLayoutAuthorityPreparing,
		persistencecassandra.SchemaLayoutAuthorityTargetReady,
		7,
	)
	require.NoError(t, err)
	require.Equal(t, persistencecassandra.SchemaLayoutAuthorityTargetReady, metadata.AuthorityState)
	require.Equal(t, int64(8), metadata.Epoch)
	require.Equal(t, 1, store.compareCalls)

	metadata, err = transitionSchemaLayoutAuthority(
		t.Context(),
		store,
		spec,
		persistencecassandra.SchemaLayoutAuthorityPreparing,
		persistencecassandra.SchemaLayoutAuthorityTargetReady,
		7,
	)
	require.NoError(t, err)
	require.Equal(t, int64(8), metadata.Epoch)
	require.Equal(t, 1, store.compareCalls)
}

func TestTransitionSchemaLayoutAuthorityToTargetOnly(t *testing.T) {
	spec := testSchemaLayoutSpec()
	store := &testSchemaLayoutMetadataOperator{
		metadata: persistencecassandra.SchemaLayoutMetadata{
			SchemaLayoutSpec: spec,
			AuthorityState:   persistencecassandra.SchemaLayoutAuthorityTargetReady,
			Epoch:            1,
		},
	}

	metadata, err := transitionSchemaLayoutAuthority(
		t.Context(),
		store,
		spec,
		persistencecassandra.SchemaLayoutAuthorityTargetReady,
		persistencecassandra.SchemaLayoutAuthorityTargetOnly,
		1,
	)
	require.NoError(t, err)
	require.Equal(t, persistencecassandra.SchemaLayoutAuthorityTargetOnly, metadata.AuthorityState)
	require.Equal(t, int64(2), metadata.Epoch)
}

func TestTransitionSchemaLayoutAuthorityTargetOnlyGate(t *testing.T) {
	spec := testSchemaLayoutSpec()
	testErr := errors.New("incomplete source inventory")
	store := &testSchemaLayoutMetadataOperator{
		metadata: persistencecassandra.SchemaLayoutMetadata{
			SchemaLayoutSpec: spec,
			AuthorityState:   persistencecassandra.SchemaLayoutAuthorityTargetReady,
			Epoch:            1,
		},
	}
	gateCalls := 0

	_, err := transitionSchemaLayoutAuthorityWithGate(
		t.Context(),
		store,
		spec,
		persistencecassandra.SchemaLayoutAuthorityTargetReady,
		persistencecassandra.SchemaLayoutAuthorityTargetOnly,
		1,
		func() error {
			gateCalls++
			return testErr
		},
	)
	require.ErrorIs(t, err, testErr)
	require.Equal(t, 1, gateCalls)
	require.Zero(t, store.compareCalls)
	require.Equal(t, persistencecassandra.SchemaLayoutAuthorityTargetReady, store.metadata.AuthorityState)
}

func TestTransitionSchemaLayoutAuthorityResumedTargetOnlySkipsGate(t *testing.T) {
	spec := testSchemaLayoutSpec()
	store := &testSchemaLayoutMetadataOperator{
		metadata: persistencecassandra.SchemaLayoutMetadata{
			SchemaLayoutSpec: spec,
			AuthorityState:   persistencecassandra.SchemaLayoutAuthorityTargetOnly,
			Epoch:            2,
		},
	}
	gateCalls := 0

	metadata, err := transitionSchemaLayoutAuthorityWithGate(
		t.Context(),
		store,
		spec,
		persistencecassandra.SchemaLayoutAuthorityTargetReady,
		persistencecassandra.SchemaLayoutAuthorityTargetOnly,
		1,
		func() error {
			gateCalls++
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, persistencecassandra.SchemaLayoutAuthorityTargetOnly, metadata.AuthorityState)
	require.Zero(t, gateCalls)
	require.Zero(t, store.compareCalls)
}

func TestSchemaLayoutHasTargetOnlyInventoryAudit(t *testing.T) {
	for _, name := range []persistencecassandra.SchemaLayoutName{
		persistencecassandra.SchemaLayoutExecutions,
		persistencecassandra.SchemaLayoutHistoryNode,
		persistencecassandra.SchemaLayoutHistoryTree,
		persistencecassandra.SchemaLayoutQueueV2Metadata,
		persistencecassandra.SchemaLayoutQueueV2Messages,
		persistencecassandra.SchemaLayoutMatchingTasks,
		persistencecassandra.SchemaLayoutMatchingTasksFair,
		persistencecassandra.SchemaLayoutTaskQueueUserData,
		persistencecassandra.SchemaLayoutLegacyQueue,
	} {
		require.True(t, schemaLayoutHasTargetOnlyInventoryAudit(name), name)
	}
}

func TestTransitionQueueV2SchemaLayoutsTargetOnlyOrdersPair(t *testing.T) {
	metadataSpec, messagesSpec := testQueueV2SchemaLayoutSpecs()
	store := newTestQueueV2SchemaLayoutMetadataOperator(metadataSpec, messagesSpec)
	gateCalls := 0

	metadata, messages, err := transitionQueueV2SchemaLayoutsTargetOnly(
		t.Context(),
		store,
		metadataSpec,
		messagesSpec,
		persistencecassandra.SchemaLayoutQueueV2Metadata,
		1,
		func() error {
			gateCalls++
			return nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, persistencecassandra.SchemaLayoutAuthorityTargetOnly, metadata.AuthorityState)
	require.Equal(t, persistencecassandra.SchemaLayoutAuthorityTargetOnly, messages.AuthorityState)
	require.Equal(t, 1, gateCalls)
	require.Equal(t, []persistencecassandra.SchemaLayoutName{
		persistencecassandra.SchemaLayoutQueueV2Messages,
		persistencecassandra.SchemaLayoutQueueV2Metadata,
	}, store.compareOrder)
}

func TestTransitionQueueV2SchemaLayoutsTargetOnlyResumesAfterMessageTransition(t *testing.T) {
	metadataSpec, messagesSpec := testQueueV2SchemaLayoutSpecs()
	store := newTestQueueV2SchemaLayoutMetadataOperator(metadataSpec, messagesSpec)
	messages := store.metadata[persistencecassandra.SchemaLayoutQueueV2Messages]
	messages.AuthorityState = persistencecassandra.SchemaLayoutAuthorityTargetOnly
	messages.Epoch = 2
	store.metadata[messages.Name] = messages
	gateCalls := 0

	metadata, messages, err := transitionQueueV2SchemaLayoutsTargetOnly(
		t.Context(),
		store,
		metadataSpec,
		messagesSpec,
		persistencecassandra.SchemaLayoutQueueV2Metadata,
		1,
		func() error {
			gateCalls++
			return nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, persistencecassandra.SchemaLayoutAuthorityTargetOnly, metadata.AuthorityState)
	require.Equal(t, persistencecassandra.SchemaLayoutAuthorityTargetOnly, messages.AuthorityState)
	require.Equal(t, 1, gateCalls)
	require.Equal(t, []persistencecassandra.SchemaLayoutName{
		persistencecassandra.SchemaLayoutQueueV2Metadata,
	}, store.compareOrder)
}

func TestTransitionQueueV2SchemaLayoutsTargetOnlyRejectsMetadataFirst(t *testing.T) {
	metadataSpec, messagesSpec := testQueueV2SchemaLayoutSpecs()
	store := newTestQueueV2SchemaLayoutMetadataOperator(metadataSpec, messagesSpec)
	metadata := store.metadata[persistencecassandra.SchemaLayoutQueueV2Metadata]
	metadata.AuthorityState = persistencecassandra.SchemaLayoutAuthorityTargetOnly
	metadata.Epoch = 2
	store.metadata[metadata.Name] = metadata

	_, _, err := transitionQueueV2SchemaLayoutsTargetOnly(
		t.Context(),
		store,
		metadataSpec,
		messagesSpec,
		persistencecassandra.SchemaLayoutQueueV2Metadata,
		1,
		func() error { return nil },
	)

	require.ErrorContains(t, err, "metadata is target-only before messages")
	require.Empty(t, store.compareOrder)
}

func TestTransitionQueueV2SchemaLayoutsTargetOnlyCompletedRetrySkipsGate(t *testing.T) {
	metadataSpec, messagesSpec := testQueueV2SchemaLayoutSpecs()
	store := newTestQueueV2SchemaLayoutMetadataOperator(metadataSpec, messagesSpec)
	for name, metadata := range store.metadata {
		metadata.AuthorityState = persistencecassandra.SchemaLayoutAuthorityTargetOnly
		metadata.Epoch = 2
		store.metadata[name] = metadata
	}

	metadata, messages, err := transitionQueueV2SchemaLayoutsTargetOnly(
		t.Context(),
		store,
		metadataSpec,
		messagesSpec,
		persistencecassandra.SchemaLayoutQueueV2Messages,
		1,
		func() error { return errors.New("gate must not run") },
	)

	require.NoError(t, err)
	require.Equal(t, persistencecassandra.SchemaLayoutAuthorityTargetOnly, metadata.AuthorityState)
	require.Equal(t, persistencecassandra.SchemaLayoutAuthorityTargetOnly, messages.AuthorityState)
	require.Empty(t, store.compareOrder)
}

func TestTransitionSchemaLayoutAuthorityFailsClosed(t *testing.T) {
	spec := testSchemaLayoutSpec()
	tests := []struct {
		name          string
		metadata      persistencecassandra.SchemaLayoutMetadata
		expectedEpoch int64
		errorContains string
	}{
		{
			name: "stale expected epoch",
			metadata: persistencecassandra.SchemaLayoutMetadata{
				SchemaLayoutSpec: spec,
				AuthorityState:   persistencecassandra.SchemaLayoutAuthorityPreparing,
				Epoch:            8,
			},
			expectedEpoch: 7,
			errorContains: "expected \"preparing\" at epoch 7",
		},
		{
			name: "unexpected state",
			metadata: persistencecassandra.SchemaLayoutMetadata{
				SchemaLayoutSpec: spec,
				AuthorityState:   persistencecassandra.SchemaLayoutAuthorityTargetOnly,
				Epoch:            9,
			},
			expectedEpoch: 7,
			errorContains: "expected \"preparing\" at epoch 7",
		},
		{
			name: "unrelated completed transition",
			metadata: persistencecassandra.SchemaLayoutMetadata{
				SchemaLayoutSpec: spec,
				AuthorityState:   persistencecassandra.SchemaLayoutAuthorityTargetReady,
				Epoch:            9,
			},
			expectedEpoch: 7,
			errorContains: "not the resumable result",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &testSchemaLayoutMetadataOperator{metadata: test.metadata}
			_, err := transitionSchemaLayoutAuthority(
				t.Context(),
				store,
				spec,
				persistencecassandra.SchemaLayoutAuthorityPreparing,
				persistencecassandra.SchemaLayoutAuthorityTargetReady,
				test.expectedEpoch,
			)
			require.ErrorContains(t, err, test.errorContains)
			require.Zero(t, store.compareCalls)
		})
	}
}

func TestSchemaLayoutAuthorityHelpersPropagateErrors(t *testing.T) {
	spec := testSchemaLayoutSpec()
	testErr := errors.New("metadata failed")
	store := &testSchemaLayoutMetadataOperator{loadErr: testErr, initializeErr: testErr}

	_, err := initializeSchemaLayoutAuthority(t.Context(), store, spec)
	require.ErrorIs(t, err, testErr)
	_, err = inspectSchemaLayoutAuthority(t.Context(), store, spec)
	require.ErrorIs(t, err, testErr)
	_, err = transitionSchemaLayoutAuthority(
		t.Context(),
		store,
		spec,
		persistencecassandra.SchemaLayoutAuthorityPreparing,
		persistencecassandra.SchemaLayoutAuthorityTargetReady,
		0,
	)
	require.ErrorIs(t, err, testErr)
}

func TestInitializeAndInspectSchemaLayoutAuthority(t *testing.T) {
	spec := testSchemaLayoutSpec()
	store := &testSchemaLayoutMetadataOperator{}

	initialized, err := initializeSchemaLayoutAuthority(t.Context(), store, spec)
	require.NoError(t, err)
	require.Equal(t, spec, initialized.SchemaLayoutSpec)
	require.Equal(t, persistencecassandra.SchemaLayoutAuthorityPreparing, initialized.AuthorityState)

	inspected, err := inspectSchemaLayoutAuthority(t.Context(), store, spec)
	require.NoError(t, err)
	require.Equal(t, initialized, inspected)
}

func TestSchemaLayoutTransitionCommandsRequireConfirmation(t *testing.T) {
	readyContext := newSchemaMigrationTestContext(
		t,
		schemaLayoutTransitionCLIFlags(cli.BoolFlag{Name: confirmSchemaLayoutTargetReadyFlag}),
		nil,
	)
	require.ErrorContains(
		t,
		markSchemaLayoutTargetReady(readyContext, log.NewNoopLogger()),
		confirmSchemaLayoutTargetReadyFlag,
	)

	targetOnlyContext := newSchemaMigrationTestContext(
		t,
		schemaLayoutTransitionCLIFlags(cli.BoolFlag{Name: confirmSchemaLayoutTargetOnlyFlag}),
		nil,
	)
	require.ErrorContains(
		t,
		markSchemaLayoutTargetOnly(targetOnlyContext, log.NewNoopLogger()),
		confirmSchemaLayoutTargetOnlyFlag,
	)
}

func testSchemaLayoutSpec() persistencecassandra.SchemaLayoutSpec {
	return persistencecassandra.SchemaLayoutSpec{
		Name:               persistencecassandra.SchemaLayoutExecutions,
		Generation:         gocql.TimeUUID(),
		Version:            1,
		ImmutableParameter: 16,
	}
}

func testQueueV2SchemaLayoutSpecs() (
	metadataSpec persistencecassandra.SchemaLayoutSpec,
	messagesSpec persistencecassandra.SchemaLayoutSpec,
) {
	return persistencecassandra.SchemaLayoutSpec{
			Name:               persistencecassandra.SchemaLayoutQueueV2Metadata,
			Generation:         gocql.TimeUUID(),
			Version:            1,
			ImmutableParameter: 64,
		}, persistencecassandra.SchemaLayoutSpec{
			Name:               persistencecassandra.SchemaLayoutQueueV2Messages,
			Generation:         gocql.TimeUUID(),
			Version:            1,
			ImmutableParameter: 4096,
		}
}

type testQueueV2SchemaLayoutMetadataOperator struct {
	metadata     map[persistencecassandra.SchemaLayoutName]persistencecassandra.SchemaLayoutMetadata
	compareOrder []persistencecassandra.SchemaLayoutName
}

func newTestQueueV2SchemaLayoutMetadataOperator(
	metadataSpec persistencecassandra.SchemaLayoutSpec,
	messagesSpec persistencecassandra.SchemaLayoutSpec,
) *testQueueV2SchemaLayoutMetadataOperator {
	return &testQueueV2SchemaLayoutMetadataOperator{
		metadata: map[persistencecassandra.SchemaLayoutName]persistencecassandra.SchemaLayoutMetadata{
			metadataSpec.Name: {
				SchemaLayoutSpec: metadataSpec,
				AuthorityState:   persistencecassandra.SchemaLayoutAuthorityTargetReady,
				Epoch:            1,
			},
			messagesSpec.Name: {
				SchemaLayoutSpec: messagesSpec,
				AuthorityState:   persistencecassandra.SchemaLayoutAuthorityTargetReady,
				Epoch:            1,
			},
		},
	}
}

func (s *testQueueV2SchemaLayoutMetadataOperator) InitializePreparing(
	context.Context,
	persistencecassandra.SchemaLayoutSpec,
) (persistencecassandra.SchemaLayoutMetadata, error) {
	return persistencecassandra.SchemaLayoutMetadata{}, errors.New("unexpected initialize")
}

func (s *testQueueV2SchemaLayoutMetadataOperator) LoadAndValidate(
	_ context.Context,
	spec persistencecassandra.SchemaLayoutSpec,
) (persistencecassandra.SchemaLayoutMetadata, error) {
	metadata, ok := s.metadata[spec.Name]
	if !ok {
		return persistencecassandra.SchemaLayoutMetadata{}, errors.New("layout metadata is missing")
	}
	if metadata.SchemaLayoutSpec != spec {
		return persistencecassandra.SchemaLayoutMetadata{}, errors.New("layout metadata identity mismatch")
	}
	return metadata, nil
}

func (s *testQueueV2SchemaLayoutMetadataOperator) CompareAndSwapAuthority(
	_ context.Context,
	metadata persistencecassandra.SchemaLayoutMetadata,
	target persistencecassandra.SchemaLayoutAuthorityState,
) (persistencecassandra.SchemaLayoutMetadata, error) {
	actual := s.metadata[metadata.Name]
	if actual != metadata {
		return persistencecassandra.SchemaLayoutMetadata{}, errors.New("layout metadata changed")
	}
	metadata.AuthorityState = target
	metadata.Epoch++
	s.metadata[metadata.Name] = metadata
	s.compareOrder = append(s.compareOrder, metadata.Name)
	return metadata, nil
}

type testSchemaLayoutMetadataOperator struct {
	metadata      persistencecassandra.SchemaLayoutMetadata
	loadErr       error
	initializeErr error
	compareErr    error
	compareCalls  int
}

func (s *testSchemaLayoutMetadataOperator) InitializePreparing(
	_ context.Context,
	spec persistencecassandra.SchemaLayoutSpec,
) (persistencecassandra.SchemaLayoutMetadata, error) {
	if s.initializeErr != nil {
		return persistencecassandra.SchemaLayoutMetadata{}, s.initializeErr
	}
	if s.metadata.Name == "" {
		s.metadata = persistencecassandra.SchemaLayoutMetadata{
			SchemaLayoutSpec: spec,
			AuthorityState:   persistencecassandra.SchemaLayoutAuthorityPreparing,
		}
	}
	return s.metadata, nil
}

func (s *testSchemaLayoutMetadataOperator) LoadAndValidate(
	context.Context,
	persistencecassandra.SchemaLayoutSpec,
) (persistencecassandra.SchemaLayoutMetadata, error) {
	if s.loadErr != nil {
		return persistencecassandra.SchemaLayoutMetadata{}, s.loadErr
	}
	return s.metadata, nil
}

func (s *testSchemaLayoutMetadataOperator) CompareAndSwapAuthority(
	_ context.Context,
	metadata persistencecassandra.SchemaLayoutMetadata,
	target persistencecassandra.SchemaLayoutAuthorityState,
) (persistencecassandra.SchemaLayoutMetadata, error) {
	s.compareCalls++
	if s.compareErr != nil {
		return persistencecassandra.SchemaLayoutMetadata{}, s.compareErr
	}
	metadata.AuthorityState = target
	metadata.Epoch++
	s.metadata = metadata
	return metadata, nil
}
