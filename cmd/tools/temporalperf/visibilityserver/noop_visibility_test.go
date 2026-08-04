package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

func TestParseCommand(t *testing.T) {
	testCases := []struct {
		name            string
		args            []string
		wantConfigFile  string
		wantAllowNoAuth bool
		wantError       string
	}{
		{
			name:            "benchmark invocation",
			args:            []string{"--config-file", "/tmp/benchmark.yaml", "--allow-no-auth", "start"},
			wantConfigFile:  "/tmp/benchmark.yaml",
			wantAllowNoAuth: true,
		},
		{
			name:           "no auth flag omitted",
			args:           []string{"--config-file=/tmp/benchmark.yaml", "start"},
			wantConfigFile: "/tmp/benchmark.yaml",
		},
		{name: "config omitted", args: []string{"start"}, wantError: "--config-file is required"},
		{
			name:      "config repeated",
			args:      []string{"--config-file", "first", "--config-file", "second", "start"},
			wantError: "may only be specified once",
		},
		{
			name:      "auth flag repeated",
			args:      []string{"--config-file", "config", "--allow-no-auth", "--allow-no-auth", "start"},
			wantError: "may only be specified once",
		},
		{
			name:      "command wrong",
			args:      []string{"--config-file", "config", "validate"},
			wantError: "exactly one start command is required",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			commandConfig, err := parseCommand(testCase.args)
			if testCase.wantError != "" {
				require.ErrorContains(t, err, testCase.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.wantConfigFile, commandConfig.configFile)
			require.Equal(t, testCase.wantAllowNoAuth, commandConfig.allowNoAuth)
		})
	}
}

func TestConfigureNoopVisibility(t *testing.T) {
	persistence := config.Persistence{
		DefaultStore:             "default",
		VisibilityStore:          "configured-visibility",
		SecondaryVisibilityStore: "secondary-visibility",
		DataStores: map[string]config.DataStore{
			"default":               {},
			"configured-visibility": {},
			"secondary-visibility":  {},
		},
	}

	require.NoError(t, configureNoopVisibility(&persistence))
	require.Equal(t, "default", persistence.DefaultStore)
	require.Equal(t, noopVisibilityStoreName, persistence.VisibilityStore)
	require.Empty(t, persistence.SecondaryVisibilityStore)
	require.Contains(t, persistence.DataStores, "configured-visibility")
	require.Contains(t, persistence.DataStores, "secondary-visibility")
	custom := persistence.DataStores[noopVisibilityStoreName].CustomDataStoreConfig
	require.NotNil(t, custom)
	require.Equal(t, noopVisibilityStoreName, custom.Name)
	require.Equal(t, noopVisibilityStoreName, custom.IndexName)
	require.Empty(t, custom.Options)
}

func TestConfigureNoopVisibilityRejectsReservedNames(t *testing.T) {
	testCases := []struct {
		name        string
		persistence config.Persistence
	}{
		{
			name: "default store",
			persistence: config.Persistence{
				DefaultStore: noopVisibilityStoreName,
				DataStores:   map[string]config.DataStore{},
			},
		},
		{
			name: "existing datastore",
			persistence: config.Persistence{
				DefaultStore: "default",
				DataStores: map[string]config.DataStore{
					noopVisibilityStoreName: {},
				},
			},
		},
		{name: "missing datastores", persistence: config.Persistence{DefaultStore: "default"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Error(t, configureNoopVisibility(&testCase.persistence))
		})
	}
}

func TestNoopVisibilityFactory(t *testing.T) {
	factory := &noopVisibilityFactory{}
	visibilityStore, err := factory.NewVisibilityStore(config.CustomDatastoreConfig{
		Name: noopVisibilityStoreName, IndexName: "workflow-history-persistence",
	}, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, noopVisibilityStoreName, visibilityStore.GetName())
	require.Equal(t, "workflow-history-persistence", visibilityStore.GetIndexName())
	require.Equal(t, noopVisibilitySummary{stores: 1}, factory.summary())

	_, err = factory.NewVisibilityStore(config.CustomDatastoreConfig{
		Name: "unexpected", IndexName: "index",
	}, nil, nil, nil, nil, nil, nil, nil)
	require.ErrorContains(t, err, "unsupported custom visibility store")

	_, err = factory.NewVisibilityStore(config.CustomDatastoreConfig{
		Name: noopVisibilityStoreName,
	}, nil, nil, nil, nil, nil, nil, nil)
	require.ErrorContains(t, err, "indexName is required")

	_, err = factory.NewVisibilityStore(config.CustomDatastoreConfig{
		Name: noopVisibilityStoreName, IndexName: "index", Options: map[string]any{"unexpected": true},
	}, nil, nil, nil, nil, nil, nil, nil)
	require.ErrorContains(t, err, "options must be empty")
}

func TestNoopVisibilityStoreWriteSemantics(t *testing.T) {
	factory := &noopVisibilityFactory{}
	visibilityStore, err := factory.NewVisibilityStore(config.CustomDatastoreConfig{
		Name: noopVisibilityStoreName, IndexName: "workflow-history-persistence",
	}, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	require.NoError(t, visibilityStore.RecordWorkflowExecutionStarted(t.Context(), nil))
	require.NoError(t, visibilityStore.RecordWorkflowExecutionClosed(t.Context(), nil))
	require.NoError(t, visibilityStore.UpsertWorkflowExecution(t.Context(), nil))
	require.NoError(t, visibilityStore.DeleteWorkflowExecution(t.Context(), nil))
	require.Equal(t, noopVisibilitySummary{writes: 4, stores: 1}, factory.summary())
}

func TestNoopVisibilityStoreReadAndAdminSemantics(t *testing.T) {
	visibilityStore := &noopVisibilityStore{indexName: "workflow-history-persistence"}
	readCalls := []func(context.Context) error{
		func(ctx context.Context) error {
			_, err := visibilityStore.ListWorkflowExecutions(ctx, nil)
			return err
		},
		func(ctx context.Context) error {
			_, err := visibilityStore.CountWorkflowExecutions(ctx, nil)
			return err
		},
		func(ctx context.Context) error {
			_, err := visibilityStore.GetWorkflowExecution(ctx, nil)
			return err
		},
		func(ctx context.Context) error {
			_, err := visibilityStore.ListChasmExecutions(ctx, nil)
			return err
		},
		func(ctx context.Context) error {
			_, err := visibilityStore.CountChasmExecutions(ctx, nil)
			return err
		},
	}
	for _, call := range readCalls {
		err := call(t.Context())
		var unavailable *serviceerror.Unavailable
		require.ErrorAs(t, err, &unavailable)
		require.Same(t, errVisibilityReadDisabled, err)
	}
	err := visibilityStore.AddSearchAttributes(t.Context(), &manager.AddSearchAttributesRequest{})
	require.Same(t, errVisibilityReadDisabled, err)
	require.Equal(t, uint64(len(readCalls)), visibilityStore.reads.Load())
	require.Equal(t, uint64(1), visibilityStore.admin.Load())
}

func TestNoopVisibilityStoreCopiesSearchAttributes(t *testing.T) {
	visibilityStore := &noopVisibilityStore{indexName: "workflow-history-persistence"}
	input := map[string]any{"CustomKeywordField": "value"}
	validated, err := visibilityStore.ValidateCustomSearchAttributes(input)
	require.NoError(t, err)
	validated["CustomKeywordField"] = "changed"
	require.Equal(t, "value", input["CustomKeywordField"])
	require.Equal(t, uint64(1), visibilityStore.validations.Load())
}

func TestNoopVisibilityStorePropagatesCanceledContext(t *testing.T) {
	visibilityStore := &noopVisibilityStore{indexName: "workflow-history-persistence"}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, visibilityStore.RecordWorkflowExecutionStarted(ctx, nil), context.Canceled)
	_, err := visibilityStore.ListWorkflowExecutions(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, visibilityStore.AddSearchAttributes(ctx, nil), context.Canceled)
	require.Zero(t, visibilityStore.writes.Load())
	require.Zero(t, visibilityStore.reads.Load())
	require.Zero(t, visibilityStore.admin.Load())
}
