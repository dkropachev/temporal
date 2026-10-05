package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

const noopVisibilityStoreName = "temporalperf-noop-visibility"

var errVisibilityReadDisabled = serviceerror.NewUnavailable(
	"visibility reads are disabled for the workflow/history persistence benchmark",
)

type noopVisibilityFactory struct {
	mu     sync.Mutex
	stores []*noopVisibilityStore
}

type noopVisibilityStore struct {
	indexName   string
	writes      atomic.Uint64
	reads       atomic.Uint64
	validations atomic.Uint64
	admin       atomic.Uint64
}

type noopVisibilitySummary struct {
	writes      uint64
	reads       uint64
	validations uint64
	admin       uint64
	stores      int
}

func (f *noopVisibilityFactory) NewVisibilityStore(
	cfg config.CustomDatastoreConfig,
	_ searchattribute.Provider,
	_ searchattribute.MapperProvider,
	_ namespace.Registry,
	_ *chasm.Registry,
	_ resolver.ServiceResolver,
	_ log.Logger,
	_ metrics.Handler,
) (store.VisibilityStore, error) {
	if cfg.Name != noopVisibilityStoreName {
		return nil, fmt.Errorf("unsupported custom visibility store %q", cfg.Name)
	}
	if cfg.IndexName == "" {
		return nil, errors.New("no-op visibility indexName is required")
	}
	if len(cfg.Options) != 0 {
		return nil, errors.New("no-op visibility options must be empty")
	}
	visibilityStore := &noopVisibilityStore{indexName: cfg.IndexName}
	f.mu.Lock()
	f.stores = append(f.stores, visibilityStore)
	f.mu.Unlock()
	return visibilityStore, nil
}

func (f *noopVisibilityFactory) summary() noopVisibilitySummary {
	f.mu.Lock()
	defer f.mu.Unlock()
	summary := noopVisibilitySummary{stores: len(f.stores)}
	for _, visibilityStore := range f.stores {
		summary.writes += visibilityStore.writes.Load()
		summary.reads += visibilityStore.reads.Load()
		summary.validations += visibilityStore.validations.Load()
		summary.admin += visibilityStore.admin.Load()
	}
	return summary
}

func (*noopVisibilityStore) Close() {}

func (*noopVisibilityStore) GetName() string {
	return noopVisibilityStoreName
}

func (s *noopVisibilityStore) GetIndexName() string {
	return s.indexName
}

func (s *noopVisibilityStore) ValidateCustomSearchAttributes(
	searchAttributes map[string]any,
) (map[string]any, error) {
	s.validations.Add(1)
	validated := make(map[string]any, len(searchAttributes))
	for name, value := range searchAttributes {
		validated[name] = value
	}
	return validated, nil
}

func (s *noopVisibilityStore) RecordWorkflowExecutionStarted(
	ctx context.Context,
	_ *store.InternalRecordWorkflowExecutionStartedRequest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.writes.Add(1)
	return nil
}

func (s *noopVisibilityStore) RecordWorkflowExecutionClosed(
	ctx context.Context,
	_ *store.InternalRecordWorkflowExecutionClosedRequest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.writes.Add(1)
	return nil
}

func (s *noopVisibilityStore) UpsertWorkflowExecution(
	ctx context.Context,
	_ *store.InternalUpsertWorkflowExecutionRequest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.writes.Add(1)
	return nil
}

func (s *noopVisibilityStore) DeleteWorkflowExecution(
	ctx context.Context,
	_ *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.writes.Add(1)
	return nil
}

func (s *noopVisibilityStore) ListWorkflowExecutions(
	ctx context.Context,
	_ *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListExecutionsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.reads.Add(1)
	return nil, errVisibilityReadDisabled
}

func (s *noopVisibilityStore) CountWorkflowExecutions(
	ctx context.Context,
	_ *manager.CountWorkflowExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.reads.Add(1)
	return nil, errVisibilityReadDisabled
}

func (s *noopVisibilityStore) GetWorkflowExecution(
	ctx context.Context,
	_ *manager.GetWorkflowExecutionRequest,
) (*store.InternalGetWorkflowExecutionResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.reads.Add(1)
	return nil, errVisibilityReadDisabled
}

func (s *noopVisibilityStore) ListChasmExecutions(
	ctx context.Context,
	_ *visibilityservice.ListChasmExecutionsRequest,
) (*store.InternalListExecutionsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.reads.Add(1)
	return nil, errVisibilityReadDisabled
}

func (s *noopVisibilityStore) CountChasmExecutions(
	ctx context.Context,
	_ *visibilityservice.CountChasmExecutionsRequest,
) (*store.InternalCountExecutionsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.reads.Add(1)
	return nil, errVisibilityReadDisabled
}

func (s *noopVisibilityStore) AddSearchAttributes(
	ctx context.Context,
	_ *manager.AddSearchAttributesRequest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.admin.Add(1)
	return errVisibilityReadDisabled
}

var _ store.VisibilityStore = (*noopVisibilityStore)(nil)
