package gocql

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var _ Session = (*session)(nil)

const sessionRefreshMinInternal = 5 * time.Second

const (
	refreshThrottleTagValue = "throttle"
	refreshErrorTagValue    = "error"
)

type (
	session struct {
		status               int32
		newClusterConfigFunc func() (*gocql.ClusterConfig, error)
		initSessionFunc      func() (*sessionHandle, error)
		handle               atomic.Pointer[sessionHandle]
		logger               log.Logger

		sync.Mutex
		sessionInitTime time.Time
		metricsHandler  metrics.Handler
	}

	sessionHandle struct {
		session *gocql.Session
		closeFn func()
	}
)

func NewSession(
	newClusterConfigFunc func() (*gocql.ClusterConfig, error),
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*session, error) {

	handle, err := initSession(logger, newClusterConfigFunc, metricsHandler)
	if err != nil {
		return nil, err
	}

	session := &session{
		status:               common.DaemonStatusStarted,
		newClusterConfigFunc: newClusterConfigFunc,
		logger:               logger,
		metricsHandler:       metricsHandler,

		sessionInitTime: time.Now().UTC(),
	}
	session.handle.Store(handle)
	return session, nil
}

func (s *session) refresh() {
	if atomic.LoadInt32(&s.status) != common.DaemonStatusStarted {
		return
	}

	s.Lock()
	defer s.Unlock()

	if atomic.LoadInt32(&s.status) != common.DaemonStatusStarted {
		return
	}
	if time.Now().UTC().Sub(s.sessionInitTime) < sessionRefreshMinInternal {
		s.logger.Warn("gocql wrapper: did not refresh gocql session because the last refresh was too close",
			tag.Duration("min_refresh_interval_seconds", sessionRefreshMinInternal))
		handler := s.metricsHandler.WithTags(metrics.FailureTag(refreshThrottleTagValue))
		metrics.CassandraSessionRefreshFailures.With(handler).Record(1)
		return
	}

	newHandle, err := s.initSession()
	if err != nil {
		s.logger.Error("gocql wrapper: unable to refresh gocql session", tag.Error(err))
		handler := s.metricsHandler.WithTags(metrics.FailureTag(refreshErrorTagValue))
		metrics.CassandraSessionRefreshFailures.With(handler).Record(1)
		return
	}

	s.sessionInitTime = time.Now().UTC()
	oldHandle := s.getHandle()
	s.handle.Store(newHandle)
	go oldHandle.close()
	s.logger.Warn("gocql wrapper: successfully refreshed gocql session")
}

func (s *session) initSession() (*sessionHandle, error) {
	if s.initSessionFunc != nil {
		return s.initSessionFunc()
	}
	return initSession(s.logger, s.newClusterConfigFunc, s.metricsHandler)
}

func initSession(
	logger log.Logger,
	newClusterConfigFunc func() (*gocql.ClusterConfig, error),
	metricsHandler metrics.Handler,
) (handle *sessionHandle, retErr error) {
	defer log.CapturePanic(logger, &retErr)
	cluster, err := newClusterConfigFunc()
	if err != nil {
		return nil, err
	}
	start := time.Now()
	defer func() {
		metrics.CassandraInitSessionLatency.With(metricsHandler).Record(time.Since(start))
	}()
	gocqlSession, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &sessionHandle{session: gocqlSession}, nil
}

func (s *session) Query(
	stmt string,
	values ...any,
) Query {
	q := s.getHandle().session.Query(stmt, values...)
	if q == nil {
		return nil
	}

	return &query{
		session:    s,
		gocqlQuery: q,
	}
}

func (s *session) NewBatch(
	batchType BatchType,
) *Batch {
	b := s.getHandle().session.Batch(mustConvertBatchType(batchType))
	if b == nil {
		return nil
	}
	return &Batch{
		session:    s,
		gocqlBatch: b,
	}
}

func (s *session) ExecuteBatch(
	b *Batch,
) (retError error) {
	defer func() { s.handleError(retError) }()

	return s.getHandle().session.ExecuteBatch(b.gocqlBatch)
}

func (s *session) MapExecuteBatchCAS(
	b *Batch,
	previous map[string]any,
) (_ bool, _ Iter, retError error) {
	defer func() { s.handleError(retError) }()

	applied, iter, err := s.getHandle().session.MapExecuteBatchCAS(b.gocqlBatch, previous)
	return applied, iter, err
}

func (s *session) AwaitSchemaAgreement(
	ctx context.Context,
) (retError error) {
	defer func() { s.handleError(retError) }()

	return s.getHandle().session.AwaitSchemaAgreement(ctx)
}

func (s *session) getHandle() *sessionHandle {
	return s.handle.Load()
}

func (s *session) Close() {
	s.Lock()
	defer s.Unlock()

	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	s.getHandle().close()
}

func (h *sessionHandle) close() {
	if h.closeFn != nil {
		h.closeFn()
		return
	}
	h.session.Close()
}

func (s *session) handleError(
	err error,
) {
	switch err {
	case gocql.ErrNoConnections,
		gocql.ErrSessionClosed:
		s.refresh()
	default:
		// noop
	}
}
