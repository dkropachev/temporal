package gocql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

func TestSessionEmitsMetricOnRefreshError(t *testing.T) {
	controller := gomock.NewController(t)
	metricsHandler := metrics.NewMockHandler(controller)
	s := session{
		status: common.DaemonStatusStarted,
		newClusterConfigFunc: func() (*gocql.ClusterConfig, error) {
			return nil, errors.New("mock error for failing cluster creation")
		},
		logger:         log.NewNoopLogger(),
		metricsHandler: metricsHandler,
	}

	metricsHandler.EXPECT().WithTags(metrics.FailureTag(refreshErrorTagValue)).Return(metricsHandler)
	metricsHandler.EXPECT().Counter(metrics.CassandraSessionRefreshFailures.Name()).Return(metrics.NoopCounterMetricFunc)

	s.refresh()
	controller.Finish()
}

func TestSessionEmitsMetricOnRefreshThrottle(t *testing.T) {
	controller := gomock.NewController(t)
	metricsHandler := metrics.NewMockHandler(controller)
	s := session{
		status:          common.DaemonStatusStarted,
		logger:          log.NewNoopLogger(),
		metricsHandler:  metricsHandler,
		sessionInitTime: time.Now().UTC(),
	}

	metricsHandler.EXPECT().WithTags(metrics.FailureTag(refreshThrottleTagValue)).Return(metricsHandler)
	metricsHandler.EXPECT().Counter(metrics.CassandraSessionRefreshFailures.Name()).Return(metrics.NoopCounterMetricFunc)

	s.refresh()
	controller.Finish()
}

func TestSessionCloseDuringRefreshClosesBothHandles(t *testing.T) {
	refreshStarted := make(chan struct{})
	continueRefresh := make(chan struct{})
	oldClosed := make(chan struct{})
	newClosed := make(chan struct{})

	oldHandle := &sessionHandle{closeFn: func() { close(oldClosed) }}
	newHandle := &sessionHandle{closeFn: func() { close(newClosed) }}
	s := session{
		status: common.DaemonStatusStarted,
		initSessionFunc: func() (*sessionHandle, error) {
			close(refreshStarted)
			<-continueRefresh
			return newHandle, nil
		},
		logger:         log.NewNoopLogger(),
		metricsHandler: metrics.NoopMetricsHandler,
	}
	s.handle.Store(oldHandle)

	refreshDone := make(chan struct{})
	go func() {
		s.refresh()
		close(refreshDone)
	}()
	<-refreshStarted

	closeDone := make(chan struct{})
	go func() {
		s.Close()
		close(closeDone)
	}()
	close(continueRefresh)

	require.Eventually(t, func() bool {
		select {
		case <-refreshDone:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		select {
		case <-closeDone:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		select {
		case <-oldClosed:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		select {
		case <-newClosed:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Equal(t, int32(common.DaemonStatusStopped), atomic.LoadInt32(&s.status))
	require.Same(t, newHandle, s.getHandle())
}

func TestPanicCapture(t *testing.T) {
	_, err := initSession(log.NewNoopLogger(), func() (*gocql.ClusterConfig, error) {
		panic("mock panic")
	}, metrics.NoopMetricsHandler)

	require.Error(t, err)
	require.Contains(t, err.Error(), "panic:")
}

func TestSchemaVersionsAgree(t *testing.T) {
	require.True(t, schemaVersionsAgree([]string{"version-1"}))
	require.True(t, schemaVersionsAgree([]string{"version-1", "version-1"}))
	require.False(t, schemaVersionsAgree(nil))
	require.False(t, schemaVersionsAgree([]string{""}))
	require.False(t, schemaVersionsAgree([]string{"version-1", "version-2"}))
}

func TestLegacySchemaAgreementHostPolicyPinsCoordinator(t *testing.T) {
	hosts := []gocql.SelectedHost{
		&testSelectedHost{info: new(gocql.HostInfo)},
		&testSelectedHost{info: new(gocql.HostInfo)},
	}
	delegate := &rotatingHostPolicy{hosts: hosts}
	policy := &legacySchemaAgreementHostPolicy{HostSelectionPolicy: delegate}
	selection := &legacySchemaAgreementHostSelection{}
	ctx := context.WithValue(context.Background(), legacySchemaAgreementHostSelectionKey{}, selection)

	firstQuery := new(gocql.Session).Query("SELECT schema_version FROM system.peers").WithContext(ctx)
	defer firstQuery.Release()
	firstCoordinator := policy.Pick(firstQuery)()
	require.NotNil(t, firstCoordinator)
	require.Same(t, firstCoordinator, selection.get())

	secondQuery := new(gocql.Session).Query("SELECT schema_version FROM system.local").WithContext(ctx)
	defer secondQuery.Release()
	secondQueryHosts := policy.Pick(secondQuery)
	require.Same(t, firstCoordinator, secondQueryHosts())
	require.Nil(t, secondQueryHosts())
	require.Equal(t, 1, delegate.pickCount)
}

func TestLegacySchemaAgreementHostPolicyPreservesOptionalCapabilities(t *testing.T) {
	delegate := &readyBulkHostPolicy{ready: true}
	policy := &legacySchemaAgreementHostPolicy{HostSelectionPolicy: delegate}
	hosts := []*gocql.HostInfo{new(gocql.HostInfo), new(gocql.HostInfo)}

	require.True(t, policy.Ready())
	policy.AddHosts(hosts)
	require.Equal(t, hosts, delegate.hosts)

	delegate.ready = false
	require.False(t, policy.Ready())
}

type rotatingHostPolicy struct {
	gocql.HostSelectionPolicy
	hosts     []gocql.SelectedHost
	pickCount int
}

type readyBulkHostPolicy struct {
	gocql.HostSelectionPolicy
	ready bool
	hosts []*gocql.HostInfo
}

func (p *readyBulkHostPolicy) Ready() bool {
	return p.ready
}

func (p *readyBulkHostPolicy) AddHosts(hosts []*gocql.HostInfo) {
	p.hosts = hosts
}

func (p *rotatingHostPolicy) Pick(gocql.ExecutableQuery) gocql.NextHost {
	host := p.hosts[p.pickCount%len(p.hosts)]
	p.pickCount++
	return singleHostIterator(host)
}

type testSelectedHost struct {
	info *gocql.HostInfo
}

func (h *testSelectedHost) Info() *gocql.HostInfo {
	return h.info
}

func (h *testSelectedHost) Token() gocql.Token {
	return nil
}

func (h *testSelectedHost) Mark(error) {}
