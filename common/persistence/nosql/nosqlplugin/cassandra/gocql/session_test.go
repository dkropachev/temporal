package gocql

import (
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
