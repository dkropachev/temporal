package quotas

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	mapRequestRateLimiterSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestMapRequestRateLimiterSuite(t *testing.T) {
	s := new(mapRequestRateLimiterSuite)
	suite.Run(t, s)
}

func (s *mapRequestRateLimiterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *mapRequestRateLimiterSuite) TestGetOrCreateRateLimiter() {
	createCount := 0
	rateLimiter := NewMapRequestRateLimiter(
		func(req Request) RequestRateLimiter {
			createCount++
			return NoopRequestRateLimiter
		},
		func(req Request) string { return req.Caller },
	)

	now := time.Now()
	req1 := Request{Caller: "namespace1"}
	req2 := Request{Caller: "namespace2"}

	// First access creates a new rate limiter
	rateLimiter.Allow(now, req1)
	s.Equal(1, createCount)

	// Same key reuses existing rate limiter
	rateLimiter.Allow(now, req1)
	s.Equal(1, createCount)

	// Different key creates new rate limiter
	rateLimiter.Allow(now, req2)
	s.Equal(2, createCount)
}

func (s *mapRequestRateLimiterSuite) TestCleanup() {
	createCount := 0
	rateLimiter := NewMapRequestRateLimiter(
		func(req Request) RequestRateLimiter {
			createCount++
			return NoopRequestRateLimiter
		},
		func(req Request) string { return req.Caller },
	)
	// Override TTL for testing
	rateLimiter.ttlNano = int64(100 * time.Millisecond)

	now := time.Now()
	req1 := Request{Caller: "namespace1"}
	req2 := Request{Caller: "namespace2"}

	// Create entries for both namespaces
	rateLimiter.Allow(now, req1)
	rateLimiter.Allow(now.Add(10*time.Millisecond), req2)
	s.Equal(2, createCount)

	// Verify both entries exist
	s.Len(rateLimiter.rateLimiters, 2)

	// Trigger cleanup after TTL expires for both
	rateLimiter.cleanup(now.Add(200 * time.Millisecond))

	// Both should be evicted
	s.Empty(rateLimiter.rateLimiters)
}

func (s *mapRequestRateLimiterSuite) TestLazyCleanupOnAccess() {
	rateLimiter := NewMapRequestRateLimiter(
		func(req Request) RequestRateLimiter { return NoopRequestRateLimiter },
		func(req Request) string { return req.Caller },
	)
	rateLimiter.ttlNano = int64(100 * time.Millisecond)
	rateLimiter.cleanupIntervalNano = int64(50 * time.Millisecond)

	now := time.Now()
	req1 := Request{Caller: "namespace1"}
	req2 := Request{Caller: "namespace2"}

	rateLimiter.Allow(now, req1)
	s.Len(rateLimiter.rateLimiters, 1)

	// A later access past both the cleanup interval and req1's TTL triggers the
	// eviction sweep, which runs in a short-lived goroutine.
	rateLimiter.Allow(now.Add(200*time.Millisecond), req2)

	s.Eventually(func() bool {
		rateLimiter.RLock()
		defer rateLimiter.RUnlock()
		_, has1 := rateLimiter.rateLimiters["namespace1"]
		_, has2 := rateLimiter.rateLimiters["namespace2"]
		return !has1 && has2
	}, time.Second, time.Millisecond)
}

func (s *mapRequestRateLimiterSuite) TestCleanupThrottledByInterval() {
	rateLimiter := NewMapRequestRateLimiter(
		func(req Request) RequestRateLimiter { return NoopRequestRateLimiter },
		func(req Request) string { return req.Caller },
	)
	rateLimiter.ttlNano = int64(10 * time.Millisecond)
	rateLimiter.cleanupIntervalNano = int64(time.Hour)

	now := time.Now()
	req1 := Request{Caller: "namespace1"}
	req2 := Request{Caller: "namespace2"}

	rateLimiter.Allow(now, req1)
	// req1 is now past its TTL, but the cleanup interval has not elapsed, so the
	// next access must not evict it.
	rateLimiter.Allow(now.Add(50*time.Millisecond), req2)
	s.Len(rateLimiter.rateLimiters, 2)
}

// TestConcurrentAccessAndCleanup drives many goroutines through the access path
// (which refreshes lastAccess under the read lock) while a dedicated goroutine
// runs cleanup (which deletes under the write lock), exercising that interaction.
// It guards the changed path against data races; run with -race. The cleanup
// interval is set high so the access path does not also spawn its own sweeps,
// keeping the concurrency driven by the explicit cleanup goroutine below.
func (s *mapRequestRateLimiterSuite) TestConcurrentAccessAndCleanup() {
	rateLimiter := NewMapRequestRateLimiter(
		func(req Request) RequestRateLimiter { return NoopRequestRateLimiter },
		func(req Request) string { return req.Caller },
	)
	rateLimiter.ttlNano = int64(time.Millisecond)
	rateLimiter.cleanupIntervalNano = int64(time.Hour)

	const (
		workers = 8
		iters   = 5000
		keys    = 16
	)
	stop := make(chan struct{})
	var cleanerWG sync.WaitGroup
	cleanerWG.Add(1)
	go func() {
		defer cleanerWG.Done()
		base := time.Now()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			rateLimiter.cleanup(base.Add(time.Duration(i) * time.Millisecond))
		}
	}()

	var accessWG sync.WaitGroup
	for w := range workers {
		accessWG.Add(1)
		go func(w int) {
			defer accessWG.Done()
			now := time.Now()
			for i := range iters {
				req := Request{Caller: fmt.Sprintf("ns-%d", (w+i)%keys)}
				rateLimiter.Allow(now.Add(time.Duration(i)*time.Microsecond), req)
			}
		}(w)
	}
	accessWG.Wait()

	close(stop)
	cleanerWG.Wait()
}

func (s *mapRequestRateLimiterSuite) TestAccessRefreshesTTL() {
	createCount := 0
	rateLimiter := NewMapRequestRateLimiter(
		func(req Request) RequestRateLimiter {
			createCount++
			return NoopRequestRateLimiter
		},
		func(req Request) string { return req.Caller },
	)
	// Override TTL for testing
	rateLimiter.ttlNano = int64(100 * time.Millisecond)

	now := time.Now()
	req1 := Request{Caller: "namespace1"}
	req2 := Request{Caller: "namespace2"}

	// Create entries for both namespaces
	rateLimiter.Allow(now, req1)
	rateLimiter.Allow(now.Add(10*time.Millisecond), req2)
	s.Equal(2, createCount)

	// Access namespace1 again to refresh its TTL
	rateLimiter.Allow(now.Add(50*time.Millisecond), req1)
	s.Equal(2, createCount) // No new rate limiter created

	// Cleanup at now+150ms should evict namespace2 (>100ms since last access at now+10ms)
	// but namespace1 should survive (last accessed at now+50ms, only 100ms ago)
	rateLimiter.cleanup(now.Add(150 * time.Millisecond))

	s.Len(rateLimiter.rateLimiters, 1)
	_, exists := rateLimiter.rateLimiters["namespace1"]
	s.True(exists)
	_, exists = rateLimiter.rateLimiters["namespace2"]
	s.False(exists)
}

func (s *mapRequestRateLimiterSuite) TestNamespaceRequestRateLimiter() {
	createCount := 0
	rateLimiter := NewNamespaceRequestRateLimiter(
		func(req Request) RequestRateLimiter {
			createCount++
			return NoopRequestRateLimiter
		},
	)

	now := time.Now()

	// Uses Caller as key
	rateLimiter.Allow(now, Request{Caller: "namespace1"})
	s.Equal(1, createCount)

	rateLimiter.Allow(now, Request{Caller: "namespace1"})
	s.Equal(1, createCount)

	rateLimiter.Allow(now, Request{Caller: "namespace2"})
	s.Equal(2, createCount)
}
