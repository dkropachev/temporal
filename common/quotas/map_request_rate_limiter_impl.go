package quotas

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	rateLimiterTTL             = 1 * time.Hour
	rateLimiterCleanupInterval = 1 * time.Hour
)

type (
	// RequestRateLimiterKeyFn extracts the map key from the request
	RequestRateLimiterKeyFn[K comparable] func(req Request) K

	rateLimiterEntry struct {
		rateLimiter RequestRateLimiter
		lastAccess  atomic.Int64
	}

	// MapRequestRateLimiterImpl holds rate limiters keyed by K, evicting entries idle
	// past the TTL. Eviction is traffic-triggered (once per interval) and swept in a
	// short-lived goroutine, so there is no long-lived goroutine or lifecycle to stop.
	MapRequestRateLimiterImpl[K comparable] struct {
		rateLimiterGenFn RequestRateLimiterFn
		rateLimiterKeyFn RequestRateLimiterKeyFn[K]

		sync.RWMutex
		rateLimiters map[K]*rateLimiterEntry
		ttlNano      int64 // TTL in nanoseconds

		cleanupIntervalNano int64
		lastCleanupNano     atomic.Int64
	}
)

func NewMapRequestRateLimiter[K comparable](
	rateLimiterGenFn RequestRateLimiterFn,
	rateLimiterKeyFn RequestRateLimiterKeyFn[K],
) *MapRequestRateLimiterImpl[K] {
	return &MapRequestRateLimiterImpl[K]{
		rateLimiterGenFn:    rateLimiterGenFn,
		rateLimiterKeyFn:    rateLimiterKeyFn,
		rateLimiters:        make(map[K]*rateLimiterEntry),
		ttlNano:             int64(rateLimiterTTL),
		cleanupIntervalNano: int64(rateLimiterCleanupInterval),
	}
}

func namespaceRequestRateLimiterKeyFn(req Request) string {
	return req.Caller
}

func NewNamespaceRequestRateLimiter(
	rateLimiterGenFn RequestRateLimiterFn,
) *MapRequestRateLimiterImpl[string] {
	return NewMapRequestRateLimiter(rateLimiterGenFn, namespaceRequestRateLimiterKeyFn)
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (r *MapRequestRateLimiterImpl[_]) Allow(
	now time.Time,
	request Request,
) bool {
	rateLimiter := r.getOrInitRateLimiter(now, request)
	return rateLimiter.Allow(now, request)
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (r *MapRequestRateLimiterImpl[_]) Reserve(
	now time.Time,
	request Request,
) Reservation {
	rateLimiter := r.getOrInitRateLimiter(now, request)
	return rateLimiter.Reserve(now, request)
}

// Wait waits till the deadline for a rate limit token to allow the request
// to go through.
func (r *MapRequestRateLimiterImpl[_]) Wait(
	ctx context.Context,
	request Request,
) error {
	rateLimiter := r.getOrInitRateLimiter(time.Now(), request)
	return rateLimiter.Wait(ctx, request)
}

func (r *MapRequestRateLimiterImpl[K]) getOrInitRateLimiter(
	now time.Time,
	req Request,
) RequestRateLimiter {
	nowNano := now.UnixNano()
	r.maybeCleanup(now, nowNano)

	key := r.rateLimiterKeyFn(req)

	// Refresh lastAccess under the read lock so a concurrent cleanup can't evict
	// the entry between lookup and refresh.
	r.RLock()
	entry, ok := r.rateLimiters[key]
	if ok {
		entry.lastAccess.Store(nowNano)
	}
	r.RUnlock()

	if ok {
		return entry.rateLimiter
	}

	newRateLimiter := r.rateLimiterGenFn(req)
	r.Lock()
	defer r.Unlock()

	if entry, ok := r.rateLimiters[key]; ok {
		entry.lastAccess.Store(nowNano)
		return entry.rateLimiter
	}

	entry = &rateLimiterEntry{rateLimiter: newRateLimiter}
	entry.lastAccess.Store(nowNano)
	r.rateLimiters[key] = entry
	return newRateLimiter
}

// maybeCleanup sweeps at most once per interval, off the request path. The CAS
// elects a single sweeper and advances lastCleanupNano before it starts.
func (r *MapRequestRateLimiterImpl[K]) maybeCleanup(now time.Time, nowNano int64) {
	last := r.lastCleanupNano.Load()
	if nowNano-last > r.cleanupIntervalNano && r.lastCleanupNano.CompareAndSwap(last, nowNano) {
		go func() {
			// recover on the sweep's own goroutine so a panic can't crash the process.
			defer func() { _ = recover() }()
			r.cleanup(now)
		}()
	}
}

// cleanup collects expired keys under the read lock, then deletes each under the
// write lock with a re-check, so reads are only briefly blocked.
func (r *MapRequestRateLimiterImpl[K]) cleanup(now time.Time) {
	nowNano := now.UnixNano()
	ttlNano := r.ttlNano

	var expiredKeys []K
	r.RLock()
	for k, e := range r.rateLimiters {
		if nowNano-e.lastAccess.Load() > ttlNano {
			expiredKeys = append(expiredKeys, k)
		}
	}
	r.RUnlock()

	for _, k := range expiredKeys {
		r.Lock()
		if e, ok := r.rateLimiters[k]; ok && nowNano-e.lastAccess.Load() > ttlNano {
			delete(r.rateLimiters, k)
		}
		r.Unlock()
	}
}
