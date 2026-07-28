package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
)

var sanitizer = tally.NewSanitizer(tally.SanitizeOptions{
	NameCharacters:       tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	KeyCharacters:        tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	ValueCharacters:      tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	ReplacementCharacter: '_',
})

type (
	excludeTags map[string]map[string]struct{}

	tallyTagCacheKey struct {
		count uint8
		tags  [maxCachedTallyTags]Tag
	}

	tallyMetricsCache struct {
		taggedHandlers sync.Map
		state          *tallyMetricsCacheState
	}

	tallyMetricsCacheState struct {
		taggedHandlersCount atomic.Int64
	}

	tallyMetricsHandlerBase struct {
		scope          tally.Scope
		perUnitBuckets map[MetricUnit]tally.Buckets
		excludeTags    excludeTags
	}

	tallyHandler interface {
		Handler
		tallyScope() tally.Scope
	}

	tallyMetricsHandler struct {
		tallyMetricsHandlerBase
		cache *tallyMetricsCache
	}

	uncachedTallyMetricsHandler struct {
		tallyMetricsHandlerBase
	}
)

const (
	maxCachedTallyTags = 8
	// Derived handlers share one budget so nested tags cannot multiply the retained cardinality.
	maxCachedTallyTagSets = 8192
)

var (
	_ BatchHandler = (*tallyMetricsHandler)(nil)
	_ BatchHandler = (*uncachedTallyMetricsHandler)(nil)
)

func NewTallyMetricsHandler(cfg ClientConfig, scope tally.Scope) *tallyMetricsHandler {
	perUnitBuckets := make(map[MetricUnit]tally.Buckets)

	for unit, boundariesList := range cfg.PerUnitHistogramBoundaries {
		perUnitBuckets[MetricUnit(unit)] = tally.ValueBuckets(boundariesList)
	}

	return &tallyMetricsHandler{
		tallyMetricsHandlerBase: tallyMetricsHandlerBase{
			scope:          scope,
			perUnitBuckets: perUnitBuckets,
			excludeTags:    configExcludeTags(cfg),
		},
		cache: &tallyMetricsCache{
			state: &tallyMetricsCacheState{},
		},
	}
}

// WithTags creates a new MetricProvder with provided []Tag
// Tags are merged with registered Tags from the source MetricsHandler
func (tmh *tallyMetricsHandler) WithTags(tags ...Tag) Handler {
	if len(tags) == 0 {
		return tmh
	}
	if tmh.cache == nil {
		return tmh.newUncachedTaggedHandler(tags)
	}
	return tmh.withTagsCached(tags)
}

func (tmh *tallyMetricsHandler) withTags(tags []Tag) tallyHandler {
	if len(tags) == 0 {
		return tmh
	}
	if tmh.cache == nil {
		return tmh.newUncachedTaggedHandler(tags)
	}
	return tmh.withTagsCached(tags)
}

func (tmh *tallyMetricsHandler) withTagsCached(tags []Tag) tallyHandler {
	cacheKey, cacheable := newTallyTagCacheKey(tags)
	if !cacheable {
		return tmh.newUncachedTaggedHandler(tags)
	}

	if taggedHandler, ok := tmh.cache.taggedHandlers.Load(cacheKey); ok {
		if typedHandler, ok := taggedHandler.(*tallyMetricsHandler); ok {
			return typedHandler
		}
		return tmh.newUncachedTaggedHandler(tags)
	}

	if !tmh.cache.state.reserveTaggedHandler() {
		return tmh.newUncachedTaggedHandler(tags)
	}

	taggedHandler := tmh.newCachedTaggedHandler(tags)
	existingHandler, loaded := tmh.cache.taggedHandlers.LoadOrStore(cacheKey, taggedHandler)
	if loaded {
		tmh.cache.state.taggedHandlersCount.Add(-1)
		if typedHandler, ok := existingHandler.(*tallyMetricsHandler); ok {
			return typedHandler
		}
		return tmh.newUncachedTaggedHandler(tags)
	}
	return taggedHandler
}

func (state *tallyMetricsCacheState) reserveTaggedHandler() bool {
	for {
		count := state.taggedHandlersCount.Load()
		if count >= maxCachedTallyTagSets {
			return false
		}
		if state.taggedHandlersCount.CompareAndSwap(count, count+1) {
			return true
		}
	}
}

func newTallyTagCacheKey(tags []Tag) (tallyTagCacheKey, bool) {
	if len(tags) > maxCachedTallyTags {
		return tallyTagCacheKey{}, false
	}

	key := tallyTagCacheKey{count: uint8(len(tags))}
	copy(key.tags[:], tags)
	return key, true
}

func (tmh *tallyMetricsHandler) scopeWithTags(tags []Tag) tally.Scope {
	if len(tags) == 0 {
		return tmh.scope
	}
	return tmh.withTags(tags).tallyScope()
}

func (tmh *tallyMetricsHandler) newCachedTaggedHandler(tags []Tag) *tallyMetricsHandler {
	base := tallyMetricsHandlerBase{
		scope:          tmh.scope.Tagged(tagsToMap(tags, tmh.excludeTags)),
		perUnitBuckets: tmh.perUnitBuckets,
		excludeTags:    tmh.excludeTags,
	}
	return &tallyMetricsHandler{
		tallyMetricsHandlerBase: base,
		cache: &tallyMetricsCache{
			state: tmh.cache.state,
		},
	}
}

func (tmh *tallyMetricsHandler) newUncachedTaggedHandler(tags []Tag) *uncachedTallyMetricsHandler {
	return &uncachedTallyMetricsHandler{
		tallyMetricsHandlerBase: tallyMetricsHandlerBase{
			scope:          tmh.scope.Tagged(tagsToMap(tags, tmh.excludeTags)),
			perUnitBuckets: tmh.perUnitBuckets,
			excludeTags:    tmh.excludeTags,
		},
	}
}

func (base *tallyMetricsHandlerBase) tallyScope() tally.Scope {
	return base.scope
}

func (tmh *uncachedTallyMetricsHandler) WithTags(tags ...Tag) Handler {
	if len(tags) == 0 {
		return tmh
	}
	return &uncachedTallyMetricsHandler{
		tallyMetricsHandlerBase: tallyMetricsHandlerBase{
			scope:          tmh.scope.Tagged(tagsToMap(tags, tmh.excludeTags)),
			perUnitBuckets: tmh.perUnitBuckets,
			excludeTags:    tmh.excludeTags,
		},
	}
}

func (tmh *uncachedTallyMetricsHandler) Counter(counter string) CounterIface {
	return CounterFunc(func(i int64, t ...Tag) {
		scope := tmh.scope
		if len(t) > 0 {
			scope = tmh.scope.Tagged(tagsToMap(t, tmh.excludeTags))
		}
		scope.Counter(counter).Inc(i)
	})
}

func (tmh *uncachedTallyMetricsHandler) Gauge(gauge string) GaugeIface {
	return GaugeFunc(func(f float64, t ...Tag) {
		scope := tmh.scope
		if len(t) > 0 {
			scope = tmh.scope.Tagged(tagsToMap(t, tmh.excludeTags))
		}
		scope.Gauge(gauge).Update(f)
	})
}

func (tmh *uncachedTallyMetricsHandler) Timer(timer string) TimerIface {
	return TimerFunc(func(d time.Duration, t ...Tag) {
		scope := tmh.scope
		if len(t) > 0 {
			scope = tmh.scope.Tagged(tagsToMap(t, tmh.excludeTags))
		}
		scope.Timer(timer).Record(d)
	})
}

func (tmh *uncachedTallyMetricsHandler) Histogram(histogram string, unit MetricUnit) HistogramIface {
	return HistogramFunc(func(i int64, t ...Tag) {
		scope := tmh.scope
		if len(t) > 0 {
			scope = tmh.scope.Tagged(tagsToMap(t, tmh.excludeTags))
		}
		scope.Histogram(histogram, tmh.perUnitBuckets[unit]).RecordValue(float64(i))
	})
}

// Counter obtains a counter for the given name.
func (tmh *tallyMetricsHandler) Counter(counter string) CounterIface {
	return CounterFunc(func(i int64, t ...Tag) {
		tmh.scopeWithTags(t).Counter(counter).Inc(i)
	})
}

// Gauge obtains a gauge for the given name.
func (tmh *tallyMetricsHandler) Gauge(gauge string) GaugeIface {
	return GaugeFunc(func(f float64, t ...Tag) {
		tmh.scopeWithTags(t).Gauge(gauge).Update(f)
	})
}

// Timer obtains a timer for the given name.
func (tmh *tallyMetricsHandler) Timer(timer string) TimerIface {
	return TimerFunc(func(d time.Duration, t ...Tag) {
		tmh.scopeWithTags(t).Timer(timer).Record(d)
	})
}

// Histogram obtains a histogram for the given name.
func (tmh *tallyMetricsHandler) Histogram(histogram string, unit MetricUnit) HistogramIface {
	return HistogramFunc(func(i int64, t ...Tag) {
		tmh.scopeWithTags(t).Histogram(histogram, tmh.perUnitBuckets[unit]).RecordValue(float64(i))
	})
}

func (*tallyMetricsHandler) Stop(log.Logger) {}

func (*tallyMetricsHandler) Close() error {
	return nil
}

func (tmh *tallyMetricsHandler) StartBatch(_ string) BatchHandler {
	return tmh
}

func (*uncachedTallyMetricsHandler) Stop(log.Logger) {}

func (*uncachedTallyMetricsHandler) Close() error {
	return nil
}

func (tmh *uncachedTallyMetricsHandler) StartBatch(_ string) BatchHandler {
	return tmh
}

func tagsToMap(t1 []Tag, e excludeTags) map[string]string {
	if len(t1) == 0 {
		return nil
	}

	m := make(map[string]string, len(t1))

	convert := func(tag Tag) {
		if vals, ok := e[tag.Key]; ok {
			if _, ok := vals[tag.Value]; !ok {
				m[tag.Key] = tagExcludedValue
				return
			}
		}

		m[tag.Key] = tag.Value
	}

	for i := range t1 {
		convert(t1[i])
	}

	return m
}
