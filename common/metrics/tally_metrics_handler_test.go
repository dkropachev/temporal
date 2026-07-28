package metrics

import (
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
)

var defaultConfig = ClientConfig{
	Tags: nil,
	ExcludeTags: map[string][]string{
		"taskqueue":    {"__sticky__"},
		"activityType": {},
		"workflowType": {},
	},
	Prefix: "",
	PerUnitHistogramBoundaries: map[string][]float64{
		Dimensionless: {0, 10, 100},
		Bytes:         {1024, 2048},
		Milliseconds:  {10, 500, 1000, 5000, 10000},
		Seconds:       {0.01, 0.5, 1, 5, 10},
	},
}

var benchmarkTallyHandlerFactory = func(scope tally.Scope) Handler {
	return NewTallyMetricsHandler(defaultConfig, scope)
}

func TestTallyScope(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	mp := NewTallyMetricsHandler(defaultConfig, scope)
	recordTallyMetrics(mp)

	snap := scope.Snapshot()
	counters, gauges, timers, histograms := snap.Counters(), snap.Gauges(), snap.Timers(), snap.Histograms()

	assert.EqualValues(t, 8, counters["test.hits+"].Value())
	assert.EqualValues(t, map[string]string{}, counters["test.hits+"].Tags())

	assert.EqualValues(t, 11, counters["test.hits-tagged+taskqueue=__sticky__"].Value())
	assert.EqualValues(t, map[string]string{"taskqueue": "__sticky__"}, counters["test.hits-tagged+taskqueue=__sticky__"].Tags())

	assert.EqualValues(t, 14, counters["test.hits-tagged-excluded+taskqueue="+tagExcludedValue].Value())
	assert.EqualValues(t, map[string]string{"taskqueue": tagExcludedValue}, counters["test.hits-tagged-excluded+taskqueue="+tagExcludedValue].Tags())

	assert.EqualValues(t, float64(-100), gauges["test.temp+location=Mare Imbrium"].Value())
	assert.EqualValues(t, map[string]string{
		"location": "Mare Imbrium",
	}, gauges["test.temp+location=Mare Imbrium"].Tags())

	assert.EqualValues(t, []time.Duration{
		1248 * time.Millisecond,
		5255 * time.Millisecond,
	}, timers["test.latency+"].Values())
	assert.EqualValues(t, map[string]string{}, timers["test.latency+"].Tags())

	assert.EqualValues(t, map[float64]int64{
		1024:            0,
		2048:            0,
		math.MaxFloat64: 1,
	}, histograms["test.transmission+"].Values())
	assert.EqualValues(t, map[time.Duration]int64(nil), histograms["test.transmission+"].Durations())
	assert.EqualValues(t, map[string]string{}, histograms["test.transmission+"].Tags())

	newTaggedHandler := mp.WithTags(NamespaceTag(uuid.NewString()))
	recordTallyMetrics(newTaggedHandler)
	snap = scope.Snapshot()
	counters = snap.Counters()

	assert.EqualValues(t, 11, counters["test.hits-tagged+taskqueue=__sticky__"].Value())
	assert.EqualValues(t, map[string]string{"taskqueue": "__sticky__"}, counters["test.hits-tagged+taskqueue=__sticky__"].Tags())
}

func TestTallyMetricsHandlerCachesTags(t *testing.T) {
	handler := NewTallyMetricsHandler(defaultConfig, tally.NewTestScope("test", nil))
	tags := []Tag{
		StringTag("operation", "GetWorkflowExecution"),
		NamespaceTag("test-namespace"),
	}

	require.Same(t, handler, handler.WithTags())
	firstTaggedHandler := handler.WithTags(tags...)
	secondTaggedHandler := handler.WithTags(tags...)
	require.Same(t, firstTaggedHandler, secondTaggedHandler)
}

func TestTallyMetricsHandlerCachesTagsConcurrently(t *testing.T) {
	const workerCount = 32

	handler := NewTallyMetricsHandler(defaultConfig, tally.NewTestScope("test", nil))
	tags := []Tag{
		StringTag("operation", "GetWorkflowExecution"),
		NamespaceTag("test-namespace"),
	}
	taggedHandlers := make([]Handler, workerCount)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workerCount)

	for i := range workerCount {
		go func() {
			defer wg.Done()
			<-start
			taggedHandlers[i] = handler.WithTags(tags...)
		}()
	}
	close(start)
	wg.Wait()

	for i := 1; i < workerCount; i++ {
		require.Same(t, taggedHandlers[0], taggedHandlers[i])
	}
	require.Equal(t, int64(1), handler.cache.state.taggedHandlersCount.Load())
}

func TestTallyMetricsHandlerBoundsCaches(t *testing.T) {
	handler := NewTallyMetricsHandler(defaultConfig, tally.NewTestScope("test", nil))
	oversizedTagSet := make([]Tag, maxCachedTallyTags+1)
	for i := range oversizedTagSet {
		oversizedTagSet[i] = StringTag("tag-"+strconv.Itoa(i), "value")
	}
	_, isUncached := handler.WithTags(oversizedTagSet...).(*uncachedTallyMetricsHandler)
	require.True(t, isUncached)
	require.Zero(t, handler.cache.state.taggedHandlersCount.Load())

	cachedParent := handler.WithTags(StringTag("operation", "GetWorkflowExecution")).(*tallyMetricsHandler)

	for i := 1; i < maxCachedTallyTagSets; i++ {
		cachedParent.WithTags(UnsafeTaskQueueTag("task-queue-" + strconv.Itoa(i)))
	}
	overflowTag := UnsafeTaskQueueTag("overflow-task-queue")
	overflowTagKey, cacheable := newTallyTagCacheKey([]Tag{overflowTag})
	require.True(t, cacheable)
	_, isUncached = handler.WithTags(overflowTag).(*uncachedTallyMetricsHandler)

	require.Same(t, handler.cache.state, cachedParent.cache.state)
	require.Equal(t, int64(maxCachedTallyTagSets), handler.cache.state.taggedHandlersCount.Load())
	require.True(t, handler.cache.state.taggedHandlersFull.Load())
	require.True(t, isUncached)
	_, bypassesCachedEntry := handler.WithTags(StringTag("operation", "GetWorkflowExecution")).(*uncachedTallyMetricsHandler)
	require.True(t, bypassesCachedEntry)
	_, ok := handler.cache.taggedHandlers.Load(overflowTagKey)
	require.False(t, ok)
}

func BenchmarkTallyMetricsHandlerRepeatedLookup(b *testing.B) {
	b.Run("counter", func(b *testing.B) {
		handler := benchmarkTallyHandlerFactory(tally.NewTestScope("benchmark", nil))
		handler.Counter("requests").Record(1)

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			handler.Counter("requests").Record(1)
		}
	})

	b.Run("tagged_counter", func(b *testing.B) {
		handler := benchmarkTallyHandlerFactory(tally.NewTestScope("benchmark", nil))
		tags := []Tag{
			StringTag("operation", "GetWorkflowExecution"),
			NamespaceTag("benchmark-namespace"),
		}
		handler.WithTags(tags...).Counter("requests").Record(1)

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			taggedHandler := handler.WithTags(tags...)
			taggedHandler.Counter("requests").Record(1)
		}
	})

	b.Run("counter_with_tags", func(b *testing.B) {
		handler := benchmarkTallyHandlerFactory(tally.NewTestScope("benchmark", nil))
		tags := []Tag{
			StringTag("operation", "GetWorkflowExecution"),
			NamespaceTag("benchmark-namespace"),
		}
		counter := handler.Counter("requests")
		counter.Record(1, tags...)

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			counter.Record(1, tags...)
		}
	})

	b.Run("parallel_tagged_counter", func(b *testing.B) {
		handler := benchmarkTallyHandlerFactory(tally.NewTestScope("benchmark", nil))
		tags := []Tag{
			StringTag("operation", "GetWorkflowExecution"),
			NamespaceTag("benchmark-namespace"),
		}
		handler.WithTags(tags...).Counter("requests").Record(1)

		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				handler.WithTags(tags...).Counter("requests").Record(1)
			}
		})
	})

	b.Run("parallel_high_cardinality_tagged_counter", func(b *testing.B) {
		const tagSetCount = 2 * maxCachedTallyTagSets

		handler := benchmarkTallyHandlerFactory(tally.NewTestScope("benchmark", nil))
		tagSets := make([][]Tag, tagSetCount)
		for i := range tagSets {
			tagSets[i] = []Tag{
				StringTag("operation", "GetWorkflowExecution"),
				UnsafeTaskQueueTag("benchmark-task-queue-" + strconv.Itoa(i)),
			}
		}
		var nextTagSet atomic.Uint64

		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				tags := tagSets[nextTagSet.Add(1)%tagSetCount]
				handler.WithTags(tags...).Counter("requests").Record(1)
			}
		})
	})
}

func recordTallyMetrics(h Handler) {
	hitsCounter := h.Counter("hits")
	gauge := h.Gauge("temp")
	timer := h.Timer("latency")
	histogram := h.Histogram("transmission", Bytes)
	hitsTaggedCounter := h.Counter("hits-tagged")
	hitsTaggedExcludedCounter := h.Counter("hits-tagged-excluded")

	hitsCounter.Record(8)
	gauge.Record(-100, StringTag("location", "Mare Imbrium"))
	timer.Record(1248 * time.Millisecond)
	timer.Record(5255 * time.Millisecond)
	histogram.Record(1234567)
	hitsTaggedCounter.Record(11, UnsafeTaskQueueTag("__sticky__"))
	hitsTaggedExcludedCounter.Record(14, UnsafeTaskQueueTag("filtered"))
}
