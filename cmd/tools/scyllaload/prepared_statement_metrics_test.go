package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadScyllaPreparedStatementMetrics(t *testing.T) {
	dir := t.TempDir()
	temporalBefore := writeMetricsFile(t, dir+"/temporal.before.prom", "temporal_requests 10\n")
	temporalAfter := writeMetricsFile(t, dir+"/temporal.after.prom", "temporal_requests 20\n")
	node1Before := writeMetricsFile(t, dir+"/node1.before.prom", `
# TYPE scylla_transport_cql_requests_count counter
scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 10
scylla_transport_cql_requests_count{kind="PREPARE",shard="1"} 5
scylla_transport_cql_requests_count{kind="EXECUTE",shard="0"} 100
# TYPE scylla_transport_cql_errors_total counter
scylla_transport_cql_errors_total{shard="0",type="unprepared"} 2
# TYPE scylla_transport_requests_forwarded_prepared_not_found counter
scylla_transport_requests_forwarded_prepared_not_found{shard="0"} 4
# TYPE scylla_query_processor_statements_prepared counter
scylla_query_processor_statements_prepared{shard="0"} 20
# TYPE scylla_cql_prepared_cache_evictions counter
scylla_cql_prepared_cache_evictions{shard="0"} 3
# TYPE scylla_cql_unprivileged_entries_evictions_on_size counter
scylla_cql_unprivileged_entries_evictions_on_size{shard="0"} 1
# TYPE scylla_cql_authorized_prepared_statements_cache_evictions counter
scylla_cql_authorized_prepared_statements_cache_evictions{shard="0"} 5
	# TYPE scylla_cql_prepared_cache_size gauge
	scylla_cql_prepared_cache_size{shard="0"} 12
	scylla_cql_prepared_cache_size{shard="1"} 13
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 1000
		scylla_reactor_awake_time_ms_total{shard="1"} 2000
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 99000
		scylla_reactor_sleep_time_ms_total{shard="1"} 198000
	`)
	node1After := writeMetricsFile(t, dir+"/node1.after.prom", `
# TYPE scylla_transport_cql_requests_count counter
scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 13
scylla_transport_cql_requests_count{kind="PREPARE",shard="1"} 7
scylla_transport_cql_requests_count{kind="EXECUTE",shard="0"} 200
# TYPE scylla_transport_cql_errors_total counter
scylla_transport_cql_errors_total{shard="0",type="unprepared"} 3
# TYPE scylla_transport_requests_forwarded_prepared_not_found counter
scylla_transport_requests_forwarded_prepared_not_found{shard="0"} 6
# TYPE scylla_query_processor_statements_prepared counter
scylla_query_processor_statements_prepared{shard="0"} 24
# TYPE scylla_cql_prepared_cache_evictions counter
scylla_cql_prepared_cache_evictions{shard="0"} 3
# TYPE scylla_cql_unprivileged_entries_evictions_on_size counter
scylla_cql_unprivileged_entries_evictions_on_size{shard="0"} 2
# TYPE scylla_cql_authorized_prepared_statements_cache_evictions counter
scylla_cql_authorized_prepared_statements_cache_evictions{shard="0"} 9
	# TYPE scylla_cql_prepared_cache_size gauge
	scylla_cql_prepared_cache_size{shard="0"} 14
	scylla_cql_prepared_cache_size{shard="1"} 15
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 1010
		scylla_reactor_awake_time_ms_total{shard="1"} 2010
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 99990
		scylla_reactor_sleep_time_ms_total{shard="1"} 198990
	`)
	node2Before := writeMetricsFile(t, dir+"/node2.before.prom", `
	# TYPE scylla_cql_prepared_cache_size gauge
	scylla_cql_prepared_cache_size{shard="0"} 10
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 3000
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 297000
	`)
	node2After := writeMetricsFile(t, dir+"/node2.after.prom", `
# TYPE scylla_transport_cql_requests_count counter
scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 1
# TYPE scylla_query_processor_statements_prepared counter
scylla_query_processor_statements_prepared{shard="0"} 1
	# TYPE scylla_cql_prepared_cache_size gauge
	scylla_cql_prepared_cache_size{shard="0"} 11
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 3010
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 297990
		`)

	beforeCapture := time.Now()
	afterCapture := beforeCapture.Add(time.Second)
	metrics, err := readScyllaPreparedStatementMetrics(
		metricSnapshotFlags{
			capturedMetricSnapshot("http://temporal/metrics", temporalBefore, beforeCapture),
			capturedMetricSnapshot("http://node1/metrics", node1Before, beforeCapture),
			capturedMetricSnapshot("http://node2/metrics", node2Before, beforeCapture),
		},
		metricSnapshotFlags{
			capturedMetricSnapshot("http://temporal/metrics", temporalAfter, afterCapture),
			capturedMetricSnapshot("http://node1/metrics", node1After, afterCapture),
			capturedMetricSnapshot("http://node2/metrics", node2After, afterCapture),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, metrics)
	require.Equal(t, 2, metrics.MetricEndpoints)
	require.InDelta(t, 6, metrics.PrepareRequests, 0)
	require.InDelta(t, 3, metrics.ReprepareAttempts, 0)
	require.InDelta(t, 1, metrics.ClientReprepareAttempts, 0)
	require.InDelta(t, 2, metrics.ForwardedReprepareAttempts, 0)
	require.InDelta(t, 5, metrics.StatementsParsed, 0)
	require.InDelta(t, 0, metrics.PreparedCacheEvictions, 0)
	require.InDelta(t, 1, metrics.OneOffPreparedCacheEvictions, 0)
	require.InDelta(t, 4, metrics.AuthorizedPreparedCacheEvictions, 0)
	require.InDelta(t, 35, metrics.PreparedCacheEntriesBefore, 0)
	require.InDelta(t, 40, metrics.PreparedCacheEntriesAfter, 0)
}

func TestReadScyllaPreparedStatementMetricsRejectsCounterReset(t *testing.T) {
	dir := t.TempDir()
	before := writeMetricsFile(t, dir+"/before.prom", `
	# TYPE scylla_transport_cql_requests_count counter
	scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 10
	scylla_transport_cql_requests_count{kind="PREPARE",shard="1"} 1
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 1000
		scylla_reactor_awake_time_ms_total{shard="1"} 2000
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 99000
		scylla_reactor_sleep_time_ms_total{shard="1"} 198000
	`)
	after := writeMetricsFile(t, dir+"/after.prom", `
	# TYPE scylla_transport_cql_requests_count counter
	scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 9
	scylla_transport_cql_requests_count{kind="PREPARE",shard="1"} 100
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 1010
		scylla_reactor_awake_time_ms_total{shard="1"} 2010
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 99990
		scylla_reactor_sleep_time_ms_total{shard="1"} 198990
		`)

	beforeCapture := time.Now()
	_, err := readScyllaPreparedStatementMetrics(
		metricSnapshotFlags{capturedMetricSnapshot("http://node/metrics", before, beforeCapture)},
		metricSnapshotFlags{capturedMetricSnapshot(
			"http://node/metrics",
			after,
			beforeCapture.Add(time.Second),
		)},
	)
	require.ErrorContains(t, err, "prepare requests counter decreased")
}

func TestReadScyllaPreparedStatementMetricsRejectsProcessGenerationChange(t *testing.T) {
	dir := t.TempDir()
	before := writeMetricsFile(t, dir+"/before.prom", `
		# TYPE scylla_transport_cql_requests_count counter
		scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 10
		# TYPE scylla_reactor_cpu_busy_ms counter
		scylla_reactor_cpu_busy_ms{shard="0"} 1000
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 20000
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 80000
		`)
	after := writeMetricsFile(t, dir+"/after.prom", `
		# TYPE scylla_transport_cql_requests_count counter
		scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 200
		# TYPE scylla_reactor_cpu_busy_ms counter
		scylla_reactor_cpu_busy_ms{shard="0"} 2000
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 30000
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 120000
		`)

	beforeCapture := time.Now()
	_, err := readScyllaPreparedStatementMetrics(
		metricSnapshotFlags{capturedMetricSnapshot("http://node/metrics", before, beforeCapture)},
		metricSnapshotFlags{capturedMetricSnapshot(
			"http://node/metrics",
			after,
			beforeCapture.Add(200*time.Second),
		)},
	)

	require.ErrorContains(t, err, "scylla reactor wall uptime advanced by 50000ms")
	require.ErrorContains(t, err, "outside the capture interval")
	require.ErrorContains(t, err, "process generation changed")
}

func TestReadScyllaPreparedStatementMetricsRejectsShardChange(t *testing.T) {
	dir := t.TempDir()
	before := writeMetricsFile(t, dir+"/before.prom", `
	# TYPE scylla_transport_cql_requests_count counter
	scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 10
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 1000
		scylla_reactor_awake_time_ms_total{shard="1"} 2000
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 99000
		scylla_reactor_sleep_time_ms_total{shard="1"} 198000
	`)
	after := writeMetricsFile(t, dir+"/after.prom", `
	# TYPE scylla_transport_cql_requests_count counter
	scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 20
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 1010
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 99990
		`)

	beforeCapture := time.Now()
	_, err := readScyllaPreparedStatementMetrics(
		metricSnapshotFlags{capturedMetricSnapshot("http://node/metrics", before, beforeCapture)},
		metricSnapshotFlags{capturedMetricSnapshot(
			"http://node/metrics",
			after,
			beforeCapture.Add(time.Second),
		)},
	)

	require.ErrorContains(t, err, "Scylla reactor wall uptime metric series shard=\"1\" disappeared")
}

func TestReadScyllaPreparedStatementMetricsRequiresGenerationMetric(t *testing.T) {
	dir := t.TempDir()
	before := writeMetricsFile(t, dir+"/before.prom", `
	# TYPE scylla_transport_cql_requests_count counter
	scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 10
	`)
	after := writeMetricsFile(t, dir+"/after.prom", `
	# TYPE scylla_transport_cql_requests_count counter
	scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 20
	`)

	beforeCapture := time.Now()
	_, err := readScyllaPreparedStatementMetrics(
		metricSnapshotFlags{capturedMetricSnapshot("http://node/metrics", before, beforeCapture)},
		metricSnapshotFlags{capturedMetricSnapshot(
			"http://node/metrics",
			after,
			beforeCapture.Add(time.Second),
		)},
	)

	require.ErrorContains(t, err, scyllaReactorAwakeTimeMetric)
	require.ErrorContains(t, err, scyllaReactorSleepTimeMetric)
	require.ErrorContains(t, err, "process generation and shard identity")
}

func TestReadScyllaPreparedStatementMetricsRequiresCaptureTimestamps(t *testing.T) {
	dir := t.TempDir()
	before := writeMetricsFile(t, dir+"/before.prom", `
		# TYPE scylla_transport_cql_requests_count counter
		scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 10
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 1000
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 99000
		`)
	after := writeMetricsFile(t, dir+"/after.prom", `
		# TYPE scylla_transport_cql_requests_count counter
		scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 20
		# TYPE scylla_reactor_awake_time_ms_total counter
		scylla_reactor_awake_time_ms_total{shard="0"} 1010
		# TYPE scylla_reactor_sleep_time_ms_total counter
		scylla_reactor_sleep_time_ms_total{shard="0"} 99990
		`)

	_, err := readScyllaPreparedStatementMetrics(
		metricSnapshotFlags{{url: "http://node/metrics", path: before}},
		metricSnapshotFlags{{url: "http://node/metrics", path: after}},
	)

	require.ErrorContains(t, err, "metric capture timestamps are required")
	require.ErrorContains(t, err, "process generation")
}

func TestReadScyllaPreparedStatementMetricsIgnoresUnmatchedNonScyllaSnapshots(t *testing.T) {
	dir := t.TempDir()
	before := writeMetricsFile(t, dir+"/before.prom", "temporal_requests 10\n")
	after := writeMetricsFile(t, dir+"/after.prom", "temporal_requests 20\n")
	unmatched := writeMetricsFile(t, dir+"/unmatched.prom", "another_service_requests 1\n")

	metrics, err := readScyllaPreparedStatementMetrics(
		metricSnapshotFlags{{url: "http://temporal/metrics", path: before}},
		metricSnapshotFlags{
			{url: "http://temporal/metrics", path: after},
			{url: "http://node/metrics", path: unmatched},
		},
	)
	require.NoError(t, err)
	require.Nil(t, metrics)
}

func TestReadScyllaPreparedStatementMetricsRejectsUnmatchedScyllaSnapshot(t *testing.T) {
	dir := t.TempDir()
	scylla := writeMetricsFile(t, dir+"/scylla.prom", `
# TYPE scylla_transport_cql_requests_count counter
scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 1
`)

	_, err := readScyllaPreparedStatementMetrics(
		nil,
		metricSnapshotFlags{{url: "http://node/metrics", path: scylla}},
	)
	require.ErrorContains(t, err, "missing from pre-run snapshots")
}

func TestReadScyllaPreparedStatementMetricsRejectsScyllaMetricsInOnePhase(t *testing.T) {
	dir := t.TempDir()
	before := writeMetricsFile(t, dir+"/before.prom", "temporal_requests 10\n")
	after := writeMetricsFile(t, dir+"/after.prom", `
# TYPE scylla_transport_cql_requests_count counter
scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 1
`)

	_, err := readScyllaPreparedStatementMetrics(
		metricSnapshotFlags{{url: "http://node/metrics", path: before}},
		metricSnapshotFlags{{url: "http://node/metrics", path: after}},
	)
	require.ErrorContains(t, err, "present in only one snapshot")
}

func TestReadScyllaPreparedStatementMetricsRejectsDuplicateEndpoints(t *testing.T) {
	_, err := readScyllaPreparedStatementMetrics(
		metricSnapshotFlags{
			{url: "http://node/metrics", path: "/tmp/first.prom"},
			{url: "http://node/metrics", path: "/tmp/second.prom"},
		},
		nil,
	)
	require.ErrorContains(t, err, `duplicate pre-run metrics endpoint "http://node/metrics"`)
}

func TestParseScyllaPreparedStatementSnapshotRejectsMalformedMetrics(t *testing.T) {
	_, err := parseScyllaPreparedStatementSnapshot(strings.NewReader("not valid prometheus text"))
	require.Error(t, err)
}

func writeMetricsFile(t *testing.T, path string, body string) string {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(strings.TrimSpace(body)+"\n"), 0o600))
	return path
}

func capturedMetricSnapshot(url string, path string, capturedAt time.Time) metricSnapshot {
	return metricSnapshot{
		url:               url,
		path:              path,
		captureStartedAt:  capturedAt,
		captureFinishedAt: capturedAt,
	}
}
