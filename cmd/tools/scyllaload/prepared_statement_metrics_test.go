package main

import (
	"os"
	"strings"
	"testing"

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
`)
	node2Before := writeMetricsFile(t, dir+"/node2.before.prom", `
# TYPE scylla_cql_prepared_cache_size gauge
scylla_cql_prepared_cache_size{shard="0"} 10
`)
	node2After := writeMetricsFile(t, dir+"/node2.after.prom", `
# TYPE scylla_transport_cql_requests_count counter
scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 1
# TYPE scylla_query_processor_statements_prepared counter
scylla_query_processor_statements_prepared{shard="0"} 1
# TYPE scylla_cql_prepared_cache_size gauge
scylla_cql_prepared_cache_size{shard="0"} 11
`)

	metrics, err := readScyllaPreparedStatementMetrics(
		metricSnapshotFlags{
			{url: "http://temporal/metrics", path: temporalBefore},
			{url: "http://node1/metrics", path: node1Before},
			{url: "http://node2/metrics", path: node2Before},
		},
		metricSnapshotFlags{
			{url: "http://temporal/metrics", path: temporalAfter},
			{url: "http://node1/metrics", path: node1After},
			{url: "http://node2/metrics", path: node2After},
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
`)
	after := writeMetricsFile(t, dir+"/after.prom", `
# TYPE scylla_transport_cql_requests_count counter
scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 9
scylla_transport_cql_requests_count{kind="PREPARE",shard="1"} 100
`)

	_, err := readScyllaPreparedStatementMetrics(
		metricSnapshotFlags{{url: "http://node/metrics", path: before}},
		metricSnapshotFlags{{url: "http://node/metrics", path: after}},
	)
	require.ErrorContains(t, err, "prepare requests counter decreased")
}

func TestReadScyllaPreparedStatementMetricsIgnoresUnmatchedAndNonScyllaSnapshots(t *testing.T) {
	dir := t.TempDir()
	before := writeMetricsFile(t, dir+"/before.prom", "temporal_requests 10\n")
	after := writeMetricsFile(t, dir+"/after.prom", "temporal_requests 20\n")
	unmatched := writeMetricsFile(t, dir+"/unmatched.prom", `
# TYPE scylla_transport_cql_requests_count counter
scylla_transport_cql_requests_count{kind="PREPARE",shard="0"} 1
`)

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
