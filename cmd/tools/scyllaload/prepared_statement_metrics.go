package main

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

const (
	scyllaPrepareRequestsMetric                  = "scylla_transport_cql_requests_count"
	scyllaCQLErrorsMetric                        = "scylla_transport_cql_errors_total"
	scyllaStatementsParsedMetric                 = "scylla_query_processor_statements_prepared"
	scyllaPreparedCacheEvictionsMetric           = "scylla_cql_prepared_cache_evictions"
	scyllaOneOffPreparedCacheEvictionsMetric     = "scylla_cql_unprivileged_entries_evictions_on_size"
	scyllaForwardedPreparedNotFoundMetric        = "scylla_transport_requests_forwarded_prepared_not_found"
	scyllaAuthorizedPreparedCacheEvictionsMetric = "scylla_cql_authorized_prepared_statements_cache_evictions"
	scyllaPreparedCacheSizeMetric                = "scylla_cql_prepared_cache_size"
	scyllaReactorAwakeTimeMetric                 = "scylla_reactor_awake_time_ms_total"
	scyllaReactorSleepTimeMetric                 = "scylla_reactor_sleep_time_ms_total"
	scyllaReactorUptimeTolerance                 = 10 * time.Millisecond
)

type (
	prepStats struct {
		MetricEndpoints                  int     `json:"metricEndpoints"`
		PrepareRequests                  float64 `json:"prepareRequests"`
		ReprepareAttempts                float64 `json:"reprepareAttempts"`
		ClientReprepareAttempts          float64 `json:"clientReprepareAttempts"`
		ForwardedReprepareAttempts       float64 `json:"forwardedReprepareAttempts"`
		StatementsParsed                 float64 `json:"statementsParsed"`
		PreparedCacheEvictions           float64 `json:"preparedCacheEvictions"`
		OneOffPreparedCacheEvictions     float64 `json:"oneOffPreparedCacheEvictions"`
		AuthorizedPreparedCacheEvictions float64 `json:"authorizedPreparedCacheEvictions"`
		PreparedCacheEntriesBefore       float64 `json:"preparedCacheEntriesBefore"`
		PreparedCacheEntriesAfter        float64 `json:"preparedCacheEntriesAfter"`
	}

	scyllaPreparedStatementSnapshot struct {
		found                            bool
		prepareRequests                  metricSeries
		clientReprepareAttempts          metricSeries
		forwardedReprepareAttempts       metricSeries
		statementsParsed                 metricSeries
		preparedCacheEvictions           metricSeries
		oneOffPreparedCacheEvictions     metricSeries
		authorizedPreparedCacheEvictions metricSeries
		preparedCacheEntries             float64
		reactorAwakeTime                 metricSeries
		reactorSleepTime                 metricSeries
	}

	metricSeries map[string]float64
)

func readScyllaPreparedStatementMetrics(
	beforeSnapshots metricSnapshotFlags,
	afterSnapshots metricSnapshotFlags,
) (*prepStats, error) {
	beforeByURL, err := indexMetricSnapshots("pre-run", beforeSnapshots)
	if err != nil {
		return nil, err
	}
	afterByURL, err := indexMetricSnapshots("post-run", afterSnapshots)
	if err != nil {
		return nil, err
	}

	metrics := &prepStats{}
	endpoints := make([]string, 0, len(beforeByURL)+len(afterByURL))
	for endpoint := range beforeByURL {
		endpoints = append(endpoints, endpoint)
	}
	for endpoint := range afterByURL {
		if _, ok := beforeByURL[endpoint]; !ok {
			endpoints = append(endpoints, endpoint)
		}
	}
	sort.Strings(endpoints)
	for _, endpoint := range endpoints {
		beforeSnapshot, hasBefore := beforeByURL[endpoint]
		afterSnapshot, hasAfter := afterByURL[endpoint]
		if !hasBefore || !hasAfter {
			snapshot := beforeSnapshot
			missingPhase := "post-run"
			if !hasBefore {
				snapshot = afterSnapshot
				missingPhase = "pre-run"
			}
			parsed, err := readScyllaPreparedStatementSnapshot(snapshot.path)
			if err != nil {
				return nil, fmt.Errorf("%s: read metrics: %w", endpoint, err)
			}
			if parsed.found {
				return nil, fmt.Errorf(
					"%s: Scylla prepared statement metrics are missing from %s snapshots",
					endpoint,
					missingPhase,
				)
			}
			continue
		}
		delta, found, err := readScyllaPreparedStatementDelta(beforeSnapshot, afterSnapshot)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", endpoint, err)
		}
		if !found {
			continue
		}
		metrics.add(delta)
	}
	if metrics.MetricEndpoints == 0 {
		return nil, nil
	}
	return metrics, nil
}

func indexMetricSnapshots(phase string, snapshots metricSnapshotFlags) (map[string]metricSnapshot, error) {
	byURL := make(map[string]metricSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		if _, ok := byURL[snapshot.url]; ok {
			return nil, fmt.Errorf("duplicate %s metrics endpoint %q", phase, snapshot.url)
		}
		byURL[snapshot.url] = snapshot
	}
	return byURL, nil
}

func readScyllaPreparedStatementDelta(
	beforeSnapshot metricSnapshot,
	afterSnapshot metricSnapshot,
) (prepStats, bool, error) {
	before, err := readScyllaPreparedStatementSnapshot(beforeSnapshot.path)
	if err != nil {
		return prepStats{}, false, fmt.Errorf("read pre-run metrics: %w", err)
	}
	after, err := readScyllaPreparedStatementSnapshot(afterSnapshot.path)
	if err != nil {
		return prepStats{}, false, fmt.Errorf("read post-run metrics: %w", err)
	}
	if !before.found && !after.found {
		return prepStats{}, false, nil
	}
	if before.found != after.found {
		return prepStats{}, false, errors.New(
			"scylla prepared statement metrics are present in only one snapshot",
		)
	}
	delta, err := calculatePreparedStatementDelta(
		before,
		after,
		beforeSnapshot,
		afterSnapshot,
	)
	return delta, true, err
}

func calculatePreparedStatementDelta(
	before scyllaPreparedStatementSnapshot,
	after scyllaPreparedStatementSnapshot,
	beforeCapture metricSnapshot,
	afterCapture metricSnapshot,
) (prepStats, error) {
	if err := validateScyllaMetricGeneration(before, after, beforeCapture, afterCapture); err != nil {
		return prepStats{}, err
	}
	delta := prepStats{
		MetricEndpoints:            1,
		PreparedCacheEntriesBefore: before.preparedCacheEntries,
		PreparedCacheEntriesAfter:  after.preparedCacheEntries,
	}
	counters := []struct {
		name        string
		before      metricSeries
		after       metricSeries
		destination *float64
	}{
		{"prepare requests", before.prepareRequests, after.prepareRequests, &delta.PrepareRequests},
		{
			"client reprepare attempts",
			before.clientReprepareAttempts,
			after.clientReprepareAttempts,
			&delta.ClientReprepareAttempts,
		},
		{
			"forwarded reprepare attempts",
			before.forwardedReprepareAttempts,
			after.forwardedReprepareAttempts,
			&delta.ForwardedReprepareAttempts,
		},
		{"statements parsed", before.statementsParsed, after.statementsParsed, &delta.StatementsParsed},
		{
			"prepared cache evictions",
			before.preparedCacheEvictions,
			after.preparedCacheEvictions,
			&delta.PreparedCacheEvictions,
		},
		{
			"one-off prepared cache evictions",
			before.oneOffPreparedCacheEvictions,
			after.oneOffPreparedCacheEvictions,
			&delta.OneOffPreparedCacheEvictions,
		},
		{
			"authorized prepared cache evictions",
			before.authorizedPreparedCacheEvictions,
			after.authorizedPreparedCacheEvictions,
			&delta.AuthorizedPreparedCacheEvictions,
		},
	}
	for _, counter := range counters {
		value, err := counterDelta(counter.name, counter.before, counter.after)
		if err != nil {
			return prepStats{}, err
		}
		*counter.destination = value
	}
	delta.ReprepareAttempts = delta.ClientReprepareAttempts + delta.ForwardedReprepareAttempts
	return delta, nil
}

func (m *prepStats) add(other prepStats) {
	m.MetricEndpoints += other.MetricEndpoints
	m.PrepareRequests += other.PrepareRequests
	m.ReprepareAttempts += other.ReprepareAttempts
	m.ClientReprepareAttempts += other.ClientReprepareAttempts
	m.ForwardedReprepareAttempts += other.ForwardedReprepareAttempts
	m.StatementsParsed += other.StatementsParsed
	m.PreparedCacheEvictions += other.PreparedCacheEvictions
	m.OneOffPreparedCacheEvictions += other.OneOffPreparedCacheEvictions
	m.AuthorizedPreparedCacheEvictions += other.AuthorizedPreparedCacheEvictions
	m.PreparedCacheEntriesBefore += other.PreparedCacheEntriesBefore
	m.PreparedCacheEntriesAfter += other.PreparedCacheEntriesAfter
}

func readScyllaPreparedStatementSnapshot(path string) (scyllaPreparedStatementSnapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return scyllaPreparedStatementSnapshot{}, err
	}
	snapshot, parseErr := parseScyllaPreparedStatementSnapshot(f)
	closeErr := f.Close()
	if parseErr != nil {
		return scyllaPreparedStatementSnapshot{}, parseErr
	}
	if closeErr != nil {
		return scyllaPreparedStatementSnapshot{}, closeErr
	}
	return snapshot, nil
}

func parseScyllaPreparedStatementSnapshot(r io.Reader) (scyllaPreparedStatementSnapshot, error) {
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return scyllaPreparedStatementSnapshot{}, err
	}

	var snapshot scyllaPreparedStatementSnapshot
	var found bool
	snapshot.prepareRequests, found = metricFamilySeries(
		families[scyllaPrepareRequestsMetric],
		map[string]string{"kind": "PREPARE"},
	)
	snapshot.found = snapshot.found || found
	snapshot.clientReprepareAttempts, found = metricFamilySeries(
		families[scyllaCQLErrorsMetric],
		map[string]string{"type": "unprepared"},
	)
	snapshot.found = snapshot.found || found
	snapshot.forwardedReprepareAttempts, found = metricFamilySeries(
		families[scyllaForwardedPreparedNotFoundMetric],
		nil,
	)
	snapshot.found = snapshot.found || found
	snapshot.statementsParsed, found = metricFamilySeries(families[scyllaStatementsParsedMetric], nil)
	snapshot.found = snapshot.found || found
	snapshot.preparedCacheEvictions, found = metricFamilySeries(families[scyllaPreparedCacheEvictionsMetric], nil)
	snapshot.found = snapshot.found || found
	snapshot.oneOffPreparedCacheEvictions, found = metricFamilySeries(
		families[scyllaOneOffPreparedCacheEvictionsMetric],
		nil,
	)
	snapshot.found = snapshot.found || found
	snapshot.authorizedPreparedCacheEvictions, found = metricFamilySeries(
		families[scyllaAuthorizedPreparedCacheEvictionsMetric],
		nil,
	)
	snapshot.found = snapshot.found || found
	snapshot.preparedCacheEntries, found = metricFamilySum(families[scyllaPreparedCacheSizeMetric], nil)
	snapshot.found = snapshot.found || found
	snapshot.reactorAwakeTime, _ = metricFamilySeries(families[scyllaReactorAwakeTimeMetric], nil)
	snapshot.reactorSleepTime, _ = metricFamilySeries(families[scyllaReactorSleepTimeMetric], nil)
	return snapshot, nil
}

func validateScyllaMetricGeneration(
	before scyllaPreparedStatementSnapshot,
	after scyllaPreparedStatementSnapshot,
	beforeCapture metricSnapshot,
	afterCapture metricSnapshot,
) error {
	beforeUptime, err := reactorUptimeSeries("pre-run", before)
	if err != nil {
		return err
	}
	afterUptime, err := reactorUptimeSeries("post-run", after)
	if err != nil {
		return err
	}
	if err := requireSameMetricSeries(
		"Scylla reactor wall uptime",
		beforeUptime,
		afterUptime,
	); err != nil {
		return err
	}
	minimumElapsed, maximumElapsed, err := metricCaptureElapsedBounds(
		beforeCapture,
		afterCapture,
	)
	if err != nil {
		return err
	}
	minimumMilliseconds := float64(max(minimumElapsed-scyllaReactorUptimeTolerance, 0)) /
		float64(time.Millisecond)
	maximumMilliseconds := float64(maximumElapsed+scyllaReactorUptimeTolerance) /
		float64(time.Millisecond)
	for series, beforeValue := range beforeUptime {
		afterValue := afterUptime[series]
		if math.IsNaN(beforeValue) || math.IsInf(beforeValue, 0) ||
			math.IsNaN(afterValue) || math.IsInf(afterValue, 0) {
			return fmt.Errorf("scylla reactor wall uptime for series %s is not finite", series)
		}
		delta := afterValue - beforeValue
		if delta < minimumMilliseconds || delta > maximumMilliseconds {
			return fmt.Errorf(
				"scylla reactor wall uptime advanced by %vms for series %s, "+
					"outside the capture interval %vms to %vms; process generation changed",
				delta,
				series,
				minimumMilliseconds,
				maximumMilliseconds,
			)
		}
	}
	return nil
}

func reactorUptimeSeries(
	phase string,
	snapshot scyllaPreparedStatementSnapshot,
) (metricSeries, error) {
	if len(snapshot.reactorAwakeTime) == 0 || len(snapshot.reactorSleepTime) == 0 {
		return nil, fmt.Errorf(
			"%s and %s are required in the %s snapshot to validate "+
				"Scylla process generation and shard identity",
			scyllaReactorAwakeTimeMetric,
			scyllaReactorSleepTimeMetric,
			phase,
		)
	}
	if err := requireSameMetricSeries(
		phase+" Scylla reactor wall uptime components",
		snapshot.reactorAwakeTime,
		snapshot.reactorSleepTime,
	); err != nil {
		return nil, err
	}
	uptime := make(metricSeries, len(snapshot.reactorAwakeTime))
	for series, awakeTime := range snapshot.reactorAwakeTime {
		uptime[series] = awakeTime + snapshot.reactorSleepTime[series]
	}
	return uptime, nil
}

func metricCaptureElapsedBounds(
	before metricSnapshot,
	after metricSnapshot,
) (minimumElapsed time.Duration, maximumElapsed time.Duration, err error) {
	if before.captureStartedAt.IsZero() || before.captureFinishedAt.IsZero() ||
		after.captureStartedAt.IsZero() || after.captureFinishedAt.IsZero() {
		return 0, 0, errors.New(
			"metric capture timestamps are required to validate Scylla process generation",
		)
	}
	if before.captureFinishedAt.Before(before.captureStartedAt) {
		return 0, 0, errors.New("pre-run metric capture finished before it started")
	}
	if after.captureFinishedAt.Before(after.captureStartedAt) {
		return 0, 0, errors.New("post-run metric capture finished before it started")
	}
	if after.captureStartedAt.Before(before.captureFinishedAt) {
		return 0, 0, errors.New("post-run metric capture started before pre-run capture finished")
	}
	return after.captureStartedAt.Sub(before.captureFinishedAt),
		after.captureFinishedAt.Sub(before.captureStartedAt),
		nil
}

func requireSameMetricSeries(name string, before metricSeries, after metricSeries) error {
	for series := range before {
		if _, ok := after[series]; !ok {
			return fmt.Errorf("%s metric series %s disappeared", name, series)
		}
	}
	for series := range after {
		if _, ok := before[series]; !ok {
			return fmt.Errorf("%s metric series %s appeared", name, series)
		}
	}
	return nil
}

func metricFamilySeries(family *dto.MetricFamily, requiredLabels map[string]string) (metricSeries, bool) {
	if family == nil {
		return nil, false
	}
	series := make(metricSeries)
	for _, metric := range family.GetMetric() {
		if !metricHasLabels(metric, requiredLabels) {
			continue
		}
		value, ok := metricValue(family.GetType(), metric)
		if !ok {
			continue
		}
		series[metricSeriesKey(metric)] = value
	}
	return series, true
}

func metricFamilySum(family *dto.MetricFamily, requiredLabels map[string]string) (float64, bool) {
	series, found := metricFamilySeries(family, requiredLabels)
	var sum float64
	for _, value := range series {
		sum += value
	}
	return sum, found
}

func metricValue(metricType dto.MetricType, metric *dto.Metric) (float64, bool) {
	switch metricType {
	case dto.MetricType_COUNTER:
		return metric.GetCounter().GetValue(), true
	case dto.MetricType_GAUGE:
		return metric.GetGauge().GetValue(), true
	case dto.MetricType_UNTYPED:
		return metric.GetUntyped().GetValue(), true
	default:
		return 0, false
	}
}

func metricSeriesKey(metric *dto.Metric) string {
	labels := make([]string, 0, len(metric.GetLabel()))
	for _, label := range metric.GetLabel() {
		labels = append(labels, fmt.Sprintf("%s=%q", label.GetName(), label.GetValue()))
	}
	sort.Strings(labels)
	return strings.Join(labels, ",")
}

func metricHasLabels(metric *dto.Metric, requiredLabels map[string]string) bool {
	for requiredName, requiredValue := range requiredLabels {
		found := false
		for _, label := range metric.GetLabel() {
			if label.GetName() == requiredName && label.GetValue() == requiredValue {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func counterDelta(name string, before metricSeries, after metricSeries) (float64, error) {
	var delta float64
	for series, beforeValue := range before {
		afterValue := after[series]
		if afterValue < beforeValue {
			return 0, fmt.Errorf(
				"%s counter decreased for series %s from %v to %v",
				name,
				series,
				beforeValue,
				afterValue,
			)
		}
		delta += afterValue - beforeValue
	}
	for series, afterValue := range after {
		if _, ok := before[series]; !ok {
			delta += afterValue
		}
	}
	return delta, nil
}
