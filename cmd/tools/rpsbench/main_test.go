package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRequestID(t *testing.T) {
	id := requestID("seed")

	require.Equal(t, id, requestID("seed"))
	require.NotEqual(t, id, requestID("other-seed"))
	require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, id)
}

func TestPercentile(t *testing.T) {
	values := []float64{40, 10, 30, 20}

	require.InDelta(t, 0, percentile(nil, 0.50), 0)
	require.InDelta(t, 10, percentile(append([]float64(nil), values...), 0), 0)
	require.InDelta(t, 20, percentile(append([]float64(nil), values...), 0.50), 0)
	require.InDelta(t, 30, percentile(append([]float64(nil), values...), 0.75), 0)
	require.InDelta(t, 40, percentile(append([]float64(nil), values...), 1), 0)
}

func TestPhaseTimingsFormat(t *testing.T) {
	var timings phaseTimings
	timings.add(e2eTimings{
		start:           10 * time.Millisecond,
		poll:            20 * time.Millisecond,
		scheduleToStart: 30 * time.Millisecond,
		complete:        40 * time.Millisecond,
	})

	formatted := timings.format()
	require.Contains(t, formatted, "start_p50_ms=10.00")
	require.Contains(t, formatted, "poll_p50_ms=20.00")
	require.Contains(t, formatted, "wft_s2s_p50_ms=30.00")
	require.Contains(t, formatted, "complete_p50_ms=40.00")
}

func TestParseDataSize(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want int
	}{
		{name: "empty", in: "", want: 0},
		{name: "bytes", in: "1024", want: 1024},
		{name: "bytes suffix", in: "1024B", want: 1024},
		{name: "kilobytes", in: "4KB", want: 4 << 10},
		{name: "short kilobytes", in: "4k", want: 4 << 10},
		{name: "megabytes", in: "2MB", want: 2 << 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDataSize(tt.in)

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}

	_, err := parseDataSize("-1")
	require.Error(t, err)

	_, err = parseDataSize("bad")
	require.Error(t, err)
}

func TestPayloadForSize(t *testing.T) {
	require.Nil(t, payloadForSize(0))

	payload := payloadForSize(4 << 10)

	require.Len(t, payload.GetPayloads(), 1)
	require.Equal(t, []byte(payloadEncoding), payload.GetPayloads()[0].GetMetadata()["encoding"])
	require.Len(t, payload.GetPayloads()[0].GetData(), 4<<10)
}

func TestPayloadTemplateBuildsFreshMessages(t *testing.T) {
	template := newPayloadTemplate(16)

	first := template.payloads()
	second := template.payloads()

	require.NotSame(t, first, second)
	require.NotSame(t, first.GetPayloads()[0], second.GetPayloads()[0])
	require.Equal(t, first.GetPayloads()[0].GetData(), second.GetPayloads()[0].GetData())
}

func TestGetSuiteProfile(t *testing.T) {
	profile, err := getSuiteProfile("default")

	require.NoError(t, err)
	require.Equal(t, []int{64, 128, 256, 512, 1024}, profile.throughputConcurrency)
	require.Equal(t, []payloadCase{{name: "tiny", size: 0}, {name: "4KB", size: 4 << 10}, {name: "32KB", size: 32 << 10}}, profile.payloadCases)
	require.InDelta(t, 0.30, profile.minFinalThroughputRetain, 0)

	profile, err = getSuiteProfile("scylla-3x4")
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 4, 8, 16}, profile.throughputConcurrency)
	require.Equal(t, []payloadCase{{name: "tiny", size: 0}, {name: "4KB", size: 4 << 10}, {name: "32KB", size: 32 << 10}}, profile.payloadCases)

	_, err = getSuiteProfile("missing")
	require.Error(t, err)
}

func TestBestConcurrency(t *testing.T) {
	summaries := []benchmarkSummary{
		{
			caseName:    "max-throughput",
			concurrency: 64,
			result:      result{counts: counters{success: 100}, elapsed: time.Second},
		},
		{
			caseName:    "payload-sensitivity",
			concurrency: 256,
			result:      result{counts: counters{success: 1000}, elapsed: time.Second},
		},
		{
			caseName:    "max-throughput",
			concurrency: 128,
			result:      result{counts: counters{success: 200}, elapsed: time.Second},
		},
		{
			caseName:    "max-throughput",
			concurrency: 256,
			result:      result{counts: counters{success: 500, failed: 1}, elapsed: time.Second},
		},
	}

	require.Equal(t, 128, bestConcurrency(summaries))
}

func TestBestConcurrencyFallsBackWhenEveryThroughputStageFails(t *testing.T) {
	summaries := []benchmarkSummary{
		{
			caseName:    "max-throughput",
			concurrency: 64,
			result:      result{counts: counters{success: 100, failed: 1}, elapsed: time.Second},
		},
		{
			caseName:    "max-throughput",
			concurrency: 128,
			result:      result{counts: counters{success: 200, failed: 1}, elapsed: time.Second},
		},
	}

	require.Equal(t, 128, bestConcurrency(summaries))
}

func TestValidateFinalThroughputRetention(t *testing.T) {
	summaries := []benchmarkSummary{
		{
			caseName: "max-throughput",
			result:   result{counts: counters{success: 100}, elapsed: time.Second},
		},
		{
			caseName: "max-throughput",
			result:   result{counts: counters{success: 30}, elapsed: time.Second},
		},
	}
	require.NoError(t, validateFinalThroughputRetention(summaries, 0.30))

	summaries[1].result.counts.success = 29
	require.Error(t, validateFinalThroughputRetention(summaries, 0.30))
}

func TestFormatBenchmarkTable(t *testing.T) {
	var timings phaseTimings
	timings.add(e2eTimings{
		start:           time.Millisecond,
		poll:            2 * time.Millisecond,
		scheduleToStart: 3 * time.Millisecond,
		complete:        4 * time.Millisecond,
	})
	summary := benchmarkSummary{
		caseName:    "max-throughput",
		mode:        "e2e-parallel",
		payloadName: "4KB",
		payloadSize: 4 << 10,
		concurrency: 128,
		duration:    2 * time.Second,
		result: result{
			counts:   counters{success: 10, failed: 1, rpc: 30, started: 10},
			elapsed:  2 * time.Second,
			firstErr: "err|line\nnext",
			phases:   &timings,
		},
	}

	table := formatBenchmarkTable([]benchmarkSummary{summary})

	require.Contains(t, table, "| Case | Mode | Payload | Bytes | Concurrency | Duration | Success | Failed | RPS | RPC RPS | Start RPS | Start p95 ms | Poll p95 ms | WFT S2S p95 ms | Complete p95 ms | First Error |")
	require.Contains(t, table, "| max-throughput | e2e-parallel | 4KB | 4096 | 128 | 2s | 10 | 1 | 5.00 | 15.00 | 5.00 | 1.00 | 2.00 | 3.00 | 4.00 | err\\|line next |")
}

func TestReleaseOutstanding(t *testing.T) {
	outstanding := make(chan struct{}, 1)
	outstanding <- struct{}{}

	releaseOutstanding(outstanding)
	require.Empty(t, outstanding)

	releaseOutstanding(outstanding)
	require.Empty(t, outstanding)
}
