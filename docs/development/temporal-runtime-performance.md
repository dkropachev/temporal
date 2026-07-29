# Temporal Runtime Performance Notes

This note tracks Temporal server CPU and allocation work that is intentionally separate from the Scylla/Cassandra
schema, data-model, driver, and persistence changes in `scylla-cassandra-performance.md`. The runtime branch is stacked
on the database branch so both use the same cluster workload harness, but its production diff does not modify
`common/persistence`, `schema`, Cassandra configuration, or the Scylla load tool.

## Acceptance Method

Each candidate starts with a focused benchmark and allocation profile. A production change is retained only when
reset-isolated live controls preserve or improve completed workflow throughput. The high-concurrency gate uses
16 task queues, 32 workers per queue, and a fully persisted 6,400-workflow backlog so a local allocation win cannot hide
a regression in write admission or worker-driven task reads.

Focused benchmarks:

```bash
go test -tags test_dep ./common/metrics -run '^$' \
  -bench '^BenchmarkTallyMetricsHandlerRepeatedLookup$' -benchmem -count=3
go test -tags test_dep ./common/quotas -run '^$' \
  -bench '^BenchmarkRateLimiterReserve$' -benchmem -count=3
go test -tags test_dep ./service/history/queues -run '^$' \
  -bench '^(BenchmarkExecutableTrackerSplit|BenchmarkGrouperNamespaceIDKey)$' -benchmem -count=3
go test -tags test_dep ./service/history/shard -run '^$' \
  -bench '^BenchmarkTaskKeyGeneratorSetTaskKeysInfoLogging$' -benchmem -count=3
go test -tags test_dep ./service/history/workflow -run '^$' \
  -bench '^BenchmarkCalculateExternalPayloadSize$' -benchmem -count=3
```

Use the persisted read/write and activity workload commands in `scylla-cassandra-performance.md` for live controls.
Reset the disposable stores before every cell, build all compared binaries with the same `CGO_ENABLED` value, reverse
binary order between controls, and compare geometric means. Every accepted cell must complete all workflows without
`ResourceExhausted`, timeout, unavailable, or current Scylla storage errors.

## Accepted Optimizations

### Tally Tag Scopes

Tally handlers reuse derived scopes for repeated tag sets. The process-wide cache retains at most 8,192 handlers. New
tag sets remain uncached after the bound, while lookups continue to reuse entries retained before saturation.

- Repeated tagged-counter lookup improved from `240-312 ns/op`, `656 B/op`, and 5 allocations to `158-175 ns/op`,
  `32 B/op`, and 1 allocation.
- Recording through an existing tagged counter improved from `208-274 ns/op`, `592 B/op`, and 3 allocations to
  `138-154 ns/op` with zero allocations.
- Sampled cumulative server allocation fell from `11.68 GiB` to `10.06 GiB` (`-13.9%`) without reducing the clean
  16-queue, 8-worker activity controls.
- After cache saturation, the mixed high-cardinality benchmark improved from `101.7-103.7 ns/op`, `656 B/op`, and
  5 allocations to `68.9-70.5 ns/op`, `344 B/op`, and 3 allocations.
- Saturated 512-worker persisted-backlog controls improved geometric drain throughput from `2,414.04` to
  `2,662.17 workflows/sec` (`+10.28%`). All 25,600 measured workflows completed.

### History Queue Grouping Keys

History queue executables retain their boxed namespace grouping key instead of converting the namespace string to
`any` whenever the tracker groups or splits a task. Other task implementations retain the namespace-ID fallback.

- The focused key benchmark improved from `12.02-12.72 ns/op`, `16 B/op`, and 1 allocation to `1.91-1.98 ns/op`
  with zero allocations.
- Moving half of 1,024 tracker tasks improved from `90.81-91.35 us/op`, `74,104 B/op`, and 1,031 allocations to
  `77.73-79.50 us/op`, `57,720 B/op`, and 7 allocations.
- Moving all tasks improved from `130.25-133.63 us/op`, `188,872 B/op`, and 1,036 allocations to
  `116.80-118.15 us/op`, `172,488 B/op`, and 12 allocations.
- A sampled server profile reduced flat tracker allocation from approximately `0.92 GiB` to `0.72 GiB`. Reset-isolated
  6,400-workflow controls retained `175.80-179.61 workflows/sec` and completed without failures.

### External Payload Accounting

External-payload accounting skips generated recursive traversal for workflow-task scheduled, started, and completed
events, plus activity-task started events without a retry failure. Event-level user metadata is checked before the fast
path; every other event type retains the generated visitor fallback.

- A representative eight-event batch improved from `1,847-1,988 ns/op`, `5,616 B/op`, and 31 allocations to
  `1,036-1,157 ns/op`, `2,912 B/op`, and 18 allocations.
- Three order-balanced live controls improved geometric activity throughput from `1,045.13` to
  `1,101.57 workflows/sec` (`+5.40%`). All 38,400 workflows completed.

### Disabled Debug Logging

History task-key generation checks whether debug logging is enabled before constructing workflow, task-type, ID, and
timestamp tags. Debug-enabled and unknown logger implementations retain the previous behavior.

- With an info-level production logger, the 100-task benchmark improved from `19.22-21.67 us/op`, `56,000 B/op`, and
  800 allocations to `441.5-457.1 ns/op` with zero bytes and zero allocations.
- Equal profiles removed the `0.16 GiB` flat allocation attributed to `taskKeyGenerator.setTaskKeys`.
- Reset-isolated live controls remained neutral-positive at `1,230.07` versus `1,233.35 workflows/sec` (`+0.27%`).
  All 102,400 workflows completed.

### Real-Time Rate Limiter Reservations

Real-time rate limiters return the underlying Go reservation instead of allocating another `ClockedReservation`.
Explicitly clocked limiters retain the wrapper required for event-time and test-clock semantics.

- The focused benchmark improved from `59.61-60.72 ns/op`, `88 B/op`, and 2 allocations to `44.51-45.81 ns/op`,
  `64 B/op`, and 1 allocation.
- Equal profiles removed `166 MiB` of flat wrapper allocation and reduced total sampled allocation by `0.75%`.
- Reset-isolated 16-queue, 8-worker activity throughput improved by `3.49%`.
- At 512 workers, persisted write admission improved from `7,365.00` to `8,797.58 requests/sec` (`+19.45%`) and
  backlog drain throughput improved from `4,520.59` to `4,723.38 workflows/sec` (`+4.49%`).
- All 128,000 measured workflows completed without load or cluster errors.

## Retained Regression Target

`BenchmarkExecutableTrackerSplit` covers no-move, half-move, and all-move splits over 1,024 tasks. The retained
implementation measures `23.385-24.033 us/op`, `90.583-91.830 us/op`, and `127.821-129.592 us/op` for those cases.

Two-pass classification and one-pass lazy-allocation implementations were rejected after reducing live throughput.
Retaining the right-side tracker map also reduced flat tracker allocation and CPU, but regressed the alternating
6,400-workflow geometric mean by `1.63%`. The benchmark remains to catch future changes without altering split
ownership.

## Rejected Candidates

- **Metric instrument handles:** Caching counter, gauge, timer, and histogram handles removed wrapper allocations and
  improved a lower-concurrency control, but the 512-worker gate reduced write admission by `13.35%` and drain
  throughput by `8.18%`.
- **Metric exclusion-key canonicalization:** Configured tag-exclusion checks raised repeated lookup latency by about
  7% and reduced a conservative live control from `181.56` to `176.32 workflows/sec`.
- **Lazy task logger tags:** The no-log benchmark fell to `25.26-26.21 ns/op`, `72 B/op`, and 2 allocations, but four
  reset-isolated controls regressed geometric throughput by `11.20%`.
- **Conditional decoded-history results:** Focused and profile allocations improved, but the 512-worker persisted
  backlog drain regressed by `6.50%`.
- **Lazy outgoing header propagation:** The no-header microbenchmark reached zero allocations, but live activity
  throughput regressed by `1.24%`, with 512-worker writes and drains down `4.13%` and `5.38%`.
- **Principal-header copy fast path:** The no-principal microbenchmark reached zero allocations, but live throughput
  regressed by `7.45%`.
- **Workflow-only worker mode:** Suppressing unused activity polling was neutral at 512 workers but reduced the
  16-worker control by `16.06%`.

These candidates are not present in production code on this branch.
