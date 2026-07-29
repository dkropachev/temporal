# Scylla/Cassandra Persistence Performance Notes

This note tracks the Scylla-focused persistence changes and the benchmark evidence still needed before calling the
optimization complete.

## Current 3 Node x 4 Shard Target

- Use 512 logical history shards by default for new Cassandra/Scylla deployments. History shards map one-to-one to
  `executions` partitions and are independent of the 12 physical Scylla shards in the target cluster.
- The history shard count is fixed when a Temporal cluster is created. Existing clusters must retain their original
  value; changing this default only affects new clusters that do not set `NUM_HISTORY_SHARDS`.
- Use 1 matching task queue read/write partition by default, and opt known hot task queues into higher partition counts
  through dynamic config after measuring the workload.
- Keep Scylla gocql shard-aware port enabled. It is enabled by default in the Scylla gocql fork; use
  `maxExcessShardConnectionsRate` to tune per-shard connection reuse/ramp behavior.

## CCM 3 Node x 4 Shard Setup

Use CCM for the local Scylla cluster. Scylla 2026 enforces RF/rack validity for keyspaces with secondary indexes, so an
RF=3 Temporal keyspace needs three racks in the single local datacenter.

```bash
ccm create temporal-bench-3x4 --scylla --version release:2026.1.7 -n 3 --ipprefix 127.0.80. --vnodes
```

Before first start, set the seed, snitch, and rack for each node:

```yaml
seed_provider:
  - class_name: org.apache.cassandra.locator.SimpleSeedProvider
    parameters:
      - seeds: "127.0.80.1"
endpoint_snitch: GossipingPropertyFileSnitch
```

Use `cassandra-rackdc.properties` values `dc=datacenter1` and `rack=rack1`, `rack=rack2`, `rack=rack3` for nodes 1-3.
Start CCM from a persistent shell/session while profiling so the Scylla processes stay alive:

```bash
ccm start --no-wait --jvm_arg=--smp=4 --jvm_arg=--memory=4G
```

Install the Temporal Cassandra schema with NetworkTopologyStrategy RF=3:

```bash
./temporal-cassandra-tool --endpoint 127.0.80.1 drop -k temporal -f
./temporal-cassandra-tool --endpoint 127.0.80.1 create -k temporal --rf 3 --dc datacenter1
./temporal-cassandra-tool --endpoint 127.0.80.1 -k temporal setup-schema -v 0.0
./temporal-cassandra-tool --endpoint 127.0.80.1 -k temporal update-schema -d ./schema/cassandra/temporal/versioned
```

## Data Model Changes

- `history_node` now uses `(tree_id, branch_id)` as the partition key.
- This reduces the large-partition pattern where all branches for a tree share one Cassandra/Scylla partition.
- Existing history reads and writes already qualify both `tree_id` and `branch_id`, so the query shape remains targeted.
- `queues` keeps its upgrade-compatible `(queue_type, queue_name)` key shape in this PR. QueueV2 list-by-type scans
  still use `ALLOW FILTERING`; changing that requires a separate migration and was left out to keep the queue range
  allocation change rolling-upgrade safe.
- Legacy `queue` message IDs are now allocated through `queue_message_id_range`, which fences cross-process writers at
  range granularity and lets individual message inserts use regular writes.
- `queue_message_id_ranges` is keyed by `(queue_type, queue_name)`, not by `queue_type` alone, so QueueV2 range
  reservation LWTs do not serialize all queues of one type through one Paxos partition.
- The legacy `queue` table still partitions messages by `queue_type`. Bucketing it by message-ID range would reduce
  partition growth, but it also requires range-aware reads and a persisted delete cursor; otherwise
  `DeleteMessagesBefore` becomes an unbounded fanout over bucket partitions as ack levels advance. This patch keeps the
  FIFO/read/delete contract intact and removes the steady-state per-message LWT from that table.

## LWT Audit

ScyllaDB LWT docs say conditional batches cannot modify multiple partitions, and advise against mixing conditional and
non-conditional writes over the same dataset. ScyllaDB also routes LWT work directly to the right core when the
shard-aware driver is used. Regular reads are still acceptable after successful LWT writes when the read consistency is
compatible with the LWT write's regular, non-serial consistency phase. The Temporal Cassandra store defaults to
local-quorum regular consistency and local-serial LWT consistency unless overridden, so local-quorum reads see completed
local-quorum LWT writes in the default configuration. Serial reads are only needed when the reader must participate in
Paxos and observe an in-flight conditional update before it completes.

Implications for Temporal:

- Do not split the `executions` table partition key by `type` without redesigning mutable-state CAS batches. Those
  batches intentionally update shard, workflow, and task rows under one conditional write path.
- QueueV2 message inserts can use regular writes only when message IDs come from a fenced allocator. This patch uses
  CAS-reserved ID ranges in `queue_message_id_ranges`; writers then use regular inserts for rows inside the reserved
  range. A writer crash can leave unused IDs in the reserved range, so QueueV2 readers and range deletes must tolerate
  gaps. `ListQueues` derives message count from min/max IDs, so that value is an upper bound if a crash leaves gaps.
  The range table is partitioned by queue identity because partitioning by `queue_type` would concentrate all range
  reservation LWTs for a queue type onto one hot Paxos partition.
- Legacy queue message inserts follow the same rule using `queue_message_id_range`. The first reservation starts after
  the current max message ID when a range marker is absent, then steady-state enqueue uses one regular insert per
  message until the local range is exhausted.
- Do not replace metadata/version LWTs with regular writes unless the caller can prove single-writer ownership or a
  separate fencing mechanism. Regular writes can resurrect stale metadata after a concurrent versioned update.
- Do not split `task_queue_user_data` by `build_id` without redesigning its update path. The CAS batch updates the
  task-queue user data row and build-id mapping rows together; splitting by `build_id` would make it multi-partition.

Reference docs:

- https://docs.scylladb.com/manual/stable/features/lwt.html
- https://docs.scylladb.com/manual/stable/kb/lwt-differences.html
- https://docs.scylladb.com/manual/stable/cql/consistency.html
- https://docs.scylladb.com/manual/stable/cql/cqlsh.html#serial-consistency

## Benchmark Checklist

Run each workload before and after the patch on the same 3 node x 4 shard cluster.

Persistence microbenchmark:

```bash
CASSANDRA_SEEDS=node1,node2,node3 \
CASSANDRA_PORT=9042 \
CASSANDRA_MAX_CONNS=12 \
CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE=2 \
go test -tags test_dep ./common/persistence/tests -run '^$' -bench 'BenchmarkCassandra(HistoryNodeAppendRead|QueueV2EnqueueRead)$' -benchtime=30s -count=3
go test -tags test_dep ./common/persistence/cassandra -run '^$' -bench 'BenchmarkReadHistoryBranch(Page|SparsePage)$' -benchmem -benchtime=10000x -count=3
```

Include the legacy namespace-replication queue benchmark when validating queue append changes:

```bash
go test -tags test_dep ./common/persistence/tests -run '^$' -bench 'BenchmarkCassandraQueueEnqueueRead$' -benchtime=30s -count=3
```

Include the matching task benchmark when validating server task-queue persistence paths:

```bash
go test -tags test_dep ./common/persistence/tests -run '^$' -bench 'BenchmarkCassandraMatchingTaskQueue$' -benchtime=50x -count=3
go test -tags test_dep ./common/persistence/cassandra -run '^$' -bench 'BenchmarkGetTasksV1ReadPage$' -benchmem -benchtime=10000x -count=3
go test -tags test_dep ./common/persistence/cassandra -run '^$' -bench 'BenchmarkListConcreteExecutionsReadPage$' -benchmem -benchtime=10000x -count=3
```

Include the task-queue user data build-ID count benchmark when validating worker-versioning metadata paths:

```bash
go test -tags test_dep ./common/persistence/tests -run '^$' -bench 'BenchmarkCassandraTaskQueueUserDataBuildIDCount$' -benchtime=100x -count=3
```

Include the shared-tree history benchmark when validating the `history_node` partition-key change:

```bash
go test -tags test_dep ./common/persistence/tests -run '^$' -bench 'BenchmarkCassandraHistoryNodeMultiBranchRead$' -benchtime=100x -count=3
```

Server workload harness:

```bash
go run ./cmd/tools/scyllaload \
  -address 127.0.0.1:7233 \
  -namespace scylla-load \
  -task-queue scylla-load \
  -workflows 10000 \
  -concurrency 200 \
  -activities-each 1 \
  -signals-each 0 \
  -eager-start \
  -eager-activities \
  -payload-bytes 256 \
  -cpu-profile /tmp/scyllaload.cpu.pprof \
  -heap-profile /tmp/scyllaload.heap.pprof \
  -server-pprof http://127.0.0.1:7936 \
  -server-cpu-profile /tmp/temporal.cpu.pprof \
  -server-heap-profile /tmp/temporal.heap.pprof \
  -profile-summary /tmp/scyllaload.cpu.pprof=/tmp/scyllaload.cpu.top.txt \
  -profile-summary /tmp/scyllaload.heap.pprof=/tmp/scyllaload.heap.top.txt \
  -profile-summary /tmp/temporal.cpu.pprof=/tmp/temporal.cpu.top.txt \
  -profile-summary /tmp/temporal.heap.pprof=/tmp/temporal.heap.top.txt \
  -metrics-snapshot-before http://127.0.0.1:8000/metrics=/tmp/temporal.before.metrics.prom \
  -metrics-snapshot-before http://scylla-node-1:9180/metrics=/tmp/scylla-node-1.before.metrics.prom \
  -metrics-snapshot-before http://scylla-node-2:9180/metrics=/tmp/scylla-node-2.before.metrics.prom \
  -metrics-snapshot-before http://scylla-node-3:9180/metrics=/tmp/scylla-node-3.before.metrics.prom \
  -metrics-snapshot-after http://127.0.0.1:8000/metrics=/tmp/temporal.after.metrics.prom \
  -metrics-snapshot-after http://scylla-node-1:9180/metrics=/tmp/scylla-node-1.after.metrics.prom \
  -metrics-snapshot-after http://scylla-node-2:9180/metrics=/tmp/scylla-node-2.after.metrics.prom \
  -metrics-snapshot-after http://scylla-node-3:9180/metrics=/tmp/scylla-node-3.after.metrics.prom \
  -result-file /tmp/scyllaload.result.json \
  -run-metadata-file /tmp/scyllaload.metadata.json
```

Use `-signals-each` to stress mutable-state update paths after workflow start, and increase `-activities-each` to
stress matching task writes and reads. Use `-eager-start` and `-eager-activities` to measure the colocated worker fast
paths; activity eager execution also requires `system.enableActivityEagerExecution` in dynamic config. Run the same
command before and after persistence changes while collecting Temporal persistence metrics and Scylla per-shard/LWT
metrics. The emitted result JSON reports completed
`workflowsPerSec` and frontend `requestsPerSec` for start/signal calls, plus the load-generator/server profile paths,
pprof top summary paths, pre-run and post-run metrics snapshot paths, the result JSON path, and the run-metadata JSON
path. The metadata file records runtime/process settings, selected Cassandra/Scylla environment variables, and
pprof/metrics endpoint reachability so before/after samples can be tied back to their CPU, heap, Prometheus, and
cluster-configuration evidence.

Many-worker task queue read/write scaling matrix:

```bash
for queues in 1 4 16; do
  for workers in 1 2 4 8 16 32; do
    go run ./cmd/tools/scyllaload \
      -address 127.0.0.1:7233 \
      -namespace scylla-load \
      -task-queue "scylla-load-${queues}q-${workers}w" \
      -task-queues "${queues}" \
      -workers-per-task-queue "${workers}" \
      -workflows 3200 \
      -concurrency 320 \
      -activities-each 1 \
      -signals-each 0 \
      -payload-bytes 256 \
      -result-file "/tmp/scyllaload.activity.${queues}q.${workers}w.json" \
      -run-metadata-file "/tmp/scyllaload.activity.${queues}q.${workers}w.metadata.json"
  done
done
```

Use the activity matrix above to measure matching task writes and reads together: each workflow produces workflow-task
traffic plus activity task enqueue, poll, and completion traffic. Run the same queue/worker matrix with
`-activities-each 0 -signals-each 0` to isolate workflow-task-only task queue polling/read pressure, and with
`-signals-each 1` to include frontend request throughput. Accept a task queue partition/configuration change only if it
improves completed workflows/sec without increasing p99 persistence latency, matching schedule-to-start latency, or
Scylla shard imbalance at the same cluster size. Reject changes that improve a single hot queue while regressing the
many-queue worker matrix.

To separate persisted task writes from worker-driven reads, first enqueue workflow tasks without workers, wait until
`DescribeTaskQueue` reports at least the expected approximate backlog, and then start the requested workers:

```bash
go run ./cmd/tools/scyllaload \
  -address 127.0.0.1:7233 \
  -namespace scylla-load \
  -task-queue scylla-load-backlog \
  -task-queues 16 \
  -workers-per-task-queue 32 \
  -workflows 3200 \
  -concurrency 320 \
  -activities-each 0 \
  -signals-each 0 \
  -eager-start=false \
  -backlog-before-workers \
  -backlog-wait-timeout 30s \
  -result-file /tmp/scyllaload.backlog.16q.32w.json
```

In backlog mode, `enqueueRequestsPerSec` measures workflow-start admission and persisted workflow-task writes while no
worker is polling. `drainWorkflowsPerSec` measures persisted task polling/reads and workflow completion after workers
start. Compare the same queue shape at 1, 2, 4, 8, 16, and 32 workers per task queue. Every cell must report the full
expected `backlogTasks`, zero `enqueueFailed`, zero `drainFailed`, and zero `ResourceExhausted` metric growth.

Default development RPS limits can hide storage scaling behind matching poll throttling. The benchmark-only dynamic
configuration used for the unthrottled matrix was:

```yaml
frontend.rps: 100000
frontend.namespaceRPS: 100000
frontend.namespaceCount: 10000
matching.rps: 100000
matching.persistenceMaxQPS: 30000
history.rps: 100000
history.persistenceMaxQPS: 30000
```

These values are measurement ceilings, not production recommendations. Check service metrics before accepting a
result: one default-limit 16-queue, 32-worker backlog run recorded 2,423 workflow-task poll and 790 activity-task poll
`ResourceExhausted` responses with the `RpsLimit` cause. Its apparent worker-scaling decline was therefore invalid.
Also monitor the Scylla data and commitlog filesystem during long matrices; an `EDQUOT` event invalidates the run even
when the load generator has not yet reported a failure.

After resetting the disposable CCM stores and raising only the benchmark limits, two 3,200-workflow controls at
16 queues and 1 worker per queue produced a `2,540.48 workflows/sec` geometric-mean drain rate. Two controls at
16 queues and 32 workers per queue (512 workers total) produced `2,796.46 workflows/sec`, a 10.08% increase. Workflow
start admission remained between `4,865.58` and `5,372.68 requests/sec`; all 12,800 workflows entered the measured
backlog and completed with zero failures. Persisted task reads therefore scale through 512 workers on this cluster,
then approach server/database capacity rather than regressing as the quota-limited matrix suggested.

A later fresh-store confirmation ran three shuffled 6,400-workflow passes at every power-of-two worker count from
1 to 32 workers per queue across 16 queues. All 115,200 workflows entered the full persisted backlog and completed
with zero load-generator failures. Write admission medians remained between `5,223.04` and `6,872.97 requests/sec`
because writes complete before workers start. The 512-worker drain median was `3,441.30 workflows/sec`; a longer
order-reversed confirmation reached `3,802.98` and `3,798.58 workflows/sec`, versus `2,971.09` and
`1,276.08 workflows/sec` with 16 workers. One short 16-worker sample was excluded after canceled task-queue
persistence calls. No `ResourceExhausted` metric series or current Scylla storage error was present. This confirms
that both persisted task writes and worker-driven reads remain healthy at 512 workers; the spread between short cells
is host scheduling and storage-latency noise, not a worker-count regression.

The matrix must use the same server build settings and should randomize cell order across repeated passes. A mismatched
`CGO_ENABLED=1` candidate initially appeared to regress the 16-queue, 8-worker activity control to
`155.35-164.09 workflows/sec`; rebuilding it with the baseline's `CGO_ENABLED=0` restored `180.12 workflows/sec`.
Record build metadata and compare medians from at least three shuffled passes before attributing a matrix difference to
the persistence change.

The final 3 node x 4 shard activity matrix on `2026-07-28` used `maxConns: 12`, one matching read/write partition,
3,200 workflows per cell, concurrency 320, one activity per workflow, and a 256-byte payload:

| Task queues | 1 worker/queue | 2 workers/queue | 4 workers/queue | 8 workers/queue |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 186.71 | 175.51 | 179.32 | 178.94 |
| 4 | 180.75 | 167.20 | 185.71 | 167.25 |
| 16 | 190.70 | 180.24 | 163.49 | 162.26 |

All 38,400 workflows completed with zero load-generator failures. The shorter cells retain visible order noise, so the
6,400-workflow controls are the acceptance signal: the original `MapScan` build reached `180.95` and
`180.65 workflows/sec` at 16 queues and 8 workers; the typed mutable-state read build reached `180.12`, and the
combined typed mutable-state/current-row build reached `180.75`. The read/write path therefore preserves capacity at
128 workers rather than showing a repeatable worker-count regression.

The workflow-task-only matrix was more order-sensitive: one ordered pass ranged from `251.58` to
`853.08 workflows/sec`, and a repeated 6,400-workflow control reversed the apparent 1-queue result
(`542.19` at 4 workers versus `568.47` at 8 workers). Final combined-build controls reached `511.17 workflows/sec` at
1 queue and 8 workers and `442.51 workflows/sec` at 16 queues and 8 workers, with all 6,400 workflows completed and no
failures. Treat one-pass rankings as diagnostic data, not a tuning decision.

Load-generator profiles can be inspected with:

```bash
go tool pprof -top /tmp/scyllaload.cpu.pprof
go tool pprof -top /tmp/scyllaload.heap.pprof
```

The `-profile-summary` flag records the same `go tool pprof -top` output during the run, after all requested profiles
are written. Keep those text summaries with the result JSON and metrics snapshots for before/after comparisons; they
are the easiest way to prove whether remaining server time is in persistence calls, matching/history scheduling, SDK
worker code, serialization, or Scylla driver work.

Temporal server profiles can also be captured manually during the same workload window. The development Cassandra
configs expose pprof on `127.0.0.1:7936`; the Docker template enables it when `PPROF_PORT` is set.

```bash
curl -fsS 'http://127.0.0.1:7936/debug/pprof/profile?seconds=30' -o /tmp/temporal.cpu.pprof
curl -fsS 'http://127.0.0.1:7936/debug/pprof/heap' -o /tmp/temporal.heap.pprof
go tool pprof -top /tmp/temporal.cpu.pprof
go tool pprof -top /tmp/temporal.heap.pprof
```

## Local 3 Node x 4 Shard Benchmark Result

Environment:

- Scylla `2026.2.0` Docker image, 3 nodes, `--smp 4` per node.
- Benchmarks ran from a Go container attached to the Scylla Docker network so peer RPC addresses were reachable.
- Existing test helper creates `SimpleStrategy`, RF=1 keyspaces. Tablets were disabled for new keyspaces because
  Scylla rejects `SimpleStrategy` with tablets enabled.
- Data directories were bind-mounted under `/tmp` to avoid host root filesystem critical disk utilization.

Baseline was `HEAD` plus only the benchmark harness backport. Optimized was this patch with
`CASSANDRA_MAX_CONNS=12` and `CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE=2`.

| Benchmark | Baseline avg ns/op | Optimized avg ns/op | Delta |
| --- | ---: | ---: | ---: |
| `HistoryNodeAppendRead/append` | 1,092,328 | 1,087,782 | -0.4% |
| `HistoryNodeAppendRead/read` | 1,227,479 | 1,212,039 | -1.3% |
| `QueueV2EnqueueRead/enqueue` | 3,389,890 | 1,122,958 | -66.9% |
| `QueueV2EnqueueRead/read` | 2,267,256 | 1,147,147 | -49.4% |
| `QueueV2EnqueueRead/range_delete` | 4,589,317 | 3,399,876 | -25.9% |
| `QueueV2EnqueueRead/list` | 114,611,145 | 112,578,046 | -1.8% |
| `QueueEnqueueRead/enqueue` | 2,256,234 | 1,082,150 | -52.0% |
| `QueueEnqueueRead/read` | 1,215,493 | 1,213,074 | -0.2% |
| `HistoryNodeMultiBranchRead` | 11,824,248 | 12,536,961 | +6.0% |
| `TaskQueueUserDataBuildIDCount/limited_count` | 1,142,512 | 1,103,693 | -3.4% |

Additional current-state matching task queue profile:

| Benchmark | Current avg ns/op |
| --- | ---: |
| `MatchingTaskQueue/legacy/create` | 1,281,397 |
| `MatchingTaskQueue/legacy/read` | 1,280,243 |
| `MatchingTaskQueue/legacy/delete` | 1,087,646 |
| `MatchingTaskQueue/fair/create` | 1,299,069 |
| `MatchingTaskQueue/fair/read` | 1,239,674 |
| `MatchingTaskQueue/fair/delete` | 1,100,506 |

## Server Workload Result

Baseline and optimized worktrees were also run against live Temporal servers backed by the same shape of Scylla cluster:
3 Scylla containers and `--smp 4` each. Baseline was clean `HEAD` plus only the load-generator harness backport, using
the existing Cassandra schema and config defaults. Optimized was this patch, with the new `history_node` and QueueV2
schemas, `numHistoryShards: 12`, `maxConns: 12`, and `maxExcessShardConnectionsRate: 2`. Each server ran all Temporal
services in one process. Rootless Podman in this environment could not apply `--cpuset-cpus`, so this validates server
behavior on 3 x 4 Scylla shards but does not perfectly reproduce CPU-affinity isolation.

The load generator was `cmd/tools/scyllaload`, run inside each server container against `127.0.0.1:7233`.

| Workload | Baseline samples workflows/sec | Optimized samples workflows/sec | Baseline avg | Optimized avg | Delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1000 workflows, concurrency 100, 1 activity, 128 byte payload | 101.55, 97.31, 94.83 | 125.43, 124.53, 125.65 | 97.90 | 125.20 | +27.9% |
| 1000 workflows, concurrency 100, 1 signal, 128 byte payload | 165.31, 164.87, 155.93 | 184.62, 194.02, 185.44 | 162.03 | 188.03 | +16.0% |

All twelve server workload samples completed `1000/1000` workflows with zero load-generator failures. Average elapsed
time improved from `10.22s` to `7.99s` for the activity workload and from `6.18s` to `5.32s` for the signal workload.
After the optimized runs, Scylla reported all three nodes `UN`; Elasticsearch visibility had 11,003 docs in
`temporal_visibility_v1_dev`. Cassandra table counts on one coordinator were `executions=34980`,
`history_node=60007`, `tasks_v2=88`, `queue_messages=0`, and `queue_message_id_ranges=0`. The QueueV2 and legacy queue
tables remain untouched by this workflow workload; queue coverage still comes from the persistence microbenchmarks
above.

Interpretation:

- The QueueV2 metadata cache removes the metadata read from hot enqueue paths after a queue is known. CAS-reserved
  message-ID ranges remove steady-state max-message-ID reads and per-message LWTs; the message row insert is now a
  regular write. Together these reduced QueueV2 enqueue latency by 66.9% in this microbenchmark.
- QueueV2 enqueues are locally serialized per queue. This avoids same-process writers exhausting or updating the cached
  ID range concurrently; cross-process writers are fenced by the range-table CAS.
- QueueV2 range reservations are partitioned by queue identity, so independent queues do not contend on one
  `queue_type`-wide LWT partition.
- QueueV2 metadata CAS conflicts invalidate the local queue metadata cache so a retry fetches the latest version instead
  of repeatedly using stale metadata.
- QueueV2 list latency improved 1.8% in the 100-queue microbenchmark after the metadata cache and iterator cleanup.
  The list path still uses the upgrade-compatible `queues` primary key and `ALLOW FILTERING`; replacing that with
  bucketed queue metadata should be handled as a separate schema migration.
- The legacy queue store now reserves message-ID ranges with CAS and uses regular inserts for the namespace replication
  queue and its DLQ path. This removes the steady-state `SELECT message_id ... ORDER BY message_id DESC LIMIT 1` read
  and per-message `IF NOT EXISTS` from same-process appends. It reduced legacy queue enqueue latency by 52.0% in the
  microbenchmark.
- The legacy queue message table remains a residual large-partition risk for very long-lived namespace replication
  queues. A full bucketed redesign should add a persisted minimum live bucket/delete cursor, then teach reads and DLQ
  range deletes to walk bucket partitions without scanning from bucket zero on every cleanup.
- The connection/config changes are otherwise neutral in this microbenchmark.
- The `history_node` partition-key change reduces partition growth and isolates branches, but this RF=1 latency
  microbenchmark does not show a read-latency win. The expected benefit should be validated with Scylla hot-partition,
  partition-size, and per-shard CPU metrics under reset/branch-heavy server workloads.
- QueueV2 no longer uses LWT for message inserts. It still uses LWT when creating queue metadata, updating queue
  metadata, and reserving message-ID ranges. Reserving ranges trades lower enqueue write latency for possible ID gaps
  after a writer crash; normal reads and range deletes are gap-tolerant, while `ListQueues` message count can overstate
  the exact row count in that crash case.
- Matching task queue writes remain on a conditional batch that includes task rows plus the task-queue metadata row.
  Splitting this into regular task inserts and a separate metadata CAS is not safe without a larger redesign: a CAS
  failure after task inserts would leave orphan/duplicate visible tasks, while a metadata success before task inserts
  can advance read levels over rows that were never written.

## CCM Profile Run

The optimized tree and regular Temporal `v1.31.2` were profiled against a CCM-managed Scylla `2026.1.7` cluster on
`2026-07-27`: 3 nodes, `--smp 4`, one datacenter, three racks, RF=3 Temporal keyspace, and Elasticsearch 7.10.1 for
visibility only. Temporal ran all services in one process with Cassandra seed `127.0.80.1`, pprof on `127.0.0.1:7936`,
and Prometheus metrics on `127.0.0.1:8000`.

Regular Temporal was the upstream `v1.31.2` tag (`19a774302`) with `numHistoryShards: 4` and the stock Cassandra
datastore config. Optimized Temporal was this patch on commit `11a35d86a`, with `numHistoryShards: 12`, Scylla gocql,
`maxConns: 12`, `maxExcessShardConnectionsRate: 2`, and the schema/query changes described above.

`ccm node1 nodetool status` confirmed all nodes `UN`:

| Address | Rack |
| --- | --- |
| `127.0.80.1` | `rack1` |
| `127.0.80.2` | `rack2` |
| `127.0.80.3` | `rack3` |

| Workload | Regular v1.31.2 | Optimized | Delta |
| --- | ---: | ---: | ---: |
| 1000 workflows, concurrency 100, 1 activity, 128 byte payload | 139.32 workflows/sec | 196.88 workflows/sec | +41.3% |
| 1000 workflows, concurrency 100, 1 signal, 128 byte payload | 253.25 workflows/sec | 286.74 workflows/sec | +13.2% |

All four CCM samples completed `1000/1000` workflows with zero load-generator failures. The load tool recorded pprof and
Prometheus endpoint reachability under `/tmp/temporal-regular-profile` and `/tmp/temporal-ccm-profile`; all Temporal
and Scylla metrics endpoints returned `200 OK`. The first temporary Elasticsearch container later reported flood-stage
disk watermark blocks while processing visibility queue retries. The regular rerun disabled ES disk thresholds. Treat
the profile data as valid for Cassandra/Scylla persistence hotspots, but not as a clean visibility backend latency
measurement.

Scylla metric deltas during the CCM runs show the remaining write contention is in mutable state and matching task CAS,
not in QueueV2:

| Workload | Regular top Paxos counters across 3 nodes | Optimized top Paxos counters across 3 nodes |
| --- | --- | --- |
| Activity | `executions$paxos` 84,112 writes / 42,066 reads; `tasks$paxos` 22,396 writes / 11,211 reads | `executions$paxos` 84,000 writes / 42,000 reads; `tasks$paxos` 25,156 writes / 12,579 reads |
| Signal | `executions$paxos` 48,060 writes / 24,030 reads; `tasks$paxos` 10,716 writes / 5,358 reads | `executions$paxos` 48,112 writes / 24,057 reads; `tasks$paxos` 9,574 writes / 4,791 reads |

Non-Paxos hot tables during the same windows were `executions`, `history_node`, and `tasks`. QueueV2 tables had no
meaningful traffic in these workflow workloads, so their LWT reduction remains covered by the persistence
microbenchmarks. The next performance redesign should target the `executions` conditional batch only if it can preserve
the shard/workflow/task atomicity guarantees described in the LWT audit above.
- Switching matching task-write batches from logged to unlogged was tested and rejected. On the same 3 node x 4 shard
  Scylla cluster, legacy create changed from `2,347,673` to `2,361,531 ns/op` (`+0.6%`) while fair create changed from
  `2,349,971` to `2,308,822 ns/op` (`-1.8%`). The mixed/noisy result is not enough to justify changing the conditional
  range-fenced write path.
- Switching the new-branch history tree+first-node write from a logged batch to an unlogged batch was tested and
  rejected. The isolated `BenchmarkCassandraHistoryNodeAppendRead/append` improved slightly from roughly `1.09 ms/op`
  to `1.07 ms/op`, but full server throughput on the same 3 node x 4 shard cluster regressed: activity workflow
  throughput changed from `187.54` to `180.38 workflows/sec`, and signal workflow throughput changed from `291.81` to
  `284.80 workflows/sec`. The small microbenchmark gain does not offset the workflow-level regression or the weaker
  cross-table branch creation failure behavior.
- Increasing Cassandra `maxConns` from `12` to `24` was tested and rejected on the same server workload. Activity
  throughput dropped to `173.08 workflows/sec`, and signal throughput dropped to `239.11 workflows/sec`, compared with
  `187.54` and `291.81 workflows/sec` at `maxConns: 12`. Matching connections to the 12 Scylla shards remains the
  better default for this target.
- Many task queues with multiple workers do not scale well when every task queue gets 12 matching read/write partitions.
  A 16-task-queue, 2-worker-per-task-queue activity workload improved from `113.01 workflows/sec` with 12 partitions to
  `154.25 workflows/sec` with 1 partition, and the signal workload improved from `217.18 workflows/sec` /
  `434.36 requests/sec` to `900.19 workflows/sec` / `1800.38 requests/sec`. Four partitions was also tested with the
  same 16-task-queue shape and reached only `115.79 workflows/sec` for activity and `255.63 workflows/sec` /
  `511.25 requests/sec` for signal. The bottleneck is matching partition fanout and manager/poller overhead across many
  queues, not Scylla write capacity. The global default is therefore 1 partition; use constrained dynamic config values
  for specifically measured hot task queues that benefit from more partitioning.
- Increasing normal matching task queue read/write partitions from `12` to `24` was tested and rejected on the same
  3 node x 4 shard cluster. Activity throughput dropped to `97.82 workflows/sec`, and signal throughput dropped to
  `240.02 workflows/sec` / `480.03 requests/sec`, compared with same-server 12-partition controls of
  `189.46 workflows/sec` for activity and `290.73 workflows/sec` / `581.46 requests/sec` for signal. More partitions
  add matching management and polling overhead without adding useful Scylla parallelism on a 12-shard target.
- Reducing matching task queue read/write partitions from `12` to `6` was tested against the earlier single-hot-queue
  target and rejected for that target. Activity
  throughput dropped to `160.39 workflows/sec`, while signal throughput was effectively flat at
  `292.64 workflows/sec` / `585.29 requests/sec`. The lower partition count reduces matching fanout overhead but
  under-spreads that activity workload's task writes. The later many-task-queue runs above showed the broader default
  should still be conservative, with higher partition counts reserved for specifically measured hot queues.
- Reusing one map across matching-task `MapScan` calls was rejected because gocql requires a new map for every row.
  Typed `Scan` removes the per-row map instead while preserving null detection for Cassandra static-only rows.
- Eager workflow start and activity dispatch were tested and accepted as load-generator controls for measuring
  colocated worker fast paths. On the same optimized server with `maxConns: 12`, the no-eager controls were
  `189.46 workflows/sec` for the one-activity workload and `290.73 workflows/sec` / `581.46 requests/sec` for the
  one-signal workload. `-eager-start` alone raised the one-activity workload to `285.29 workflows/sec`;
  `-eager-activities` alone raised it to `219.81 workflows/sec`; both flags together reached
  `304.25 workflows/sec`. The one-signal workload, which has no activities, reached `1255.46 workflows/sec` /
  `2510.91 requests/sec` with eager start enabled. This does not change the schema patch, but it gives cluster
  throughput tests an explicit way to separate persistence/data-model bottlenecks from matching round trips.
- Cluster membership queries no longer append `ALLOW FILTERING` for partition-local scans or full primary-key equality
  reads. The store still keeps `ALLOW FILTERING` for host-ID-without-role, RPC address, session-start, and heartbeat
  filters that cannot be served by the table's primary key alone.
- Worker-versioning build-ID limit checks now pass the configured threshold into persistence. Cassandra/Scylla uses a
  `SELECT task_queue_name ... LIMIT ?` query for that path instead of `COUNT(*)`, so reads stop once the limit is
  reached. The 100-mapping microbenchmark shows only a 3.4% latency reduction, but the data read is bounded by the
  configured limit rather than by all task queues mapped to a build ID.
- Worker-versioning build-ID listing now issues the next Cassandra page explicitly when `PageState()` is returned.
  Previously the loop checked the page token without creating a new iterator, which could spin on large build-ID
  mappings instead of advancing to the next page. It also stops if Cassandra returns the same empty page token twice,
  matching the QueueV2 list guard against repeated empty paging tokens.
- Task-queue user data scans now close Cassandra iterators before returning malformed-row errors, and successful CAS
  user-data updates return iterator close errors instead of ignoring them. This is an error-path resource cleanup rather
  than a steady-state throughput optimization, but it avoids leaking driver-side scan resources under bad persisted data
  or close failures.
- History branch reads and history-tree branch scans now read the Cassandra page token after consuming each iterator,
  avoiding dropped next-page tokens on large histories or many branches.
- History branch row conversion now returns typed field errors instead of panicking on malformed rows, and closes the
  iterator before returning those errors.
- History scheduled-task and timer-task scan templates now keep all `executions` predicates separated in the generated
  CQL. This is a correctness cleanup in the same hot table path, not a benchmarked latency change.
- Cassandra matching and history task reads now preallocate response task slices from the request page or batch size,
  matching the SQL store pattern and reducing allocation growth in hot paged task scans.
- Cassandra matching task reads now use typed iterator scans instead of allocating a new `MapScan` result map for
  every task. A focused 100-task V1 read-page benchmark improved from `15.752-16.915 us/op`, `43176-43178 B/op`, and
  308 allocations to `6.016-6.073 us/op`, `14432-14433 B/op`, and 211 allocations. V1 and V2 retain nullable task-ID
  handling so static-only rows are skipped without treating task ID zero as a sentinel.
- Cassandra history branch reads now use typed scans for both full-node and metadata-only query shapes. A focused
  100-node page benchmark improved from `20.460-22.430 us/op`, `45896-45897 B/op`, and 311 allocations to
  `4.624-4.852 us/op`, `12456-12457 B/op`, and 116 allocations. Reverse-order reads retain the same fixed column shape,
  and metadata-only reads scan only the three selected ID columns.
- Cassandra history branch reads now size their node slice from the number of rows in the returned gocql page instead
  of the requested event page size. A one-row page with `PageSize=256` improved from `1,146-1,269 ns/op`,
  `10,552-10,553 B/op`, and 17 allocations to `714.8-965.6 ns/op`, `1,112 B/op`, and 17 allocations. The full
  100-row benchmark retains its `12,456-12,458 B/op` footprint and 116 allocations.

  Equal 6,400-workflow live profiles reduced flat `HistoryStore.ReadHistoryBranch` allocation from `176.01 MiB` to
  `8.00 MiB` (`-95.45%`) and total sampled server allocation from `7.66 GiB` to `6.61 GiB` (`-13.75%`). Longer
  reverse-order controls used 16 task queues, 8 workers per queue, 25,600 workflows per cell, concurrency 320, one
  activity per workflow, and a 256-byte payload. Baseline cells reached `941.05` and `926.86 workflows/sec`;
  candidate cells reached `981.38` and `913.48`. Geometric throughput improved from `933.93` to
  `946.82 workflows/sec` (`+1.38%`). All 102,400 workflows completed with zero load failures, all three Scylla nodes
  remained up, and Scylla logged no run-time errors during the controls.
- Cassandra workflow mutable-state reads now scan their 22 fixed columns into a typed row and pre-size decoded
  activity, timer, child, cancel, signal, and CHASM maps. The focused benchmark improved from
  `3.599-4.350 us/op`, `5120-5121 B/op`, and 42 allocations to `1.751-1.868 us/op`, `3688-3690 B/op`, and
  37 allocations. Matched live 6,400-workflow activity controls remained neutral: `193.19 workflows/sec` before and
  `194.79 workflows/sec` after at 1 queue and 4 workers, and `180.65-180.95` before versus `180.12` after at
  16 queues and 8 workers.
- Cassandra current-execution reads now select and scan only `current_run_id`, `execution_state`, and
  `execution_state_encoding`; the unused execution blob, execution encoding, and last-write version no longer cross
  the wire. The focused benchmark reduced `1384-1385 B/op` to `1160-1161 B/op`; latency and allocation count were
  neutral at roughly `0.8 us/op` and 19 allocations because workflow-state protobuf decoding dominates the harness.
  The combined live build reached `188.21 workflows/sec` at 1 queue and 4 workers and `180.75 workflows/sec` at
  16 queues and 8 workers, completing all 12,800 workflows with zero failures.
- Cassandra/Scylla defaults now use 512 logical history shards instead of tying the count to the target cluster's
  12 physical Scylla shards. Each history shard is one `executions` partition, and the conditional mutable-state batch
  intentionally keeps its shard, workflow, and generated task rows in that partition. More logical shards distribute
  those atomic batches without weakening their fencing or splitting them across partitions.

  Fresh-store screening used the same 3-node x 4-shard Scylla cluster, 16 task queues, 32 workers per queue,
  6,400 persisted workflow tasks, and concurrency 640:

  | History shards | Start admission | Backlog drain | Max `executions` partition | Server RSS after run |
  | ---: | ---: | ---: | ---: | ---: |
  | 12 | `5,536.51 starts/sec` | `2,539.45 workflows/sec` | `1,358,102 bytes` | `903,484 KiB` |
  | 128 | `9,736.00 starts/sec` | `6,714.81 workflows/sec` | `219,342 bytes` | `1,052,408 KiB` |
  | 512 | `11,853.46 starts/sec` | `7,064.67 workflows/sec` | `88,148 bytes` | `1,154,016 KiB` |

  A fresh 512-shard confirmation reached `12,252.66 starts/sec` and `7,083.41 workflows/sec`, then a final
  12-shard control reached `5,386.68` and `3,245.43`. The two-control geometric means improved from `5,461.08` to
  `12,051.41 starts/sec` (`+120.68%`) and from `2,870.82` to `7,074.04 drain workflows/sec` (`+146.41%`).
  Maximum sampled compacted partition size fell from a `1,629,722-byte` geometric mean to `88,148 bytes` (`-94.59%`).

  A mixed one-activity workload with the same 512-worker shape completed `2,275.83 workflows/sec` at 512 history
  shards versus `984.04` at 12 (`+131.27%`). All 44,800 measured workflows across the shard-count screening,
  confirmations, and mixed controls completed with zero failures, no `ResourceExhausted`/`RpsLimit` metric series,
  no dropped mutations, and no current Scylla storage errors. The measured server RSS geometric mean increased from
  `894,136 KiB` to `1,154,652 KiB` (`+29.14%`); operators choosing a lower count for memory-constrained new clusters
  trade away partition distribution and future history-service scale-out headroom.
- Cassandra `ListConcreteExecutions` now preallocates its result slice from the requested page size. A focused 100-state
  page benchmark improved from `455.4-487.2 ns/op`, `2168 B/op`, and 8 allocations to `144.9-157.9 ns/op`, `896 B/op`,
  and 1 allocation. This reduces Go allocation and GC pressure during shard-level `executions` table scans without
  changing CQL, consistency, or paging behavior.
- `ListConcreteExecutions` also uses a typed iterator scan for its six fixed columns and skips the unused run ID
  destination. Its focused 100-row read-page benchmark improved from `26.790-27.397 us/op`, `65480-65481 B/op`, and
  506 allocations to `11.382-13.610 us/op`, `41616-41619 B/op`, and 411 allocations. Current-record rows with an empty
  execution blob remain excluded.
- `ListConcreteExecutions` now closes the Cassandra iterator on normal completion and malformed-row early exits, and
  returns close errors on the normal path. This avoids leaking driver-side scan resources during shard-level
  `executions` table walks.
- Matching `GetTasks` now closes the Cassandra iterator before returning malformed-row errors in both classic and fair
  task stores. Normal reads already closed the iterator; this covers the early-exit path on the matching task hot table.
- Nexus endpoint listing now closes iterators before version-conflict and malformed-row errors on first-page and
  continuation scans, matching the same driver-resource cleanup pattern used in the hotter persistence scans.
- The range-delete row was measured separately with `-benchtime=100x -count=3` and `CASSANDRA_MAX_CONNS=12` on both
  baseline and optimized worktrees. The optimized samples were `3,439,679`, `3,391,419`, and `3,368,529 ns/op`.
- The list row was measured separately with `-benchtime=100x -count=3`, `CASSANDRA_MAX_CONNS=12`, and 100 seeded queue
  metadata rows. Baseline samples were `115,401,614`, `114,936,553`, and `113,495,269 ns/op`; optimized samples were
  `111,834,750`, `111,807,282`, and `114,092,106 ns/op`.
- The final QueueV2 enqueue row includes the message-ID range allocator. It was measured separately with
  `-benchtime=100x -count=3` and `CASSANDRA_MAX_CONNS=12`. The optimized samples were `1,147,301`, `1,113,305`, and
  `1,108,269 ns/op`. Compared with the prior cached-LWT optimized average of `1,184,106 ns/op`, the allocator reduces
  enqueue latency by another 5.2%.
- QueueV2 cached enqueue now checks queue existence without cloning cached queue metadata. The local lookup benchmark
  showed the old clone path at `277.5-373.4 ns/op`, `308 B/op`, and `6 allocs/op`, while the existence-only path was
  `14.41-15.40 ns/op` with zero allocations. This removes per-enqueue CPU/allocation overhead after the queue metadata
  is known.
- QueueV2 list pagination remains covered by unit tests for invalid page tokens and repeated empty Cassandra page
  tokens. A bucketed QueueV2 metadata schema was investigated but not kept in this PR because it needs a separate
  migration for existing `queues` rows.
- QueueV2 `ListQueues` now closes the metadata-list iterator before returning row-level metadata/count errors, avoiding
  driver-side scan resource leaks while walking queue metadata rows.
- The legacy queue rows were measured separately with `-benchtime=100x -count=3`, `CASSANDRA_MAX_CONNS=12`, and
  `CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE=2`. Baseline was `HEAD` plus the benchmark harness and test-helper
  benchmark compatibility only. Baseline enqueue samples were `2,255,904`, `2,247,092`, and `2,265,705 ns/op`;
  optimized enqueue samples with the range allocator were `1,078,713`, `1,086,525`, and `1,081,212 ns/op`. Baseline
  read samples were `1,193,265`, `1,193,471`, and `1,259,743 ns/op`; optimized read samples were `1,193,336`,
  `1,212,139`, and `1,223,748 ns/op`. Compared with the prior cached-LWT optimized enqueue average of
  `1,157,858 ns/op`, the range allocator reduces enqueue latency by another 6.5%.
- The matching task queue rows were measured separately on the optimized worktree with `-benchtime=50x -count=3` and
  `CASSANDRA_MAX_CONNS=12`. Legacy samples were: create `1,278,914`, `1,310,330`, `1,254,947`; read `1,268,179`,
  `1,287,101`, `1,285,449`; delete `1,097,834`, `1,086,432`, `1,084,673 ns/op`. Fair queue samples were: create
  `1,317,606`, `1,275,299`, `1,304,303`; read `1,208,190`, `1,292,227`, `1,218,606`; delete `1,122,207`,
  `1,086,902`, `1,092,410 ns/op`.
- The task-queue user data build-ID count row was measured separately on the optimized worktree with
  `-benchtime=100x -count=3`, `CASSANDRA_MAX_CONNS=12`, and 100 task queues mapped to one build ID. The exact
  `COUNT(*)` samples were `1,131,529`, `1,150,767`, and `1,145,239 ns/op`; the limited threshold-read samples were
  `1,103,459`, `1,105,617`, and `1,102,004 ns/op`.

Collect Temporal metrics:

- persistence latency: `persistence_latency`, tagged by `operation`
- persistence request/error rate: `persistence_requests`, `persistence_errors`, `persistence_error_with_type`
- service latency and task latency: `service_latency`, `task_schedule_to_start_latency`, `task_load_latency`
- focus operation tags: `CreateWorkflowExecution`, `UpdateWorkflowExecution`, `AppendRawHistoryNodes`,
  `ReadHistoryBranch`, `ReadHistoryBranchReverse`, `CreateTasks`, `GetTasks`, `CompleteTasksLessThan`,
  `EnqueueMessage`, `ReadQueueMessages`, `DeleteMessagesBefore`, `CountTaskQueuesByBuildId`

Example Prometheus queries with the development configs' histogram timers:

```promql
histogram_quantile(0.99, sum by (le, operation) (rate(persistence_latency_bucket[5m])))
sum by (operation) (rate(persistence_requests[5m]))
sum by (operation) (rate(persistence_errors[5m]))
histogram_quantile(0.99, sum by (le) (rate(task_schedule_to_start_latency_bucket[5m])))
```

Collect Scylla metrics:

- per-shard CPU utilization
- coordinator foreground read/write latency
- LWT/paxos latency and contention counters
- large partition warnings
- hot partition/table metrics for `executions`, `history_node`, `tasks_v2`, `queue_messages`

Workloads:

- high workflow start rate across many workflow IDs
- high signal/update rate against active workflows
- high activity and workflow task queue load on a small set of task queues
- history-heavy workflows with long event histories and branch reads
- DLQ/QueueV2 enqueue, read, and range-delete stress
- `cmd/tools/scyllaload` for repeatable workflow start/activity/signal load against a running server

Compare:

- p50/p95/p99 Temporal persistence latency
- completed workflows per second
- top CPU and heap profile entries from the load generator and Temporal server
- `scyllaload.metadata.json` endpoint status and Scylla/Temporal environment settings
- Scylla CPU balance across 12 shards
- LWT operation rate and p99 latency
- partition size and hot-partition warnings before/after
