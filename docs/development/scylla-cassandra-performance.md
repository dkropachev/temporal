# Scylla/Cassandra Persistence Performance Notes

This note tracks the Scylla-focused persistence changes and the benchmark evidence still needed before calling the
optimization complete.

## Current 3 Node x 4 Shard Target

- Consider 512 logical history shards for new Cassandra/Scylla deployments. History shards map one-to-one to
  `executions` partitions and are independent of the 12 physical Scylla shards in the target cluster.
- The history shard count is fixed when a Temporal cluster is created. Defaults remain at 4 so upgrades cannot
  silently change it; set `NUM_HISTORY_SHARDS=512` explicitly before creating a cluster that should use 512.
- Keep the backend-neutral matching default at 4 read/write partitions. Measured Scylla deployments with many cold
  task queues can set both values to 1 through constrained dynamic config; retain reads on old partitions until they
  are drained.
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

- `history_node` keeps the rollback-compatible `((tree_id), branch_id, node_id, txn_id)` primary key.
- `history_node_v2` uses `(tree_id, branch_id)` as the partition key. This reduces the large-partition pattern where all
  branches for a tree share one Cassandra/Scylla partition.
- History reads and mutations are controlled by `historyNodeMigrationMode`. Startup validates that the configured mode
  matches the actual Cassandra primary keys before creating the execution store.
- `queues` keeps its upgrade-compatible `(queue_type, queue_name)` key shape in this PR. QueueV2 list-by-type scans
  still use `ALLOW FILTERING`; changing that requires a separate migration.
- Queue and QueueV2 message IDs continue to use max-ID reads followed by `IF NOT EXISTS` inserts. The range allocator
  experiment was removed because independent writers could commit reserved ranges out of order, causing a reader to
  advance past messages that committed later. Crashes also left gaps that made QueueV2 list and delete counts inexact.
- Schema `v1.14` remains unchanged so clusters that already applied this branch's range-table migration can advance to
  `v1.15`. Those tables are unused but remain in both the fresh schema and upgraded keyspaces so the same reported
  schema version converges on the same table set.
- The legacy `queue` table still partitions messages by `queue_type`. Bucketing it by message-ID range would reduce
  partition growth, but it also requires range-aware reads and a persisted delete cursor; otherwise
  `DeleteMessagesBefore` becomes an unbounded fanout over bucket partitions as ack levels advance.

Layout-aware history pagination keeps each Cassandra continuation on the table that issued it while that physical
layout remains valid. Raw tokens issued by older binaries are accepted in the pre-cutover source modes, but they do
not identify their source layout. Drain them or let clients restart pagination before entering a read-cutover mode.
The server rejects a continuation with a layout that may have been destructively recreated instead of passing stale
Cassandra state to a different table generation. Canonical-V2 continuations guard the raw Cassandra page state in an
envelope that migration-aware binaries unwrap. An older binary cannot silently apply that state to V1; Cassandra
rejects the deliberately malformed legacy view of the envelope.

### History Upgrade From V1

1. Apply Cassandra schema `v1.15`.
2. Roll out this server version with `historyNodeMigrationMode: legacy-v1-rebuild-v2`. Reads and authoritative
   mutations use V1; V2 is an optional mirror. This remains compatible with source-only older binaries.
3. After every writer is in rebuild mode, clear any rows made stale by source-only deletes:

   ```bash
   temporal-cassandra-tool --endpoint HOST --keyspace KEYSPACE \
     recreate-history-node-v2 --confirm-source-rebuild
   ```

4. Run `backfill-history-node-v2 --checkpoint-file ./history-node-v2-initial.json`. Writes continue against V1 while
   V2 is absent and during the backfill.
5. Roll every process to `historyNodeMigrationMode: legacy-v1-dual`, which requires both writes while still reading
   V1.
6. Roll every process to `historyNodeMigrationMode: legacy-v1-cutover-dual`. Reads remain on V1, but every append now
   writes V2 first.
7. Run `backfill-history-node-v2 --checkpoint-file ./history-node-v2-cutover.json` after the cutover-mode rollout
   completes. Use a new checkpoint because this is a new complete pass. This repairs any V1-only append left by a
   pre-cutover writer; a cutover-mode process that exits between statements has already written the future V2
   authority.
8. Roll every process to `historyNodeMigrationMode: canonical-dual`. Reads now use `history_node_v2`; mutations keep
   the same V2-first order.

Before switching reads back to V1, first ensure V1 has the legacy layout and has been rebuilt from V2. Then roll every
process to `v1-cutover-dual`, which still reads V2 but writes V1 first, run
`backfill-history-node-v1 --checkpoint-file ./history-node-v1-cutover.json` after that rollout, and only then roll to
`legacy-v1-rollback-dual`. This mode rejects raw page tokens that cannot prove they were issued after the V1 table
recreation. Before introducing an older binary, drain or abandon every unguarded raw token issued against a prior
physical `history_node`; older binaries discard generation metadata and cannot detect those stale tokens.
Canonical-V2 tokens are guarded and fail safely on an older binary. If an older version writes history during a
rollback, repeat the V2 rebuild before returning to `canonical-dual`.

### Online Upgrade From The Previous Branch V2 Layout

Clusters freshly installed from the previous branch have a branch-partitioned table named `history_node`. They can be
converted without stopping Temporal writes:

1. Apply Cassandra schema `v1.15`.
2. Roll every Temporal process to `historyNodeMigrationMode: old-v2-rebuild-v2`. The old branch-partitioned
   `history_node` remains authoritative and V2 is an optional mirror, so source-only old binaries can coexist.
3. After every process is in rebuild mode, run `recreate-history-node-v2 --confirm-source-rebuild`.
4. Run `backfill-history-node-v2 --checkpoint-file ./old-v2-to-history-node-v2-initial.json`.
5. Roll every process to `historyNodeMigrationMode: old-v2-dual`, which requires both writes and still reads the old
   table.
6. Roll every process to `historyNodeMigrationMode: old-v2-prepare-cutover-dual`. Reads remain on the old table, but
   every append now writes `history_node_v2` first.
7. Run `backfill-history-node-v2 --checkpoint-file ./old-v2-to-history-node-v2-cutover.json` after the
   prepare-cutover rollout completes.
8. Roll every process to `historyNodeMigrationMode: old-v2-cutover-dual`. Reads switch to `history_node_v2` without
   changing the V2-first mutation order.
9. Roll every process to `historyNodeMigrationMode: v1-rebuild-dual`. V2 is now authoritative and the old table is an
   optional mirror.
10. Recreate the rollback-compatible table:

   ```bash
   temporal-cassandra-tool --endpoint HOST --keyspace KEYSPACE \
     recreate-history-node-v1 --confirm-v1-rebuild
   ```

11. Restore historical rows to the recreated table:

   ```bash
   temporal-cassandra-tool --endpoint HOST --keyspace KEYSPACE \
     backfill-history-node-v1 --checkpoint-file ./history-node-v1-rebuild.json
   ```

12. Roll every process to `historyNodeMigrationMode: canonical-dual`.
13. Validate both read paths before considering the migration complete.

Do not recreate either inactive table until every process is in its corresponding rebuild mode. Each command requires
an explicit confirmation flag, validates the authoritative layout, and is restartable if it exits after the drop.
Appends in rebuild mode commit the authoritative table first and suppress only an exact missing-mirror-table error.
History-node deletes in rebuild mode likewise commit the authoritative table first and suppress only an exact
missing-mirror-table error. When both tables are required, deletes remain in one logged batch. Backfill inserts preserve
the source write timestamp, so a newer delete tombstone written to a recreated mirror wins if it races the copy.

Backfills divide the Murmur3 ring into 4096 ranges and atomically checkpoint each completed range. Reuse the same
checkpoint path after a failure; the command verifies the keyspace, table generations, partitioner, source layout,
direction, and range count before resuming, then revalidates the table generations and partitioner after the copy.
Non-Murmur3 clusters are rejected. Use a new path for each deliberate complete pass. The default page size is 16 so a
page of maximum-size 4 MiB history blobs remains well below the driver's 256 MiB frame limit. Lower the page size
further if rows or protocol overhead require it; `--token-ranges`, `--page-size`, and `--concurrency` are configurable.

History event blobs are inserted into the source and mirror with separate idempotent statements so Cassandra and Scylla
do not reject a cross-table batch containing two copies of a large blob. Both statements use one explicit timestamp,
so a delete interleaved between them wins in both tables. Workflow mutations append history before committing mutable
state; a required mirror error aborts that state commit, and the retrying persistence client completes the pair. The
prepare-cutover modes change write order before the final backfill, ensuring that a process exit cannot leave the
future read authority missing an append after that pass. Raw-history appends are not coupled to a mutable-state commit,
so the same staged order is required. Backfills preserve source timestamps, allowing newer concurrent target
mutations and tombstones to win.

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
- Queue message inserts retain `IF NOT EXISTS`. A range-reservation LWT only establishes ID ownership; it does not
  establish commit order between writers, so it cannot preserve Temporal's page-token/FIFO assumptions by itself.
- Successful queue IDs stay contiguous. Competing writers can choose the same next ID, one insert wins, and the loser
  retries after reading the new maximum. This keeps QueueV2 `MessageCount` and `MessagesDeleted` exact.
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

Include the shared-tree history benchmark when validating `history_node_v2` reads:

```bash
go test -tags test_dep ./common/persistence/tests -run '^$' -bench 'BenchmarkCassandraHistoryNodeMultiBranchRead$' -benchtime=100x -count=3
```

Server workload harness:

```bash
go run ./cmd/tools/temporalperf \
  -config-file ./config/development-cass-es.yaml \
  -server-binary ./temporal-server \
  -namespace temporal-perf \
  -profiles tiny,medium,big \
  -payload-bytes 128,4096,65536,1048576 \
  -task-queues 16 \
  -workers-per-task-queue 8 \
  -concurrency 640 \
  -warmup 30s \
  -measurement 1m \
  -trials 3 \
  -batch-workflows 10000 \
  -output-dir /tmp/temporalperf-scylla
```

`temporalperf` starts a real Temporal server for every sample using the standard config file unchanged, and owns the
generic Temporal workflow, activity, worker, pacing, and result logic. Database schema reset, metrics capture, server
profiling, and database cleanup belong in the reset and cleanup commands so the same workload can run unchanged against
every persistence backend. The hooks must not start Temporal in managed-server mode. Run the same matrix before and
after persistence changes and compare the generated `suite.json` files together with externally collected Temporal and
database metrics. Omitting the hooks above reuses an initialized database; supply database-specific schema reset and
cleanup commands for clean-store comparisons.

Many-worker task queue read/write scaling matrix:

```bash
for queues in 1 4 16; do
  for workers in 1 2 4 8 16 32; do
    go run ./cmd/tools/temporalperf \
      -config-file ./config/development-cass-es.yaml \
      -server-binary ./temporal-server \
      -namespace temporal-perf \
      -profiles tiny \
      -payload-bytes 256 \
      -task-queues "${queues}" \
      -workers-per-task-queue "${workers}" \
      -concurrency 320 \
      -warmup 30s \
      -measurement 1m \
      -trials 3 \
      -batch-workflows 3200 \
      -output-dir "/tmp/temporalperf-${queues}q-${workers}w"
  done
done
```

Use the activity matrix above to measure matching task writes and reads together: each workflow produces workflow-task
traffic plus activity task enqueue, poll, and completion traffic. Accept a task queue partition/configuration change only if it
improves completed workflows/sec without increasing p99 persistence latency, matching schedule-to-start latency, or
Scylla shard imbalance at the same cluster size. Reject changes that improve a single hot queue while regressing the
many-queue worker matrix.

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

Temporal server profiles can be captured during the same workload window. The development Cassandra
configs expose pprof on `127.0.0.1:7936`; the Docker template enables it when `PPROF_PORT` is set.

```bash
curl -fsS 'http://127.0.0.1:7936/debug/pprof/profile?seconds=30' -o /tmp/temporal.cpu.pprof
curl -fsS 'http://127.0.0.1:7936/debug/pprof/heap' -o /tmp/temporal.heap.pprof
go tool pprof -top /tmp/temporal.cpu.pprof
go tool pprof -top /tmp/temporal.heap.pprof
```

## Post-Safety Persistence Result

The final online-migration, legacy queue, and history design was rerun against exact base on a local Scylla `2026.1.7`
cluster with three nodes, four shards per node, and 4 GiB per node. Persistence test keyspaces use RF=1. Each operation
ran for 50 iterations in three fresh-keyspace samples; the table reports the median. Both revisions used
`CASSANDRA_MAX_CONNS=12`; the optimized revision also used
`CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE=2`.

After the final QueueV2 metadata-freshness correction, its four rows were rerun against exact base on the same
three-node topology temporarily limited to two shards and 2 GiB per node, with `CASSANDRA_MAX_CONNS=4` on both
revisions. Those rows compare only samples from that identical recovery-cluster configuration.

| Benchmark | Base median ns/op | Final median ns/op | Delta |
| --- | ---: | ---: | ---: |
| `HistoryNodeAppendRead/append` | 1,089,627 | 1,162,919 | +6.73% |
| `HistoryNodeAppendRead/read` | 1,242,014 | 1,194,998 | -3.79% |
| `HistoryNodeV2OnlyAppendRead/append` | 1,089,627 | 1,091,929 | +0.21% |
| `HistoryNodeV2OnlyAppendRead/read` | 1,242,014 | 1,202,721 | -3.16% |
| `QueueV2EnqueueRead/enqueue` | 3,417,950 | 2,330,037 | -31.83% |
| `QueueV2EnqueueRead/read` | 2,246,261 | 2,226,298 | -0.89% |
| `QueueV2EnqueueRead/range_delete` | 4,525,653 | 4,555,883 | +0.67% |
| `QueueV2EnqueueRead/list` | 113,220,069 | 112,666,554 | -0.49% |
| `QueueEnqueueRead/enqueue` | 2,293,566 | 2,300,431 | +0.30% |
| `QueueEnqueueRead/read` | 1,206,100 | 1,210,576 | +0.37% |

The bounded QueueV2 existence cache retains a 31.83% enqueue gain. Metadata-dependent first-page reads and range
deletes now reload versioned queue metadata, leaving read, range-delete, and list latency within 1% of base.
Conditional, contiguous message-ID allocation leaves the legacy queue at baseline behavior. Final `canonical-dual`
history append pays a 6.73% latency cost to maintain the rollback table, while V2 branch reads improve by 3.79%.
Temporary `v2-only` append is within 0.21% of base.

`HistoryNodeMultiBranchRead` medians were 19,793,834 ns/op for base and 15,898,516 ns/op for the final branch, but
individual samples overlapped and ranged from 15.9 to 23.5 ms. That result is too noisy to rank the implementations.
The V2 model's intended benefit is bounded partition size and hot-partition isolation at production scale, which this
fresh RF=1 latency test does not demonstrate.

## Online-Migration Live Result

The exact base and final `canonical-dual` branch were also run with fresh RF=3 stores against the same three-node,
four-shard Scylla cluster. Each sample followed schema and server stabilization waits plus a 1,000-workflow warmup.
The workload used 6,400 workflows, 16 task queues, 32 workers per queue, concurrency 640, and a 256-byte payload.

The E2E binaries predate the final review-only corrections to QueueV2, schema-layout validation, pagination
termination, and benchmark timer reporting. Those corrections do not change the workflow history or matching data
paths exercised here. QueueV2 was rerun separately after its metadata-freshness correction, as described in the
post-safety persistence section. The E2E numbers therefore remain evidence for the database-placement change, but they
are not a bit-for-bit benchmark of the final commit.

| Workload | Base samples | Base median | Final samples | Final median | Delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| One activity, workflows/s | 413.43; 410.08; 401.15 | 410.08 | 2,034.82; 2,088.26; 2,073.86 | 2,073.86 | +405.7% |
| Backlog enqueue, requests/s | 2,429.30; 2,386.17; 2,397.99 | 2,397.99 | 11,480.81; 11,291.21; 11,608.53 | 11,480.81 | +378.8% |
| 512-worker backlog drain, workflows/s | 1,490.84; 1,377.20; 1,430.95 | 1,430.95 | 6,640.56; 6,479.60; 6,603.51 | 6,603.51 | +361.5% |

All twelve measured runs completed 6,400 workflows with zero load failures; each backlog run observed all 6,400
persisted tasks. Scylla read-failure, write-failure, and write-timeout counters remained unchanged in every measured
window.

| Workload | Revision | Post-warmup `PREPARE` requests | Reprepare attempts | Prepared-plan cache evictions |
| --- | --- | ---: | ---: | ---: |
| Activity | Base | 9; 9; 9 | 0; 0; 0 | 0; 0; 0 |
| Activity | Final | 1; 0; 1 | 0; 0; 0 | 0; 0; 0 |
| Backlog | Base | 9; 9; 11 | 0; 0; 0 | 0; 0; 0 |
| Backlog | Final | 3; 3; 9 | 0; 0; 0 | 0; 0; 0 |

The low `PREPARE` counts are lazy preparation on a host or operation path not exercised by the 1,000-workflow warmup;
none followed an `UNPREPARED` response. There were also no internal forwarded reprepares, prepared-plan cache
evictions, or one-off-plan evictions. No persistence query or cache-size fix is indicated by these runs. The
authorization prepared cache did expire entries during longer samples, as expected from Scylla's permission-cache
validity window; those evictions do not remove CQL plans or cause statement repreparation.

| Workload | Metric | Base median | Final median | Delta |
| --- | --- | ---: | ---: | ---: |
| Activity | I/O-queue write bytes/workflow | 377,508 | 332,699 | -11.87% |
| Activity | I/O-queue write operations/workflow | 21.410 | 6.978 | -67.41% |
| Activity | Maximum compacted `executions` partition | 10,090,808 bytes | 126,934 bytes | -98.74% |
| Backlog | I/O-queue write bytes/workflow | 181,594 | 150,440 | -17.16% |
| Backlog | I/O-queue write operations/workflow | 11.545 | 3.271 | -71.67% |
| Backlog | Maximum compacted `executions` partition | 7,007,506 bytes | 105,778 bytes | -98.49% |

The branch-level throughput gain is primarily a database placement result: 512 logical history-shard partitions
instead of 4, one matching partition instead of 4, and Scylla-aware connection defaults. It is not attributed to
Temporal runtime changes. The physical `history_node_v2` benefit is branch isolation and bounded partition growth;
ordinary one-branch workflows do not isolate that effect.

## Pre-Safety Local 3 Node x 4 Shard Benchmark Result

Environment:

- Scylla `2026.2.0` Docker image, 3 nodes, `--smp 4` per node.
- Benchmarks ran from a Go container attached to the Scylla Docker network so peer RPC addresses were reachable.
- Existing test helper creates `SimpleStrategy`, RF=1 keyspaces. Tablets were disabled for new keyspaces because
  Scylla rejects `SimpleStrategy` with tablets enabled.
- Data directories were bind-mounted under `/tmp` to avoid host root filesystem critical disk utilization.

Baseline was `HEAD` plus only the benchmark harness backport. Optimized was this patch with
`CASSANDRA_MAX_CONNS=12` and `CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE=2`.

> These measurements predate the queue allocator safety correction and the rollback-compatible history dual-write.
> Queue and history rows below describe the rejected implementation, not the final branch, and must not be used as
> evidence for the current code.

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
the existing Cassandra schema and config defaults. Optimized was the pre-safety patch, with its replaced `history_node`
schema and QueueV2
schemas, `numHistoryShards: 12`, `maxConns: 12`, and `maxExcessShardConnectionsRate: 2`. Each server ran all Temporal
services in one process. Rootless Podman in this environment could not apply `--cpuset-cpus`, so this validates server
behavior on 3 x 4 Scylla shards but does not perfectly reproduce CPU-affinity isolation.

> This server comparison also predates the final history migration/dual-write design. It is retained as experiment
> history and is not a performance claim for the current branch.

These historical measurements used an earlier standalone workflow generator inside each server container against
`127.0.0.1:7233`; use `cmd/tools/temporalperf` for new comparable runs.

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

- The bounded QueueV2 existence cache removes the metadata read from hot enqueue paths after a queue is known. The
  final path still performs a max-ID read and conditional insert for every message.
- QueueV2 first-page reads and range deletes reload versioned metadata. Continuation-page reads use their message-ID
  token; a process with known queue existence does not need another metadata query, while a cold process validates the
  queue first. This prevents a long-lived process from repeatedly scanning an obsolete tombstone range or overlooking
  a newer partition layout without allowing a continuation token to bypass queue validation.
- QueueV2 enqueues are locally serialized per queue to avoid same-process conflicts. Independent queue names use
  one of 256 bounded lock stripes; cross-process conflicts are resolved by the conditional insert and retry.
- QueueV2 metadata CAS conflicts invalidate the local existence entry. Metadata itself is not reused by reads or
  deletes.
- The list path still uses the upgrade-compatible `queues` primary key and `ALLOW FILTERING`; replacing that with
  bucketed queue metadata requires a separate schema migration.
- The legacy queue store retains its max-ID read and conditional insert. The prior enqueue-latency result came from the
  unsafe range allocator and does not apply to the final branch.
- The legacy queue message table remains a residual large-partition risk for very long-lived namespace replication
  queues. A full bucketed redesign should add a persisted minimum live bucket/delete cursor, then teach reads and DLQ
  range deletes to walk bucket partitions without scanning from bucket zero on every cleanup.
- The connection/config changes are otherwise neutral in this microbenchmark.
- `history_node_v2` reduces partition growth and isolates branches. The final implementation also dual-writes the
  legacy table for rollback safety, so both read distribution and extra write cost must be included in new benchmarks.
  Ordinary appends use separate idempotent inserts to keep duplicated event blobs out of cross-table batches; required
  mirror failures fail the operation for retry.
- QueueV2 uses LWT for message inserts, queue creation, and queue metadata updates.
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
  queues, not Scylla write capacity. This Scylla-specific workload supports a constrained value of 1, but does not
  justify changing the backend-neutral default for SQL, Apache Cassandra, or hot queues. Keep reads on old partitions
  until they are drained, and use constrained dynamic config only for measured task queues.
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
- Cassandra paged result readers now preallocate response slices from the request page or batch size, including
  matching tasks, history tasks, history-tree scans, and workflow-state listings. Capacities are capped at 1,000
  entries so request-controlled sizes cannot trigger oversized speculative allocations. This matches the SQL store
  pattern and reduces allocation growth in hot paged scans.
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
- New Cassandra/Scylla clusters can opt into 512 logical history shards instead of tying the count to the target
  cluster's 12 physical Scylla shards. The upgrade-safe default remains 4 because this value is immutable after
  cluster creation. Each history shard is one `executions` partition, and the conditional mutable-state batch
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
- The safety-corrected QueueV2 rerun used `-benchtime=50x -count=3`. Base range-delete samples were `4,525,653`,
  `4,455,202`, and `4,534,400 ns/op`; final samples were `4,651,480`, `4,555,883`, and `4,498,415 ns/op`.
  Base list samples were `114,479,360`, `111,389,592`, and `113,220,069 ns/op`; final samples were `114,246,413`,
  `112,666,554`, and `111,313,916 ns/op`.
- The rejected QueueV2 range-allocator experiment measured `1,147,301`, `1,113,305`, and `1,108,269 ns/op`. It was
  removed because it could skip late-committing messages and make counts inexact; these samples are not final-branch
  results.
- QueueV2 cached enqueue checks queue existence without cloning mutable queue metadata. The final bounded lookup
  measured `27.78-27.97 ns/op` with zero allocations. This removes per-enqueue clone overhead while bounding cache and
  lock memory.
- QueueV2 list pagination remains covered by unit tests for invalid page tokens and repeated empty Cassandra page
  tokens. A bucketed QueueV2 metadata schema was investigated but not kept in this PR because it needs a separate
  migration for existing `queues` rows.
- QueueV2 `ListQueues` now closes the metadata-list iterator before returning row-level metadata/count errors, avoiding
  driver-side scan resource leaks while walking queue metadata rows.
- The rejected legacy queue range-allocator experiment measured enqueue at `1,078,713`, `1,086,525`, and
  `1,081,212 ns/op`, versus baseline samples `2,255,904`, `2,247,092`, and `2,265,705 ns/op`. The final branch restores
  the baseline max-ID plus conditional-insert algorithm, so this apparent gain is intentionally discarded.
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
- `cmd/tools/temporalperf` for repeatable workflow/activity load against a running server

Compare:

- p50/p95/p99 Temporal persistence latency
- completed workflows per second
- top CPU and heap profile entries from the load generator and Temporal server
- per-phase `temporalperf` metadata and lifecycle logs
- Scylla CPU balance across 12 shards
- LWT operation rate and p99 latency
- partition size and hot-partition warnings before/after
