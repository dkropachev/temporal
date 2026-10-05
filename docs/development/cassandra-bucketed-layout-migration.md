# Cassandra bucketed-layout migration

This runbook migrates an existing Cassandra/Scylla Temporal keyspace to the bucketed layouts added in schema
`v1.16`. It is an online migration: Temporal remains available while data is copied and validated. Cutover seals one
shard, tree, queue, or namespace at a time; an operation on that entity may receive retryable `Unavailable` (or the
equivalent persistence ownership/condition error) during the short seal and succeeds on retry. There is no
cluster-wide stop-the-world cutover.

The families are independent, except that one `queueV2MigrationMode` controls both QueueV2 layout records and one
`matchingTaskMigrationMode` controls both classic and fair matching stores.

| Layout name | Source | Target | Immutable parameter | Final mode |
| --- | --- | --- | ---: | --- |
| `executions` | `executions` | `executions_v2` | execution buckets, default `16` | `executionMigrationMode: target-only` |
| `history_node` | `history_node` | `history_node_v2` | `0` | `historyNodeMigrationMode: v2-only` |
| `history_tree` | `history_tree` | `history_tree_v2` | fixed buckets, `16` | `historyTreeMigrationMode: target-only` |
| `matching_tasks` | `tasks` | `tasks_v3` | storage buckets, default `16` | `matchingTaskMigrationMode: target-only` |
| `matching_tasks_fair` | `tasks_v2` | `tasks_v3_fair` | storage buckets, default `16` | `matchingTaskMigrationMode: target-only` |
| `task_queue_user_data` | `task_queue_user_data` | `task_queue_user_data_v2` | buckets, default `64` | `taskQueueUserDataMigrationMode: target-only` |
| `legacy_queue` | `queue` | `legacy_queue_v2_*` | message-ID span, default `4096` | `legacyQueueMigrationMode: target-only` |
| `queue_v2_metadata` | `queues` | `queues_v2` | fixed metadata buckets, `64` | `queueV2MigrationMode: target-only` |
| `queue_v2_messages` | `queue_messages` | `queue_messages_v3` | message-ID span, default `4096` | `queueV2MigrationMode: target-only` |

The immutable values identify a physical layout generation. Use the same values in the server configuration and every
migration command. Changing one requires a new target-table generation and a new migration; do not edit an existing
`schema_layout_metadata` row.

## Preparation and source-capable barrier

Back up the keyspace, test this procedure on a restored copy, and build `temporal-cassandra-tool` from the same
revision as the server. Put durable checkpoint files on storage that survives operator-host restarts. The examples
below use the defaults selected for the current four-history-shard deployment:

```bash
export CASSANDRA_ENDPOINT=HOST
export TEMPORAL_KEYSPACE=KEYSPACE

tool() {
  ./temporal-cassandra-tool \
    --endpoint "$CASSANDRA_ENDPOINT" \
    --keyspace "$TEMPORAL_KEYSPACE" \
    "$@"
}
```

Apply only the additive schema first. It creates the target and authority tables and adds source fencing columns; it
does not route service traffic to a target:

```bash
tool update-schema \
  --schema-dir ./schema/cassandra/temporal/versioned \
  --version 1.16
tool validate-health
```

Roll this binary to every Temporal service while retaining source-authoritative modes. Verify membership and the
deployment controller show no older process, and prevent an older binary from restarting. This is the
source-capable barrier: only after it is complete may an operator activate source fencing or use a command whose
confirmation flag asserts that barrier.

For executions and history tree, first roll all writers to `source-rebuild`. Then clear their inactive targets before
recording the physical target generations:

```bash
tool prepare-executions-v2-backfill \
  --buckets 16 \
  --confirm-source-rebuild
tool recreate-history-tree-v2 --confirm-source-rebuild
```

For a vanilla `history_node` V1 source, also enter `legacy-v1-rebuild-v2` everywhere and run
`recreate-history-node-v2 --confirm-source-rebuild`. A cluster carrying the older branch-partitioned V2 layout must
instead follow [Cassandra history V2 migration](cassandra-history-v2-migration.md). Recreate a target before, never
after, initializing its layout record: metadata binds the Cassandra table UUID and rejects a recreated generation.

Initialize every layout in `preparing` state:

```bash
tool initialize-schema-layout --layout-name executions --immutable-parameter 16
tool initialize-schema-layout --layout-name history_node --immutable-parameter 0
tool initialize-schema-layout --layout-name history_tree --immutable-parameter 16
tool initialize-schema-layout --layout-name matching_tasks --immutable-parameter 16
tool initialize-schema-layout --layout-name matching_tasks_fair --immutable-parameter 16
tool initialize-schema-layout --layout-name task_queue_user_data --immutable-parameter 64
tool initialize-schema-layout --layout-name legacy_queue --immutable-parameter 4096
tool initialize-schema-layout --layout-name queue_v2_metadata --immutable-parameter 64
tool initialize-schema-layout --layout-name queue_v2_messages --immutable-parameter 4096
```

`initialize-schema-layout` is idempotent only for the same table UUID and immutable parameter. Inspect and archive
each returned generation, authority state, and epoch:

```bash
tool inspect-schema-layout --layout-name executions --immutable-parameter 16
```

Repeat `inspect-schema-layout` for the other names and parameters above. Never infer an epoch: every authority change
uses the exact epoch printed immediately before it.

## Populate and validate source-authoritative targets

Commands with a checkpoint are restartable with that same path. A new full pass requires a new path. Checkpoints bind
the operation, keyspace, source and target table UUIDs, partitioner, and options, so a stale checkpoint fails closed.
Reduce `--page-size` or `--concurrency` if foreground latency rises; the defaults bound memory and fanout and do not
change correctness.

### Executions and history

With executions still in `source-rebuild`, run an initial copy:

```bash
tool backfill-executions-v2 \
  --buckets 16 \
  --checkpoint-file /durable/executions-initial.json
```

Roll every process to `executionMigrationMode: source-dual`, repeat the copy with a new checkpoint, and validate both
directions:

```bash
tool backfill-executions-v2 \
  --buckets 16 \
  --checkpoint-file /durable/executions-final.json
tool validate-executions-v2 \
  --buckets 16 \
  --checkpoint-file /durable/executions-validation.json
```

For a vanilla history-node source, follow the V1 sequence in
[Scylla/Cassandra persistence performance notes](scylla-cassandra-performance.md#history-upgrade-from-v1): initial
backfill, full `legacy-v1-dual` and `legacy-v1-cutover-dual` rollouts, a new final backfill, and
`validate-history-node-v2`. Drain or restart old pagination before changing its read layout.

With history tree in `source-rebuild`, run its initial copy, then roll all processes to `source-dual` and repeat with a
new checkpoint:

```bash
tool backfill-history-tree-v2 \
  --checkpoint-file /durable/history-tree-initial.json
tool backfill-history-tree-v2 \
  --checkpoint-file /durable/history-tree-final.json
```

Roll every process to `historyTreeMigrationMode: target-prepare`. Reads remain on `history_tree`, while writes are
target-first and guarded by the per-tree authority row. Reconcile all token ranges and validate:

```bash
tool reconcile-history-tree-v2 \
  --checkpoint-file /durable/history-tree-reconcile.json
tool validate-history-tree-v2
```

`reconcile-history-tree-v2` seals, exactly repairs, validates, and publishes target authority one tree at a time. It
is the cutover operation for an existing tree; the bulk backfill alone is not sufficient.

### Matching tasks

Roll every process to `matchingTaskMigrationMode: source-dual`, with
`matchingTaskStorageBucketCount: 16`. Backfill classic and fair rows independently:

```bash
tool backfill-matching-tasks-v3 \
  --buckets 16 \
  --range-size 100000 \
  --checkpoint-file /durable/matching-classic.json
tool backfill-matching-tasks-v3 \
  --buckets 16 \
  --range-size 100000 \
  --fair \
  --checkpoint-file /durable/matching-fair.json
```

Build an exhaustive worklist from every `(namespace_id, task_queue_name, task_queue_type)` partition in both `tasks`
and `tasks_v2`. Validate every classic queue and then every fair queue using the values from that worklist:

```bash
tool validate-matching-tasks-v3 \
  --namespace-id "$NAMESPACE_ID" \
  --task-queue "$PHYSICAL_TASK_QUEUE" \
  --task-queue-type "$TASK_QUEUE_TYPE" \
  --buckets 16
tool validate-matching-tasks-v3 \
  --namespace-id "$NAMESPACE_ID" \
  --task-queue "$PHYSICAL_TASK_QUEUE" \
  --task-queue-type "$TASK_QUEUE_TYPE" \
  --buckets 16 \
  --fair
```

Use physical queue names, including generated sticky and versioned queues; a namespace-level or logical queue list is
not exhaustive.

### Task-queue user data

Roll every process to `taskQueueUserDataMigrationMode: source-dual`, with
`taskQueueUserDataBucketCount: 64`, then copy and recover any interrupted V2 transactions:

```bash
tool backfill-task-queue-user-data-v2 \
  --buckets 64 \
  --checkpoint-file /durable/user-data-backfill.json
tool recover-task-queue-user-data-v2 \
  --buckets 64 \
  --checkpoint-file /durable/user-data-recovery.json
```

Build the namespace worklist from every distinct `namespace_id` in `task_queue_user_data`. Supply all of them to the
validator; `--namespace-id` is repeatable:

```bash
tool validate-task-queue-user-data-v2 \
  --namespace-id "$NAMESPACE_ID_1" \
  --namespace-id "$NAMESPACE_ID_2" \
  --buckets 64
```

### Legacy queues and DLQs

Roll every process to `legacyQueueMigrationMode: source-dual`, with
`legacyQueueMessageBucketSize: 4096`. Inventory every distinct `queue_type` in `queue`; the negative DLQ type is a
separate physical queue and must appear as a separate work item. Activate fencing, copy, exactly reconcile, and
validate each observed type:

```bash
tool activate-legacy-queue-v2-source-fence \
  --queue-type "$QUEUE_TYPE" \
  --message-bucket-size 4096 \
  --confirm-source-dual-rollout
tool backfill-legacy-queue-v2 \
  --queue-type "$QUEUE_TYPE" \
  --message-bucket-size 4096
tool reconcile-legacy-queue-v2 \
  --queue-type "$QUEUE_TYPE" \
  --message-bucket-size 4096 \
  --confirm-source-authoritative
tool validate-legacy-queue-v2 \
  --queue-type "$QUEUE_TYPE" \
  --message-bucket-size 4096
```

The explicit source-fence command is mandatory. Once it succeeds, never run a pre-v1.16 legacy-queue writer again.
The source sentinel, target state, every target message bucket, and the target delete-range partition persist the same
per-queue generation and authority. Cutover first seals all of those partitions, performs only partition-local guarded
repairs, activates every target partition, and only then publishes target authority in the source sentinel.

### QueueV2 metadata and messages

Roll every process to `queueV2MigrationMode: source-dual`, with `queueV2MessageBucketSpan: 4096`. Inventory every
`(queue_type, queue_name)` row from `queues`; do not assume the built-in queue types are the complete live inventory.

For every distinct queue type, migrate and validate metadata with separate durable checkpoints:

```bash
tool backfill-queue-v2-metadata \
  --queue-type "$QUEUE_TYPE" \
  --checkpoint-file "/durable/queue-v2-metadata-${QUEUE_TYPE}.json"
tool validate-queue-v2-metadata \
  --queue-type "$QUEUE_TYPE" \
  --checkpoint-file "/durable/queue-v2-metadata-validation-${QUEUE_TYPE}.json"
```

For every physical queue in the worklist, copy, repeat the source-authoritative copy, and validate its message stream:

```bash
tool backfill-queue-v2-messages-v3 \
  --queue-type "$QUEUE_TYPE" \
  --queue-name "$QUEUE_NAME" \
  --message-bucket-span 4096 \
  --checkpoint-file "$QUEUE_CHECKPOINT" \
  --confirm-source-authoritative
tool reconcile-queue-v2-messages-v3 \
  --queue-type "$QUEUE_TYPE" \
  --queue-name "$QUEUE_NAME" \
  --message-bucket-span 4096 \
  --checkpoint-file "$QUEUE_RECONCILE_CHECKPOINT" \
  --confirm-source-authoritative
tool validate-queue-v2-messages-v3 \
  --queue-type "$QUEUE_TYPE" \
  --queue-name "$QUEUE_NAME" \
  --message-bucket-span 4096
```

Checkpoint paths must be unique per queue because checkpoint identity deliberately prevents one file from being reused
for a different stream.

## Publish target-ready

Only publish a family after its entire live inventory completed a stable, bidirectional validation. Immediately inspect
the record, copy the printed epoch, and use it in the compare-and-swap command. For example:

```bash
tool inspect-schema-layout --layout-name executions --immutable-parameter 16
export EXECUTIONS_PREPARING_EPOCH='PASTE_INSPECTED_EPOCH'
tool mark-schema-layout-target-ready \
  --layout-name executions \
  --immutable-parameter 16 \
  --expected-epoch "$EXECUTIONS_PREPARING_EPOCH" \
  --confirm-target-validated
```

Repeat the same exact `inspect-schema-layout` and `mark-schema-layout-target-ready` commands for all layout names in
the table at the start of this runbook, substituting their immutable parameter. QueueV2 must have both
`queue_v2_metadata` and `queue_v2_messages` at `target-ready`; matching must have both `matching_tasks` and
`matching_tasks_fair` at `target-ready`. The command is resumable only when the current record is exactly the one-step
result of the supplied epoch; any other epoch conflict requires a new inspection.

After `history_node` is target-ready, complete its documented rollout to `canonical-dual`. After `history_tree` is
target-ready, roll to `historyTreeMigrationMode: target-dual`.

## Target-dual per-entity cutover

Roll every process to these modes before cutting over the remaining entities:

```yaml
executionMigrationMode: target-dual
executionStorageBuckets: 16
historyNodeMigrationMode: canonical-dual
historyTreeMigrationMode: target-dual
matchingTaskMigrationMode: target-dual
matchingTaskStorageBucketCount: 16
taskQueueUserDataMigrationMode: target-dual
taskQueueUserDataBucketCount: 64
legacyQueueMigrationMode: target-dual
legacyQueueMessageBucketSize: 4096
queueV2MigrationMode: target-dual
queueV2MessageBucketSpan: 4096
```

Wait until no source-mode process remains. `target-dual` resolves persisted per-entity authority, so a process reads
the source before that entity is cut over and reads the target afterward. It maintains the inactive copy, but the
cross-partition mirror is not an atomic commit; always run the fenced reverse reconcile and validation immediately
before an authority rollback.

Executions need no operator cutover command. Each of the four logical history shards seals and exactly reconciles
itself on its next shard ownership/range update. This is independent of the 12 physical Scylla shards in a three-node,
four-shard-per-node deployment.

History trees were already cut over individually by `reconcile-history-tree-v2`; `target-dual` changes their read
layout after the global target-ready barrier.

Cut over every classic and fair matching work item:

```bash
tool cutover-matching-tasks-v3 \
  --namespace-id "$NAMESPACE_ID" \
  --task-queue "$PHYSICAL_TASK_QUEUE" \
  --task-queue-type "$TASK_QUEUE_TYPE" \
  --buckets 16 \
  --confirm-no-pre-fence-matching-writers
tool cutover-matching-tasks-v3 \
  --namespace-id "$NAMESPACE_ID" \
  --task-queue "$PHYSICAL_TASK_QUEUE" \
  --task-queue-type "$TASK_QUEUE_TYPE" \
  --buckets 16 \
  --fair \
  --confirm-no-pre-fence-matching-writers
```

Cut over every user-data namespace:

```bash
tool cutover-task-queue-user-data-v2 \
  --namespace-id "$NAMESPACE_ID" \
  --buckets 64 \
  --confirm-no-source-only-writers
```

Cut over every positive and negative legacy queue type:

```bash
tool cutover-legacy-queue-v2 \
  --queue-type "$QUEUE_TYPE" \
  --message-bucket-size 4096 \
  --confirm-source-dual-rollout
```

This cutover is safe to retry after a crash or a competing invocation. A stale reconciler cannot mutate a partition
after that partition has activated target authority.

Cut over every QueueV2 physical queue. This one command seals and exactly repairs both its metadata and message
layouts, validates the messages, and publishes one target authority identity:

```bash
tool cutover-queue-v2 \
  --queue-type "$QUEUE_TYPE" \
  --queue-name "$QUEUE_NAME" \
  --message-bucket-span 4096 \
  --confirm-source-writers-fenced
```

These commands are idempotent after interruption; rerun the same entity and options. Do not bypass a mismatch or
manually edit a `sealing` row.

## Exhaustive inventory and global target-only barrier

Regenerate all worklists from the source tables after the per-entity pass. This catches entities created while the
initial worklist was being processed. Repeat cutover until two consecutive inventories contain no source-authoritative
entity, and rerun every family validator.

The built-in audits use these source inventories. Run equivalent paged scans when building the operator worklists;
do not load an unbounded result into `cqlsh` or memory:

| Family | Inventory query |
| --- | --- |
| executions | `SELECT DISTINCT shard_id FROM executions` |
| classic matching | `SELECT DISTINCT namespace_id, task_queue_name, task_queue_type FROM tasks` |
| fair matching | `SELECT DISTINCT namespace_id, task_queue_name, task_queue_type FROM tasks_v2` |
| user data | `SELECT DISTINCT namespace_id FROM task_queue_user_data` |
| legacy queue | `SELECT DISTINCT queue_type FROM queue` |
| history tree | `SELECT DISTINCT tree_id FROM history_tree` |
| QueueV2 | `SELECT queue_type, queue_name FROM queues` |

For every history-tree ID, the reserved source row at branch ID
`00000000-0000-0000-0000-000000000000` must have migration authority `3` (target). For every QueueV2 work item, read
the authority columns from its `queues` row and the static authority columns from `queue_messages` partition `0`;
both must be `3`, use the same nonzero migration generation and epoch, and report span `4096`. The corresponding
target metadata and message-directory records must have that same identity. These checks are in addition to comparing
the logical data with the validators.

The final metadata transition performs a bounded source inventory audit for every family. It rejects a missing,
non-target-authoritative, identity-mismatched, or logically different target. History-node and history-tree audits
compare both layouts in both directions; the history-tree audit also requires target authority for every source tree.
Each QueueV2 audit checks both global layout identities and, for every source queue, the source and target metadata,
all four canonical authority records, the target directory and active-bucket state, and all messages in both
directions. For executions the audit also requires the immutable Temporal history-shard count; this deployment must
report exactly shards `1..4`, each with all 16 target partitions.

Inspect each target-ready record to obtain its current epoch, then transition it. The executions command is:

```bash
tool inspect-schema-layout --layout-name executions --immutable-parameter 16
export EXECUTIONS_READY_EPOCH='PASTE_INSPECTED_EPOCH'
tool mark-schema-layout-target-only \
  --layout-name executions \
  --immutable-parameter 16 \
  --expected-epoch "$EXECUTIONS_READY_EPOCH" \
  --expected-execution-shards 4 \
  --confirm-target-only-cutover
```

For each remaining family, use the same command without `--expected-execution-shards`:

```bash
tool mark-schema-layout-target-only --layout-name matching_tasks --immutable-parameter 16 \
  --expected-epoch "$MATCHING_READY_EPOCH" --confirm-target-only-cutover
tool mark-schema-layout-target-only --layout-name matching_tasks_fair --immutable-parameter 16 \
  --expected-epoch "$MATCHING_FAIR_READY_EPOCH" --confirm-target-only-cutover
tool mark-schema-layout-target-only --layout-name task_queue_user_data --immutable-parameter 64 \
  --expected-epoch "$USER_DATA_READY_EPOCH" --confirm-target-only-cutover
tool mark-schema-layout-target-only --layout-name legacy_queue --immutable-parameter 4096 \
  --expected-epoch "$LEGACY_QUEUE_READY_EPOCH" --confirm-target-only-cutover
```

```bash
tool mark-schema-layout-target-only --layout-name history_node --immutable-parameter 0 \
  --expected-epoch "$HISTORY_NODE_READY_EPOCH" --confirm-target-only-cutover
tool mark-schema-layout-target-only --layout-name history_tree --immutable-parameter 16 \
  --expected-epoch "$HISTORY_TREE_READY_EPOCH" --confirm-target-only-cutover
tool mark-schema-layout-target-only --layout-name queue_v2_metadata --immutable-parameter 64 \
  --expected-epoch "$QUEUE_V2_METADATA_READY_EPOCH" --confirm-target-only-cutover
```

The QueueV2 metadata command audits and transitions both QueueV2 records as one crash-resumable operation, always
publishing the message record before the metadata record. Do not issue a separate message-layout transition. A crash
between the two compare-and-swaps leaves a safe intermediate state: `target-dual` can restart and continues using the
paired per-queue authority, while `target-only` refuses to start until both records are target-only. Rerun the same
metadata command and expected epoch to finish the transition.

Do not continue until all nine `inspect-schema-layout` commands report `target-only` with the expected target table
UUID and immutable parameter. Target-dual processes accept both target-ready and target-only global metadata and use
per-entity target authority throughout this rolling window, so publish this global barrier before changing service
configuration.

Now roll every process to the fully migrated configuration:

```yaml
historyNodeMigrationMode: v2-only
historyTreeMigrationMode: target-only
executionMigrationMode: target-only
executionStorageBuckets: 16
queueV2MigrationMode: target-only
queueV2MessageBucketSpan: 4096
legacyQueueMigrationMode: target-only
legacyQueueMessageBucketSize: 4096
matchingTaskMigrationMode: target-only
matchingTaskStorageBucketCount: 16
taskQueueUserDataMigrationMode: target-only
taskQueueUserDataBucketCount: 64
```

After the rollout, these code paths issue no source reads, source writes, mirror writes, or shadow comparisons.

## Rollback windows and cleanup

Before an entity publishes target authority, keep the new binary and return to its source-capable source mode if a
rollout problem is found. Never reintroduce a pre-v1.16 process after source fencing has been activated.

After an entity publishes target authority, leave services in `target-dual` while investigating. It continues serving
the target and mirrors successful mutations to the source. Do not merely change that entity back to a source mode:
the persisted fence correctly rejects such a writer, and a crash between the primary commit and mirror can leave a
repairable gap.

Legacy-queue target-to-source authority rollback is not an online-safe operation and is not exposed by the migration
tool. Treat legacy-queue cutover as one-way. `ReconcileSourceLegacyQueueFromV2` is only a repair primitive: callers
must first fence traffic for that one physical queue, drain in-flight operations, explicitly confirm that the queue is
quiescent, and keep it fenced through reconciliation and the before/after authority validation. It does not authorize
or perform an authority reversal. Do not use it while target-dual traffic is running.

The `target-ready -> target-only` metadata transition is monotonic. Treat it as the end of the online rollback window.
Do not drop source tables immediately: retain backups and the source tables for an operational observation period.
Remove them only in a later reviewed schema version, after every process is confirmed target-only (not merely
target-dual), no older configuration can restart, and source-table read/write counters have remained flat. Never
recreate or truncate a target whose recorded generation is active.

## Fully migrated benchmark proof

Benchmark this branch only after the complete target-only rollout above. Use managed-server mode and the strict flag;
it rejects a missing config file, any non-Cassandra default store, any migration mode that can touch a source layout,
or a non-positive immutable bucket/span:

```bash
./temporalperf \
  --config-file ./config/target-only-benchmark.yaml \
  --server-binary ./temporalperf-server \
  --require-cassandra-target-only \
  --profiles tiny,big \
  --payload-bytes 182,1048576 \
  --trials 3 \
  --output-dir /durable/temporalperf-target-only
```

Keep `suite.json` and require `cassandraTargetOnlyRequired: true`, all sample `server.log` files, the nine
target-only `inspect-schema-layout` outputs, and the inventory/validation artifacts with the result. On a disposable
benchmark keyspace, dropping the source tables before server startup is the strongest negative proof that no hidden
source access remains. Otherwise, capture per-table Scylla read/write counters before and after the suite and require
zero deltas for `executions`, `history_node`, `history_tree`, `tasks`, `tasks_v2`, `task_queue_user_data`, `queue`,
`queues`, and `queue_messages`. The target-only branch result must not include dual-read or dual-write overhead.
Compare it with `fork/scylla-gocql-v1.31.2` at
`cc9d6f11398185e5268205e1b4e01cbd80a7ae74`: that revision retains the stock schema but uses the same Scylla gocql
driver. Backport only the identical harness and use the same cluster, reset policy, four logical history shards, and
matrix; do not enable the target-only flag on a baseline that does not implement these layouts.
