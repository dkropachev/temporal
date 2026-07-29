# Pre-existing Cassandra and Scylla Issues

These issues were verified against merge base
`cc9d6f11398185e5268205e1b4e01cbd80a7ae74`. They are not regressions from
this branch and are intentionally not fixed in this PR.

## Legacy queue hot partition and conditional tail writes

The legacy `queue` table partitions every message of a queue type together:
`PRIMARY KEY (queue_type, message_id)`. Enqueue reads the maximum message ID and
then inserts the next ID with `IF NOT EXISTS`.

This creates an ever-growing partition and concentrates reads, writes, and LWT
contention at its tail. A safe redesign needs bucket-aware reads and a persisted
delete cursor. Reserving ID ranges alone is insufficient because independent
writers can commit ranges out of order or leave permanent gaps after a crash.

Relevant code:

- `schema/cassandra/temporal/schema.cql`
- `common/persistence/cassandra/queue_store.go`

## QueueV2 list-by-type scan

The `queues` table uses `((queue_type, queue_name))` as its partition key, while
`ListQueues` filters only by `queue_type`. Cassandra and Scylla therefore require
`ALLOW FILTERING`, which scans table partitions instead of performing a bounded
partition read.

A scalable replacement needs a separately partitioned list/index table and a
migration that keeps it consistent with queue creation and metadata updates.

Relevant code:

- `schema/cassandra/temporal/versioned/v1.9/queues.cql`
- `common/persistence/cassandra/queue_v2_store.go`

## Namespace-wide task queue user-data partition

`task_queue_user_data` stores all task-queue user data and build-ID mappings for
a namespace in one partition:
`PRIMARY KEY ((namespace_id), build_id, task_queue_name)`. Large namespaces can
therefore approach Cassandra partition limits and concentrate CAS traffic.

This PR bounds the build-ID limit check, but it does not change the table
topology. Splitting the partition requires redesigning the conditional batch
that atomically updates task-queue data and its build-ID mappings.

Relevant code:

- `schema/cassandra/temporal/versioned/v1.8/task_queue_user_data.cql`
- `common/persistence/cassandra/matching_task_store_user_data.go`

## Existing clusters with low history-shard counts

The `executions` table partitions mutable state and history tasks by
`shard_id`. A low shard count creates larger, hotter partitions and limits
parallelism. This branch raises the Cassandra default for new clusters, but an
existing cluster's shard count is persisted in cluster metadata and cannot be
changed by configuration.

Existing low-shard clusters need a supported online resharding/data-migration
design to receive the same partition-size and throughput benefits.

Relevant code:

- `schema/cassandra/temporal/schema.cql`
- `temporal/fx.go`

## Rollback-compatible V1 history copy

The V1 `history_node` table partitions all branches in a tree together. The new
`history_node_v2` layout removes that read-side large-partition pattern, but
canonical dual mode still writes V1 as a rollback copy.

After the rollback window closes and both tables have been validated, operators
can move to `v2-only` to stop paying the V1 mirror-write and storage cost.
Dropping the V1 table should be handled by a separate, explicit retirement
procedure.

Relevant code:

- `schema/cassandra/temporal/schema.cql`
- `common/persistence/cassandra/history_store.go`
