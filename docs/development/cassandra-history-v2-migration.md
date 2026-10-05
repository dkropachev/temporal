# Cassandra history V2 migration

`historyNodeMigrationMode` and `historyTreeMigrationMode` are independent. This permits an already migrated
`history_node_v2` deployment to migrate `history_tree` without changing history-node routing.

## History tree migration

1. Apply schema version `v1.16` while `historyTreeMigrationMode` is unset or `source-only`.
2. Deploy `source-rebuild`, then run `recreate-history-tree-v2 --confirm-source-rebuild` on one operator host.
3. Run `backfill-history-tree-v2` with a new durable checkpoint path.
4. Deploy `source-dual`, then repeat the forward backfill with another checkpoint path. This mode initializes a
   source-authority marker in every active tree and fences each source mutation with it.
5. Deploy `target-prepare` everywhere. Reads remain on `history_tree`, but every data mutation reserves a monotonic
   timestamp and writes `history_tree_v2` first.
6. Run `reconcile-history-tree-v2` with a new durable checkpoint path. It seals one tree at a time, resets all 16
   target partitions below a reserved repair timestamp, rewrites the source rows, validates them, and publishes
   target authority. Reads continue during repair; a mutation racing the short per-tree seal is rejected and retried.
7. Run `validate-history-tree-v2`. Do not mark the global history-tree layout target-ready unless reconciliation and
   validation both finish successfully.
8. Deploy `target-dual`. Reads use `history_tree_v2`; writes still maintain `history_tree` for rollback.
9. After the rollback window, validate again, mark the layout target-only, and deploy `target-only`.

`target-only` does not read or write `history_tree`. It also rejects legacy raw scan tokens; callers must restart an
unfinished `GetAllHistoryTreeBranches` pagination. The old table may be dropped in a later schema version.

Rollback from `target-dual` requires `reconcile-history-tree-v1` with a new checkpoint, validation, then a rollout to
`source-dual`. Reverse reconciliation uses the same source-table authority marker, including for an empty target tree,
and removes legacy extras and wrong-bucket target duplicates before restoring source authority.

The insert-only `backfill-history-tree-v2` and `backfill-history-tree-v1` commands are bulk-copy accelerators. They do
not establish cutover safety: only the fenced reconciliation commands remove stale rows and publish per-tree
authority. A crashed reconciliation can be restarted with the same checkpoint; a tree left in `sealing` reuses its
reserved repair timestamp.

## Fully migrated benchmarks

Run performance tests only with both settings below on every Temporal process:

```yaml
historyNodeMigrationMode: v2-only
historyTreeMigrationMode: target-only
```

Apply the complete schema and finish both validators before starting measurements. These modes issue no legacy
history data reads, mirror writes, or migration comparisons during the benchmark.
