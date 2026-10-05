CREATE TABLE IF NOT EXISTS history_tree_v2
(
    tree_id         uuid,
    branch_bucket   int,
    branch_id       uuid,
    branch          blob,
    branch_encoding text,
    PRIMARY KEY ((tree_id, branch_bucket), branch_id)
) WITH CLUSTERING ORDER BY (branch_id ASC)
  AND COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };
