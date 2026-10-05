CREATE TABLE IF NOT EXISTS history_node_v2
(
    tree_id       uuid,
    branch_id     uuid,
    node_id       bigint,
    txn_id        bigint,
    prev_txn_id   bigint,
    data          blob,
    data_encoding text,
    PRIMARY KEY ((tree_id, branch_id), node_id, txn_id)
) WITH CLUSTERING ORDER BY (node_id ASC, txn_id DESC)
  AND COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };
