-- Stores task queue user data and build ID mappings in bounded namespace buckets.
-- pending_* columns implement atomic updates that span more than one bucket.
ALTER TABLE task_queue_user_data ADD migration_authority int static;
ALTER TABLE task_queue_user_data ADD migration_bucket_count smallint static;
ALTER TABLE task_queue_user_data ADD migration_generation uuid static;

CREATE TABLE IF NOT EXISTS task_queue_user_data_v2 (
  namespace_id          uuid,
  bucket_id             smallint,
  build_id              text,
  task_queue_name       text,
  data                  blob,
  data_encoding         text,
  version               bigint,
  present               boolean,
  pending_txn_id        text,
  pending_data          blob,
  pending_data_encoding text,
  pending_version       bigint,
  pending_present       boolean,
  PRIMARY KEY ((namespace_id, bucket_id), build_id, task_queue_name)
) WITH COMPACTION = {
  'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};

-- One independently distributed coordinator partition per multi-bucket update.
-- Rows are retained until no participant references them, allowing deterministic
-- recovery after an arbitrarily long process or cluster outage.
CREATE TABLE IF NOT EXISTS task_queue_user_data_v2_txn (
  namespace_id          uuid,
  txn_id                text,
  state                 text,
  expires_at            timestamp,
  active_txn_id         text,
  epoch                 bigint,
  migration_authority   int,
  migration_bucket_count smallint,
  migration_generation uuid,
  PRIMARY KEY ((namespace_id), txn_id)
) WITH COMPACTION = {
  'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};
