-- Matching tasks rotate across storage buckets when range ownership changes.
-- The metadata row in the active bucket fences task creation in the same LWT partition.
CREATE TABLE IF NOT EXISTS tasks_v3 (
  namespace_id             uuid,
  task_queue_name          text,
  task_queue_type          int,
  storage_bucket           smallint,
  type                     int,
  task_id                  bigint,
  range_id                 bigint,
  metadata_state           int,
  bucket_count             smallint,
  migration_authority      int,
  next_range_id            bigint,
  next_task_queue          blob,
  next_task_queue_encoding text,
  task                     blob,
  task_encoding            text,
  task_queue               blob,
  task_queue_encoding      text,
  PRIMARY KEY ((namespace_id, task_queue_name, task_queue_type, storage_bucket), type, task_id)
) WITH CLUSTERING ORDER BY (type ASC, task_id ASC)
  AND COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TABLE IF NOT EXISTS tasks_v3_fair (
  namespace_id             uuid,
  task_queue_name          text,
  task_queue_type          int,
  storage_bucket           smallint,
  type                     int,
  pass                     bigint,
  task_id                  bigint,
  range_id                 bigint,
  metadata_state           int,
  bucket_count             smallint,
  migration_authority      int,
  next_range_id            bigint,
  next_task_queue          blob,
  next_task_queue_encoding text,
  task                     blob,
  task_encoding            text,
  task_queue               blob,
  task_queue_encoding      text,
  PRIMARY KEY ((namespace_id, task_queue_name, task_queue_type, storage_bucket), type, pass, task_id)
) WITH CLUSTERING ORDER BY (type ASC, pass ASC, task_id ASC)
  AND COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };
