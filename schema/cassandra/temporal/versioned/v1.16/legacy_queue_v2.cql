ALTER TABLE queue ADD migration_authority int;
ALTER TABLE queue ADD migration_generation uuid;
ALTER TABLE queue ADD message_bucket_size bigint;

CREATE TABLE IF NOT EXISTS legacy_queue_v2_state (
  queue_type         int,
  active_bucket      bigint,
  minimum_message_id bigint,
  cleanup_message_id bigint,
  version            bigint,
  migration_authority int,
  migration_generation uuid,
  message_bucket_size bigint,
  PRIMARY KEY (queue_type)
) WITH COMPACTION = {
  'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};

CREATE TABLE IF NOT EXISTS legacy_queue_v2_messages (
  queue_type       int,
  bucket_id        bigint,
  row_type         smallint,
  message_id       bigint,
  message_payload  blob,
  message_encoding text,
  last_message_id  bigint,
  version          bigint,
  migration_authority int static,
  migration_generation uuid static,
  message_bucket_size bigint static,
  PRIMARY KEY ((queue_type, bucket_id), row_type, message_id)
) WITH CLUSTERING ORDER BY (row_type ASC, message_id ASC)
  AND COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TABLE IF NOT EXISTS legacy_queue_v2_delete_ranges (
  queue_type       int,
  deletion_id      text,
  first_message_id bigint,
  last_message_id  bigint,
  migration_authority int static,
  migration_generation uuid static,
  message_bucket_size bigint static,
  PRIMARY KEY ((queue_type), deletion_id)
) WITH COMPACTION = {
  'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};
