ALTER TABLE queues ADD migration_authority int;
ALTER TABLE queues ADD migration_generation uuid;
ALTER TABLE queues ADD migration_epoch bigint;
ALTER TABLE queues ADD message_bucket_span bigint;

CREATE TABLE IF NOT EXISTS queues_v2
(
    queue_type        int,
    metadata_bucket   int,
    queue_name        text,
    metadata_payload  blob,
    metadata_encoding text,
    version           bigint,
    migration_authority int,
    migration_generation uuid,
    migration_epoch bigint,
    message_bucket_span bigint,
    PRIMARY KEY ((queue_type, metadata_bucket), queue_name)
) WITH CLUSTERING ORDER BY (queue_name ASC)
   AND COMPACTION = {
       'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
       };
