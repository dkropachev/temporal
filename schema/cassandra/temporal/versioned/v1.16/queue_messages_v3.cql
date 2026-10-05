ALTER TABLE queue_messages ADD migration_authority int static;
ALTER TABLE queue_messages ADD migration_generation uuid static;
ALTER TABLE queue_messages ADD migration_epoch bigint static;
ALTER TABLE queue_messages ADD bucket_span bigint static;

CREATE TABLE IF NOT EXISTS queue_messages_v3
(
    queue_type            int,
    queue_name            text,
    message_bucket        bigint,
    row_type              smallint,
    message_id            bigint,
    message_payload       blob,
    message_encoding      text,
    active_message_bucket bigint,
    bucket_span           bigint static,
    migration_authority   int static,
    migration_generation  uuid static,
    migration_epoch       bigint static,
    last_message_id       bigint,
    version               bigint,
    PRIMARY KEY ((queue_type, queue_name, message_bucket), row_type, message_id)
) WITH CLUSTERING ORDER BY (row_type ASC, message_id ASC)
   AND COMPACTION = {
       'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
       };
