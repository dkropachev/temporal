ALTER TABLE tasks ADD migration_authority int;
ALTER TABLE tasks ADD migration_bucket_count smallint;
ALTER TABLE tasks ADD migration_timestamp bigint;
ALTER TABLE tasks_v2 ADD migration_authority int;
ALTER TABLE tasks_v2 ADD migration_bucket_count smallint;
ALTER TABLE tasks_v2 ADD migration_timestamp bigint;
