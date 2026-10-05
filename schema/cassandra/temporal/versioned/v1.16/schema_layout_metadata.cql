CREATE TABLE IF NOT EXISTS schema_layout_metadata (
  layout_name         text,
  generation          uuid,
  layout_version      int,
  immutable_parameter bigint,
  authority_state     text,
  epoch               bigint,
  updated_at          timestamp,
  PRIMARY KEY (layout_name)
) WITH COMPACTION = {
  'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};
