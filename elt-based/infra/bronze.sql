CREATE TABLE IF NOT EXISTS lab.bronze_events (
 event_id String, kind String, id String, revision UInt64,
 op String, data String, source_ms UInt64, source_lsn UInt64,
 topic String, partition_id Int32, offset_id Int64,
 ingested_at DateTime64(3,'UTC') DEFAULT now64(3)
) ENGINE=MergeTree ORDER BY (kind,id,revision,event_id);
