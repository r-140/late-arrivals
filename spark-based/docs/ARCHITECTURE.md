# Classical ETL architecture
## Source and extraction
`entities` stores logical orders, customers and historical customer attributes. The trigger writes JSON change records in the same PostgreSQL transaction, including the previous payload on delete. This is a compact source integration example; real deployments could expose an API returning these records.

Extract claims committed, unassigned queue rows with FOR UPDATE SKIP LOCKED and stores exact membership in a durable batch. No max(event_id) predicate is used. Lower sequence IDs that commit later remain unassigned and are collected next time. One local pipeline mutex serializes the whole run; queue row locks protect membership. This mutex is not a distributed coordination protocol.

The exact batch is serialized as JSON Lines with deterministic keys, written via temporary file, fsync, rename, and SHA-256. A failed extraction can recreate the same membership. JSON replay verifies checksum. Unclaimed records arriving during transformation remain for the next batch.

## Transform in Python/PySpark
An explicit JSON schema parses fields, including Decimal(18,2) money. `events` Delta MERGE inserts only unseen event IDs. Full event history is resolved by highest application revision and then source event ID. Producer revision is the ordering contract; source event ID is only a tie breaker, not a global commit checkpoint.

SCD2 customer history is canonicalized by customer/effective date. Spark lead creates half-open intervals [valid_from,valid_to). A SHA-256 of customer and effective date creates a stable surrogate key. One inferred unknown member allows unresolved facts to remain in the star; subsequent runs relink them.

All known orders are reconstructed, including deleted tombstones. A late dimension or backdated interval can change an existing fact without another order event. Deleted/cancelled facts are excluded from revenue. All groups are recomputed, so a segment reassignment removes the old aggregate contribution.

Each output is a Delta table (`dim_customer`, `fact_order`, `daily_revenue`) with Parquet data and _delta_log. Its version is stored in the batch manifest. Separate Delta table commits are not one multi-table transaction; recording the manifest only after all writes succeed gives the loader a completed snapshot set. If transform fails before the manifest, it reruns using idempotent raw MERGE.

## Load and publication
Loader reads each table at its recorded versionAsOf. It streams rows through the driver in bounded blocks into a fresh generation in ClickHouse. It validates key uniqueness, aggregate totals and dimension foreign keys before inserting one publication pointer.

Public star views choose the most recently published batch's generation. Dimensions/facts from incomplete generations remain invisible. After a crash before publication, retry creates another staging generation. After a crash after publication but before PostgreSQL acknowledgment, retry recognizes the batch in ClickHouse and marks it loaded without republishing.

Queries spanning separate statements can straddle a publication; this does not provide a session snapshot across multiple queries. Orphan and obsolete generations deliberately remain for forensic inspection; add retention/garbage collection for long-running use.

## Failure states
claimed → extracted → transformed → loaded. Error text does not erase the last completed stage. Recovery prioritizes oldest incomplete batch. Delta versions and JSON files must be retained until all pending loads and replays are complete; VACUUM is not run.

## Limitations and scaling
Full-history Spark windows, snapshot Delta overwrite and full warehouse generations cost more than affected-key reconciliation. No distributed Spark cluster, source API auth, multi-worker coordination, schema evolution governance or production retention policy. hourly polling controls latency; no Structured Streaming event-time watermark is configured.

References: https://docs.delta.io/releases/ ; https://docs.delta.io/latest/delta-update.html ; https://spark.apache.org/docs/3.5.5/sql-data-sources-json.html
