# Architecture and correctness
PostgreSQL stores `entities(kind,id,revision,data)` to keep the CDC transport compact. `data` contains typed business attributes; customer_history is a separate logical entity kind. There is no FK so the missing-dimension case can be created intentionally.

Debezium publishes `lab.public.entities`. The consumer preserves Kafka topic/partition/offset identity, source LSN/time, operation and application revision. Insert is synchronous; Kafka offsets commit only after successful ClickHouse insertion. Crash between insert and commit produces duplicates, which Silver resolves. This is at-least-once delivery.

## Three clocks and ordering
- `data.event_time`: when the order happened.
- `source_ms`: when PostgreSQL changed, as reported by Debezium.
- `ingested_at`: when Bronze received the record.
- `data.effective_from`: business validity of a historical segment, distinct from all three clocks.

Application revision is monotonic per entity and takes priority in Silver; LSN and Kafka offset break ties, including a delete with the same revision as its previous value. This is an explicit producer contract, not a claim that arrival timestamps can order changes. Kafka preserves partition ordering. The out-of-order demo deliberately reverses delivery AFTER consumption to simulate a downstream writer that reorders records.

## Layers
Bronze MergeTree retains all delivery attempts. It is not ReplacingMergeTree; correctness does not depend on background merges or FINAL. Silver uses row_number over full history and retains the latest tombstone. Customer history resolves revisions per history record, canonicalizes equal effective dates deterministically, and derives half-open validity intervals [from,to).

Gold orders uses dbt-clickhouse's legacy incremental replacement strategy and emits ALL known order keys each run. This refreshes facts whose dimensions or historical intervals changed without a new order event. Cancelled/deleted orders remain explicit rows so an incremental merge cannot silently preserve an old active fact. Revenue is rebuilt from active paid orders; old days and removed groups are corrected.

The deliberately naive model applies a two-day business-time filter and misses late facts. It is an illustration, not the reference result.

## Scale and failure boundaries
Full-history scans and full-key replacement are expensive. A next version can persist successful ingestion checkpoints and affected entity/date sets, include dimension changes, and recalculate both old and new aggregate groups. Do not switch to a fixed event-time lookback alone.

Builds are not an atomic multi-table transaction. Consumers can ingest during a build and dashboards can see intermediate model states. Demo scripts wait for target events before starting a build and require no concurrent writer or scheduled DAG. For production use a bounded Bronze batch and publish a completed Gold generation.

Held buffers are in memory, bounded only by local demo workload. Restart replays uncommitted data; do not hold a production stream this way. One partition and one writer are intended. Airflow max_active_runs=1 prevents overlapping DAG runs, but direct CLI builds must also be serialized.

## References
- https://debezium.io/documentation/reference/3.0/connectors/postgresql.html
- https://github.com/ClickHouse/dbt-clickhouse
- https://clickhouse.com/docs/integrations/dbt
