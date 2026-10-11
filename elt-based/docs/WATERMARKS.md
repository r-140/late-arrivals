# Watermark columns and late CDC arrivals
## What this implementation actually does
This lab currently uses NO incremental watermark column. silver_state reads the complete Bronze event log on every build. gold_orders is materialized as incremental, but its SELECT emits every known order key on every run. Therefore neither source updated_at nor ingested_at can exclude an old event here.

This is a correctness-first reference implementation, not an optimized append filter. Incremental materialization and incremental input selection are separate decisions.

## Which clock means what?
| Field | Meaning | Suitable as sole input watermark? |
| --- | --- | --- |
| data.event_time | Business occurrence time | No: a week-old order can arrive today |
| source_ms / source updated_at | Time source change occurred | No for unbounded CDC delivery delay; also commit timing/precision matters |
| ingested_at | ClickHouse Bronze arrival time | Better for detecting late arrivals, but not sufficient alone without safe batch boundaries and dedup |
| Kafka topic, partition, offset | Immutable transport event identity and per-partition ordering | Useful for per-partition checkpoints; preserve membership/ack protocol |
| revision | Per-entity application version | Resolves entity state; not a global ingestion watermark |

## updated_at is not categorically forbidden
The name does not determine semantics. A warehouse-managed updated_at that genuinely means processing/ingestion time may be appropriate with a sound checkpoint protocol. A source updated_at is dangerous as a Bronze incremental filter when a source change dated 10:00 reaches Bronze at 14:00 after the cutoff advanced beyond 10:00.

A simple `source_updated_at > max(source_updated_at)` loses that record. A simple `ingested_at > max(ingested_at)` also needs care: timestamps can collide, ingestion can overlap a build, and an old record arriving while the build reads can be excluded if a checkpoint acknowledges rows the build never saw. Do not claim an arbitrary ingestion sequence is automatically commit ordered.

A bounded lookback plus dedup is valid only under a documented lateness/retention guarantee. For arbitrary delayed delivery use durable batch membership or checkpoints per Kafka partition with an exact processed range, advancing acknowledgment only after successful publication. For production capture a stable Bronze batch or manifest.

## Dimension changes need propagation
Detecting incoming events is only the first step. A newly arrived customer or retroactive SCD2 change must add affected order keys to reconciliation even if no order row changed. Revenue reconciliation must cover both old and new day/segment groups. This lab handles that by scanning all history and re-emitting all known keys.

## Code
- [Consumer: event identity, source time and insert-before-commit](../app/consumer.py)
- [Bronze ingested_at](../infra/bronze.sql)
- [Full-history Silver resolver](../dbt/models/silver_state.sql)
- [Full-key Gold reconciliation](../dbt/models/gold_orders.sql)
- [Business-time filter deliberately demonstrating failure](../dbt/models/naive_recent_orders.sql)

The sibling Spark ETL variant uses source change_queue.batch_no membership instead of a scalar updated_at watermark; its own WATERMARKS guide also distinguishes batch extraction checkpoints from Structured Streaming watermarks.
