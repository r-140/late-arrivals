# Grafana monitoring
Dashboard: http://localhost:23000/d/spark-etl-late-arrivals — admin/lab. All business gauges query the currently published ClickHouse star; PostgreSQL gauges query batch/queue state. Prometheus scrapes the worker HTTP endpoint. Units and neutral colors are explicit for informational metrics.

| Panel | Interpretation |
| --- | --- |
| Pending source changes | Visible committed queue rows not assigned to a batch; rises after inject, falls at extraction |
| Incomplete batches | Claimed/extracted/transformed batches awaiting final load acknowledgment |
| Failed batches | Incomplete attempts retaining an error; run clears the error and resumes |
| Extract/Transform/Load seconds | Stage durations from latest batch; not p95 or full history |
| Unresolved customers | Active facts still missing current customer |
| Unknown historical segment | Active facts with no matching valid historical member |
| Published revenue | Sum of active paid facts; stays old during failed staging |
| Deleted orders | Tombstones retained in published fact table |
| Published order count | Includes retained cancelled/deleted rows |
| Seconds since publication | Age of last acknowledged completed batch; grows on idle source too |

A metrics scrape returning 503 means a store query failed, not zero events. Prometheus up exposes that distinction. Failed stages can leave prior duration values intact; inspect batches/error and Airflow logs.

Observe each demo using seed/inject/run manually: the automatic demo intentionally moves quickly. All case IDs are isolated but dashboards accumulate. This version does not implement per-model historical SLA alerts or full Spark executor telemetry.
