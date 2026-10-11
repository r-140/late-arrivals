# Grafana interpretation
Dashboards and both data sources are provisioned automatically. Grafana installs the official ClickHouse plugin on startup; internet access is required for that installation. An unavailable plugin prevents SQL panels from working.

## Pipeline Health
- Consumer reachable: 1 means Prometheus can scrape worker, not that end-to-end CDC is healthy.
- Kafka lag: source topic high watermark minus consumer committed position. During hold it rises; release should drain it. This demo has one partition.
- Held events: in-memory withheld changes. Expect 1 for cdc-delay and 2 for out-of-order.
- Delivered events: cumulative attempts since worker process start, including replay.
- Seconds since delivery: idle streams naturally increase this; it is not itself an SLA breach.
- P95 delivery delay: source change to insertion, computed over five minutes; low sample counts can yield no data.
- Latest dbt success/duration/age: read from the most recent shared `run_results.json`, including Airflow builds. Before any build values are zero. A process failure before writing an artifact can leave a previous successful result; use Airflow task state/logs to diagnose that case. These are not a complete Airflow metrics exporter or dbt failure-rate history.

## Late Arrivals & Corrections
SQL panels query current ClickHouse results. Unknown customer and historical segment drop after missing-dimension correction. Revenue changes after late facts, updates and cancellation. Replay deliveries is Bronze count minus unique event IDs. Cancelled/deleted counts increase in late-delete. Business event age shows the oldest order, NOT CDC transport delay.

Dashboard sums accumulate when multiple demos run. Use `./bin/lab inspect NAME` for case-specific evidence. A held CDC update leaves business totals unchanged until release and build. A dbt delay leaves Gold unchanged despite fresh Bronze. During a build tables may briefly be replaced: refresh after completion.

Not yet implemented: per-model SLA alerting, historical correction queue, dbt error-rate timeseries, broker JVM exporter and full Airflow scheduler telemetry. Do not interpret the provided panels as those metrics.
