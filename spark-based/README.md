# Spark ETL Late Arrivals Lab
Python/PySpark batch ETL: PostgreSQL → immutable JSON → Spark + Delta Lake → ClickHouse star schema. Airflow runs hourly; Grafana observes batches and published results.

Read [START_HERE](docs/START_HERE.md) and [RETURNING_TO_PROJECT](docs/RETURNING_TO_PROJECT.md). All scenarios have manual steps and automatic assertions in [DEMO_INDEX](docs/DEMO_INDEX.md).

## Important implementation decisions
- Trigger-captured source changes, including deletes, are extracted once per batch. This is batch polling; no Kafka or Debezium is required in this variant.
- Batch membership, not max(updated_at) or max(sequence), is the extraction checkpoint.
- Delta stores immutable event identities and transformed snapshots. It is Parquet plus transaction logs, not plain Parquet.
- Spark builds dimension validity intervals, surrogate keys and facts; ClickHouse receives the final star.
- Failed loads stage a new generation. One publication pointer makes the completed generation visible; recovery after publish recognizes an already published batch.
- Full-history transformation and full-snapshot loading intentionally prioritize correctness. This is a local educational reference, not a scalable incremental implementation.

Code: [Extract](app/extract.py), [Transform](app/transform.py), [Load](app/load.py), [recovery orchestration](app/pipeline.py), [scenarios](app/scenarios.py), [Airflow](airflow/dags/spark_etl.py).

See [ARCHITECTURE](docs/ARCHITECTURE.md), [WATERMARKS](docs/WATERMARKS.md), [GRAFANA](docs/GRAFANA.md), [VALIDATION](docs/VALIDATION.md).
