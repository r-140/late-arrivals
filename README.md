# Late Arrivals — Python Labs
Two independent runnable implementations in ONE IDE project:

| | elt-based | spark-based |
| --- | --- | --- |
| Processing | Python CDC consumer + dbt SQL | Python / PySpark batch ETL |
| Source | PostgreSQL → Debezium/Kafka | PostgreSQL change queue polled hourly |
| Durable inputs | ClickHouse Bronze journal | Immutable JSON batches |
| Transform storage | ClickHouse Silver/Gold | Delta Lake: Parquet + transaction log |
| Final analytics | ClickHouse facts/aggregates | ClickHouse dim_customer / fact_order / daily_revenue |
| Checkpoint | Full-history reads; no input watermark | Durable change membership + batch stage acknowledgment |
| Orchestration | Airflow medallion, every 5 min | Airflow spark_etl, hourly |
| Grafana | localhost:13000 | localhost:23000 |
| Airflow | localhost:18080 | localhost:28080 |

Open this root in your IDE; Spark jobs and controllers are Python, dbt models remain SQL. Compose project names, volumes and default host ports are separate. Neither variant resets the other's data.

## ELT
```bash
cd elt-based
./bin/lab up
./bin/lab demo late-fact
```
Read [ELT start](elt-based/docs/START_HERE.md), [watermark clarification](elt-based/docs/WATERMARKS.md), [ELT architecture](elt-based/docs/ARCHITECTURE.md).

## Spark ETL
```bash
cd spark-based
./bin/lab up
./bin/lab demo late-fact
```
Read [Spark start](spark-based/docs/START_HERE.md), [eleven demos](spark-based/docs/DEMO_INDEX.md), [recovery protocol](spark-based/docs/ARCHITECTURE.md), [batch watermarks](spark-based/docs/WATERMARKS.md).

If your previous ELT checkout already has running volumes, the same elt-based wrapper retains Compose project late-arrivals-lab and therefore reuses those volumes. Do not reseed existing cases; inspect/verify them. The Spark version starts under a different project name.

## Validation
ELT demo paths, Airflow and Grafana were verified on the user's local Docker environment. Spark implementation includes offline extractor tests and real Spark/Delta integration tests plus acceptance demos, but its Docker stack has not been executed in the artifact creation environment. See the per-variant VALIDATION documents.
