# Late Arrivals Lab
PostgreSQL → Debezium → Kafka → ClickHouse Bronze → dbt Silver/Gold, orchestrated by Airflow and observed in Grafana.

Start at [START_HERE](docs/START_HERE.md). Returning after a break? Read [RETURNING_TO_PROJECT](docs/RETURNING_TO_PROJECT.md).

## Eight reproducible cases
- [late-fact](docs/demos/late-fact.md)
- [cdc-delay](docs/demos/cdc-delay.md)
- [missing-dimension](docs/demos/missing-dimension.md)
- [out-of-order](docs/demos/out-of-order.md)
- [late-scd2](docs/demos/late-scd2.md)
- [dbt-delay](docs/demos/dbt-delay.md)
- [replay](docs/demos/replay.md)
- [late-delete](docs/demos/late-delete.md)

## Scope and validation
This is an educational local stack. Full-history Silver and full-key incremental Gold deliberately prioritize correctness over scale. It does not implement production affected-key queues, Kafka transactional exactly-once, or a distributed checkpoint protocol.

Versions are pinned baselines, not a claim to use the latest releases. Docker integration has NOT been executed in the artifact creation environment (Docker unavailable). See [VALIDATION](docs/VALIDATION.md) for completed checks and the required local acceptance run.

## Key files
- [Consumer](app/consumer.py), [scenario controller](app/scenarios.py)
- [Silver state](dbt/models/silver_state.sql), [SCD2](dbt/models/silver_customer_history.sql)
- [Incremental facts](dbt/models/gold_orders.sql), [aggregates](dbt/models/gold_revenue.sql)
- [Airflow DAG](airflow/dags/medallion.py)
- [Architecture](docs/ARCHITECTURE.md), [Grafana guide](docs/GRAFANA.md)
