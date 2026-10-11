# Start here
## Requirements
Docker + Compose v2 on Linux/macOS, curl, about 8 GB free RAM and 10 GB disk. Python source is edited in the same IDE as the ELT version. Docker builds need PyPI and Maven Central access: Spark and Delta versions are pinned and Delta JARs are cached during build. JDK 17 is installed in the worker image.

```bash
chmod +x bin/lab bin/acceptance
./bin/lab up
./bin/lab doctor
./bin/lab demo late-fact
```
`up` waits for PostgreSQL and ClickHouse readiness via the worker. No Spark job runs until a demo or run command. Each Spark session runs local[2], two shuffle partitions, and stops after the batch. First builds take longer than dbt builds.

## Interfaces
- Grafana: http://localhost:23000 — admin / lab.
- Airflow: http://localhost:28080 — admin; `./bin/lab password` prints standalone password. If empty, use `docker compose -p spark-late-arrivals-lab exec airflow airflow users reset-password --username admin`.
- Worker health: http://localhost:28090/health.
- Prometheus: http://localhost:29090.
- Spark UI: http://localhost:24040 — only while a Spark session is running.
- PostgreSQL: localhost:25432, lab/lab/lab.
- ClickHouse HTTP: localhost:28123, lab/lab.

Copy `.env.example` to `.env` to customize only host ports. Internal addresses stay fixed. All ports bind localhost.

## Manual extraction and recovery
```bash
./bin/lab run
./bin/lab batches
./bin/lab json 1
./bin/lab delta fact_order
./bin/lab delta dim_customer
./bin/lab files
```
There are no new changes? run returns without creating another batch. `json` takes the batch number printed by `batches`; do not assume a particular number after running multiple cases.

## Orchestration
`./bin/lab trigger` unpauses the hourly DAG and creates a manual run. Use `./bin/lab pause` after verification; scheduled runs can consume changes before manual demos observe them. Airflow calls the worker HTTP API and prints its job output. Spark/dbt dependencies never enter Airflow's Python environment.

## Full acceptance
On a fresh lab, `./bin/acceptance` runs real Spark/Delta tests, all eleven scenarios and triggers Airflow. This may take several minutes. Cases can run once per reset. `reset` deletes this variant's PostgreSQL, ClickHouse, Delta/JSON and Airflow volumes. The ELT stack is unaffected. `down` preserves data.
