# Validation record
## Completed in artifact creation environment
- Python sources compile.
- YAML configuration parses; dashboard and connector JSON parse.
- Bash command wrapper syntax passes.
- Relative documentation links resolve.
- Consumer envelope normalization unit tests pass (schemas enabled/disabled, delete, JSON payload, Kafka identity).

## Not executed here
Docker is unavailable. Image pulls/builds, PostgreSQL replication, Kafka delivery, ClickHouse SQL compilation, dbt adapter integration, Airflow task execution and Grafana plugin rendering must be validated locally. This artifact is an implementation candidate, not an integration-certified release.

## Local acceptance
Run `./bin/lab up`, `doctor`, and all eight `demo` commands from START_HERE. Each invokes dbt tests plus case assertions and two builds. Then trigger Airflow, confirm task success and dashboard values. Save any failing SQL/stack trace before modifying dependencies.

## Repeat offline checks
```bash
python3 -m unittest discover -s tests -v
bash -n bin/lab
python3 -m compileall -q app airflow
```
For an unused stack, `./bin/acceptance` automates all eight demos and triggers Airflow. It refuses to silently reuse previously seeded case IDs.
