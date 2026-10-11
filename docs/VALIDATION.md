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

## Airflow dependency isolation fix
The original image installed worker/dbt requirements into Airflow's own environment and failed dependency resolution. The revised Dockerfile installs only dbt requirements into `/opt/dbt-venv`; the DAG invokes its dbt executable explicitly. The base Airflow environment is not modified. Docker build runs pip check and dbt --version in the isolated environment. This image build still requires local validation.

Dependency resolution dry-runs passed for both worker and isolated dbt requirements on Python 3.12. The images use Python 3.11; their build-time pip check and dbt --version remain the authoritative checks for that runtime. dbt-adapters/common versions are pinned to the successfully resolved pair to avoid extensive resolver backtracking.

## User-side build confirmation and host port fix
User log confirms both images build, including the isolated dbt environment checks on Python 3.11. Startup then failed because host port 8080 was occupied. All host ports are now configurable using .env.example, with defaults moved to a separate range. Internal service addresses are unchanged. Full end-to-end acceptance is still pending.

## Empty-source startup fix
User logs confirm PostgreSQL snapshot completes with zero records and WAL streaming starts, while worker repeatedly exits on UNKNOWN_TOPIC_OR_PART. up now creates lab.public.entities explicitly (idempotently), and worker treats missing topic as a startup wait. Connect readiness requires at least one running source task. Readiness unit tests cover empty tasks, failed tasks and running tasks. Kafka topic creation and end-to-end integration require user-side Docker validation.
