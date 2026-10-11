# Start here
## Prerequisites
Linux/macOS with Docker Engine and Compose v2, curl, Python 3. Approximately 8 GB free RAM and 10 GB disk. Default host ports 15432, 18123, 18083, 18080, 19090, 19101 and 13000 must be free. To customize them, copy `.env.example` to `.env` and edit the values; both Compose and bin/lab read them. This stack exposes ports on localhost only and uses demo passwords.

```bash
chmod +x bin/lab
./bin/lab up
./bin/lab doctor
./bin/lab demo late-fact
```
`up` builds Python images, starts all services, idempotently creates the one-partition CDC topic, then registers PostgreSQL CDC and waits up to three minutes for its source task to become RUNNING. A fresh empty source table does not need to emit an event to create the topic. A running container is not necessarily ready: inspect `doctor`, Connect's task status must be RUNNING, and retry a demo only after readiness. The consumer retries startup by container restart.

The first demo seeds a customer and historical segment, builds the baseline, inserts an old paid order into PostgreSQL, waits for real CDC, rebuilds dbt, asserts the result, and rebuilds again to check idempotence. Expected order: amount 100, known customer, standard segment, paid, not deleted. The intentionally naive two-day model misses it.

## Interfaces
- Grafana: http://localhost:13000 — admin / lab.
- Airflow: http://localhost:18080 — standalone prints the generated admin password in `./bin/lab logs airflow`; the password is also stored inside the airflow volume. Do not commit it.
- Prometheus: http://localhost:19090.
- Connect: http://localhost:18083/connectors/lab-source/status.

Airflow's DAG is paused initially to prevent scheduled builds from hiding intermediate demo states. Trigger manually with `./bin/lab trigger`, or unpause in the UI for normal five-minute scheduling. `./bin/lab build` runs the identical dbt command directly for deterministic demo steps.

## All cases
```bash
for name in late-fact cdc-delay missing-dimension out-of-order late-scd2 dbt-delay replay late-delete; do
  ./bin/lab demo "$name"
done
```
Distinct case IDs isolate verification; the dashboard totals accumulate across cases. Each case can run once per reset. `./bin/lab reset` deletes ALL data in this Compose project's volumes, including Airflow metadata, then rebuilds the clean stack. `down` keeps volumes.
