# Demo: replay
## Business situation
A committed event is delivered a second time.

## Preconditions
Stack healthy (`../START_HERE.md`). Airflow paused. This case has not previously run since reset. Other cases may exist; SQL and verification use unique case IDs. Automatic path: `./bin/lab demo replay`.

## Step 1 — Seed and build baseline
```bash
./bin/lab seed replay
./bin/lab inspect replay
```
Expected: One order amount 100.
Source writes are committed PostgreSQL transactions. The script waits for target CDC records before building. Inspect prints Bronze versions and Gold facts separately.

## Step 2 — Introduce the problem
```bash
./bin/lab inject replay
./bin/lab inspect replay
```
Expected: Copy the existing Bronze row including its Kafka identity. This intentionally simulates post-crash delivery at the sink, not a new PostgreSQL change.
Keep this state visible while inspecting Grafana. Automatic demo mode advances immediately, so use the step-by-step path when watching panels.

## Step 3 — Deliver and reconcile
```bash
./bin/lab build
./bin/lab inspect replay
./bin/lab verify replay
```
Expected: One Gold order and amount 100; Bronze has duplicate event IDs.
`verify` asserts the exact fact tuple and fails the command if it differs. `dbt build` also checks unique order keys, positive history intervals and aggregate/fact sum consistency.

## Step 4 — Prove idempotence
```bash
./bin/lab build
./bin/lab verify replay
```
The fact tuple and revenue must remain identical. Bronze may contain replay copies; Gold must not double count.

## Grafana
Replay deliveries increases by one; business totals remain unchanged.
See [panel interpretation](../GRAFANA.md). Panels accumulate across cases; use case-specific inspect output for exact expectations.

## Code map
- [Scenario branches: seed/inject/release/verify](../../app/scenarios.py)
- [CDC transport and hold/reverse release](../../app/consumer.py)
- [Version resolver](../../dbt/models/silver_state.sql)
- [SCD2 intervals](../../dbt/models/silver_customer_history.sql)
- [Fact reconciliation](../../dbt/models/gold_orders.sql)
- [Revenue](../../dbt/models/gold_revenue.sql)
- [Airflow](../../airflow/dags/medallion.py)

## Diagnose unexpected results
Check Connect RUNNING, consumer logs, held-event count and the printed Bronze revision/operation. If Bronze is correct but Gold stale, run build and inspect its error. If Gold changed before your build, pause Airflow. For a clean rerun use `./bin/lab reset` (deletes this lab's data), then repeat step 1.
