# Demo: late-delete
## Business situation
A cancellation and then physical deletion arrive after revenue was reported.

## Preconditions
Stack healthy (`../START_HERE.md`). Airflow paused. This case has not previously run since reset. Other cases may exist; SQL and verification use unique case IDs. Automatic path: `./bin/lab demo late-delete`.

## Step 1 — Seed and build baseline
```bash
./bin/lab seed late-delete
./bin/lab inspect late-delete
```
Expected: Paid order amount 100 included in revenue.
Source writes are committed PostgreSQL transactions. The script waits for target CDC records before building. Inspect prints Bronze versions and Gold facts separately.

## Step 2 — Introduce the problem
```bash
./bin/lab inject late-delete
./bin/lab inspect late-delete
```
Expected: Cancellation revision 2 arrives; script builds it so revenue drops to zero. Then PostgreSQL DELETE produces a tombstone.
Keep this state visible while inspecting Grafana. Automatic demo mode advances immediately, so use the step-by-step path when watching panels.

## Step 3 — Deliver and reconcile
```bash
./bin/lab build
./bin/lab inspect late-delete
./bin/lab verify late-delete
```
Expected: Gold retains one cancelled tombstone (deleted=1); revenue excludes it.
`verify` asserts the exact fact tuple and fails the command if it differs. `dbt build` also checks unique order keys, positive history intervals and aggregate/fact sum consistency.

## Step 4 — Prove idempotence
```bash
./bin/lab build
./bin/lab verify late-delete
```
The fact tuple and revenue must remain identical. Bronze may contain replay copies; Gold must not double count.

## Grafana
Cancelled/deleted count increases; revenue for the case falls by 100.
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
