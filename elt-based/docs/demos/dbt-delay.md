# Demo: dbt-delay
## Business situation
Bronze keeps receiving updates while no transformation runs.

## Preconditions
Stack healthy (`../START_HERE.md`). Airflow paused. This case has not previously run since reset. Other cases may exist; SQL and verification use unique case IDs. Automatic path: `./bin/lab demo dbt-delay`.

## Step 1 — Seed and build baseline
```bash
./bin/lab seed dbt-delay
./bin/lab inspect dbt-delay
```
Expected: Order amount 100.
Source writes are committed PostgreSQL transactions. The script waits for target CDC records before building. Inspect prints Bronze versions and Gold facts separately.

## Step 2 — Introduce the problem
```bash
./bin/lab inject dbt-delay
./bin/lab inspect dbt-delay
```
Expected: Revision 2 amount 120 reaches Bronze; Gold still has 100.
Keep this state visible while inspecting Grafana. Automatic demo mode advances immediately, so use the step-by-step path when watching panels.

## Step 3 — Deliver and reconcile
```bash
./bin/lab build
./bin/lab inspect dbt-delay
./bin/lab verify dbt-delay
```
Expected: Next build catches up to 120; subsequent build remains 120.
`verify` asserts the exact fact tuple and fails the command if it differs. `dbt build` also checks unique order keys, positive history intervals and aggregate/fact sum consistency.

## Step 4 — Prove idempotence
```bash
./bin/lab build
./bin/lab verify dbt-delay
```
The fact tuple and revenue must remain identical. Bronze may contain replay copies; Gold must not double count.

## Grafana
Delivery stays healthy while dbt age grows. Gold revenue changes only on build.
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
