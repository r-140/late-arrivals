# Demo: out-of-order
## Business case
The source integrates a stale revision after a newer update.

## Preconditions
Stack ready; Airflow paused (`./bin/lab pause`). Case not previously seeded since reset. Other cases can exist, but dashboards sum all cases. `./bin/lab demo out-of-order` automates all transitions, validates failure visibility where relevant, and replays exact extracted order events to test idempotence.

## Step 1 — Create baseline and publish it
```bash
./bin/lab seed out-of-order
./bin/lab inspect out-of-order
./bin/lab batches
```
Expected: Order revision 1 amount 100.
Seed writes PostgreSQL records, trigger creates queue rows, ETL claims a batch, serializes JSON, transforms using Spark, commits Delta outputs, stages ClickHouse tables, and publishes a complete star.

## Step 2 — Introduce late data
```bash
./bin/lab inject out-of-order
./bin/lab inspect out-of-order
```
Expected: Revision 3 amount 150 and later revision 2 amount 120 are in the same JSON batch.
Until a completed run publishes another generation, ClickHouse views retain the baseline (except late-delete's intermediate cancellation, deliberately run inside inject).

## Step 3 — Process/recover and verify
```bash
./bin/lab run
./bin/lab inspect out-of-order
./bin/lab verify out-of-order
```
Expected fact tuple: 150, known customer, standard, paid, deleted=0. verify also checks global fact uniqueness and exact revenue reconciliation.

## Step 4 — Inspect stored artifacts
```bash
./bin/lab batches
# Replace N with the actual batch number above.
./bin/lab json N
./bin/lab delta dim_customer
./bin/lab delta fact_order
./bin/lab delta daily_revenue
./bin/lab sql "SELECT * FROM daily_revenue ORDER BY day,segment"
```
JSON contains source payload, source time, operation, revision and event identity. Delta history shows persisted Parquet/log commits. etl_batches.delta_manifest pins the output versions read by Load. Inspect business event time/effective_from separately from extraction time.

## Step 5 — Confirm stable repeat
```bash
./bin/lab run
./bin/lab verify out-of-order
```
With no pending changes no new batch is created. For a real replay of event membership use automatic demo on a fresh lab; it intentionally marks the original order events unclaimed, runs another batch, and verifies unchanged results.

## Grafana expectations
Latest-version window must prefer revision 3. Inspect JSON event IDs alongside application revisions.
See [GRAFANA](../GRAFANA.md) for units and accumulated values.

## Code links
- [seed/inject/demo/verify](../../app/scenarios.py)
- [Extract membership and immutable JSON](../../app/extract.py)
- [Spark windows, SCD2, Delta MERGE](../../app/transform.py)
- [Load generation and publish acknowledgment](../../app/load.py)
- [Pipeline stages/recovery](../../app/pipeline.py)
- [Source trigger](../../infra/source.sql)
- [Star views](../../infra/warehouse.sql)

## Troubleshooting / repeat
If source changes exist but no batch appears, inspect the queue and worker job output. If batch is transformed but old facts remain, inspect error then run recovery. If a previously executed case refuses seed, use inspect/verify or full reset; reset deletes all data in THIS variant. Do not rerun inject after an interrupted case until the incomplete batch is recovered.
