# Demo: after-publish
## Business case
ClickHouse publication succeeds but PostgreSQL acknowledgment is interrupted.

## Preconditions
Stack ready; Airflow paused (`./bin/lab pause`). Case not previously seeded since reset. Other cases can exist, but dashboards sum all cases. `./bin/lab demo after-publish` automates all transitions, validates failure visibility where relevant, and replays exact extracted order events to test idempotence.

## Step 1 — Create baseline and publish it
```bash
./bin/lab seed after-publish
./bin/lab inspect after-publish
./bin/lab batches
```
Expected: Published amount 100.
Seed writes PostgreSQL records, trigger creates queue rows, ETL claims a batch, serializes JSON, transforms using Spark, commits Delta outputs, stages ClickHouse tables, and publishes a complete star.

## Step 2 — Introduce late data
```bash
./bin/lab inject after-publish
./bin/lab inspect after-publish
```
Expected: The pointer switches to 120, then an error leaves the batch transformed.
Until a completed run publishes another generation, ClickHouse views retain the baseline (except late-delete's intermediate cancellation, deliberately run inside inject).

## Step 3 — Inject the stage failure
```bash
./bin/lab run --fail after-publish
./bin/lab batches
./bin/lab inspect after-publish
```
A nonzero exit with `Injected failure` is EXPECTED. Other error messages are not the demo result. Published amount remains 100 except after-publish, where the completed star already shows 120. Use the printed batch number to inspect JSON and pinned Delta versions.

## Step 4 — Process/recover and verify
```bash
./bin/lab run
./bin/lab inspect after-publish
./bin/lab verify after-publish
```
Expected fact tuple: 120, known customer, standard, paid, deleted=0. verify also checks global fact uniqueness and exact revenue reconciliation.

## Step 5 — Inspect stored artifacts
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

## Step 6 — Confirm stable repeat
```bash
./bin/lab run
./bin/lab verify after-publish
```
With no pending changes no new batch is created. For a real replay of event membership use automatic demo on a fresh lab; it intentionally marks the original order events unclaimed, runs another batch, and verifies unchanged results.

## Grafana expectations
Published revenue already reflects 120 while batch is incomplete. Retry detects existing publication and acknowledges it without a second publish.
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
