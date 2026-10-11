# Returning after a break
1. From the combined repository enter `spark-based/`.
2. `./bin/lab up`, `doctor`, then `pause` to avoid scheduled batch consumption during demos.
3. `./bin/lab batches` explains what was extracted, transformed, published, or failed.
4. Choose a case in DEMO_INDEX; inspect previously run cases rather than seeding them again.
5. Read one batch JSON and Delta history. Batch manifest pins Delta versions used by the loader.
6. Open Grafana and compare source queue, failed batch state and published revenue.
7. Recover a failed batch with `./bin/lab run` before introducing new changes. It resumes the oldest incomplete batch.
8. Export artifacts with `./bin/lab export-data ./exported-data` if you want to inspect Parquet/log files outside Docker. Never edit live _delta_log files.
9. `down` keeps your lab. `reset` starts every demo from scratch and deletes all this variant's data.

## Troubleshooting
- Maven/JAR download failure: inspect build output and network/proxy; required coordinate is io.delta:delta-spark_2.12:3.3.1.
- JVM gateway exits: inspect worker job output, RAM and Java availability.
- Worker API health fails: stores may still be initializing; logs worker/postgres/clickhouse.
- No changes extracted: queue is empty or previous batch already claimed them. Inspect batches.
- Failure-injection error: expected only if the log starts with `Injected failure`; other errors must not be swallowed.
- Published result changes too early: pause Airflow before manual steps.
- Latest attempt failed but dashboard shows old facts: expected for failure before publication; old generation remains visible.
