# Returning to the project
1. Read the architecture paragraph in README; remember: business time is not ingestion time.
2. Run `./bin/lab up`, then `./bin/lab doctor`. Check Connect task RUNNING and worker reachable.
3. Open Grafana at localhost:3000. Keep Airflow medallion paused while inspecting intermediate states.
4. Choose one document in `docs/demos`. Use `seed`, `inject`, optional `release`, `build`, `verify` to see every transition. Use `demo` to automate the entire case.
5. If the chosen case already exists, either choose another case or run `reset` after accepting that this deletes the lab data.
6. Inspect code through the relative links in the selected document. All verification values are listed there.
7. Stop with `down` when finished. A held in-memory batch is replayed from Kafka after worker restart because its offsets were not committed. Control mode persists; clear it with release or reset.

## Fast troubleshooting
- Connect missing: `./bin/lab register`.
- CDC timeout: `./bin/lab logs connect`, then `./bin/lab logs worker`; inspect status and held-events panel.
- dbt SQL/adapter issue: `./bin/lab logs airflow` or direct `build` output. Preserve error before changing versions.
- Grafana SQL panel errors before first build: expected, Gold tables do not yet exist. Build, then refresh.
- Airflow permissions: `up` prepares writable target/log directories; re-run it after moving the checkout.
- Unexpected auto-correction: pause the Airflow DAG.
- Port conflict: change only the localhost port mappings in compose.yaml.
