# Validation status
Implemented local Docker stack, eleven scenarios and recovery paths. Python AST/compile checks, Bash syntax, YAML/JSON and relative documentation links are checked during artifact creation. Offline extractor tests check immutable JSON retry and delayed lower sequence IDs.

Docker is unavailable in the creation environment. Installation of a local Spark runtime was blocked by network policy, so real Spark/Delta tests, image builds, warehouse SQL and Airflow execution are NOT claimed as verified here. Run `./bin/acceptance` on a fresh lab. The separate ELT variant's original user-side validation does not certify this Spark version.

The worker image checks pip dependencies and starts a real Spark/Delta session to cache JARs at build time. `./bin/lab test` includes a real Spark test for missing dimensions, backdated SCD2, exact event replay and Delta version reads. All demos run real PostgreSQL/Spark/Delta/ClickHouse assertions; injected failures also assert publication visibility.
