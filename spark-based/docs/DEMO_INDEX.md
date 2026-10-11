# Demo index

- [late-fact](demos/late-fact.md): An order happened seven days ago but is inserted today.
- [missing-dimension](demos/missing-dimension.md): Order arrives before customer/history.
- [out-of-order](demos/out-of-order.md): The source integrates a stale revision after a newer update.
- [late-scd2](demos/late-scd2.md): A historical premium segment is added retroactively.
- [etl-delay](demos/etl-delay.md): Hourly ETL is paused while source continues changing.
- [replay](demos/replay.md): Same business version and then exact extracted event membership are replayed.
- [late-delete](demos/late-delete.md): Cancellation and physical deletion happen after reporting.
- [after-extract](demos/after-extract.md): Process fails after immutable JSON extraction.
- [after-transform](demos/after-transform.md): Process fails after Spark has committed all Delta outputs.
- [during-load](demos/during-load.md): Load fails after dimensions are staged but before facts and publication.
- [after-publish](demos/after-publish.md): ClickHouse publication succeeds but PostgreSQL acknowledgment is interrupted.

Use `./bin/lab demo CASE` for automated assertions and exact event replay; manual commands expose intermediate states. A case may be seeded once per full lab reset.
