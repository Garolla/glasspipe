# transform (SQLMesh)

SQLMesh owns the **staging** and **marts** layers only.

`raw.raw_nrt` and `raw.raw_batch` are *not* SQLMesh models -- their DDL
lives in `clickhouse/init/`, versioned but outside this project. This
isn't the original plan (see `ARCHITECTURE.md`, which describes
`raw.raw_nrt` as a SQLMesh incremental model reading Parquet directly);
it changed during implementation for a concrete reason:

`sqlglot`'s ClickHouse dialect (the parser SQLMesh is built on, version
30.8.0 at the time of writing) does not correctly parse ClickHouse's
`file()` table function -- `FROM file('nrt/**/*.parquet', 'Parquet')`
gets misparsed as a table literally named `file` with a bogus column-alias
list, which is invalid SQL once rendered back out. Confirmed directly
against the installed `sqlglot`/`sqlmesh` versions, not assumed.

Given that, the Parquet-to-ClickHouse load for `raw.raw_nrt` is a plain
Python step (`orchestration/orchestration/raw_loader.py`, using pyarrow +
clickhouse-connect, run as a Dagster asset) instead of a SQLMesh model.
It still uses the same idempotent pattern the architecture calls for -- a
manifest table (`raw._loaded_files`) tracks which Parquet files have
already been loaded, so re-running never double-inserts a file. `raw_batch`
was always going to be populated directly by `batch_extract` (see the
architecture diagram), so it was already outside SQLMesh; `raw_nrt` just
joined it for a different reason. Column-level lineage still holds from
`staging.*` onward, which is where SQLMesh's CI-gate value (principle 1)
actually lives -- catching a column that's dropped upstream but still
read by a mart.

If a future sqlglot release fixes ClickHouse table-function parsing,
`raw.raw_nrt` can move back into this project as originally designed;
`raw_loader.py` and the manifest table can retire at that point.

## Layout

- `models/stg_events_nrt.sql` -- dedup view over `raw.raw_nrt` (at-least-once
  delivery means duplicates are possible; resolved once, here).
- `models/stg_events_batch.sql` -- pass-through view over `raw.raw_batch`.
- `models/edits_hourly.sql` -- NRT lane aggregate.
- `models/pageviews_top.sql` -- batch lane aggregate. Not joined with the
  NRT lane (principle 5).
- `audits/not_null_wiki.sql` -- example audit wired to `edits_hourly`.

## Running locally

Connection settings come from `SQLMESH__GATEWAYS__CLICKHOUSE__CONNECTION__*`
env vars (SQLMesh's native convention), set in `docker-compose.yml` for the
`orchestration` service. To run standalone against a ClickHouse already
listening on `localhost:8123`:

```bash
export SQLMESH__GATEWAYS__CLICKHOUSE__CONNECTION__HOST=localhost
export SQLMESH__GATEWAYS__CLICKHOUSE__CONNECTION__USERNAME=default
sqlmesh plan
```

This project's models have been validated by parsing/rendering them
against a real `sqlmesh`/`sqlglot` install (`Context(paths=...)`,
`model.render_query()`), not executed against a live ClickHouse -- there
wasn't one available in the environment this was built in. `sqlmesh plan`
against a real database is the next step before trusting this in
production.
