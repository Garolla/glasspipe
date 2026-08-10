# transform (SQLMesh)

For what the tables actually mean — grain, columns, lineage — see
[`DATA_MODEL.md`](DATA_MODEL.md). This file is about the tool, not the model.

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

## State store

SQLMesh needs somewhere to keep its own bookkeeping -- which time
intervals of each incremental model have already been backfilled, model
fingerprints (to tell a real logic change from a cosmetic one), and
which physical table `prod`'s views currently point at (SQLMesh deploys
new model versions blue/green-style, swapping the pointer once the new
table is ready). This is metadata *about* the pipeline, not pipeline
data -- it doesn't live in `raw`/`staging`/`marts`.

ClickHouse can't hold it: SQLMesh refuses to use the same engine as a
`gateways.clickhouse.connection` for `state_connection`, because
ClickHouse doesn't give SQLMesh the transactional guarantees its state
layer needs. This only became visible once the stack was actually run
against a live ClickHouse (`sqlmesh plan` failing outright) -- the
earlier `Context(paths=...)` / `render_query()` validation parsed and
rendered the models fine without ever touching state.

`transform/config.yaml` points `state_connection` at an embedded DuckDB
file instead of standing up another service -- consistent with KISS
(`ARCHITECTURE.md`). This is a different role than the "DuckDB as the
warehouse" option `ARCHITECTURE.md` already dropped: DuckDB's
single-writer limitation ruled it out for concurrent read/write serving,
but here only SQLMesh's own subprocess ever touches this file, so
single-writer is fine.

That file lives at `/opt/transform/.state/sqlmesh_state.db` inside the
`orchestration-webserver`/`orchestration-daemon` containers, on the
`sqlmesh_state` Docker volume (`docker-compose.yml`). Without that
volume, the file sits on the container's own writable layer and
disappears on every image rebuild -- SQLMesh then has no memory of what
it already computed, so the next `plan` treats every model as brand new
and re-backfills every incremental model's entire configured range from
scratch. For `edits_hourly` that's cheap today (a handful of hours of
Wikimedia data); it stops being cheap once real history accumulates in
`raw.raw_nrt`.

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

This project's models were originally validated only by parsing/rendering
them against a real `sqlmesh`/`sqlglot` install (`Context(paths=...)`,
`model.render_query()`), not executed against a live ClickHouse -- there
wasn't one available in the environment this was built in. `sqlmesh plan`
has since been run against a real ClickHouse (see the "State store"
section above for what that run caught), and `staging.*`/`marts.*` build
and query correctly.
