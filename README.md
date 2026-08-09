# glasspipe

A data pipeline covering near-real-time and batch ingestion with lineage
and observability as system properties, not bolted-on components. See
[`ARCHITECTURE.md`](ARCHITECTURE.md) for the design and the diagram.

This is a first implementation pass: real, runnable code, but built and
validated in a sandbox with **no Docker daemon and no network access to
wikimedia.org**. Everything below states plainly what was actually
verified and what wasn't.

## Layout

```
packages/glasspipe_common/  shared event schema (pydantic) + Parquet path conventions
services/bridge/            SSE -> Redpanda, the one non-declarative piece
services/landing/           Redpanda -> Parquet, validates + dead-letters
services/batch_extract/     scheduled pull -> ClickHouse raw_batch directly
clickhouse/init/            versioned DDL (raw_nrt, raw_batch, manifest)
clickhouse/config/          Prometheus metrics endpoint config
transform/                  SQLMesh project: staging + marts
orchestration/              Dagster project: assets, sensors, checks, cleanup job
observability/alloy/        Grafana Alloy config (logs + metrics -> your VPS)
docker-compose.yml          the whole stack
```

## Running the tests

Each package under `packages/` and `services/`, plus `orchestration/`, is
independent (its own `pyproject.toml`, its own dependencies) -- install
and test them one at a time, not all together in a single `pytest`
invocation from the repo root (several share test filenames like
`test_writer.py`, which collide if pytest treats them as one namespace):

```bash
for d in packages/glasspipe_common services/bridge services/landing services/batch_extract orchestration; do
  pip install -e "$d[dev]"
  pytest "$d"
done
```

## Running it

```bash
cp .env.example .env   # fill in LOKI_ENDPOINT_URL / PROMETHEUS_REMOTE_WRITE_URL at minimum
docker compose up --build
```

Dagster UI at `http://localhost:3000`. ClickHouse at `localhost:8123`
(HTTP, for `clickhouse-connect`/DBeaver) and `localhost:9000` (native).

The first time, `raw.raw_batch`, `raw.raw_nrt`, and `raw._loaded_files` are
created by `clickhouse/init/*.sql`. SQLMesh's `staging.*`/`marts.*` tables
are created on first asset materialization (each `orchestration` asset
runs `sqlmesh plan --auto-apply`, which creates-or-updates).

## What's been verified, and how

Nothing here was run end-to-end in live containers -- this sandbox has no
Docker daemon (`docker info` fails: no `/var/run/docker.sock`) and its
network egress blocks `wikimedia.org`. What *was* checked, for real,
against the actual installed tools:

- **All unit tests pass** (`pytest`) in each of `packages/glasspipe_common`,
  `services/bridge`, `services/landing`, `services/batch_extract`,
  `orchestration` -- SSE parsing, checkpoint atomicity, Parquet
  partitioning/schema, offset tracking, the pageviews API client, the
  ClickHouse insert shape, the raw loader's manifest logic.
- **The SQLMesh project parses and renders correctly** against a real
  `sqlmesh`/`sqlglot` install (`Context(paths=...)`, `render_query()`,
  audits attached) -- this is what caught the `file()` table-function
  limitation documented in `transform/README.md` and changed the design
  from what `ARCHITECTURE.md` originally described.
- **The Dagster project validates against the real CLI**:
  `dagster definitions validate -w workspace.yaml` passes, and a
  dedicated test asserts the asset graph shape -- including that
  `pageviews_top` (batch lane) has no path back to `raw_nrt`/`stg_events_nrt`
  (NRT lane), which is principle 5 checked in code, not just asserted in
  prose.
- **`docker compose config` validates** the compose file's syntax and
  interpolation.

What's *not* verified: a live `docker compose up`, a real connection to
Wikimedia's SSE endpoint, a real ClickHouse taking real inserts, the
Alloy config against a real Alloy binary (see `observability/README.md`),
and the `batch_extract` pageviews API response shape against a live call
(see `services/batch_extract/batch_extract/client.py` docstring). All of
these are the natural next step, in an environment that can actually
reach the network and run containers.

## Simplifications from the original design

- `raw.raw_nrt` is loaded by a plain Python step
  (`orchestration/orchestration/raw_loader.py`), not a SQLMesh model
  reading Parquet directly -- see `transform/README.md`.
- `PARQUET_RETENTION_DAYS` cleanup only ever deletes files already
  confirmed loaded (tracked in `raw._loaded_files`), matching what was
  discussed, but there's no separate "safety margin" logic beyond that --
  it's a hard confirmed-loaded check, which is simpler and strictly safer.
- The batch lane's source is Wikimedia's pageviews "top articles" API --
  chosen because it's free, real, and genuinely a different shape of data
  (daily aggregate, not events) from the NRT lane, to make principle 5
  concrete rather than simulated.
