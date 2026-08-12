# glasspipe

A data pipeline covering near-real-time and batch ingestion with lineage
and observability as system properties, not bolted-on components. See
[`ARCHITECTURE.md`](ARCHITECTURE.md) for the system design, and
[`transform/DATA_MODEL.md`](transform/DATA_MODEL.md) for the data model
itself — what each table means, its grain, how tables relate. Start
there if you're trying to understand what glasspipe actually produces.

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
cp .env.example .env
docker compose up --build
```

`.env` can be left mostly blank for a local run. Alloy (`observability/`)
is behind a Compose profile and doesn't start by default -- it needs
`LOKI_ENDPOINT_URL` pointing at a real Loki, which you won't have on a
laptop. Only bring it up if you actually have one to point it at:

```bash
docker compose --profile observability up --build
```

Dagster UI at `http://localhost:3000`. ClickHouse at `localhost:8123`
(HTTP, for `clickhouse-connect`/DBeaver) and `localhost:19000` (native protocol -- published off the default 9000 to avoid colliding with other common services on that port).

The first time, `raw.raw_batch`, `raw.raw_nrt`, and `raw._loaded_files` are
created by `clickhouse/init/*.sql`. SQLMesh's `staging.*`/`marts.*` tables
are created on first asset materialization (each `orchestration` asset
runs `sqlmesh plan --auto-apply`, which creates-or-updates).

## Deploying

Deliberately out of scope for this repo: how/where you run this in
production is a CI/CD and infrastructure choice, not something the
pipeline's own code should assume or bake in. What this repo commits to
is the interface: `docker-compose.yml` plus a populated `.env` (see
`.env.example`) is a complete, self-sufficient deploy target. Any CI/CD
system pointed at this repo can drive it the same way local dev does:

```bash
docker compose --env-file /path/to/your/.env --profile observability up -d --build
```

`--profile observability` is what actually starts Alloy -- local dev
above deliberately leaves it off (see "Running it"), but a real deploy
should include it so logs/metrics actually ship somewhere.

One detail worth knowing if you're wiring that up: `docker-compose.yml`
pins `name: glasspipe` at the top. Without that, Compose infers the
project name from the current directory's basename -- fine locally, but
a CI runner that checks out into a fresh temp path on every run would
make Compose treat each deploy as a new stack and mint fresh volumes
instead of reusing `clickhouse_data`. Whatever runs the deploy should
also keep `.env` at a fixed path outside the checkout, for the same
reason -- it isn't and shouldn't be committed.

Another: Dagster (`3000`), ClickHouse (`8123`/`19000`), and Redpanda
(`9092`) are published bound to `127.0.0.1` only, not `0.0.0.0` -- none
of them have auth, so on a host with a public IP they should never be
reachable directly from the internet. Get to them from outside the host
via an SSH tunnel, or put a reverse proxy with auth in front (out of
scope here -- that's host config, not this repo's concern, same
reasoning as the deploy pipeline itself).

## What's been verified, and how

This has since been run end-to-end against real infrastructure: a live
`docker compose up --profile observability`, a real connection to
Wikimedia's SSE firehose, real inserts into ClickHouse, SQLMesh runs
against it, and Alloy shipping logs to a real Loki. What was checked
before that first live run, against the actual installed tools in a
sandbox with no Docker daemon and no network egress to `wikimedia.org`:

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

### Lessons from the first live deploy

A few things only surfaced once this ran for real, against a live
Wikimedia connection and a shared host:

- **Port collisions matter more than they look.** ClickHouse's native
  protocol port (`9000`) is a common default other software also uses --
  the compose file now publishes it off that default (`19000`) for
  exactly this reason. If a container that other services `depends_on:
  condition: service_healthy` fails to bind its port, Compose blocks
  those dependents from starting at all rather than crash-looping them --
  worth knowing when diagnosing why half a stack never came up.
- **A stale SSE checkpoint is a real operational hazard, not a
  theoretical one.** Wikimedia EventStreams replays everything since the
  stored `Last-Event-ID` on reconnect. If the bridge has been down (or
  bounced repeatedly) for a while, resuming from an old checkpoint can
  mean hours of backlog delivered in a burst -- enough to spike CPU/disk
  I/O on a small host. The bridge now discards checkpoints older than
  `MAX_CHECKPOINT_AGE_SECONDS` (default 900s) and resumes from "now"
  instead, trading a small data gap for bounded load. See
  `services/bridge/bridge/main.py`'s `resolve_resume_id`.
  Redeploying frequently (e.g. a short polling interval bouncing
  containers) compounds this, since every restart is another chance to
  replay a growing backlog -- this is a deploy-cadence concern for
  whatever CI/CD points at this repo, not something the pipeline itself
  can fully guard against.
- **Container log volume needs an operator-side limit.** This repo's
  services log at a reasonable volume, but the default Docker logging
  driver has no size cap or rotation -- on any host running this
  long-term, set `log-opts` (`max-size`/`max-file`) at the Docker daemon
  level, or per-service in a deploy overlay. Out of scope for this repo
  the same way the rest of deploy infra is (see "Deploying" above), but
  worth knowing before running this unattended for weeks.
- **Redpanda's default topic retention (7 days, unbounded bytes) is too
  generous for a small host.** The bridge only needs Redpanda as a short
  buffer ahead of `landing`, which consumes near-real-time and writes to
  Parquet -- it doesn't need days of history sitting in the broker. Left
  at Kafka/Redpanda defaults, an unfiltered global firehose filled several
  GB in under a day on a small VPS, most of it in an anonymous volume
  Compose doesn't even name. A `redpanda-init` step now creates (or
  alters) the topic with an explicit `retention.ms`/`retention.bytes`
  pair (`KAFKA_TOPIC_RETENTION_MS`/`KAFKA_TOPIC_RETENTION_BYTES` in
  `.env.example`), bridge/landing wait on it via
  `service_completed_successfully`.
- **One Parquet file per wiki+date per flush doesn't scale once
  downstream falls behind.** The landing writer buffers correctly (a
  30s/500-record flush, not one file per event -- see
  `services/landing/landing/writer.py`), but Wikimedia's `recentchange`
  stream spans hundreds of concurrently active wikis, so even a healthy
  flush cycle writes hundreds of files. Over a day that's tens of
  thousands of small files; if the loader stalls for any reason the
  on-disk backlog compounds fast (observed: 190k+ files / ~2.9GB
  accumulated during one such stall). This is a real scaling limit of
  the current write pattern, not just a downstream-consumption problem
  -- flagged here rather than considered solved by the bound below.
- **Unbounded backlog scans don't just get slow, they OOM-kill the
  process outright.** `raw_nrt`'s load step and the Parquet cleanup job
  both listed the entire on-disk buffer and the entire `_loaded_files`
  manifest table on every run, with no bound -- fine at hundreds of
  files, but at 180k+ it OOM-killed the step's subprocess
  (`ChildProcessCrashException`, confirmed via the kernel's cgroup OOM
  killer log). Both now take an explicit trailing-window `since` (see
  `orchestration/orchestration/raw_loader.py`,
  `orchestration/orchestration/jobs.py`) so the working set stays
  proportional to a day or two of traffic instead of the whole history
  -- the same bound `parquet_landing_sensor` already had, on the code
  path that's actually active (that sensor defaults to `STOPPED` and
  never ran).
- **Dagster's default workspace loading re-imports the whole app on
  every reload.** With `workspace.yaml` pointed at a Python module
  instead of a running server, the daemon and webserver each spun up a
  fresh ephemeral `dagster api grpc` subprocess -- reimporting
  dagster/pyarrow/sqlmesh/clickhouse-connect from scratch -- on every
  periodic workspace reload, under a hardcoded 20s heartbeat timeout. On
  a CPU-shared host that import routinely took longer than 20s, so the
  daemon killed the subprocess as unresponsive roughly once a minute and
  never reaped it. A dedicated `orchestration-code-server` service now
  loads the module once and stays up, referenced via `grpc_server` in
  `workspace.yaml` instead of `python_module`.
- **SQLMesh's local DuckDB state file isn't safe for concurrent
  writers, and `pipeline_job` runs lanes in parallel by design.** The
  NRT lane (`raw_nrt` → `stg_events_nrt` → `edits_hourly`) and the batch
  lane (`stg_events_batch` → `pageviews_top`) are independent in the
  asset graph, so Dagster's executor runs their steps concurrently --
  and both lanes call into SQLMesh, which stores its own state in one
  local DuckDB file (`sqlmesh_state` volume). When both lanes' SQLMesh
  calls land close together, one fails outright rather than waiting for
  the lock; re-running the same model in isolation immediately after
  always succeeds, which is what points at contention rather than a
  data problem. Not yet fixed -- the two lanes' SQLMesh calls need to be
  serialized (or moved off a single-writer state backend) before this
  stops being intermittent.

What's *not* yet verified: the `batch_extract` pageviews API response
shape against a live call (see
`services/batch_extract/batch_extract/client.py` docstring) -- the batch
lane's polling interval (default 24h) means it hasn't had a live cycle
confirmed yet at time of writing.

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
