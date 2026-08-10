# glasspipe — architecture

glasspipe is a data pipeline built around one idea: lineage and observability should fall out of how the system is built, not get bolted on afterward. Wikimedia EventStreams (the public `recentchange` SSE feed) is used as the first near-real-time source to prove the design end to end.

This document is the *system* — services, data flow, infrastructure choices. For the *data model* — what each table means, its grain, how tables relate — see [`transform/DATA_MODEL.md`](transform/DATA_MODEL.md). That document is the one to keep authoritative as the schema evolves.

## Principles

1. **Lineage and observability are system properties**, not add-ons.
2. **KISS.** A single `docker-compose` deploy for the whole stack.
3. **Modern orchestration.**
4. **Versioned code, including the DB schema.**
5. **Both near-real-time and batch feeds are covered, without being forced to reconcile.**

## Diagram

![glasspipe architecture](docs/architecture.svg)

The SSE bridge and the landing layer write to the broker and to Parquet continuously; Dagster doesn't execute those steps but observes them via a sensor. From Parquet onward, Dagster loads `raw.raw_nrt` incrementally and SQLMesh transforms inside ClickHouse in orchestrated runs. `raw_nrt` and `raw_batch` stay as distinct tables — nothing in the system forces them to reconcile; downstream models decide if and how to combine them.

*(Implementation note: the Parquet → `raw_nrt` load is a plain Python Dagster asset, not a SQLMesh model reading Parquet directly as first planned — a `sqlglot`/ClickHouse limitation with the `file()` table function, found while building it. Column-level lineage still covers everything from `staging.*` onward. Full explanation in `transform/README.md`.)*

*(Implementation note: ClickHouse can't hold SQLMesh's own state either — SQLMesh refuses to use it as a `state_connection`, since it lacks the transactional guarantees SQLMesh's bookkeeping needs. This only surfaced once the stack was run against a live ClickHouse; the earlier `sqlmesh`/`sqlglot` parse-and-render validation didn't exercise it. State now lives in an embedded DuckDB file on its own Docker volume (`sqlmesh_state`), so backfill history survives image rebuilds. Full explanation in `transform/README.md`.)*

## Principles → decisions

**01 · Lineage and observability as a system property**
Dagster's asset graph isn't documentation added after the fact: observed materializations (bridge, broker, Parquet) and executed ones (SQLMesh) live in the same graph. SQLMesh adds column-level lineage as a CI gate — drop a column that's still read downstream and the PR fails on its own; nobody has to remember to check.

**02 · KISS — a single deploy**
One `docker-compose` for the whole stack. No Kafka Connect / JVM worker where a small piece of code does the same job; no stream-processing engine (Flink and friends) as long as the NRT cleaning stays stateless per event — stateful logic is pushed into idempotent batch recompute instead.

**03 · Modern orchestration**
Dagster, asset-based: orchestrates the batch/SQLMesh runs and exposes the always-on pieces it doesn't execute directly (bridge, broker, landing) as observed external assets, so freshness and failures surface in the same place as everything else.

**04 · Versioned code, including the schema**
SQLMesh models (SQL) and the raw layer's validation contracts live in Git. Every change goes through a PR — the DB schema is literally the diff being reviewed.

**05 · NRT and batch, without forced reconciliation**
Two lanes into the warehouse: `raw_nrt` (bridge → Redpanda → Parquet buffer → incremental load) and `raw_batch` (direct scheduled extraction). They land as separate ClickHouse tables; nothing in the system merges them automatically.

## Components

| Role | Technology | Why |
|---|---|---|
| NRT source (POC) | Wikimedia EventStreams | Public, free SSE stream — a good test bed for resume/checkpoint behavior |
| SSE → broker bridge | custom service (Python) | The one non-declarative piece: handles reconnect and `Last-Event-ID` |
| Broker | Redpanda | Generic ingestion boundary, single container, Kafka wire protocol |
| Landing + cleaning | consumer + schema validation | Writes append-only Parquet; invalid records go to dead-letter |
| Durable buffer | Parquet, TTL ~3 days | Decouples the NRT write rate from the DB; deleted only after a confirmed load |
| Storage / serving | ClickHouse | Self-hosted OLAP, real write concurrency, inspectable from DBeaver, native Prometheus metrics |
| Transformation | SQLMesh | Native column-level lineage, CI gate on breaking changes |
| SQLMesh state store | Embedded DuckDB, on a persisted Docker volume | ClickHouse can't back SQLMesh's own bookkeeping (backfilled intervals, model fingerprints, prod's pointers) -- no transactional guarantees. Only SQLMesh's own subprocess touches this file, so single-writer is fine here, unlike the warehouse role DuckDB was dropped for below |
| Orchestration | Dagster | Native assets + observed external assets, freshness/quality asset checks |
| Logs | Grafana Alloy → Loki | Reuses the Loki already running on the VPS; push-based, works with any deployer's Loki with no assumptions about their setup |
| Metrics | The deployer's own Prometheus, scraping ClickHouse/Redpanda directly | Not pushed through Alloy -- vanilla Prometheus doesn't accept remote-write by default, found running this for real (see `observability/README.md`) |

## Alternatives considered and dropped

- **Kafka Connect connector** (`conduktor/kafka-connect-wikimedia`) for the SSE bridge — would still require hosting a separate JVM Kafka Connect worker, and its resume/offset behavior isn't documented. The small custom bridge gives the same result with less infrastructure and a known checkpoint strategy.
- **Prefect** as the orchestrator — nicer flow-as-functions DX, but no native data lineage graph; would mean bolting lineage on via OpenLineage/Marquez, which is exactly what principle 1 rules out.
- **dbt** for transformation — solid, but lacks native column-level lineage, which is required to satisfy "a developer can forget about lineage and the system still catches an orphaned column."
- **DuckDB** as the warehouse — great for KISS (embedded, no server), but single-writer concurrency didn't fit the concurrent read/write pattern needed for serving.
- **AlloyDB / CockroachDB** as the warehouse — AlloyDB isn't self-hostable/open source in its managed form; CockroachDB solves distributed OLTP, not OLAP aggregation on a single node. Neither fit the actual workload.
- **TimescaleDB** — a real contender (stays on Postgres, continuous aggregates map well to idempotent batch recompute), dropped in favor of ClickHouse's stronger OLAP fit and native Prometheus metrics, given the event/analytics shape of the data.

## Status

First implementation pass done — see [`README.md`](README.md) for what's built, what's been verified, and how (short version: unit-tested and validated against the real SQLMesh/Dagster/docker-compose tooling, but not yet run end-to-end against live containers). Next step is exactly that: `docker compose up` against real infrastructure and a real Wikimedia connection.
