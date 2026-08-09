# glasspipe — architecture

glasspipe is a data pipeline built around one idea: lineage and observability should fall out of how the system is built, not get bolted on afterward. Wikimedia EventStreams (the public `recentchange` SSE feed) is used as the first near-real-time source to prove the design end to end.

## Principles

1. **Lineage and observability are system properties**, not add-ons.
2. **KISS.** A single `docker-compose` deploy for the whole stack.
3. **Modern orchestration.**
4. **Versioned code, including the DB schema.**
5. **Both near-real-time and batch feeds are covered, without being forced to reconcile.**

## Diagram

![glasspipe architecture](docs/architecture.svg)

The SSE bridge and the landing layer write to the broker and to Parquet continuously; Dagster doesn't execute those steps but observes them via a sensor. From Parquet onward, SQLMesh loads and transforms inside ClickHouse in orchestrated runs. `raw_nrt` and `raw_batch` stay as distinct tables — nothing in the system forces them to reconcile; downstream models decide if and how to combine them.

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
| Orchestration | Dagster | Native assets + observed external assets, freshness/quality asset checks |
| Logs and metrics | Grafana Alloy → Loki/Grafana | Reuses the Grafana + Loki stack already running on the VPS; Alloy scrapes ClickHouse/Redpanda's `/metrics` |

## Alternatives considered and dropped

- **Kafka Connect connector** (`conduktor/kafka-connect-wikimedia`) for the SSE bridge — would still require hosting a separate JVM Kafka Connect worker, and its resume/offset behavior isn't documented. The small custom bridge gives the same result with less infrastructure and a known checkpoint strategy.
- **Prefect** as the orchestrator — nicer flow-as-functions DX, but no native data lineage graph; would mean bolting lineage on via OpenLineage/Marquez, which is exactly what principle 1 rules out.
- **dbt** for transformation — solid, but lacks native column-level lineage, which is required to satisfy "a developer can forget about lineage and the system still catches an orphaned column."
- **DuckDB** as the warehouse — great for KISS (embedded, no server), but single-writer concurrency didn't fit the concurrent read/write pattern needed for serving.
- **AlloyDB / CockroachDB** as the warehouse — AlloyDB isn't self-hostable/open source in its managed form; CockroachDB solves distributed OLTP, not OLAP aggregation on a single node. Neither fit the actual workload.
- **TimescaleDB** — a real contender (stays on Postgres, continuous aggregates map well to idempotent batch recompute), dropped in favor of ClickHouse's stronger OLAP fit and native Prometheus metrics, given the event/analytics shape of the data.

## Status

Draft, not yet implemented. Next steps: scaffold the `docker-compose`, the bridge service, and the first SQLMesh models against a single filtered Wikimedia stream (e.g. one wiki) before wiring up the batch lane.
