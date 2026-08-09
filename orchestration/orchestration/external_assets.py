"""Assets Dagster does not execute -- it only observes them.

The bridge, the landing service, and batch_extract run as their own
always-on containers, outside Dagster's control. These AssetSpecs give
them a place in the same asset graph as the SQLMesh-driven assets, so
freshness and materialization history show up in one place regardless of
who actually did the work (see ARCHITECTURE.md, principle 1 and 3).
"""

from dagster import AssetSpec

bridge_heartbeat = AssetSpec(
    key="bridge_heartbeat",
    description=(
        "Liveness of the SSE -> Redpanda bridge (services/bridge). "
        "Observed via a sensor reading the heartbeat file the bridge touches "
        "periodically; Dagster never runs the bridge itself."
    ),
    group_name="ingestion",
)

parquet_raw_events = AssetSpec(
    key="parquet_raw_events",
    description=(
        "Parquet files landed by the landing service (services/landing) "
        "from Redpanda. Observed via a sensor that scans the buffer "
        "directory for new files."
    ),
    group_name="ingestion",
    deps=[bridge_heartbeat],
)

batch_raw_events = AssetSpec(
    key="batch_raw_events",
    description=(
        "raw.raw_batch rows written directly by batch_extract "
        "(services/batch_extract) on its own schedule. Observed via a "
        "sensor that checks the most recent fetched_at in ClickHouse."
    ),
    group_name="ingestion",
)

__all__ = ["bridge_heartbeat", "parquet_raw_events", "batch_raw_events"]
