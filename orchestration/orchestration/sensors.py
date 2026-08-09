from __future__ import annotations

import json
import time
from pathlib import Path

from dagster import AssetMaterialization, AssetObservation, SensorEvaluationContext, SkipReason, sensor

from glasspipe_common.paths import ParquetPaths

from orchestration.config import OrchestrationConfig
from orchestration.raw_loader import get_clickhouse_client


@sensor(minimum_interval_seconds=30, description="Observes bridge liveness via its heartbeat file.")
def bridge_heartbeat_sensor(context: SensorEvaluationContext):
    config = OrchestrationConfig.from_env()
    path = Path(config.bridge_heartbeat_path)

    if not path.exists():
        return SkipReason("bridge heartbeat file does not exist yet")

    age_seconds = time.time() - path.stat().st_mtime
    context.instance.report_runless_asset_event(
        AssetObservation(
            asset_key="bridge_heartbeat",
            metadata={"heartbeat_age_seconds": round(age_seconds, 1)},
        )
    )
    return SkipReason(f"bridge heartbeat age={age_seconds:.0f}s")


@sensor(minimum_interval_seconds=30, description="Observes new Parquet files landed by the landing service.")
def parquet_landing_sensor(context: SensorEvaluationContext):
    config = OrchestrationConfig.from_env()
    paths = ParquetPaths(base_dir=Path(config.parquet_dir))

    current_files = {str(f) for f in paths.iter_nrt_files()}
    seen_before = set(json.loads(context.cursor)) if context.cursor else set()
    new_files = current_files - seen_before

    context.update_cursor(json.dumps(sorted(current_files)))

    if not new_files:
        return SkipReason("no new parquet files since last check")

    context.instance.report_runless_asset_event(
        AssetMaterialization(
            asset_key="parquet_raw_events",
            metadata={"new_files": len(new_files), "files_on_disk": len(current_files)},
        )
    )
    return SkipReason(f"observed {len(new_files)} new parquet file(s)")


@sensor(minimum_interval_seconds=60, description="Observes new rows in raw.raw_batch written by batch_extract.")
def batch_raw_events_sensor(context: SensorEvaluationContext):
    config = OrchestrationConfig.from_env()
    client = get_clickhouse_client(config)
    result = client.query("SELECT max(fetched_at), count() FROM raw_batch")
    max_fetched_at, row_count = result.result_rows[0]

    if max_fetched_at is None:
        return SkipReason("raw.raw_batch is empty so far")

    last_seen = context.cursor
    if last_seen == str(max_fetched_at):
        return SkipReason("no new raw_batch rows since last check")

    context.instance.report_runless_asset_event(
        AssetObservation(
            asset_key="batch_raw_events",
            metadata={"row_count": row_count, "max_fetched_at": str(max_fetched_at)},
        )
    )
    context.update_cursor(str(max_fetched_at))
    return SkipReason(f"observed raw_batch up to {max_fetched_at}")
