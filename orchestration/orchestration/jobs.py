from pathlib import Path

from dagster import OpExecutionContext, define_asset_job, job, op

from glasspipe_common.paths import ParquetPaths

from orchestration.assets import edits_hourly, pageviews_top, raw_nrt, stg_events_batch, stg_events_nrt
from orchestration.config import OrchestrationConfig
from orchestration.raw_loader import get_clickhouse_client, list_loaded_files

pipeline_job = define_asset_job(
    name="pipeline_job",
    selection=[raw_nrt, stg_events_nrt, stg_events_batch, edits_hourly, pageviews_top],
    description=(
        "Loads new Parquet into raw_nrt and runs every SQLMesh model -- the "
        "recurring heartbeat of the pipeline. Doesn't touch the always-on "
        "services (bridge/landing/batch_extract); those keep running "
        "independently and are only observed (see external_assets.py)."
    ),
)


@op(description="Deletes Parquet files past the retention window -- but only ones confirmed loaded into ClickHouse.")
def cleanup_loaded_parquet(context: OpExecutionContext) -> None:
    config = OrchestrationConfig.from_env()
    paths = ParquetPaths(base_dir=Path(config.parquet_dir))
    client = get_clickhouse_client(config)

    already_loaded = list_loaded_files(client)
    cutoff_seconds = config.parquet_retention_days * 86400

    deleted = 0
    for path in paths.iter_nrt_files():
        if str(path) not in already_loaded:
            continue  # never delete a file that hasn't been confirmed loaded
        if paths.file_age_seconds(path) < cutoff_seconds:
            continue
        path.unlink()
        deleted += 1

    context.log.info(f"deleted {deleted} parquet file(s) past the {config.parquet_retention_days}-day retention window")


@job(description="Daily Parquet buffer cleanup.")
def cleanup_parquet_job() -> None:
    cleanup_loaded_parquet()
