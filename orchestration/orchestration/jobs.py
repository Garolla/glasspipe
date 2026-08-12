from datetime import date, timedelta
from pathlib import Path

from dagster import OpExecutionContext, define_asset_job, job, multiprocess_executor, op

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
    # Default multiprocess concurrency is os.cpu_count() (4 on the VPS), so
    # raw_nrt/stg_events_batch/pageviews_top -- independent lanes with no
    # dependency forcing them apart -- were forking up to 3 step subprocesses
    # at once. Each reimports the full stack (pyarrow, dagster, sqlmesh,
    # clickhouse-connect), and the two SQLMesh lanes shell out to their own
    # `sqlmesh` subprocess on top of that. All of it runs inside
    # orchestration-code-server's 768m cgroup (see its mem_limit comment in
    # docker-compose.yml), which the concurrent peak blew past every single
    # hourly tick -- the OOM killer took out whichever step process was
    # heaviest, and the run failed. One step at a time keeps peak RSS inside
    # the container instead of raising the limit and hoping the shared VPS
    # has headroom.
    executor_def=multiprocess_executor.configured({"max_concurrent": 1}),
)


@op(description="Deletes Parquet files past the retention window -- but only ones confirmed loaded into ClickHouse.")
def cleanup_loaded_parquet(context: OpExecutionContext) -> None:
    config = OrchestrationConfig.from_env()
    paths = ParquetPaths(base_dir=Path(config.parquet_dir))
    client = get_clickhouse_client(config)

    # Bound the scan the same way raw_loader does (see its comment): files
    # past retention are always recent by definition, so scanning/looking up
    # the entire history here is pure waste that scales with the whole
    # backlog instead of with retention_days. A few days of buffer beyond
    # retention_days is enough to still catch anything that slipped a cycle.
    since = date.today() - timedelta(days=config.parquet_retention_days + 2)
    already_loaded = list_loaded_files(client, since=since)
    cutoff_seconds = config.parquet_retention_days * 86400

    deleted = 0
    for path in paths.iter_nrt_files(since=since):
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
