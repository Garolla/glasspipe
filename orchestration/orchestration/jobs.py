from pathlib import Path

from dagster import OpExecutionContext, job, op

from glasspipe_common.paths import ParquetPaths

from orchestration.config import OrchestrationConfig
from orchestration.raw_loader import get_clickhouse_client, list_loaded_files


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
