from __future__ import annotations

from pathlib import Path

from dagster import AssetCheckResult, asset_check

from glasspipe_common.paths import ParquetPaths

from orchestration.assets import raw_nrt
from orchestration.config import OrchestrationConfig
from orchestration.kafka_lag import get_consumer_lag
from orchestration.raw_loader import find_new_files, get_clickhouse_client, list_loaded_files

MAX_BACKLOG_FILES = 200
MAX_CONSUMER_LAG = 5000


@asset_check(
    asset=raw_nrt,
    description="Files landed but not yet loaded into ClickHouse stays bounded (landing isn't outrunning the loader).",
)
def parquet_backlog_reasonable() -> AssetCheckResult:
    config = OrchestrationConfig.from_env()
    paths = ParquetPaths(base_dir=Path(config.parquet_dir))
    client = get_clickhouse_client(config)

    already_loaded = list_loaded_files(client)
    backlog = len(find_new_files(paths, already_loaded))

    return AssetCheckResult(
        passed=backlog <= MAX_BACKLOG_FILES,
        metadata={"backlog_files": backlog, "threshold": MAX_BACKLOG_FILES},
    )


@asset_check(
    asset=raw_nrt,
    description=(
        "Redpanda consumer lag between bridge (producer) and landing (consumer) stays "
        "bounded -- catches landing falling behind the live SSE stream before it shows "
        "up as a Parquet backlog."
    ),
)
def redpanda_consumer_lag_reasonable() -> AssetCheckResult:
    config = OrchestrationConfig.from_env()
    lag = get_consumer_lag(
        bootstrap_servers=config.kafka_bootstrap_servers,
        group_id=config.kafka_group_id,
        topic=config.kafka_topic,
    )

    return AssetCheckResult(
        passed=lag.total_lag <= MAX_CONSUMER_LAG,
        metadata={
            "consumer_lag": lag.total_lag,
            "threshold": MAX_CONSUMER_LAG,
            "per_partition": str(lag.partition_lags),
        },
    )
