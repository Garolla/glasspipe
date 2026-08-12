from datetime import date, timedelta
from pathlib import Path

from dagster import AssetExecutionContext, MetadataValue, asset

from glasspipe_common.paths import ParquetPaths

from orchestration.config import OrchestrationConfig
from orchestration.external_assets import batch_raw_events, parquet_raw_events
from orchestration.raw_loader import get_clickhouse_client, load_new_files
from orchestration.sqlmesh_runner import run_sqlmesh_plan


@asset(
    deps=[parquet_raw_events],
    group_name="ingestion",
    description=(
        "Incremental, idempotent load of new Parquet files into raw.raw_nrt. "
        "Plain Python, not a SQLMesh model -- see transform/README.md."
    ),
)
def raw_nrt(context: AssetExecutionContext) -> None:
    config = OrchestrationConfig.from_env()
    paths = ParquetPaths(base_dir=Path(config.parquet_dir))
    client = get_clickhouse_client(config)

    since = date.today() - timedelta(days=1)
    result = load_new_files(client, paths, since=since)
    context.add_output_metadata(
        {
            "files_loaded": result.files_loaded,
            "rows_loaded": result.rows_loaded,
        }
    )


def _make_sqlmesh_asset(*, asset_key: str, model_fqn: str, deps: list, group_name: str, description: str):
    @asset(name=asset_key, deps=deps, group_name=group_name, description=description)
    def _sqlmesh_asset(context: AssetExecutionContext) -> None:
        config = OrchestrationConfig.from_env()
        result = run_sqlmesh_plan(config.sqlmesh_project_dir, select_model=model_fqn)

        context.add_output_metadata(
            {
                "sqlmesh_model": model_fqn,
                "success": result.success,
                "output_tail": MetadataValue.text((result.stdout or "")[-2000:]),
            }
        )

        if not result.success:
            raise RuntimeError(f"sqlmesh plan failed for {model_fqn}:\n{result.stderr}")

    return _sqlmesh_asset


stg_events_nrt = _make_sqlmesh_asset(
    asset_key="stg_events_nrt",
    model_fqn="staging.stg_events_nrt",
    deps=[raw_nrt],
    group_name="transform",
    description="Dedup view over raw.raw_nrt (SQLMesh model staging.stg_events_nrt).",
)

stg_events_batch = _make_sqlmesh_asset(
    asset_key="stg_events_batch",
    model_fqn="staging.stg_events_batch",
    deps=[batch_raw_events],
    group_name="transform",
    description="Pass-through view over raw.raw_batch (SQLMesh model staging.stg_events_batch).",
)

edits_hourly = _make_sqlmesh_asset(
    asset_key="edits_hourly",
    model_fqn="marts.edits_hourly",
    deps=[stg_events_nrt],
    group_name="marts",
    description="NRT lane hourly edit aggregate (SQLMesh model marts.edits_hourly).",
)

pageviews_top = _make_sqlmesh_asset(
    asset_key="pageviews_top",
    model_fqn="marts.pageviews_top",
    deps=[stg_events_batch],
    group_name="marts",
    description="Batch lane top-pageviews mart (SQLMesh model marts.pageviews_top). Not reconciled with edits_hourly -- principle 5.",
)
