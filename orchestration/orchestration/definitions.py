from dagster import Definitions

from orchestration.assets import edits_hourly, pageviews_top, raw_nrt, stg_events_batch, stg_events_nrt
from orchestration.checks import parquet_backlog_reasonable
from orchestration.external_assets import batch_raw_events, bridge_heartbeat, parquet_raw_events
from orchestration.jobs import cleanup_parquet_job
from orchestration.schedules import cleanup_parquet_schedule
from orchestration.sensors import batch_raw_events_sensor, bridge_heartbeat_sensor, parquet_landing_sensor

defs = Definitions(
    assets=[
        bridge_heartbeat,
        parquet_raw_events,
        batch_raw_events,
        raw_nrt,
        stg_events_nrt,
        stg_events_batch,
        edits_hourly,
        pageviews_top,
    ],
    asset_checks=[parquet_backlog_reasonable],
    sensors=[bridge_heartbeat_sensor, parquet_landing_sensor, batch_raw_events_sensor],
    jobs=[cleanup_parquet_job],
    schedules=[cleanup_parquet_schedule],
)
