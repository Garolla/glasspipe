from dagster import ScheduleDefinition

from orchestration.jobs import cleanup_parquet_job

cleanup_parquet_schedule = ScheduleDefinition(
    job=cleanup_parquet_job,
    cron_schedule="0 3 * * *",
    description="Daily at 03:00 -- delete confirmed-loaded Parquet files past retention.",
)
