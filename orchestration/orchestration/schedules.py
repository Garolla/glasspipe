from dagster import DefaultScheduleStatus, ScheduleDefinition

from orchestration.jobs import cleanup_parquet_job, pipeline_job

cleanup_parquet_schedule = ScheduleDefinition(
    job=cleanup_parquet_job,
    cron_schedule="0 3 * * *",
    description="Daily at 03:00 -- delete confirmed-loaded Parquet files past retention.",
)

pipeline_schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="0 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Hourly, on the hour -- matches edits_hourly's own cron "
        "(transform/config.yaml model_defaults.cron). Starts enabled: "
        "without this, nothing ever triggered the pipeline job "
        "automatically, only manual `dagster asset materialize` runs did."
    ),
)
