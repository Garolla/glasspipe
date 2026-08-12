from dagster import DefaultScheduleStatus, ScheduleDefinition

from orchestration.jobs import cleanup_parquet_job, pipeline_job

cleanup_parquet_schedule = ScheduleDefinition(
    job=cleanup_parquet_job,
    cron_schedule="10 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Hourly at :10, ten minutes after pipeline_job_schedule's :00 -- delete "
        "confirmed-loaded Parquet files past retention. Was daily and STOPPED by "
        "default, which let the buffer grow unbounded since nothing ever cleared it."
    ),
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
