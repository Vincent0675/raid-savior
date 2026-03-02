from dagster import ScheduleDefinition, DefaultScheduleStatus
from src.pipeline.jobs import medallion_pipeline_job

medallion_weekly_schedule = ScheduleDefinition(
    name="medallion_weekly_schedule",
    job=medallion_pipeline_job,
    cron_schedule="0 6 * * 0",           # domingos a las 06:00 UTC
    description="Pipeline Medallion completo — domingos 06:00 UTC",
    default_status=DefaultScheduleStatus.RUNNING,  # activo al arrancar dagster
)
