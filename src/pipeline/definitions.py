from dagster import Definitions, load_assets_from_modules
from src.pipeline import assets
from src.pipeline.jobs import medallion_pipeline_job
from src.pipeline.schedules import medallion_weekly_schedule

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    jobs=[medallion_pipeline_job],
    schedules=[medallion_weekly_schedule],
)
