from dagster import define_asset_job, AssetSelection

medallion_pipeline_job = define_asset_job(
    name="medallion_pipeline_job",
    selection=AssetSelection.groups("medallion"),
    description="Pipeline completo Bronze → Silver → Gold (Medallion Architecture)",
)
