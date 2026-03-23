from functools import lru_cache

from pydantic_settings import BaseSettings
from pydantic import Field


class APISettings(BaseSettings):
    # --- Entorno ---
    environment: str = Field(
        "development", description="development | staging | production"
    )
    debug: bool = Field(False, description="Modo debug de FastAPI")

    # --- MinIO / S3 ---
    s3_endpoint_url: str = Field(
        ..., description="URL del endpoint MinIO, e.g. http://localhost:9000"
    )
    s3_access_key: str = Field(..., description="MinIO access key")
    s3_secret_key: str = Field(..., description="MinIO secret key")
    s3_bucket_gold: str = Field("gold", description="Bucket de la capa Gold")

    # --- Iceberg ---
    warehouse_bucket: str = Field(..., description="Bucket raíz del catálogo Hadoop")
    iceberg_rest_uri: str = Field(
        "http://localhost:8181", description="URI del Iceberg REST Catalog"
    )
    iceberg_catalog_name: str = Field("wow", description="Nombre del catálogo Iceberg")
    iceberg_expected_tables: str = Field(
        "gold.dim_player,gold.dim_raid,gold.fact_player_raid_stats,gold.fact_raid_summary,silver.raid_events",
        description="Lista CSV namespace.table esperada en catálogo",
    )
    readiness_timeout_seconds: int = Field(
        5, description="Timeout para validaciones de readiness"
    )

    # --- API ---
    api_port: int = Field(8000, description="Puerto de escucha del servidor")
    workers: int = Field(2, description="Número de workers Gunicorn")
    timeout: int = Field(120, description="Timeout de query en segundos")

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
    }


@lru_cache
def get_settings() -> APISettings:
    return APISettings()


def clear_settings_cache() -> None:
    get_settings.cache_clear()
