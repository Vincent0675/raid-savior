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


settings = APISettings()
