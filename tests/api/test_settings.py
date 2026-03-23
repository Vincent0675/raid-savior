import pytest
from pydantic import ValidationError


def test_settings_fail_fast_sin_env_vars():
    """Sin variables requeridas, APISettings lanza ValidationError al instanciar."""
    from src.api.settings import APISettings

    with pytest.raises(ValidationError):
        APISettings(
            s3_endpoint_url=None,
            s3_access_key=None,
            s3_secret_key=None,
            warehouse_bucket=None,
        )


def test_settings_carga_desde_env(monkeypatch):
    """Con todas las variables en entorno, APISettings se instancia correctamente."""
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("S3_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("WAREHOUSE_BUCKET", "warehouse")

    from src.api.settings import APISettings

    s = APISettings()
    assert s.s3_endpoint_url == "http://minio:9000"
    assert s.s3_access_key == "minioadmin"
    assert s.workers == 2  # valor por defecto
    assert s.debug is False  # valor por defecto
    assert s.iceberg_rest_uri == "http://localhost:8181"


def test_settings_valores_por_defecto(monkeypatch):
    """Los campos con Field() tienen los valores por defecto correctos."""
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("S3_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("WAREHOUSE_BUCKET", "warehouse")

    from src.api.settings import APISettings

    s = APISettings()
    assert s.environment == "development"
    assert s.api_port == 8000
    assert s.timeout == 120
    assert s.s3_bucket_gold == "gold"
    assert s.iceberg_catalog_name == "wow"
    assert "gold.fact_raid_summary" in s.iceberg_expected_tables
