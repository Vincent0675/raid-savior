import src.api.services.iceberg_service as iceberg_module
from src.api.services.iceberg_service import IcebergService


def test_expected_tables_parsing(monkeypatch):
    service = IcebergService()

    class _Settings:
        iceberg_expected_tables = "gold.dim_player, silver.raid_events ,gold.fact_raid_summary"
        iceberg_rest_uri = "http://localhost:8181"

    monkeypatch.setattr(iceberg_module.IcebergService, "_settings", staticmethod(lambda: _Settings()))
    parsed = service._expected_tables()
    assert parsed == {
        "gold.dim_player",
        "silver.raid_events",
        "gold.fact_raid_summary",
    }


def test_readiness_reports_missing_tables(monkeypatch):
    service = IcebergService()

    class _Settings:
        iceberg_rest_uri = "http://localhost:8181"

    monkeypatch.setattr(iceberg_module.IcebergService, "_settings", staticmethod(lambda: _Settings()))

    monkeypatch.setattr(service, "_expected_tables", lambda: {"gold.dim_player", "gold.dim_raid"})
    monkeypatch.setattr(service, "_validate_rest_catalog", lambda: None)
    monkeypatch.setattr(service, "_list_catalog_tables", lambda: {"gold.dim_player"})

    readiness = service.check_readiness()

    assert readiness["ready"] is False
    assert readiness["missing_tables"] == ["gold.dim_raid"]
    assert readiness["errors"]
