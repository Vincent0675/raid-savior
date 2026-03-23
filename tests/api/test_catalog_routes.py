import asyncio

from fastapi import HTTPException
import pytest

from src.api.routes import catalog


class _StubCatalogService:
    def list_namespaces(self):
        return ["gold", "silver"]

    def list_catalog_tables(self):
        return ["gold.dim_player", "gold.fact_raid_summary"]


class _StubFailingCatalogService:
    def list_namespaces(self):
        raise RuntimeError("boom")

    def list_catalog_tables(self):
        raise RuntimeError("boom")


def test_list_namespaces_ok(monkeypatch):
    monkeypatch.setattr(catalog, "iceberg_service", _StubCatalogService())
    response = asyncio.run(catalog.list_namespaces())
    assert response == {"namespaces": ["gold", "silver"]}


def test_list_tables_ok(monkeypatch):
    monkeypatch.setattr(catalog, "iceberg_service", _StubCatalogService())
    response = asyncio.run(catalog.list_tables())
    assert response == {"tables": ["gold.dim_player", "gold.fact_raid_summary"]}


def test_catalog_unavailable(monkeypatch):
    monkeypatch.setattr(catalog, "iceberg_service", _StubFailingCatalogService())
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(catalog.list_tables())

    exc = exc_info.value
    assert exc.status_code == 503
    assert "Catalog unavailable" in exc.detail
