import asyncio

from fastapi import HTTPException
import pytest

from src.api.routes import health


class _StubReadyService:
    def check_readiness(self):
        return {
            "ready": True,
            "errors": [],
            "missing_tables": [],
            "catalog_tables": ["gold.dim_player"],
            "expected_tables": ["gold.dim_player"],
            "catalog_uri": "http://localhost:8181",
        }


class _StubNotReadyService:
    def check_readiness(self):
        return {
            "ready": False,
            "errors": ["REST catalog no accesible"],
            "missing_tables": ["gold.dim_raid"],
            "catalog_tables": [],
            "expected_tables": ["gold.dim_raid"],
            "catalog_uri": "http://localhost:8181",
        }


def test_readiness_ready_response(monkeypatch):
    monkeypatch.setattr(health, "iceberg_service", _StubReadyService())
    response = asyncio.run(health.readiness())
    assert response["status"] == "ready"
    assert response["ready"] is True


def test_readiness_503(monkeypatch):
    monkeypatch.setattr(health, "iceberg_service", _StubNotReadyService())
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(health.readiness())

    exc = exc_info.value
    assert exc.status_code == 503
    assert exc.detail["ready"] is False
