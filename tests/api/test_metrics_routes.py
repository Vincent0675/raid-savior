import asyncio

from fastapi import HTTPException
import pytest

from src.api.routes import metrics


class _StubMetricsService:
    def get_global_metrics(self):
        return {
            "total_raids": 10,
            "success_raids": 7,
            "wipe_raids": 3,
            "wipe_rate_pct": 30.0,
            "avg_raid_dps": 12345.6,
        }


class _StubFailingService:
    def get_global_metrics(self):
        raise RuntimeError("boom")


def test_global_metrics_ok(monkeypatch):
    monkeypatch.setattr(metrics, "iceberg_service", _StubMetricsService())
    response = asyncio.run(metrics.global_metrics())
    assert response["total_raids"] == 10
    assert response["wipe_rate_pct"] == 30.0


def test_global_metrics_catalog_unavailable(monkeypatch):
    monkeypatch.setattr(metrics, "iceberg_service", _StubFailingService())
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(metrics.global_metrics())

    exc = exc_info.value
    assert exc.status_code == 503
    assert "Catalog unavailable" in exc.detail
