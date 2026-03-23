import asyncio

from fastapi import HTTPException
import pytest

from src.api.routes import raids


class _StubRaidsService:
    def list_raids(self, limit=50, offset=0):
        return {
            "raids": [{"raid_id": "raid001", "raid_outcome": "success"}],
            "total": 1,
            "limit": limit,
            "offset": offset,
        }

    def get_raid_summary(self, raid_id):
        if raid_id == "raid001":
            return {"raid_id": "raid001", "raid_outcome": "success"}
        return None

    def list_raid_players(self, raid_id):
        if raid_id == "raid001":
            return [{"player_id": "p1", "damage_total": 1000.0}]
        return []


class _StubFailingService:
    def list_raids(self, limit=50, offset=0):
        raise RuntimeError("boom")

    def get_raid_summary(self, raid_id):
        raise RuntimeError("boom")

    def list_raid_players(self, raid_id):
        raise RuntimeError("boom")


def test_list_raids_ok(monkeypatch):
    monkeypatch.setattr(raids, "iceberg_service", _StubRaidsService())
    response = asyncio.run(raids.list_raids(limit=10, offset=0))
    assert response["total"] == 1
    assert response["raids"][0]["raid_id"] == "raid001"


def test_get_raid_ok(monkeypatch):
    monkeypatch.setattr(raids, "iceberg_service", _StubRaidsService())
    response = asyncio.run(raids.get_raid("raid001"))
    assert response["raid_id"] == "raid001"


def test_get_raid_not_found(monkeypatch):
    monkeypatch.setattr(raids, "iceberg_service", _StubRaidsService())
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(raids.get_raid("raid404"))

    exc = exc_info.value
    assert exc.status_code == 404


def test_get_raid_players_ok(monkeypatch):
    monkeypatch.setattr(raids, "iceberg_service", _StubRaidsService())
    response = asyncio.run(raids.get_raid_players("raid001"))
    assert response["raid_id"] == "raid001"
    assert len(response["players"]) == 1


def test_get_raid_players_not_found(monkeypatch):
    monkeypatch.setattr(raids, "iceberg_service", _StubRaidsService())
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(raids.get_raid_players("raid404"))

    exc = exc_info.value
    assert exc.status_code == 404


def test_raids_catalog_unavailable(monkeypatch):
    monkeypatch.setattr(raids, "iceberg_service", _StubFailingService())
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(raids.list_raids(limit=10, offset=0))

    exc = exc_info.value
    assert exc.status_code == 503
    assert "Catalog unavailable" in exc.detail
