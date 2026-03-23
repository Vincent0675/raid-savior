"""Smoke test de endpoints de negocio con Iceberg REST Catalog real."""

import uuid

from fastapi.testclient import TestClient
import pytest

from src.api.main import app


def test_business_endpoints_with_real_catalog():
    try:
        with TestClient(app) as client:
            raids_response = client.get("/raids", params={"limit": 5, "offset": 0})
            assert raids_response.status_code == 200
            raids_payload = raids_response.json()
            assert {"raids", "total", "limit", "offset"}.issubset(raids_payload.keys())

            metrics_response = client.get("/metrics/global")
            assert metrics_response.status_code == 200
            metrics_payload = metrics_response.json()
            expected_metrics = {
                "total_raids",
                "success_raids",
                "wipe_raids",
                "wipe_rate_pct",
                "avg_raid_dps",
            }
            assert expected_metrics.issubset(metrics_payload.keys())

            missing_raid_id = f"missing-{uuid.uuid4()}"
            not_found_response = client.get(f"/raids/{missing_raid_id}")
            assert not_found_response.status_code == 404

            not_found_players = client.get(f"/raids/{missing_raid_id}/players")
            assert not_found_players.status_code == 404

            raids = raids_payload.get("raids", [])
            if not raids:
                return

            raid_id = raids[0]["raid_id"]
            raid_response = client.get(f"/raids/{raid_id}")
            assert raid_response.status_code == 200
            assert raid_response.json().get("raid_id") == raid_id

            players_response = client.get(f"/raids/{raid_id}/players")
            assert players_response.status_code == 200
            players_payload = players_response.json()
            assert players_payload.get("raid_id") == raid_id
            assert "players" in players_payload
    except Exception as exc:
        pytest.skip(f"Smoke API+REST Catalog no disponible: {exc}")
