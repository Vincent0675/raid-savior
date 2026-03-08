import pytest
import time
import requests
from datetime import datetime, timezone

from src.generators.raid_event_generator import WoWEventGenerator
from src.etl.bronze_to_silver import BronzeToSilverETL

TEST_RAID_ID = "raid666"
NUM_EVENTOS = 10
RECEPTOR_URL = "http://localhost:5000/events"


@pytest.fixture(scope="session")
def eventos():
    generator = WoWEventGenerator(seed=42)
    session = generator.generate_raid_session(
        raid_id=TEST_RAID_ID,
        num_players=20,
        duration_s=60,
    )
    return generator.generate_events(session, num_events=NUM_EVENTOS)


@pytest.fixture(scope="session")
def batch_id(eventos):
    payload = [e.model_dump(mode="json") for e in eventos]
    response = requests.post(RECEPTOR_URL, json=payload, timeout=5)
    assert response.status_code == 201, (
        f"Ingesta fallida: {response.status_code} — {response.text}"
    )
    return response.json()["batch_id"]


@pytest.fixture(scope="session")
def s3_path(batch_id):
    time.sleep(2)  # dar margen a MinIO
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bronze_key = (
        f"wow_raid_events/v1/raidid={TEST_RAID_ID}"
        f"/ingest_date={ingest_date}/batch_{batch_id}.json"
    )
    etl = BronzeToSilverETL()
    result = etl.run(bronze_key)
    assert result["status"] == "success", f"ETL fallido: {result.get('reason')}"
    return result["storage"]["s3_path"]
