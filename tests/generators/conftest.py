# tests/generators/conftest.py
import pytest
from datetime import datetime, timezone, timedelta
from src.generators.raid_event_generator import WoWEventGenerator, BossPhase

@pytest.fixture(scope="module")   # ← "module": se crea UNA vez por archivo de test
def generator():
    return WoWEventGenerator(seed=42)

@pytest.fixture(scope="module")   # ← depende de generator, mismo scope
def session(generator):
    return generator.generate_raid_session(raid_id="raid001", num_players=20)

@pytest.fixture                   # ← sin scope: se recrea en cada test que la pide
def dummy_phase():
    return BossPhase(
        phase_number=1, phase_name="Test", duration_s=300,
        damage_multiplier=1.0, healing_multiplier=1.0, death_probability=0.0
    )

@pytest.fixture
def past_timestamp():
    return datetime.now(timezone.utc) - timedelta(hours=1)

@pytest.fixture(scope="module")
def events(generator, session):
    return generator.generate_events(session, num_events=1000)