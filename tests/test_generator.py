# tests/test_generator.py

from src.generators.raid_event_generator import WoWEventGenerator
from src.schemas.eventos_schema import WoWRaidEvent


def test_generate_events_all_validate():
    gen = WoWEventGenerator(seed=42)
    session = gen.generate_raid_session(raid_id="raid001", num_players=20, duration_s=60)
    events = gen.generate_events(session=session, num_events=1000)

    assert len(events) == 1000
    assert all(isinstance(e, WoWRaidEvent) for e in events)


def test_timestamps_sorted():
    gen = WoWEventGenerator(seed=42)
    session = gen.generate_raid_session(raid_id="raid001", num_players=20, duration_s=60)
    events = gen.generate_events(session=session, num_events=500)

    for i in range(len(events) - 1):
        assert events[i].timestamp <= events[i + 1].timestamp


def test_event_id_unique():
    gen = WoWEventGenerator(seed=42)
    session = gen.generate_raid_session(raid_id="raid001", num_players=20, duration_s=60)
    events = gen.generate_events(session=session, num_events=1000)

    ids = [e.event_id for e in events]
    assert len(set(ids)) == len(ids)
