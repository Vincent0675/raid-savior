#!/usr/bin/env python3
"""
Generate LARGE-SCALE synthetic WoW raid telemetry dataset.
"""

from pathlib import Path
import json
from datetime import datetime

# ✅ CAMBIO: Usar la clase actualizada (mismo nombre, nuevo código)
from src.generators.raid_event_generator import WoWEventGenerator  # ← No cambiar nombre


def main():
    print("=" * 70)
    print("WoW Telemetry Pipeline - LARGE DATASET Generator v2")
    print("=" * 70)
    print()
    
    # Configuration
    SEED = 42
    NUM_EVENTS = 100_000
    RAID_ID = "raid001"
    NUM_PLAYERS = 25
    DURATION_SECONDS = 480
    BOSS_NAME = "Lich King"
    
    print("Configuration:")
    print(f"  Seed: {SEED} (reproducible)")
    print(f"  Target events: {NUM_EVENTS:,}")
    print(f"  Raid ID: {RAID_ID}")
    print(f"  Players: {NUM_PLAYERS}")
    print(f"  Duration: {DURATION_SECONDS}s ({DURATION_SECONDS//60}m {DURATION_SECONDS%60}s)")
    print(f"  Boss: {BOSS_NAME}")
    print()
    
    # Initialize generator
    print("Initializing enhanced generator...")
    gen = WoWEventGenerator(seed=SEED)  # ← Mismo nombre de clase
    
    # Generate raid session
    print("Generating raid session with boss phases...")
    session = gen.generate_raid_session(
        raid_id=RAID_ID,
        num_players=NUM_PLAYERS,
        duration_s=DURATION_SECONDS,
        boss_name=BOSS_NAME,
    )
    
    print(f"  Raid: {session.raid_id}")
    print(f"  Boss: {session.boss_name}")
    print(f"  Players: {len(session.players)}")
    print(f"  Phases: {len(session.phases)}")
    for phase in session.phases:
        print(f"    - {phase.phase_name} ({phase.duration_s}s)")
    print()
    
    # Generate events
    print(f"Generating {NUM_EVENTS:,} events with realistic patterns...")
    events = gen.generate_events(session=session, num_events=NUM_EVENTS)
    
    # Stats
    event_counts = {}
    for e in events:
        event_counts[e.event_type.value] = event_counts.get(e.event_type.value, 0) + 1
    
    print(f"\n  Events generated: {len(events):,}")
    for etype, count in sorted(event_counts.items()):
        pct = count / len(events) * 100
        print(f"    {etype:20s}: {count:6d} ({pct:5.2f}%)")
    print(f"  Pydantic validation: 100% (schema-on-write)")
    print()
    
    # Prepare output
    out_path = Path("data/bronze/datos_generados_large.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Serialize
    print("Serializing events to JSON...")
    payload = [e.model_dump(mode="json") for e in events]
    
    # Write
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    
    # Stats
    file_size_mb = out_path.stat().st_size / (1024 * 1024)
    
    print()
    print("=" * 70)
    print("LARGE DATASET GENERATED SUCCESSFULLY")
    print("=" * 70)
    print(f"Path: {out_path}")
    print(f"Size: {file_size_mb:.2f} MB JSON (esperado: ~{file_size_mb * 0.15:.2f} MB Parquet)")
    print(f"Events: {len(events):,}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    print("✅ Ready for Phase 2->3->4 pipeline (Bronze → Silver → Gold)")
    print("=" * 70)


if __name__ == "__main__":
    main()
