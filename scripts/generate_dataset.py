#!/usr/bin/env python3
"""
Generate synthetic WoW raid telemetry dataset for Bronze layer.
Uses Pydantic validation (schema-on-write) and exports to JSON.

Phase: 1 (Schema + Synthetic Data Generation)
Output: data/bronze/datos_generados.json
"""

from pathlib import Path
import json
from datetime import datetime

from src.generators.raid_event_generator import WoWEventGenerator


def main():
    print("=" * 70)
    print("WoW Telemetry Pipeline - Synthetic Dataset Generator")
    print("=" * 70)
    print()
    
    # Configuration
    SEED = 42
    NUM_EVENTS = 2000
    RAID_ID = "raid001"
    NUM_PLAYERS = 20
    DURATION_SECONDS = 300
    
    print("Configuration:")
    print(f"  Seed: {SEED} (reproducible)")
    print(f"  Target events: {NUM_EVENTS}")
    print(f"  Raid ID: {RAID_ID}")
    print(f"  Players: {NUM_PLAYERS}")
    print(f"  Duration: {DURATION_SECONDS}s")
    print()
    
    # Initialize generator
    print("Initializing generator...")
    gen = WoWEventGenerator(seed=SEED)
    
    # Generate raid session
    print("Generating raid session...")
    session = gen.generate_raid_session(
        raid_id=RAID_ID,
        num_players=NUM_PLAYERS,
        duration_s=DURATION_SECONDS
    )
    
    print(f"  Raid: {session.raid_id}")
    print(f"  Boss: {session.boss_name}")
    print(f"  Players: {len(session.players)}")
    print()
    
    # Generate events
    print(f"Generating {NUM_EVENTS} events with NumPy distributions...")
    events = gen.generate_events(session=session, num_events=NUM_EVENTS)
    
    # Quick stats
    damage_count = sum(1 for e in events if 'DAMAGE' in str(e.event_type).upper())
    heal_count = sum(1 for e in events if 'HEAL' in str(e.event_type).upper())
    
    print(f"  Events generated: {len(events)}")
    print(f"  Damage: {damage_count} ({damage_count/len(events)*100:.1f}%)")
    print(f"  Heal: {heal_count} ({heal_count/len(events)*100:.1f}%)")
    print(f"  Pydantic validation: 100% (schema-on-write)")
    print()
    
    # Prepare output
    out_path = Path("data/bronze/datos_generados.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Serialize with Pydantic v2 (mode="json" converts UUID/datetime)
    print("Serializing events to JSON...")
    payload = [e.model_dump(mode="json") for e in events]
    
    # Write JSON with readable format
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    
    # File stats
    file_size_mb = out_path.stat().st_size / (1024 * 1024)
    
    print()
    print("=" * 70)
    print("DATASET GENERATED SUCCESSFULLY")
    print("=" * 70)
    print(f"Path: {out_path}")
    print(f"Size: {file_size_mb:.2f} MB")
    print(f"Events: {len(events)}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    print("Bronze layer ready for Phase 2 (MinIO ingestion)")
    print("=" * 70)


if __name__ == "__main__":
    main()
