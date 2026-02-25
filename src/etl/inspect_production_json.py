"""
Inspecciona el dataset de producción organizado como:

  data/bronze/
  ├── raid001/
  │   ├── batch_000.json
  │   ├── batch_001.json
  │   └── ... (101 archivos × 500 eventos)
  ├── raid002/
  └── ...

Maneja dos formatos de JSON:
  - Lista plana:   [{event_id: ..., event_type: ...}, ...]
  - Batch object:  {"batch_id": ..., "events": [{...}, ...]}
"""

import json
from pathlib import Path
from collections import Counter, defaultdict

# ─── CONFIGURA ESTE PATH ────────────────────────────────────────────────────
BRONZE_ROOT = Path("data/bronze/production")   # ← ajusta si tu carpeta tiene otro nombre
# ────────────────────────────────────────────────────────────────────────────


def load_events_from_file(filepath: Path) -> list[dict]:
    """Carga eventos de un JSON, sea lista plana o batch object."""
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data                         # formato lista plana
    if isinstance(data, dict) and "events" in data:
        return data["events"]               # formato batch object
    # Fallback: intenta tratar el dict como evento único
    return [data]


def format_pct(n: int, total: int) -> str:
    return f"{n/total*100:5.1f}%" if total else "  N/A"


def inspect_raid(raid_dir: Path) -> dict:
    """Procesa todos los JSON de una carpeta raid y devuelve stats."""
    json_files = sorted(raid_dir.glob("*.json"))
    if not json_files:
        return {}

    total_events   = 0
    type_counter   = Counter()
    class_counter  = Counter()
    role_counter   = Counter()
    damage_values  = []
    heal_values    = []
    boss_phases    = 0
    file_count     = len(json_files)

    for jf in json_files:
        events = load_events_from_file(jf)
        total_events += len(events)

        for e in events:
            etype = e.get("event_type", "unknown")
            type_counter[etype] += 1

            src_class = e.get("source_player_class")
            if src_class:
                class_counter[src_class] += 1

            src_role = e.get("source_player_role")
            if src_role:
                role_counter[src_role] += 1

            if etype == "combat_damage" and e.get("damage_amount"):
                damage_values.append(float(e["damage_amount"]))

            if etype == "heal" and e.get("healing_amount"):
                heal_values.append(float(e["healing_amount"]))

            if etype == "boss_phase":
                boss_phases += 1

    return {
        "raid_id":       raid_dir.name,
        "file_count":    file_count,
        "total_events":  total_events,
        "type_counter":  type_counter,
        "class_counter": class_counter,
        "role_counter":  role_counter,
        "damage_values": damage_values,
        "heal_values":   heal_values,
        "boss_phases":   boss_phases,
    }


def print_separator(char="─", width=60):
    print(char * width)


def main():
    if not BRONZE_ROOT.exists():
        print(f"❌ No encontré la carpeta: {BRONZE_ROOT.resolve()}")
        print("   Ajusta BRONZE_ROOT en el script.")
        return

    # Descubrir subdirectorios de raids
    raid_dirs = sorted(d for d in BRONZE_ROOT.iterdir() if d.is_dir())

    if not raid_dirs:
        # Quizás los JSON están directamente en BRONZE_ROOT sin subcarpetas
        raid_dirs = [BRONZE_ROOT]

    print_separator("═")
    print(f"  WoW Raid Telemetry — Inspección Dataset de Producción")
    print(f"  Root: {BRONZE_ROOT.resolve()}")
    print(f"  Raids encontradas: {len(raid_dirs)}")
    print_separator("═")

    # ── Acumuladores globales ──────────────────────────────────────────────
    global_events  = 0
    global_types   = Counter()
    global_classes = Counter()
    global_roles   = Counter()
    global_damage  = []
    global_heal    = []

    # ── Por raid ──────────────────────────────────────────────────────────
    for raid_dir in raid_dirs:
        stats = inspect_raid(raid_dir)
        if not stats:
            print(f"  ⚠️  {raid_dir.name}: sin archivos JSON")
            continue

        n = stats["total_events"]
        global_events  += n
        global_types   += stats["type_counter"]
        global_classes += stats["class_counter"]
        global_roles   += stats["role_counter"]
        global_damage  += stats["damage_values"]
        global_heal    += stats["heal_values"]

        print(f"\n  📁 {stats['raid_id']}  "
              f"({stats['file_count']} archivos · {n:,} eventos)")

        for etype, count in sorted(stats["type_counter"].items(),
                                   key=lambda x: -x[1]):
            print(f"     {etype:<25} {count:>7,}  {format_pct(count, n)}")

        print(f"     boss_phase transitions: {stats['boss_phases']}")

    # ── Resumen global ─────────────────────────────────────────────────────
    print()
    print_separator("═")
    print(f"  RESUMEN GLOBAL — {global_events:,} eventos totales")
    print_separator("═")

    print("\n  Distribución por event_type:")
    for etype, count in sorted(global_types.items(), key=lambda x: -x[1]):
        bar = "█" * int(count / global_events * 40)
        print(f"   {etype:<25} {count:>8,}  {format_pct(count, global_events)}  {bar}")

    print("\n  Distribución por rol (source_player_role):")
    for role, count in sorted(global_roles.items(), key=lambda x: -x[1]):
        print(f"   {role:<12} {count:>8,}  {format_pct(count, sum(global_roles.values()))}")

    print("\n  Top 10 clases (source_player_class):")
    for cls, count in global_classes.most_common(10):
        print(f"   {cls:<15} {count:>8,}  {format_pct(count, sum(global_classes.values()))}")

    # ── Stats numéricas de damage y heal ──────────────────────────────────
    if global_damage:
        import statistics
        d_mean  = statistics.mean(global_damage)
        d_stdev = statistics.stdev(global_damage)
        d_min   = min(global_damage)
        d_max   = max(global_damage)
        print(f"\n  damage_amount  →  mean={d_mean:,.0f}  σ={d_stdev:,.0f}"
              f"  min={d_min:,.0f}  max={d_max:,.0f}")

    if global_heal:
        import statistics
        h_mean  = statistics.mean(global_heal)
        h_stdev = statistics.stdev(global_heal)
        print(f"  healing_amount →  mean={h_mean:,.0f}  σ={h_stdev:,.0f}")

    # ── Validación rápida (checkpoints) ───────────────────────────────────
    print()
    print_separator()
    print("  CHECKPOINTS")
    print_separator()

    checks = [
        (len(raid_dirs) == 10,
         f"10 raids detectadas          → {len(raid_dirs)} encontradas"),
        (global_events >= 490_000,
         f"~500k eventos totales        → {global_events:,}"),
        ("combat_damage" in global_types,
         "Tipo 'combat_damage' presente"),
        (global_types.get("combat_damage", 0) / max(global_events, 1) > 0.40,
         f"combat_damage > 40%          → {format_pct(global_types.get('combat_damage',0), global_events)}"),
        (global_types.get("boss_phase", 0) >= 10,
         f"boss_phase >= 10 (1+/raid)   → {global_types.get('boss_phase', 0)}"),
    ]

    all_ok = True
    for ok, msg in checks:
        icon = "✅" if ok else "❌"
        print(f"  {icon}  {msg}")
        if not ok:
            all_ok = False

    print()
    if all_ok:
        print("  ✅ Dataset válido. Listo para ingesta en Bronze (MinIO).")
    else:
        print("  ⚠️  Revisa los checkpoints fallidos antes de continuar.")
    print()


if __name__ == "__main__":
    main()