"""
Gold Layer Inspector — WoW Raid Telemetry Pipeline
===================================================
Inspecciona y valida las 4 tablas de la capa Gold para una partición dada.

Tablas inspeccionadas:
    - dim_player             -> Atributos de jugadores únicos
    - dim_raid               -> Metadatos del encuentro
    - fact_raid_summary      -> KPIs macro por raid
    - fact_player_raid_stats -> KPIs micro por jugador/raid

Uso:
    python src/analytics/inspect_gold.py --raid-id raid001 --event-date 2026-02-19

Autor: Byron V. Blatch Rodriguez
Version: 2.0 - Fase 4 (modelo semidimensional)
"""

import argparse
import io
import os
import sys

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.config import Config
from src.storage.minio_client import MinIOStorageClient


# ============================================================================
# UTILIDADES
# ============================================================================

def format_bytes(size_bytes: int) -> str:
    """Convierte bytes a formato legible (B, KB, MB)."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes / (1024 ** 2):.2f} MB"


def _separator(title: str = "", width: int = 70) -> None:
    print("\n" + "=" * width)
    if title:
        print(title)
        print("=" * width)


def _read_parquet(storage: MinIOStorageClient, bucket: str, key: str) -> pd.DataFrame:
    """Lee un Parquet desde MinIO y lo devuelve como DataFrame."""
    raw = storage.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(raw))


def _object_size(storage: MinIOStorageClient, bucket: str, key: str) -> int:
    """Devuelve el tamano en bytes de un objeto en MinIO."""
    response = storage.s3.head_object(Bucket=bucket, Key=key)
    return response["ContentLength"]


# ============================================================================
# SECCIONES DE INSPECCION
# ============================================================================

def inspect_dim_player(storage: MinIOStorageClient, bucket: str) -> pd.DataFrame:
    """Inspecciona dim_player: jugadores unicos, roles y clases."""
    key = "dim_player/player_id=all/dim_player.parquet"

    _separator("[ dim_player ] Registro de Jugadores")

    try:
        size = _object_size(storage, bucket, key)
        df   = _read_parquet(storage, bucket, key)

        print(f"  Ruta             : s3://{bucket}/{key}")
        print(f"  Tamano           : {format_bytes(size)}")
        print(f"  Jugadores unicos : {len(df)}")

        print("\n  Distribucion por rol:")
        for role, count in df["player_role"].value_counts().items():
            print(f"    {role:>15} : {count} jugador(es)")

        print("\n  Distribucion por clase:")
        for cls, count in df["player_class"].value_counts().items():
            print(f"    {cls:>15} : {count} jugador(es)")

        print("\n  Muestra (3 filas):")
        cols = ["player_id", "player_name", "player_class",
                "player_role", "first_seen_date", "last_seen_date", "total_raids"]
        print(df[cols].head(3).to_string(index=False))

        unknown = (df["player_role"] == "unknown").sum()
        if unknown > 0:
            print(f"\n  [WARN] {unknown} jugador(es) con rol 'unknown' "
                  f"(campos opcionales sin asignar en Silver)")

        return df

    except Exception as exc:
        print(f"  [ERROR] No se pudo leer dim_player: {exc}")
        return pd.DataFrame()


def inspect_dim_raid(
    storage: MinIOStorageClient, bucket: str, raid_id: str
) -> pd.DataFrame:
    """Inspecciona dim_raid: metadatos del encuentro."""
    key = f"dim_raid/raid_id={raid_id}/dim_raid.parquet"

    _separator(f"[ dim_raid ] Metadatos del Encuentro ({raid_id})")

    try:
        size = _object_size(storage, bucket, key)
        df   = _read_parquet(storage, bucket, key)
        row  = df.iloc[0]

        print(f"  Ruta   : s3://{bucket}/{key}")
        print(f"  Tamano : {format_bytes(size)}")

        print(f"\n  Datos del encuentro:")
        print(f"    Raid ID           : {row['raid_id']}")
        print(f"    Fecha             : {row['event_date']}")
        print(f"    Boss              : {row['boss_name']}")
        print(f"    Dificultad        : {row['difficulty']}")
        print(f"    Tamano de raid    : {int(row['raid_size'])} jugadores")
        dur_min = row["duration_target_ms"] / 60_000
        dur_seg = int(row["duration_target_ms"] / 1000)
        print(f"    Duracion objetivo : {dur_seg} seg ({dur_min:.1f} min)")

        return df

    except Exception as exc:
        print(f"  [ERROR] No se pudo leer dim_raid: {exc}")
        return pd.DataFrame()


def inspect_fact_raid_summary(
    storage: MinIOStorageClient, bucket: str, raid_id: str, event_date: str
) -> pd.DataFrame:
    """Inspecciona fact_raid_summary: KPIs macro del encuentro."""
    key = (f"fact_raid_summary/raid_id={raid_id}/"
           f"event_date={event_date}/fact_raid_summary.parquet")

    _separator(f"[ fact_raid_summary ] KPIs Macro ({raid_id})")

    try:
        size = _object_size(storage, bucket, key)
        df   = _read_parquet(storage, bucket, key)
        row  = df.iloc[0]

        print(f"  Ruta   : s3://{bucket}/{key}")
        print(f"  Tamano : {format_bytes(size)}")

        outcome_tag = "[SUCCESS]" if row["raid_outcome"] == "success" else "[WIPE]"
        print(f"\n  RESULTADO: {outcome_tag}")

        dur_seg = row["duration_ms"] / 1000
        dur_min = row["duration_ms"] / 60_000
        print(f"\n  Duracion real : {dur_seg:.1f} seg ({dur_min:.2f} min)")

        print(f"\n  Metricas de combate:")
        print(f"    Dano total     : {row['total_damage']:>15,.0f}")
        print(f"    Curacion total : {row['total_healing']:>15,.0f}")
        print(f"    Muertes        : {int(row['total_player_deaths'])}")
        print(f"    DPS global     : {row['raid_dps']:>12,.1f}")
        print(f"    HPS global     : {row['raid_hps']:>12,.1f}")
        print(f"    Boss HP minimo : {row['boss_min_hp_pct']:.2f}%")

        print(f"\n  Composicion:")
        print(f"    Jugadores : {int(row['n_players'])}")
        print(f"    Tanks     : {int(row['n_tanks'])}")
        print(f"    Healers   : {int(row['n_healers'])}")
        print(f"    DPS       : {int(row['n_dps'])}")

        return df

    except Exception as exc:
        print(f"  [ERROR] No se pudo leer fact_raid_summary: {exc}")
        return pd.DataFrame()


def inspect_fact_player_stats(
    storage: MinIOStorageClient, bucket: str, raid_id: str, event_date: str
) -> pd.DataFrame:
    """Inspecciona fact_player_raid_stats: KPIs por jugador con insights de negocio."""
    key = (f"fact_player_raid_stats/raid_id={raid_id}/"
           f"event_date={event_date}/fact_player_raid_stats.parquet")

    _separator(f"[ fact_player_raid_stats ] KPIs por Jugador ({raid_id})")

    try:
        size = _object_size(storage, bucket, key)
        df   = _read_parquet(storage, bucket, key)

        print(f"  Ruta   : s3://{bucket}/{key}")
        print(f"  Tamano : {format_bytes(size)}")
        print(f"  Filas  : {len(df)} jugadores")

        # Top 3 DPS
        top_dps = df.nlargest(3, "dps")[
            ["player_name", "player_class", "player_role", "dps", "damage_share"]
        ]
        print("\n  Top 3 por DPS:")
        for _, p in top_dps.iterrows():
            print(f"    {p['player_name']:>12} ({p['player_class']:>12}, {p['player_role']:>7})"
                  f" -> DPS: {p['dps']:>8,.1f} | share: {p['damage_share'] * 100:.1f}%")

        # Top 3 HPS (solo quienes curaron)
        healers = df[df["hps"] > 0]
        if not healers.empty:
            top_hps = healers.nlargest(3, "hps")[
                ["player_name", "player_class", "player_role", "hps", "healing_share"]
            ]
            print("\n  Top 3 por HPS:")
            for _, p in top_hps.iterrows():
                print(f"    {p['player_name']:>12} ({p['player_class']:>12}, {p['player_role']:>7})"
                      f" -> HPS: {p['hps']:>8,.1f} | share: {p['healing_share'] * 100:.1f}%")

        # Jugadores con mas muertes
        with_deaths = df[df["player_deaths"] > 0]
        if not with_deaths.empty:
            print("\n  Jugadores con mas muertes:")
            for _, p in with_deaths.nlargest(3, "player_deaths").iterrows():
                print(f"    {p['player_name']:>12} ({p['player_role']:>7})"
                      f" -> {int(p['player_deaths'])} muerte(s)")
        else:
            print("\n  Sin muertes registradas en esta raid.")

        # Top crit_rate entre jugadores activos
        active = df[(df["damage_events"] + df["healing_events"]) > 0]
        if not active.empty:
            top_crit = active.nlargest(3, "crit_rate")[["player_name", "crit_rate"]]
            print("\n  Top 3 por Tasa de Criticos:")
            for _, p in top_crit.iterrows():
                print(f"    {p['player_name']:>12} -> {p['crit_rate'] * 100:.1f}%")

        return df

    except Exception as exc:
        print(f"  [ERROR] No se pudo leer fact_player_raid_stats: {exc}")
        return pd.DataFrame()


# ============================================================================
# VERIFICACIONES DE COHERENCIA
# ============================================================================

def check_coherence(
    df_dim_player: pd.DataFrame,
    df_dim_raid: pd.DataFrame,
    df_fact_summary: pd.DataFrame,
    df_fact_players: pd.DataFrame,
) -> None:
    """
    Verifica la integridad referencial y coherencia de valores entre las 4 tablas.

    Checks aplicados:
        1. FK: player_ids en facts ⊆ dim_player
        2. FK: raid_id consistente entre dim_raid y facts
        3. damage_share total ≈ 1.0
        4. healing_share total ≈ 1.0 (si hay curacion)
        5. n_tanks + n_healers + n_dps <= n_players
        6. crit_rate en [0, 1]
        7. boss_min_hp_pct en [0, 100]
        8. DPS y HPS >= 0
    """
    _separator("[ Coherencia ] Verificaciones de Integridad")

    passed = 0
    failed = 0

    def ok(msg: str) -> None:
        nonlocal passed
        passed += 1
        print(f"  [OK]   {msg}")

    def fail(msg: str) -> None:
        nonlocal failed
        failed += 1
        print(f"  [FAIL] {msg}")

    def warn(msg: str) -> None:
        print(f"  [WARN] {msg}")

    # 1. FK: player_ids en facts ⊆ dim_player
    if not df_dim_player.empty and not df_fact_players.empty:
        dim_ids  = set(df_dim_player["player_id"])
        fact_ids = set(df_fact_players["player_id"])
        orphans  = fact_ids - dim_ids
        if orphans:
            fail(f"FK dim_player: {len(orphans)} player_id(s) sin entrada en dim_player -> {orphans}")
        else:
            ok("FK dim_player: todos los player_ids de facts existen en dim_player")

    # 2. FK: raid_id consistente
    if not df_dim_raid.empty and not df_fact_summary.empty:
        dim_rid  = df_dim_raid["raid_id"].iloc[0]
        fact_rid = df_fact_summary["raid_id"].iloc[0]
        if dim_rid == fact_rid:
            ok(f"FK dim_raid: raid_id '{fact_rid}' consistente entre todas las tablas")
        else:
            fail(f"FK dim_raid: dim='{dim_rid}' vs fact='{fact_rid}'")

    # 3. damage_share ≈ 1.0
    if not df_fact_players.empty:
        total_ds = df_fact_players["damage_share"].sum()
        if total_ds == 0.0:
            warn("damage_share total = 0.0 (sin eventos de dano en esta particion)")
        elif abs(total_ds - 1.0) < 0.01:
            ok(f"damage_share total ≈ 1.0 (actual: {total_ds:.6f})")
        else:
            fail(f"damage_share total = {total_ds:.6f} (esperado ≈ 1.0 +/- 0.01)")

    # 4. healing_share ≈ 1.0
    if not df_fact_players.empty:
        total_hs = df_fact_players["healing_share"].sum()
        if total_hs == 0.0:
            warn("healing_share total = 0.0 (sin eventos de curacion en esta particion)")
        elif abs(total_hs - 1.0) < 0.01:
            ok(f"healing_share total ≈ 1.0 (actual: {total_hs:.6f})")
        else:
            fail(f"healing_share total = {total_hs:.6f} (esperado ≈ 1.0 +/- 0.01)")

    # 5. Composicion de roles <= n_players
    if not df_fact_summary.empty:
        row         = df_fact_summary.iloc[0]
        total_roles = int(row["n_tanks"]) + int(row["n_healers"]) + int(row["n_dps"])
        n_players   = int(row["n_players"])
        if total_roles <= n_players:
            ok(f"Composicion: tanks+healers+dps ({total_roles}) <= n_players ({n_players})")
        else:
            fail(f"Composicion: tanks+healers+dps ({total_roles}) > n_players ({n_players})")

    # 6. crit_rate en [0, 1]
    if not df_fact_players.empty:
        bad_crit = df_fact_players[
            (df_fact_players["crit_rate"] < 0) | (df_fact_players["crit_rate"] > 1)
        ]
        if bad_crit.empty:
            ok("crit_rate: todos los valores en [0, 1]")
        else:
            fail(f"crit_rate fuera de [0, 1] en {len(bad_crit)} fila(s)")

    # 7. boss_min_hp_pct en [0, 100]
    if not df_fact_summary.empty:
        hp = df_fact_summary["boss_min_hp_pct"].iloc[0]
        if 0.0 <= hp <= 100.0:
            ok(f"boss_min_hp_pct = {hp:.2f}% (dentro de [0, 100])")
        else:
            fail(f"boss_min_hp_pct fuera de rango: {hp:.2f}%")

    # 8. DPS y HPS >= 0
    if not df_fact_players.empty:
        neg_dps = (df_fact_players["dps"] < 0).sum()
        neg_hps = (df_fact_players["hps"] < 0).sum()
        if neg_dps == 0 and neg_hps == 0:
            ok("DPS y HPS: todos los valores >= 0")
        else:
            fail(f"Valores negativos -> DPS: {neg_dps} caso(s), HPS: {neg_hps} caso(s)")

    # Resumen
    print(f"\n  {'─' * 48}")
    print(f"  Resultado: {passed} verificacion(es) pasada(s) | {failed} fallo(s)")
    if failed == 0:
        print("  Gold layer coherente y lista para consumo analitico.")
    else:
        print("  Revisa los fallos antes de usar Gold en dashboards o modelos.")


# ============================================================================
# CLI
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspector de la capa Gold -- WoW Raid Telemetry Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplo:\n"
            "  python src/analytics/inspect_gold.py --raid-id raid001 --event-date 2026-02-19"
        ),
    )
    parser.add_argument(
        "--raid-id",
        type=str,
        required=True,
        help="Identificador de la raid (ej. raid001)",
    )
    parser.add_argument(
        "--event-date",
        type=str,
        required=True,
        help="Fecha del evento en formato YYYY-MM-DD",
    )
    return parser.parse_args()


def main() -> None:
    args       = parse_args()
    raid_id    = args.raid_id
    event_date = args.event_date

    config  = Config()
    storage = MinIOStorageClient()
    bucket  = config.S3_BUCKET_GOLD

    print("\nINSPECTOR DE CAPA GOLD -- WoW Raid Telemetry Pipeline")
    print(f"  Raid  : {raid_id}")
    print(f"  Fecha : {event_date}")
    print(f"  Bucket: s3://{bucket}/")

    df_dim_player = inspect_dim_player(storage, bucket)
    df_dim_raid   = inspect_dim_raid(storage, bucket, raid_id)
    df_summary    = inspect_fact_raid_summary(storage, bucket, raid_id, event_date)
    df_players    = inspect_fact_player_stats(storage, bucket, raid_id, event_date)

    check_coherence(df_dim_player, df_dim_raid, df_summary, df_players)

    _separator()
    print("INSPECCION COMPLETADA\n")


if __name__ == "__main__":
    main()
