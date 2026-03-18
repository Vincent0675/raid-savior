"""
=====================================================================
SCRIPT DE DEMOSTRACIÓN — NO es ETL de producción
Subfase 7.5: Time Travel y correcciones de negocio sobre Iceberg Gold

Tablas objetivo: wow.gold.dim_raid
Capacidades demostradas:
  - Consulta por snapshot_id (VERSION AS OF)
  - Consulta por timestamp  (TIMESTAMP AS OF)
  - UPDATE Iceberg + verificación de estado previo vía time travel
=====================================================================
"""

from pyspark.sql import SparkSession

from src.etl.spark_session import get_spark_session, stop_spark_session

ICEBERG_TABLE = "wow.gold.dim_raid"


# ─────────────────────────────────────────────────────────────────────────────
# UTILIDAD: mostrar snapshots de una tabla Iceberg
# ─────────────────────────────────────────────────────────────────────────────


def show_snapshots(spark: SparkSession, table: str) -> list[dict]:
    """Devuelve lista de snapshots y los imprime por pantalla."""
    print(f"\n>>> HISTORIAL DE SNAPSHOTS — {table}")
    df_snaps = spark.sql(
        f"SELECT snapshot_id, committed_at, operation "
        f"FROM {table}.snapshots "
        f"ORDER BY committed_at"
    )
    df_snaps.show(truncate=False)
    return [row.asDict() for row in df_snaps.collect()]


# ─────────────────────────────────────────────────────────────────────────────
# PARTE 0 — Inspección del estado actual
# ─────────────────────────────────────────────────────────────────────────────


def parte_0_inspeccion(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("PARTE 0 — Estado actual de dim_raid")
    print("=" * 60)

    print("\n>>> Filas actuales con boss_name = 'Unknown Boss':")
    spark.sql(f"""
        SELECT raid_id, boss_name, event_date
        FROM {ICEBERG_TABLE}
        WHERE boss_name = 'Unknown Boss'
        ORDER BY raid_id
    """).show(truncate=False)

    show_snapshots(spark, ICEBERG_TABLE)


# ─────────────────────────────────────────────────────────────────────────────
# PARTE A — Time Travel (lectura de snapshots anteriores)
# ─────────────────────────────────────────────────────────────────────────────


def parte_a_time_travel(spark: SparkSession, snapshots: list[dict]) -> None:
    print("\n" + "=" * 60)
    print("PARTE A — Time Travel: VERSION AS OF / TIMESTAMP AS OF")
    print("=" * 60)

    if len(snapshots) < 1:
        print("⚠️  No hay snapshots suficientes para demostrar time travel.")
        return

    # A1: Consulta por snapshot_id (el más antiguo disponible)
    oldest_snapshot_id = snapshots[0]["snapshot_id"]
    oldest_committed_at = snapshots[0]["committed_at"]
    print(f"\n>>> A1: VERSION AS OF {oldest_snapshot_id}")
    print(f"    (snapshot creado en: {oldest_committed_at})")
    spark.sql(f"""
        SELECT raid_id, boss_name, event_date
        FROM {ICEBERG_TABLE} VERSION AS OF {oldest_snapshot_id}
        ORDER BY raid_id
    """).show(truncate=False)

    # A2: Consulta por timestamp — usa el timestamp del snapshot más antiguo
    ts_str = str(oldest_committed_at)
    print(f"\n>>> A2: TIMESTAMP AS OF '{ts_str}'")
    spark.sql(f"""
        SELECT raid_id, boss_name, event_date
        FROM {ICEBERG_TABLE} TIMESTAMP AS OF '{ts_str}'
        ORDER BY raid_id
    """).show(truncate=False)


# ─────────────────────────────────────────────────────────────────────────────
# PARTE B — Corrección de negocio: UPDATE + verificación con time travel
# ─────────────────────────────────────────────────────────────────────────────


def parte_b_correccion(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("PARTE B — Corrección de negocio: UPDATE boss_name")
    print("=" * 60)

    # B1: Capturar snapshot PRE-corrección
    snaps_pre = show_snapshots(spark, ICEBERG_TABLE)
    if not snaps_pre:
        print("⚠️  La tabla no tiene snapshots. Abortando.")
        return

    snapshot_pre_id = snaps_pre[-1]["snapshot_id"]
    snapshot_pre_ts = snaps_pre[-1]["committed_at"]
    print(
        f"\n>>> Snapshot PRE-corrección capturado: {snapshot_pre_id} ({snapshot_pre_ts})"
    )

    # B2: Aplicar UPDATE — corrige los "Unknown Boss"
    # NOTA: En datos sintéticos, asignamos nombres de boss ficticios por raid_id
    print(
        "\n>>> Aplicando UPDATE en dim_raid (boss_name Unknown Boss → nombre real)..."
    )
    spark.sql(f"""
        UPDATE {ICEBERG_TABLE}
        SET boss_name = CASE
            WHEN raid_id = 'raid001' THEN 'Onyxia'
            WHEN raid_id = 'raid002' THEN 'Ragnaros'
            WHEN raid_id = 'raid003' THEN 'Nefarian'
            WHEN raid_id = 'raid004' THEN 'C''Thun'
            WHEN raid_id = 'raid005' THEN 'Kel''Thuzad'
            WHEN raid_id = 'raid006' THEN 'Illidan'
            WHEN raid_id = 'raid007' THEN 'Arthas'
            WHEN raid_id = 'raid008' THEN 'Archimonde'
            WHEN raid_id = 'raid009' THEN 'Kil''jaeden'
            WHEN raid_id = 'raid010' THEN 'Deathwing'
            WHEN raid_id = 'raid666' THEN 'Sargeras'
            WHEN raid_id = 'raid999' THEN 'The Jailer'
            ELSE CONCAT('Boss_', raid_id)
        END
        WHERE boss_name = 'Unknown Boss'
    """)
    print("    UPDATE aplicado.")

    # B3: Verificar estado POST-corrección
    print("\n>>> Estado POST-corrección (tabla actual):")
    spark.sql(f"""
        SELECT raid_id, boss_name, event_date
        FROM {ICEBERG_TABLE}
        ORDER BY raid_id
    """).show(truncate=False)

    # B4: Verificar estado PRE-corrección vía time travel
    print(f"\n>>> Estado PRE-corrección (VERSION AS OF {snapshot_pre_id}):")
    spark.sql(f"""
        SELECT raid_id, boss_name, event_date
        FROM {ICEBERG_TABLE} VERSION AS OF {snapshot_pre_id}
        ORDER BY raid_id
    """).show(truncate=False)

    # B5: Historial de snapshots final — debe mostrar el nuevo snapshot del UPDATE
    show_snapshots(spark, ICEBERG_TABLE)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    spark: SparkSession = get_spark_session("Gold_Iceberg_TimeTravel_Demo")
    try:
        parte_0_inspeccion(spark)

        # Capturamos snapshots después de la inspección para pasarlos a Parte A
        snapshots = [
            row.asDict()
            for row in spark.sql(
                f"SELECT snapshot_id, committed_at, operation "
                f"FROM {ICEBERG_TABLE}.snapshots ORDER BY committed_at"
            ).collect()
        ]

        parte_a_time_travel(spark, snapshots)
        parte_b_correccion(spark)

    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
