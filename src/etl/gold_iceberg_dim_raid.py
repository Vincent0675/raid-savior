"""
Subfase 7.4 — Silver Iceberg → Tabla Iceberg wow.gold.dim_player_stats
Dimensión de jugadores con upsert (MERGE INTO) sobre clave player_id.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.etl.spark_session import get_spark_session, stop_spark_session

SILVER_DIM_RAID_EVENTS = "wow.silver.raid_events"
ICEBERG_TABLE = "wow.gold.dim_raid"


def main() -> None:
    spark: SparkSession = get_spark_session("Gold_Iceberg_DimRaid")
    try:
        # ── 1. LEER Silver Iceberg ───────────────────────────────────────────────
        print(f">>> Leyendo Silver Iceberg {SILVER_DIM_RAID_EVENTS}")
        df_raw = spark.table(SILVER_DIM_RAID_EVENTS)

        # ── 2. CONSTRUIR la dimensión ────────────────────────────────────────────
        # La función original necesitaba fact_raid_summary para n_players.
        # Aquí lo calculamos directamente desde Silver con countDistinct —
        # misma lógica, sin dependencia externa.
        #
        # DIFERENCIA clave vs dim_player:
        #   - dim_player: groupBy("source_player_id") → 1 fila por jugador
        #   - dim_raid:   groupBy("raid_id")          → 1 fila por raid
        #
        # F.to_date("timestamp") — igual que en dim_player, Silver usa "timestamp"
        # (nanosegundos epoch), NO "event_date".

        df_dim = (
            df_raw.groupBy("raid_id")
            .agg(
                F.countDistinct("source_player_id").alias("raid_size"),  # <- n_players
                F.min(F.to_date("timestamp")).alias("event_date"),  # <- fecha inmutable
            )
            # Columnas literales - placeholders hasta Fase posterior (Warcraft Logs API)
            .withColumn("boss_name", F.lit("Unknown Boss"))
            .withColumn("difficulty", F.lit("Normal"))
            .withColumn("duration_target_ms", F.lit(360_000.0))
            .withColumn("ingested_at", F.current_timestamp())
        )

        total_rows = df_dim.count()
        print(f"     Raids únicos: {total_rows:,}")

        # ── 3. REGISTRAR como vista temporal para el MERGE ───────────────────────
        # Spark SQL no puede hacer MERGE contra un DataFrame directamente ─
        # necesita un nombre de tabla/vista. Esta vista vive solo en esta sesión.
        df_dim.createOrReplaceTempView("staging_dim_raid")

        # ── 4. CREAR tabla Iceberg si no existe ──────────────────────────────────
        # Particionamos por event_date (baja cardinalidad, acceso temporal),
        # NO por raid_id (UUID -> miles de particiones pequeñas en MinIO).
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
                raid_id            STRING,
                raid_size          BIGINT,
                event_date         DATE,
                boss_name          STRING,
                difficulty         STRING,
                duration_target_ms DOUBLE,
                ingested_at        TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (event_date)
                TBLPROPERTIES (
                    'write.parquet.compression-codec'  = 'snappy',
                    'write.metadata.compression-codec' = 'gzip'
                )
            """)

        # ── 5. MERGE INTO ────────────────────────────────────────────────────────
        # Clave natural: raid_id (un raid es único por definición).
        #
        # UPDATE SET excluye:
        #   - raid_id     → es la clave del JOIN, no se toca nunca
        #   - event_date  → inmutable: la fecha del raid no cambia una vez registrada
        #                   (equivalente a first_seen_date en dim_player)
        #
        # WHEN NOT MATCHED → INSERT * inserta todas las columnas de staging.
        spark.sql(f"""
            MERGE INTO {ICEBERG_TABLE} AS target
            USING staging_dim_raid AS source
            ON target.raid_id = source.raid_id
            WHEN MATCHED THEN UPDATE SET
                raid_size          = source.raid_size,
                boss_name          = source.boss_name,
                difficulty          = source.difficulty,
                duration_target_ms = source.duration_target_ms,
                ingested_at        = source.ingested_at
            WHEN NOT MATCHED THEN INSERT *
        """)

        # ── 6. VERIFICAR snapshot y conteo final ─────────────────────────────────
        print("\n>>> VERIFICACIÓN POST-INGESTA")
        iceberg_count = spark.table(ICEBERG_TABLE).count()
        print(f"  Filas en Iceberg : {iceberg_count:,}")
        print(f"  Filas en Silver  : {total_rows:,}")
        print(f"  Diferencia       : {abs(iceberg_count - total_rows)}")

        print("\n>>> HISTORIAL DE SNAPSHOTS")
        spark.sql(
            f"SELECT snapshot_id, committed_at, operation FROM {ICEBERG_TABLE}.snapshots"
        ).show(truncate=False)

    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
