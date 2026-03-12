"""
Subfase 7.3 — Ingesta Gold Parquet → Tabla Iceberg wow.gold.fact_player_raid_stats
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.etl.spark_session import get_spark_session, stop_spark_session

# ── Schema explícito ──────────────────────────────────────────────────────────
# Declaramos el schema en lugar de inferirlo: más rápido (evita scan previo)
# y garantiza Schema-on-Write correcto en Iceberg desde el primer snapshot.
GOLD_SCHEMA_PLAYER_STATS = StructType(
    [
        StructField("raid_id", StringType(), True),
        StructField("source_player_id", StringType(), True),
        StructField("player_name", StringType(), True),
        StructField("player_class", StringType(), True),
        StructField("player_role", StringType(), True),
        StructField("total_events", LongType(), True),
        StructField("total_damage", DoubleType(), True),
        StructField("total_healing", DoubleType(), True),
        StructField("avg_damage", DoubleType(), True),
        StructField("n_critical_hits", LongType(), True),
        StructField("critical_hit_rate_pct", DoubleType(), True),
    ]
)

GOLD_PARQUET_PLAYER_STATS = "s3a://gold/spark/fact_player_raid_stats/"
ICEBERG_TABLE = "wow.gold.fact_player_raid_stats"


def main() -> None:
    spark: SparkSession = get_spark_session("Gold_Iceberg_PlayerStats")
    try:
        # ── 1. LEER toda la capa Silver con schema explícito ──────────────────────
        print(f">>> Leyendo Parquets desde {GOLD_PARQUET_PLAYER_STATS}")
        df_raw = (
            spark.read.schema(GOLD_SCHEMA_PLAYER_STATS)
            # mergeSchema=false: todos los ficheros deben cumplir el mismo schema.
            # Falla rápido si hay algún fichero con schema distinto (detección temprana).
            .option("mergeSchema", "false")
            .option("basePath", GOLD_PARQUET_PLAYER_STATS)
            .parquet(GOLD_PARQUET_PLAYER_STATS)
        )

        total_rows = df_raw.count()
        print(f"    Filas leídas : {total_rows:,}")

        df = df_raw.withColumn("ingested_at", F.current_timestamp())

        # ── 2. CREAR tabla Iceberg si no existe ───────────────────────────────────
        # TBLPROPERTIES      → write.parquet.compression-codec garantiza Snappy
        print(f"\n>>> Creando tabla {ICEBERG_TABLE} si no existe...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
                raid_id                         STRING,
                source_player_id                STRING,
                player_name                     STRING,
                player_class                    STRING,
                player_role                     STRING,
                total_events                    BIGINT,
                total_damage                    DOUBLE,
                total_healing                   DOUBLE,
                avg_damage                      DOUBLE,
                n_critical_hits                 BIGINT,
                critical_hit_rate_pct           DOUBLE,
                ingested_at                     TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (raid_id)
            TBLPROPERTIES (
                'write.parquet.compression-codec' = 'snappy',
                'write.metadata.compression-codec' = 'gzip'
            )
        """)
        print("    Tabla creada (o ya existía).")

        # ── 3. INGESTAR: Parquet → Iceberg (primer snapshot) ─────────────────────
        # overwritePartitions garantiza idempotencia: si relanzas el script,
        # sobreescribe las particiones afectadas en lugar de duplicar filas.
        print(f"\n>>> Ingestando {total_rows:,} filas en {ICEBERG_TABLE}...")
        (
            df.writeTo(ICEBERG_TABLE)
            .option("write.parquet.compression-codec", "snappy")
            .overwritePartitions()
        )
        print("    Ingesta completada.")

        # ── 4. VERIFICAR snapshot y conteo final ──────────────────────────────────
        print("\n>>> VERIFICACIÓN POST-INGESTA")

        iceberg_count = spark.table(ICEBERG_TABLE).count()
        print(f"  Filas en Iceberg : {iceberg_count:,}")
        print(f"  Filas en Parquet : {total_rows:,}")
        print(f"  Diferencia       : {abs(iceberg_count - total_rows)}")

        print("\n>>> HISTORIAL DE SNAPSHOTS")
        spark.sql(
            f"SELECT snapshot_id, committed_at, operation FROM {ICEBERG_TABLE}.snapshots"
        ).show(truncate=False)

    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
