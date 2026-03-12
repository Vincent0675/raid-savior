"""
Subfase 7.4 — Silver Iceberg → Tabla Iceberg wow.gold.dim_player_stats
Dimensión de jugadores con upsert (MERGE INTO) sobre clave player_id.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.etl.spark_session import get_spark_session, stop_spark_session

"""
GOLD_SCHEMA_DIM_PLAYER = StructType(
    [
        StructField("player_id", StringType(), True),
        StructField("player_name", StringType(), True),
        StructField("player_class", StringType(), True),
        StructField("player_role", StringType(), True),
        StructField("total_raids", LongType(), True),
        StructField("first_seen_date", DateType(), True),
        StructField("last_seen_date", DateType(), True),
        StructField("ingested_at", TimestampType(), True)
    ]
) # Esto es solo documentación del contrato de salida 
"""

SILVER_DIM_PLAYER = "wow.silver.raid_events"
ICEBERG_TABLE = "wow.gold.dim_player"


def main() -> None:
    spark: SparkSession = get_spark_session("Gold_Iceberg_DimPlayer")
    try:
        # ── 1. LEER Silver Iceberg ───────────────────────────────────────────────
        print(f">>> Leyendo tabla Iceberg {SILVER_DIM_PLAYER}")

        df_raw = spark.table(SILVER_DIM_PLAYER)

        # ── 2. AGREGAR -> construir la dimensión ─────────────────────────────────

        df_players = df_raw.filter(F.col("source_player_id").startswith("player_"))

        df_dim = (
            df_players.groupBy("source_player_id")
            .agg(
                F.first("source_player_name").alias("player_name"),
                F.first("source_player_class").alias("player_class"),
                F.first("source_player_role").alias("player_role"),
                F.countDistinct("raid_id").alias("total_raids"),
                F.min(F.to_date("timestamp")).alias("first_seen_date"),
                F.max(F.to_date("timestamp")).alias("last_seen_date"),
            )
            .withColumnRenamed("source_player_id", "player_id")
            .withColumn("ingested_at", F.current_timestamp())
        )

        total_rows = df_dim.count()

        print(f"    Jugadores únicos : {total_rows:,}")

        # ── 3. REGISTRAR como vista para el MERGE ─────────────────────────────────

        df_dim.createOrReplaceTempView("staging_dim_player")

        # ── 4. CREAR tabla Iceberg si no existe ───────────────────────────────────

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
                player_id       STRING,
                player_name     STRING,
                player_class    STRING,
                player_role     STRING,
                total_raids     BIGINT,
                first_seen_date DATE,
                last_seen_date  DATE,
                ingested_at     TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (player_class)
            TBLPROPERTIES (
                'write.parquet.compression-codec' = 'snappy',
                'write.metadata.compression-codec' = 'gzip'
            )
        """)

        # ── 5. MERGE INTO ─────────────────────────────────────────────────────────
        spark.sql(f"""
            MERGE INTO {ICEBERG_TABLE} AS target
            USING staging_dim_player AS source
            ON target.player_id = source.player_id
            WHEN MATCHED THEN UPDATE SET
                player_name    = source.player_name,
                player_class   = source.player_class,
                player_role    = source.player_role,
                total_raids    = source.total_raids,
                last_seen_date = source.last_seen_date,
                ingested_at    = source.ingested_at
            WHEN NOT MATCHED THEN INSERT *          
        """)

        # ── 6. VERIFICAR snapshot y conteo final ──────────────────────────────────
        print("\n>>> VERIFICACIÓN POST-INGESTA")

        iceberg_count = spark.table(ICEBERG_TABLE).count()
        print(f"  Filas en Iceberg : {iceberg_count:,}")
        print(f"  Filas en Silver : {total_rows:,}")
        print(f"  Diferencia       : {abs(iceberg_count - total_rows)}")

        print("\n>>> HISTORIAL DE SNAPSHOTS")
        spark.sql(
            f"SELECT snapshot_id, committed_at, operation FROM {ICEBERG_TABLE}.snapshots"
        ).show(truncate=False)

    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
