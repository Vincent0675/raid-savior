"""
Subfase 7.2 — Ingesta Silver Parquet → Tabla Iceberg wow.silver.raid_events
Operación: CREATE tabla + carga completa de los 1892 Parquets (1.3M filas).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.etl.spark_session import get_spark_session, stop_spark_session

# ── Schema explícito ──────────────────────────────────────────────────────────
# Declaramos el schema en lugar de inferirlo: más rápido (evita scan previo)
# y garantiza Schema-on-Write correcto en Iceberg desde el primer snapshot.
SILVER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("encounter_duration_ms", LongType(), True),
        StructField("source_player_id", StringType(), True),
        StructField("source_player_name", StringType(), True),
        StructField("source_player_role", StringType(), True),
        StructField("source_player_class", StringType(), True),
        StructField("source_player_level", LongType(), True),
        StructField("ability_id", StringType(), True),
        StructField("ability_name", StringType(), True),
        StructField("ability_school", StringType(), True),
        StructField("damage_amount", DoubleType(), True),
        StructField("healing_amount", DoubleType(), True),
        StructField("is_critical_hit", BooleanType(), True),
        StructField("critical_multiplier", DoubleType(), True),
        StructField("is_resisted", BooleanType(), True),
        StructField("is_blocked", BooleanType(), True),
        StructField("is_absorbed", BooleanType(), True),
        StructField("target_entity_id", StringType(), True),
        StructField("target_entity_name", StringType(), True),
        StructField("target_entity_type", StringType(), True),
        StructField("target_entity_health_pct_before", DoubleType(), True),
        StructField("target_entity_health_pct_after", DoubleType(), True),
        StructField("resource_type", StringType(), True),
        StructField("resource_amount_before", DoubleType(), True),
        StructField("resource_amount_after", DoubleType(), True),
        StructField("resource_regeneration_rate", DoubleType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("data_quality_flags", ArrayType(StringType(), True), True),
        StructField("server_latency_ms", LongType(), True),
        StructField("client_latency_ms", LongType(), True),
        StructField("is_massive_hit", BooleanType(), True),
        StructField("raid_id", StringType(), True),
    ]
)

SILVER_PARQUET_PATH = "s3a://silver/wow_raid_events/v1/"
ICEBERG_TABLE = "wow.silver.raid_events"


def main() -> None:
    spark: SparkSession = get_spark_session("Silver_Parquet_to_Iceberg")

    # ── 1. LEER toda la capa Silver con schema explícito ──────────────────────
    print(f">>> Leyendo Parquets desde {SILVER_PARQUET_PATH}")
    df = (
        spark.read.schema(SILVER_SCHEMA)
        # mergeSchema=false: todos los ficheros deben cumplir el mismo schema.
        # Falla rápido si hay algún fichero con schema distinto (detección temprana).
        .option("mergeSchema", "false")
        .parquet(SILVER_PARQUET_PATH)
    )

    df = df.drop("event_date")

    total_rows = df.count()
    print(f"    Filas leídas : {total_rows:,}")

    # ── 2. CREAR tabla Iceberg si no existe ───────────────────────────────────
    # USING iceberg      → table format Iceberg
    # PARTITIONED BY     → particionado lógico en Iceberg (no carpetas Hive)
    #                      event_type tiene baja cardinalidad → buena partición
    # TBLPROPERTIES      → write.parquet.compression-codec garantiza Snappy
    print(f"\n>>> Creando tabla {ICEBERG_TABLE} si no existe...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            event_id                        STRING,
            event_type                      STRING,
            timestamp                       TIMESTAMP,
            encounter_id                    STRING,
            encounter_duration_ms           BIGINT,
            source_player_id                STRING,
            source_player_name              STRING,
            source_player_role              STRING,
            source_player_class             STRING,
            source_player_level             BIGINT,
            ability_id                      STRING,
            ability_name                    STRING,
            ability_school                  STRING,
            damage_amount                   DOUBLE,
            healing_amount                  DOUBLE,
            is_critical_hit                 BOOLEAN,
            critical_multiplier             DOUBLE,
            is_resisted                     BOOLEAN,
            is_blocked                      BOOLEAN,
            is_absorbed                     BOOLEAN,
            target_entity_id                STRING,
            target_entity_name              STRING,
            target_entity_type              STRING,
            target_entity_health_pct_before DOUBLE,
            target_entity_health_pct_after  DOUBLE,
            resource_type                   STRING,
            resource_amount_before          DOUBLE,
            resource_amount_after           DOUBLE,
            resource_regeneration_rate      DOUBLE,
            ingestion_timestamp             STRING,
            source_system                   STRING,
            data_quality_flags              ARRAY<STRING>,
            server_latency_ms               BIGINT,
            client_latency_ms               BIGINT,
            is_massive_hit                  BOOLEAN,
            raid_id                         STRING
        )
        USING iceberg
        PARTITIONED BY (event_type, raid_id)
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

    print("\n>>> DISTRIBUCIÓN POR event_type (partición Iceberg)")
    spark.table(ICEBERG_TABLE).groupBy("event_type").agg(
        F.count("*").alias("filas")
    ).orderBy("event_type").show()

    stop_spark_session(spark)


if __name__ == "__main__":
    main()
