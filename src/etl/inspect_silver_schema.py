from src.etl.spark_session import get_spark_session, stop_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    BooleanType, ArrayType, IntegerType, DateType
)

# source_player_level se lee como DOUBLE (tipo físico real en algunos ficheros)
# y se castea a LONG en memoria después de la lectura
SILVER_SCHEMA_READ = StructType([
    StructField("event_id",                         StringType(),          True),
    StructField("event_type",                       StringType(),          True),
    StructField("timestamp",                        LongType(),            True),
    StructField("encounter_id",                     StringType(),          True),
    StructField("encounter_duration_ms",            LongType(),            True),
    StructField("source_player_id",                 StringType(),          True),
    StructField("source_player_name",               StringType(),          True),
    StructField("source_player_role",               StringType(),          True),
    StructField("source_player_class",              StringType(),          True),
    StructField("source_player_level",              DoubleType(),          True),  # ← DOUBLE para lectura
    StructField("ability_id",                       StringType(),          True),
    StructField("ability_name",                     StringType(),          True),
    StructField("ability_school",                   StringType(),          True),
    StructField("damage_amount",                    DoubleType(),          True),
    StructField("healing_amount",                   DoubleType(),          True),
    StructField("is_critical_hit",                  BooleanType(),         True),
    StructField("critical_multiplier",              DoubleType(),          True),
    StructField("is_resisted",                      BooleanType(),         True),
    StructField("is_blocked",                       BooleanType(),         True),
    StructField("is_absorbed",                      BooleanType(),         True),
    StructField("target_entity_id",                 StringType(),          True),
    StructField("target_entity_name",               StringType(),          True),
    StructField("target_entity_type",               StringType(),          True),
    StructField("target_entity_health_pct_before",  DoubleType(),          True),
    StructField("target_entity_health_pct_after",   DoubleType(),          True),
    StructField("resource_type",                    StringType(),          True),
    StructField("resource_amount_before",           DoubleType(),          True),
    StructField("resource_amount_after",            DoubleType(),          True),
    StructField("resource_regeneration_rate",       DoubleType(),          True),
    StructField("ingestion_timestamp",              StringType(),          True),
    StructField("source_system",                    StringType(),          True),
    StructField("data_quality_flags",  ArrayType(IntegerType()),           True),
    StructField("server_latency_ms",                LongType(),            True),
    StructField("client_latency_ms",                LongType(),            True),
    StructField("is_massive_hit",                   BooleanType(),         True),
    StructField("raid_id",                          StringType(),          True),
    StructField("event_date",                       DateType(),            True),
])

def main():
    spark = get_spark_session("InspectSilver")

    df = spark.read \
        .schema(SILVER_SCHEMA_READ) \
        .parquet("s3a://silver/wow_raid_events/")

    # Normalización post-lectura: castear a los tipos canónicos correctos
    df = df.withColumn("source_player_level", F.col("source_player_level").cast("long"))

    print("\n── Schema canónico aplicado ──")
    df.printSchema()

    print("\n── Total filas ──")
    print(df.count())

    print("\n── Particiones (top 5) ──")
    df.select("raid_id", "event_date").distinct().show(5, truncate=False)

    print("\n── Muestra de 3 filas (columnas clave) ──")
    df.select(
        "event_id", "event_type", "raid_id",
        "source_player_name", "source_player_level",
        "damage_amount", "is_critical_hit"
    ).show(3, truncate=False)

    stop_spark_session(spark)

if __name__ == "__main__":
    main()
