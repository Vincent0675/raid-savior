"""
spark_silver_to_gold.py — ETL Silver → Gold usando PySpark.

Lee todas las particiones Silver de MinIO via S3A y calcula
las tablas Gold con la DataFrame API distribuida.

Tablas producidas:
    s3a://gold/fact_raid_summary/
    s3a://gold/fact_player_raid_stats/
"""
from __future__ import annotations
import time

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, BooleanType,
    ArrayType, DateType
)

from src.etl.spark_session import get_spark_session, stop_spark_session
from src.config import Config

logger = logging.getLogger(__name__)

SILVER_PATH = "s3a://silver/wow_raid_events/v1/"
GOLD_PATH   = "s3a://gold/"

# --- Schema canónico Silver — tipos "más anchos" ganan siempre ---
SILVER_SCHEMA = StructType([
    StructField("event_id",                      StringType(),  True),
    StructField("event_type",                    StringType(),  True),
    StructField("timestamp",                     LongType(),    True),
    StructField("encounter_id",                  StringType(),  True),
    StructField("encounter_duration_ms",         LongType(),    True),
    StructField("source_player_id",              StringType(),  True),
    StructField("source_player_name",            StringType(),  True),
    StructField("source_player_role",            StringType(),  True),
    StructField("source_player_class",           StringType(),  True),
    StructField("source_player_level",           DoubleType(),  True),  # DOUBLE gana
    StructField("ability_id",                    StringType(),  True),
    StructField("ability_name",                  StringType(),  True),
    StructField("ability_school",                StringType(),  True),
    StructField("damage_amount",                 DoubleType(),  True),
    StructField("healing_amount",                DoubleType(),  True),
    StructField("is_critical_hit",               BooleanType(), True),
    StructField("critical_multiplier",           DoubleType(),  True),
    StructField("is_resisted",                   BooleanType(), True),
    StructField("is_blocked",                    BooleanType(), True),
    StructField("is_absorbed",                   BooleanType(), True),
    StructField("target_entity_id",              StringType(),  True),
    StructField("target_entity_name",            StringType(),  True),
    StructField("target_entity_type",            StringType(),  True),
    StructField("target_entity_health_pct_before", DoubleType(), True),
    StructField("target_entity_health_pct_after",  DoubleType(), True),
    StructField("resource_type",                 StringType(),  True),  # STRING gana
    StructField("resource_amount_before",        DoubleType(),  True),
    StructField("resource_amount_after",         DoubleType(),  True),
    StructField("resource_regeneration_rate",    DoubleType(),  True),  # DOUBLE gana
    StructField("ingestion_timestamp",           StringType(),  True),
    StructField("source_system",                 StringType(),  True),
    StructField("data_quality_flags", ArrayType(StringType()), True),   # STRING gana
    StructField("server_latency_ms",             LongType(),    True),
    StructField("client_latency_ms",             LongType(),    True),
    StructField("is_massive_hit",                BooleanType(), True),
])

def read_silver(spark: SparkSession) -> DataFrame:
    """
    Lee TODAS las particiones Silver con partición Hive-style.
    PySpark infiere automáticamente la columna 'raid_id' del path.
    """
    logger.info("[read_silver] Leyendo: %s", SILVER_PATH)
    df = (
        spark.read
        .schema(SILVER_SCHEMA)          # schema explícito — no inferencia
        .parquet(SILVER_PATH)           # Spark descubre raid_id= y event_date= automáticamente
    )
    logger.info("[read_silver] Filas: %d | Particiones RDD: %d",
                df.count(), df.rdd.getNumPartitions())
    return df

def compute_fact_raid_summary(df: DataFrame) -> DataFrame:
    """
    KPIs macro por raid. Equivalente a build_raid_summary() de aggregators.py.
    timestamp está en nanosegundos (LongType) → dividir entre 1e9 para segundos.
    """
    return df.groupBy("raid_id").agg(
        # Volumen
        F.count("event_id")                              .alias("total_events"),
        F.countDistinct("source_player_id")              .alias("n_players"),

        # Daño y curación
        F.round(F.sum("damage_amount"),  2)              .alias("total_damage"),
        F.round(F.sum("healing_amount"), 2)              .alias("total_healing"),
        F.round(F.avg("damage_amount"),  2)              .alias("avg_damage_per_event"),

        # Críticos
        F.sum(F.col("is_critical_hit").cast("int"))      .alias("n_critical_hits"),

        # Duración del raid en segundos (nanos → segundos)
        F.round(
            (F.max("timestamp") - F.min("timestamp")) / 1_000_000_000, 2
        )                                                .alias("raid_duration_s"),

        # Latencia media
        F.round(F.avg("server_latency_ms"), 2)           .alias("avg_server_latency_ms"),
    )

def compute_fact_player_raid_stats(df: DataFrame) -> DataFrame:
    """
    KPIs por (source_player_id, raid_id).
    Solo eventos de jugadores reales (source_player_id empieza por 'player_').
    """
    df_players = df.filter(F.col("source_player_id").startswith("player_"))

    return df_players.groupBy("source_player_id", "raid_id").agg(
        F.first("source_player_name")                    .alias("player_name"),
        F.first("source_player_class")                   .alias("player_class"),
        F.first("source_player_role")                    .alias("player_role"),

        F.count("event_id")                              .alias("total_events"),
        F.round(F.sum("damage_amount"),  2)              .alias("total_damage"),
        F.round(F.sum("healing_amount"), 2)              .alias("total_healing"),
        F.round(F.avg("damage_amount"),  2)              .alias("avg_damage"),
        F.sum(F.col("is_critical_hit").cast("int"))      .alias("n_critical_hits"),
        F.round(
            F.sum(F.col("is_critical_hit").cast("int")) /
            F.count("event_id") * 100, 2
        )                                                .alias("critical_hit_rate_pct"),
    )

def compute_dim_player(df: DataFrame) -> DataFrame:
    """
    Dimensión de jugadores. Un registro por source_player_id.
    Equivalente a _build_dim_player() de gold_layer.py.
    """
    df_players = df.filter(F.col("source_player_id").startswith("player_"))

    return df_players.groupBy("source_player_id").agg(
        F.first("source_player_name")   .alias("player_name"),
        F.first("source_player_class")  .alias("player_class"),
        F.first("source_player_role")   .alias("player_role"),
        F.countDistinct("raid_id")      .alias("total_raids"),
        F.min("event_date")             .alias("first_seen_date"),
        F.max("event_date")             .alias("last_seen_date"),
    ).withColumnRenamed("source_player_id", "player_id")


def compute_dim_raid(df: DataFrame, fact_raid_summary: DataFrame) -> DataFrame:
    """
    Dimensión de raids. Un registro por raid_id.
    Equivalente a _build_dim_raid() de gold_layer.py.
    boss_name y difficulty son placeholders hasta Fase D (Warcraft Logs API).
    """
    dim = fact_raid_summary.select("raid_id", "n_players").withColumn(
        "boss_name",          F.lit("Unknown Boss")  # placeholder Fase D
    ).withColumn(
        "difficulty",         F.lit("Normal")         # placeholder Fase D
    ).withColumn(
        "raid_size",          F.col("n_players")
    ).withColumn(
        "duration_target_ms", F.lit(360_000.0)        # 6 min — regla de negocio
    ).drop("n_players")

    # Añadir event_date desde el DataFrame Silver
    event_dates = df.groupBy("raid_id").agg(
        F.min("event_date").alias("event_date")
    )
    return dim.join(event_dates, on="raid_id", how="left")


def write_gold(df: DataFrame, table_name: str, partition_col: str = "raid_id") -> None:
    """
    Escribe una tabla Gold en MinIO particionada por raid_id.
    Usa mode='overwrite' para idempotencia (re-ejecutable).
    """
    path = f"{GOLD_PATH}spark/{table_name}/"
    logger.info("[write_gold] Escribiendo %s → %s", table_name, path)
    writer = df.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)
    logger.info("[write_gold] %s OK.", table_name)

def main() -> None:
    t_start = time.perf_counter()
    spark = get_spark_session(app_name="WoWRaidGoldETL")
    try:
        df_silver = read_silver(spark)

        # ── Tablas de hechos ──────────────────────────────────────
        logger.info("[main] Calculando fact_raid_summary...")
        fact_raid    = compute_fact_raid_summary(df_silver)

        logger.info("[main] Calculando fact_player_raid_stats...")
        fact_players = compute_fact_player_raid_stats(df_silver)

        # ── Dimensiones ───────────────────────────────────────────
        logger.info("[main] Construyendo dim_player...")
        dim_player   = compute_dim_player(df_silver)

        logger.info("[main] Construyendo dim_raid...")
        dim_raid     = compute_dim_raid(df_silver, fact_raid)

        # ── Escritura Gold ────────────────────────────────────────
        write_gold(fact_raid,    "fact_raid_summary")          # partitionBy raid_id
        write_gold(fact_players, "fact_player_raid_stats")     # partitionBy raid_id
        write_gold(dim_player,   "dim_player",   None)         # sin partición
        write_gold(dim_raid,     "dim_raid",     "raid_id")    # partitionBy raid_id

        t_end = time.perf_counter()
        logger.info("[main] ✅ Pipeline Gold Spark completo — 4 Tablas escritas totales — %.1fs totales.", t_end - t_start)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
