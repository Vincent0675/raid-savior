from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue


@asset(
    group_name="medallion",
    deps=["bronze_raw_events"],
    description="ETL Bronze → Silver: transforma JSON raw a Parquet+Snappy particionado",
)
def silver_events(context: AssetExecutionContext) -> MaterializeResult:

    # 1. Importa las dos clases que ya tienes en src/etl/
    #    (las mismas que usa run_bronze_to_silver.py)
    from src.storage.minio_client import MinIOStorageClient
    from src.etl.bronze_to_silver import BronzeToSilverETL

    # 2. Instancia los clientes (igual que en main())
    storage = MinIOStorageClient()
    etl = BronzeToSilverETL()

    # 3. Lista los archivos JSON de Bronze
    #    (la función list_bronze_files ya existe en run_bronze_to_silver.py,
    #     pero aquí la reescribimos inline usando storage.s3.list_objects_v2)
    context.log.info("Listando archivos JSON en Bronze...")
    response = storage.s3.list_objects_v2(Bucket="bronze")
    bronze_files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".json")
    ]
    context.log.info(f"Encontrados {len(bronze_files)} archivos")

    # 4. Itera y ejecuta etl.run() para cada archivo
    #    Acumula: successful, failed, total_rows (igual que en main())
    successful, skipped, failed, total_rows = 0, 0, 0, 0

    for key in bronze_files:
        result = etl.run(key)
        status = result.get("status")

        if result.get("status") == "success":
            successful += 1
            rows = result.get("storage", {}).get("rows", 0)
            total_rows += rows
        elif status == "skipped":
            skipped += 1
            context.log.debug(f"Omitido (ya procesado): {key}")
        else:
            failed += 1
            context.log.warning(f"Fallo en: {key} → {result}")

    # 5. Devuelve MaterializeResult con metadata
    #    Esto es lo que aparece en la Dagster UI
    return MaterializeResult(
        metadata={
            "archivos_procesados": MetadataValue.int(len(bronze_files)),
            "exitosos": MetadataValue.int(successful),
            "omitidos": MetadataValue.int(skipped),
            "fallidos": MetadataValue.int(failed),
            "total_filas": MetadataValue.int(total_rows),
        }
    )


@asset(
    group_name="medallion",
    description="Ingesta local → MinIO Bronze: sube JSON de data/bronze/production",
)
def bronze_raw_events(context: AssetExecutionContext) -> MaterializeResult:

    from pathlib import Path
    from datetime import datetime, timezone
    from src.storage.minio_client import MinIOStorageClient
    from src.etl.ingest_bronze_production import collect_files, ensure_bucket

    BRONZE_ROOT = Path("data/bronze/production")
    BUCKET = "bronze"
    S3_PREFIX = "wow_raid_events/v1"
    INGEST_DATE = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d"
    )  # ← dentro de la función para que no se congele

    storage = MinIOStorageClient()
    ensure_bucket(storage, BUCKET)

    files = collect_files(BRONZE_ROOT)
    raids = sorted({raid_id for raid_id, _ in files})
    context.log.info(f"Raids detectados: {raids}")
    context.log.info(f"Total archivos a subir: {len(files)}")

    success, failed, total_bytes = 0, 0, 0

    for raid_id, fpath in files:
        s3_key = f"{S3_PREFIX}/raid_id={raid_id}/ingest_date={INGEST_DATE}/{fpath.name}"
        try:
            raw_bytes = fpath.read_bytes()
            storage.s3.put_object(
                Bucket=BUCKET,
                Key=s3_key,
                Body=raw_bytes,
                ContentType="application/json",
            )
            success += 1
            total_bytes += len(raw_bytes)
        except Exception as e:
            failed += 1
            context.log.warning(f"Fallo subiendo {fpath.name}: {e}")

    return MaterializeResult(
        metadata={
            "raids_detectados": MetadataValue.int(len(raids)),
            "archivos_subidos": MetadataValue.int(success),
            "fallidos": MetadataValue.int(failed),
            "mb_transferidos": MetadataValue.float(round(total_bytes / 1024 / 1024, 2)),
        }
    )


@asset(
    group_name="medallion",
    deps=["silver_events"],
    description="ETL Silver → Gold distribuido: 4 tablas Gold via PySpark S3A",
)
def gold_tables_spark(context: AssetExecutionContext) -> MaterializeResult:
    import time
    from src.etl.spark_session import get_spark_session, stop_spark_session
    from src.etl.spark_silver_to_gold import (
        read_silver,
        compute_fact_raid_summary,
        compute_fact_player_raid_stats,
        compute_dim_player,
        compute_dim_raid,
        write_gold,
    )

    t_start = time.perf_counter()
    spark = get_spark_session(app_name="WoWRaidGoldETL_Dagster")

    try:
        context.log.info("Leyendo capa Silver desde MinIO...")
        df_silver = read_silver(spark)
        n_filas = df_silver.count()
        context.log.info(f"Filas Silver cargadas: {n_filas:,}")

        fact_raid = compute_fact_raid_summary(df_silver)
        fact_players = compute_fact_player_raid_stats(df_silver)
        dim_player = compute_dim_player(df_silver)
        dim_raid = compute_dim_raid(df_silver, fact_raid)

        # Métricas antes de escribir (count() fuerza la evaluación lazy de Spark)
        n_raids = fact_raid.count()
        n_players = dim_player.count()

        write_gold(fact_raid, "fact_raid_summary")
        write_gold(fact_players, "fact_player_raid_stats")
        write_gold(dim_player, "dim_player", None)
        write_gold(dim_raid, "dim_raid", "raid_id")

        elapsed = round(time.perf_counter() - t_start, 2)
        context.log.info(f"Gold completo en {elapsed}s")

        return MaterializeResult(
            metadata={
                "filas_silver_procesadas": MetadataValue.int(n_filas),
                "raids_procesados": MetadataValue.int(n_raids),
                "jugadores_unicos": MetadataValue.int(n_players),
                "tablas_gold_escritas": MetadataValue.int(4),
                "duracion_s": MetadataValue.float(elapsed),
            }
        )
    finally:
        stop_spark_session(spark)  # ← se ejecuta siempre, incluso si hay excepción


@asset(
    group_name="medallion",
    deps=["silver_events"],
    description="ETL Silver → Gold Pandas/DuckDB: 4 tablas Gold por partición con validación Pydantic",
)
def gold_tables_duckdb(context: AssetExecutionContext) -> MaterializeResult:
    from src.analytics.gold_layer import GoldLayerETL

    etl = GoldLayerETL()
    context.log.info("Descubriendo particiones Silver...")

    summary = etl.run_all()  # descubre + procesa todas las particiones

    # Loguear particiones fallidas individualmente si las hay
    for err in summary.get("errors", []):
        context.log.warning(
            f"Partición fallida: raid_id={err['raid_id']} "
            f"event_date={err['event_date']} → {err['error']}"
        )

    return MaterializeResult(
        metadata={
            "particiones_procesadas": MetadataValue.int(summary["total_partitions"]),
            "exitosas": MetadataValue.int(summary["successful"]),
            "fallidas": MetadataValue.int(summary["failed"]),
        }
    )
