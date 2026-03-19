# CATEGORÍA: bootstrap / infraestructura
# CUÁNDO EJECUTAR: una vez por entorno, o si se pierde el volumen iceberg_catalog
# PREREQUISITOS: docker compose up -d (minio + iceberg-rest healthy)

import boto3
from botocore.client import Config
from src.etl.spark_session import get_spark_session, stop_spark_session
from src.config import Config as AppConfig

TABLES = [
    "gold/dim_player",
    "gold/dim_raid",
    "gold/fact_player_raid_stats",
    "gold/fact_raid_summary",
    "silver/raid_events",
]


def get_current_metadata_path(s3_client, bucket: str, table_path: str) -> str:
    """Lee version-hint.text y devuelve la ruta al metadata file activo."""
    hint_key = f"{table_path}/metadata/version-hint.text"
    response = s3_client.get_object(Bucket=bucket, Key=hint_key)
    version = response["Body"].read().decode("utf-8").strip()

    # Busca el fichero con ese número de versión (puede ser .gz o no)
    prefix = f"{table_path}/metadata/v{version}"
    result = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    candidates = [
        obj["Key"]
        for obj in result.get("Contents", [])
        if obj["Key"].endswith(".metadata.json")
    ]

    if not candidates:
        raise FileNotFoundError(
            f"No metadata file found for version {version} in {table_path}"
        )

    # Devuelve s3://warehouse/<path>
    return f"s3://warehouse/{candidates[0]}"


def register_tables():
    s3 = boto3.client(
        "s3",
        endpoint_url=AppConfig.S3_ENDPOINT_URL,
        aws_access_key_id=AppConfig.S3_ACCESS_KEY,
        aws_secret_access_key=AppConfig.S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )

    spark = get_spark_session("RegisterTablesRESTCatalog")

    for table_path in TABLES:
        namespace = table_path.split("/")[0]  # "gold" o "silver"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS wow.{namespace}")

    for table_path in TABLES:
        namespace, table_name = table_path.split("/")
        fqn = f"wow.{namespace}.{table_name}"

        metadata_path = get_current_metadata_path(
            s3, AppConfig.S3_BUCKET_WAREHOUSE, table_path
        )
        print(f"[INFO] Registrando {fqn} → {metadata_path}")

        spark.sql(f"""
            CALL wow.system.register_table(
                table => '{fqn}',
                metadata_file => '{metadata_path}'
            )
        """)
        print(f"[OK]   {fqn}")

    print("\n[INFO] Verificación final:")
    spark.sql("SHOW TABLES IN wow.gold").show()
    spark.sql("SHOW TABLES IN wow.silver").show()

    stop_spark_session(spark)


if __name__ == "__main__":
    register_tables()
