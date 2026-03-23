"""Bootstrap idempotente para registrar tablas en Iceberg REST Catalog.

Uso recomendado:
    python scripts/bootstrap/register_tables_rest_catalog.py
"""

from __future__ import annotations

import re

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from src.config import Config as AppConfig
from src.etl.spark_session import get_spark_session, stop_spark_session

TABLES = [
    "gold/dim_player",
    "gold/dim_raid",
    "gold/fact_player_raid_stats",
    "gold/fact_raid_summary",
    "silver/raid_events",
]

METADATA_VERSION_REGEX = re.compile(r".*/metadata/v(\d+)(?:\.gz)?\.metadata\.json$")


def _table_exists(spark, namespace: str, table_name: str) -> bool:
    rows = spark.sql(f"SHOW TABLES IN wow.{namespace}").collect()
    return any(row.tableName == table_name for row in rows)


def _load_version_hint(s3_client, bucket: str, table_path: str) -> int | None:
    hint_key = f"{table_path}/metadata/version-hint.text"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=hint_key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in {"NoSuchKey", "404"}:
            return None
        raise

    version_text = response["Body"].read().decode("utf-8").strip()
    return int(version_text) if version_text.isdigit() else None


def _resolve_latest_metadata_key(s3_client, bucket: str, table_path: str) -> str:
    metadata_prefix = f"{table_path}/metadata/"
    result = s3_client.list_objects_v2(Bucket=bucket, Prefix=metadata_prefix)
    candidates: list[tuple[int, str]] = []

    for obj in result.get("Contents", []):
        key = obj["Key"]
        match = METADATA_VERSION_REGEX.match(key)
        if match:
            candidates.append((int(match.group(1)), key))

    if not candidates:
        raise FileNotFoundError(
            "No se encontraron archivos metadata Iceberg. "
            f"Tabla: {table_path}. Prefix: {metadata_prefix}"
        )

    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def get_current_metadata_path(s3_client, bucket: str, table_path: str) -> str:
    """Resuelve metadata activo por version-hint o fallback a versión más alta."""
    hinted_version = _load_version_hint(s3_client, bucket, table_path)
    if hinted_version is not None:
        prefix = f"{table_path}/metadata/v{hinted_version}"
        result = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in result.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".metadata.json"):
                return f"s3://{bucket}/{key}"

    latest_key = _resolve_latest_metadata_key(s3_client, bucket, table_path)
    return f"s3://{bucket}/{latest_key}"


def register_tables() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=AppConfig.S3_ENDPOINT_URL,
        aws_access_key_id=AppConfig.S3_ACCESS_KEY,
        aws_secret_access_key=AppConfig.S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )

    spark = get_spark_session("RegisterTablesRESTCatalog")
    failures: list[str] = []

    try:
        for table_path in TABLES:
            namespace = table_path.split("/")[0]
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS wow.{namespace}")

        for table_path in TABLES:
            namespace, table_name = table_path.split("/")
            fqn = f"wow.{namespace}.{table_name}"

            if _table_exists(spark, namespace, table_name):
                print(f"[SKIP] {fqn} ya existe en REST Catalog")
                continue

            try:
                metadata_path = get_current_metadata_path(
                    s3, AppConfig.S3_BUCKET_WAREHOUSE, table_path
                )
                print(f"[INFO] Registrando {fqn} -> {metadata_path}")
                spark.sql(
                    f"""
                    CALL wow.system.register_table(
                        table => '{fqn}',
                        metadata_file => '{metadata_path}'
                    )
                    """
                )
                print(f"[OK]   {fqn}")
            except Exception as exc:  # pragma: no cover - script operativo
                message = (
                    f"[ERROR] {fqn}: {exc}. "
                    "Accion sugerida: validar que existe metadata en "
                    f"s3://{AppConfig.S3_BUCKET_WAREHOUSE}/{table_path}/metadata/ "
                    "y que iceberg-rest esta healthy (http://localhost:8181/v1/config)."
                )
                print(message)
                failures.append(message)

        print("\n[INFO] Verificacion final:")
        spark.sql("SHOW TABLES IN wow.gold").show(truncate=False)
        spark.sql("SHOW TABLES IN wow.silver").show(truncate=False)

        if failures:
            raise RuntimeError(
                "Fallo el registro de una o mas tablas. Revisar errores previos."
            )
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    register_tables()
