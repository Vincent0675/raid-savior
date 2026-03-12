import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.config import Config

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env", override=True)

# Fallback: construye las rutas por defecto si no hay variable de entorno

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 3g pyspark-shell")

_jar_default = (
    str(Path.home() / "spark-jars" / "hadoop-aws-3.3.4.jar")
    + ","
    + str(Path.home() / "spark-jars" / "aws-java-sdk-bundle-1.12.262.jar")
    + ","
    + str(Path.home() / "spark-jars" / "iceberg-spark-runtime-3.5_2.12-1.7.1.jar")
)

SPARK_JARS = os.getenv("SPARK_JARS_PATH", _jar_default)


# --- Función principal ---
def get_spark_session(app_name: str = "WoWRaidTelemetry") -> SparkSession:
    """
    Crea o reutiliza una SparkSession configurada para MinIO via S3A.
    Usa AWS SDK v1 (hadoop-aws 3.3.4 + aws-java-sdk-bundle-1.12.262).
    """
    return (
        SparkSession.builder.master(
            "local[*]"
        )  # usa todos los cores disponibles del TUF
        .appName(app_name)
        .config("spark.jars", SPARK_JARS)
        .config(
            "spark.driver.extraClassPath",
            str(
                Path.home() / "spark-jars" / "iceberg-spark-runtime-3.5_2.12-1.7.1.jar"
            ),
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.wow", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.wow.type", "hadoop")
        .config("spark.sql.catalog.wow.warehouse", "s3a://warehouse/")
        .config("spark.sql.shuffle.partitions", "8")
        .config(
            "spark.driver.extraJavaOptions", "-XX:+UseSerialGC -XX:-TieredCompilation"
        )
        .config(
            "spark.executor.extraJavaOptions", "-XX:+UseSerialGC -XX:-TieredCompilation"
        )
        # ── Entorno de desarrollo (laptop 4GB) ────────────────────────────────────────
        # Deshabilita el lector vectorizado Arrow de Iceberg.
        # Arrow requiere memoria off-heap que no está configurada en local.
        # En producción: revertir a True y configurar spark.memory.offHeap.size.
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        # Configuración de producción — NO para el TUF
        # .config("spark.memory.offHeap.enabled", "true")
        # .config("spark.memory.offHeap.size", "1g")  # ajustar según nodo
        .config("spark.hadoop.fs.s3a.endpoint", Config.S3_ENDPOINT_URL)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", Config.S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", Config.S3_SECRET_KEY)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .getOrCreate()
    )


# --- Función de limpieza ---
def stop_spark_session(spark: SparkSession) -> None:
    """Detiene la SparkSession activa y libera recursos JVM."""
    spark.stop()
    print("[SparkSession] Sesión detenida correctamente.")
