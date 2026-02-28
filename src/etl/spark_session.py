import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.config import Config

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env", override=True)

# Fallback: construye las rutas por defecto si no hay variable de entorno

_jar_default = (
    str(Path.home() / "spark-jars" / "hadoop-aws-3.3.6.jar")
    + ","
    + str(Path.home() / "spark-jars" / "aws-java-sdk-bundle-1.12.786.jar")
)

SPARK_JARS = os.getenv("SPARK_JARS_PATH", _jar_default)


# --- Función principal ---
def get_spark_session(app_name: str = "WoWRaidTelemetry") -> SparkSession:
    """
    Crea o reutiliza una SparkSession configurada para MinIO via S3A.
    Usa AWS SDK v1 (hadoop-aws 3.3.6 + aws-java-sdk-bundle-1.12.786).
    """
    return (
        SparkSession.builder
        .master("local[*]")               # usa todos los cores del TUF
        .appName(app_name)
        .config("spark.jars", SPARK_JARS) # los dos JARs que descargaste

        # Memoria del driver — conservador para laptop
        .config("spark.driver.memory", "2g")

        # ── Conector S3A → MinIO ──────────────────────────────────────
        # Dónde está MinIO (leído de Config, igual que en tu config.py)
        .config("spark.hadoop.fs.s3a.endpoint", Config.S3_ENDPOINT_URL)

        # MinIO usa path-style: localhost:9000/bucket (no bucket.localhost)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

        # Credenciales estáticas (las mismas de tu .env)
        .config("spark.hadoop.fs.s3a.access.key",  Config.S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",  Config.S3_SECRET_KEY)

        # Con SDK v2, forzar proveedor de claves estáticas
        # Sin esto, SDK v2 busca IAM roles de AWS real → falla en local
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .getOrCreate()
    )

# --- Función de limpieza ---
def stop_spark_session(spark: SparkSession) -> None:
    """Detiene la SparkSession activa y libera recursos JVM."""
    spark.stop()
    print("[SparkSession] Sesión detenida correctamente.")
