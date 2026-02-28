import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables del .env si existe
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

class Config:
    """Configuración centralizada para la aplicación Flask."""
    
    # Flask Settings
    # Usamos os.getenv para leer variables de entorno, con valores por defecto seguros
    FLASK_HOST = os.getenv("FLASK_HOST", "0.0.0.0")
    FLASK_PORT = int(os.getenv("FLASK_PORT", 5000))
    DEBUG = os.getenv("FLASK_DEBUG", "True").lower() == "true"
    
    # MinIO / S3 Settings
    S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minio")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minio123")
    S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "bronze")
    S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "silver")
    S3_BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD", "gold")
    
    # Pipeline Settings
    MAX_EVENTS_PER_BATCH = int(os.getenv("MAX_EVENTS_PER_BATCH", 1000))
