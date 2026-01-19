import os
from dotenv import load_dotenv

# Cargar variables del .env si existe (buena práctica local)
load_dotenv()

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
    
    # Pipeline Settings
    MAX_EVENTS_PER_BATCH = int(os.getenv("MAX_EVENTS_PER_BATCH", 1000))
