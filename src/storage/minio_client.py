import os
import json
import uuid
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from typing import Dict, Any, List

class MinIOStorageClient:
    """
    Cliente wrapper para MinIO/S3 enfocado en la capa Bronze y Silver.
    Maneja lectura/escritura y cálculo de rutas estándar.
    """

    def __init__(self):
        # Carga configuración desde variables de entorno (12-factor app)
        # Usa 'localhost' por defecto para desarrollo local
        self.endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
        self.access_key = os.getenv("S3_ACCESS_KEY", "minio")
        self.secret_key = os.getenv("S3_SECRET_KEY", "minio123")
        self.bucket = os.getenv("S3_BUCKET_BRONZE", "bronze")

        # Cliente boto3 puro (bajo nivel)
        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1"  # MinIO ignora esto, pero boto3 lo pide
        )

    def calculate_object_key(self, raidid: str, ingest_timestamp: str, batch_id: str) -> str:
        """
        Calcula la ruta determinista donde vivirá el archivo en Bronze.
        Pattern: wow_raid_events/v1/raidid=<id>/ingest_date=<YYYY-MM-DD>/batch_<uuid>.json
        """
        # Extraer fecha YYYY-MM-DD del timestamp ISO
        ingest_date = ingest_timestamp[:10]
        return f"wow_raid_events/v1/raidid={raidid}/ingest_date={ingest_date}/batch_{batch_id}.json"

    def save_batch(self, raidid: str, batch_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Persiste un batch validado en Bronze.
        """
        batch_id = batch_data["batch_id"]
        ingest_timestamp = batch_data["ingest_timestamp"]

        # 1. Calcular dónde va (Key)
        key = self.calculate_object_key(raidid, ingest_timestamp, batch_id)

        # 2. Serializar a JSON (bytes)
        body_bytes = json.dumps(batch_data, default=str).encode('utf-8')

        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body_bytes,
                ContentType="application/json",
                Metadata={
                    "source": "wow-telemetry-pipeline",
                    "layer": "bronze",
                    "raidid": raidid
                }
            )
            return {"status": "success", "s3_path": f"s3://{self.bucket}/{key}"}
        
        except ClientError as e:
            # Aquí podrías loguear el error real
            raise ConnectionError(f"Error escribiendo en MinIO: {e}")

    # --- MÉTODOS AÑADIDOS PARA FASE 3 (Silver ETL) ---

    def get_object(self, bucket_name: str, object_name: str):
        """
        Lee un objeto desde S3/MinIO.
        Retorna un objeto StreamingBody que se comporta como un archivo abierto.
        """
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=object_name)
            return response['Body']
        except ClientError as e:
            raise FileNotFoundError(f"No se pudo leer {object_name} de {bucket_name}: {e}")

    def put_object(self, bucket_name: str, object_name: str, data: Any, length: int, content_type: str = "application/octet-stream"):
        """
        Método genérico para escribir cualquier tipo de dato (Parquet, JSON, etc.)
        """
        try:
            self.s3.put_object(
                Bucket=bucket_name,
                Key=object_name,
                Body=data,
                ContentLength=length,
                ContentType=content_type
            )
        except ClientError as e:
            raise ConnectionError(f"Error escribiendo {object_name} en {bucket_name}: {e}")
