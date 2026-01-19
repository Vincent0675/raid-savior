import os
import json
import uuid
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from typing import Dict, Any, List

class MinIOStorageClient:
    """
    Cliente wrapper para MinIO/S3 enfocado en la capa Bronze.
    Implementa el contrato de escritura: wow_raid_events/v1/raidid=X/ingest_date=Y/
    """
    
    def __init__(self):
        # Carga configuración desde variables de entorno (12-factor app)
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
        CONTRATO DE ESCRITURA:
        Calcula la ruta determinista donde vivirá el archivo.
        Pattern: wow_raid_events/v1/raidid=<id>/ingest_date=<YYYY-MM-DD>/batch_<uuid>.json
        """
        # Extraer fecha YYYY-MM-DD del timestamp ISO
        # Asumimos formato ISO 8601 (ej: 2026-01-19T18:30:00...)
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
