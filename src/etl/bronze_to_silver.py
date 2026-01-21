"""
ETL Principal: Bronze -> Silver.
Lectura de JSON crudo -> Transformación Pandas -> Escritura Parquet particionado.
"""

import sys
import os
import json
import pandas as pd
from typing import Dict, Tuple
from datetime import datetime, timezone
import io  # Para manejar streams de bytes

# Aseguramos que Python encuentre nuestros módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.storage.minio_client import MinIOStorageClient
from src.etl.transformers import SilverTransformer

class BronzeToSilverETL:
    def __init__(self):
        # Cliente MinIO ya existente de la Fase 2
        self.storage = MinIOStorageClient()
        # Leemos buckets del .env (con valores por defecto por seguridad)
        self.bucket_bronze = os.getenv("S3_BUCKET_BRONZE", "bronze")
        self.bucket_silver = os.getenv("S3_BUCKET_SILVER", "silver")
        self.transformer = SilverTransformer()

    def read_bronze_batch(self, batch_key: str) -> Dict:
        """Descarga y deserializa el JSON de Bronze"""
        try:
            response = self.storage.get_object(self.bucket_bronze, batch_key)
            # MinIO devuelve un stream, lo leemos y decodificamos
            content = response.read().decode('utf-8')
            return json.loads(content)
        except Exception as e:
            raise IOError(f"Error leyendo Bronze [{batch_key}]: {e}")

    def save_silver(self, df: pd.DataFrame, raid_id: str, batch_id: str) -> Dict:
        """
        Guarda el DataFrame como Parquet comprimido con Snappy.
        Ruta: raid_id=X / event_date=Y / part-Z.parquet
        """
        if df.empty:
            return {"status": "skipped", "reason": "empty_dataframe"}

        # Obtenemos la fecha para la partición Hive-style
        event_date = df['event_date'].iloc[0] if 'event_date' in df.columns else "unknown"
        
        # Construimos la ruta destino (Key)
        s3_key = f"wow_raid_events/v1/raid_id={raid_id}/event_date={event_date}/part-{batch_id}.parquet"
        
        # --- CORRECCIÓN: Prevenir conflicto de particiones ---
        # Hacemos una copia para no afectar al DataFrame original en memoria
        df_to_save = df.copy()
        
        # Eliminamos las columnas que YA están en la ruta de carpetas (partition keys)
        # para que PyArrow no se confunda al leer.
        cols_to_drop = []
        if 'raid_id' in df_to_save.columns:
            cols_to_drop.append('raid_id')
        if 'event_date' in df_to_save.columns:
            cols_to_drop.append('event_date')
            
        if cols_to_drop:
            df_to_save = df_to_save.drop(columns=cols_to_drop)
        
        try:
            # 1. Serializar a Buffer en memoria (usando df_to_save)
            out_buffer = io.BytesIO()
            df_to_save.to_parquet(
                out_buffer,
                index=False,
                engine='pyarrow',
                compression='snappy'
            )
            
            # 2. Subir a MinIO
            out_buffer.seek(0)
            data_len = out_buffer.getbuffer().nbytes
            
            self.storage.put_object(
                bucket_name=self.bucket_silver,
                object_name=s3_key,
                data=out_buffer,
                length=data_len,
                content_type="application/octet-stream"
            )
            return {
                "status": "success",
                "s3_path": f"s3://{self.bucket_silver}/{s3_key}",
                "rows": len(df),
                "bytes": data_len
            }
            
        except Exception as e:
            raise IOError(f"Error escribiendo Silver: {e}")

    def run(self, bronze_key: str) -> Dict:
        """
        Ejecuta el ciclo completo para un archivo específico.
        """
        print(f"⚡ [ETL] Procesando: {bronze_key}")
        
        # 1. READ
        raw_data = self.read_bronze_batch(bronze_key)
        
        # 2. TRANSFORM
        # Extraemos la lista de eventos del JSON envelope
        events_list = raw_data.get('events', [])
        if not events_list:
            return {"status": "skipped", "reason": "no_events_in_batch"}
            
        df_raw = pd.DataFrame(events_list)
        df_silver, metadata = self.transformer.transform_pipeline(df_raw)
        
        # 3. WRITE
        batch_id = raw_data.get('batch_id', 'unknown')
        raid_id = df_silver['raid_id'].iloc[0] if 'raid_id' in df_silver.columns else 'unknown'
        
        storage_result = self.save_silver(df_silver, raid_id, batch_id)
        
        return {
            "status": "success",
            "metadata": metadata,
            "storage": storage_result
        }
