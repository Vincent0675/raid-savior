"""
Script de Inspección: Bronze vs Silver
Visualiza las transformaciones aplicadas en el ETL.
"""

import sys
import os
import json
import pandas as pd
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.storage.minio_client import MinIOStorageClient

def format_bytes(size_bytes):
    """Convierte bytes a formato legible (KB, MB)"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes/1024:.2f} KB"
    else:
        return f"{size_bytes/(1024**2):.2f} MB"

def find_latest_batch():
    """Encuentra el batch más reciente en Bronze"""
    storage = MinIOStorageClient()
    bronze_bucket = os.getenv("S3_BUCKET_BRONZE", "bronze")
    
    # Listar objetos en Bronze
    paginator = storage.s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bronze_bucket, Prefix='wow_raid_events/')
    
    latest_file = None
    latest_time = None
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.json'):
                    if latest_time is None or obj['LastModified'] > latest_time:
                        latest_file = obj['Key']
                        latest_time = obj['LastModified']
    
    return latest_file

def inspect_bronze_file(bronze_key):
    """Lee y analiza el archivo JSON de Bronze"""
    storage = MinIOStorageClient()
    bronze_bucket = os.getenv("S3_BUCKET_BRONZE", "bronze")
    
    print("="*70)
    print("📦 CAPA BRONZE (JSON Crudo)")
    print("="*70)
    
    # Obtener metadatos del archivo
    response = storage.s3.head_object(Bucket=bronze_bucket, Key=bronze_key)
    size_bytes = response['ContentLength']
    
    print(f"📍 Ruta: s3://{bronze_bucket}/{bronze_key}")
    print(f"📏 Tamaño: {format_bytes(size_bytes)}")
    print(f"🕒 Última modificación: {response['LastModified']}")
    
    # Leer contenido
    obj = storage.get_object(bronze_bucket, bronze_key)
    content = obj.read().decode('utf-8')
    data = json.loads(content)
    
    print(f"\n📊 Estructura del Batch:")
    print(f"   - Batch ID: {data.get('batch_id')}")
    print(f"   - Timestamp de ingesta: {data.get('ingest_timestamp')}")
    print(f"   - Número de eventos: {data.get('event_count')}")
    
    # Convertir a DataFrame para análisis
    df_bronze = pd.DataFrame(data['events'])
    
    print(f"\n🔍 Muestra de Datos Crudos (primeras 2 filas):")
    print(df_bronze[['event_id', 'event_type', 'timestamp', 'damage_amount', 'healing_amount']].head(2).to_string())
    
    print(f"\n📋 Tipos de Datos en Bronze (JSON strings):")
    for col in ['timestamp', 'damage_amount', 'raid_id', 'is_critical_hit']:
        if col in df_bronze.columns:
            sample_value = df_bronze[col].iloc[0]
            print(f"   - {col}: {type(sample_value).__name__} = {sample_value}")
    
    return data, df_bronze

def inspect_silver_file(batch_id, raid_id):
    """Lee y analiza el archivo Parquet de Silver"""
    storage = MinIOStorageClient()
    silver_bucket = os.getenv("S3_BUCKET_SILVER", "silver")
    
    print("\n" + "="*70)
    print("✨ CAPA SILVER (Parquet Optimizado)")
    print("="*70)
    
    # Construir la ruta esperada en Silver
    # Necesitamos la fecha del evento
    ingest_date = datetime.now().strftime("%Y-%m-%d")
    silver_key = f"wow_raid_events/v1/raid_id={raid_id}/event_date={ingest_date}/part-{batch_id}.parquet"
    
    try:
        # Obtener metadatos
        response = storage.s3.head_object(Bucket=silver_bucket, Key=silver_key)
        size_bytes = response['ContentLength']
        
        print(f"📍 Ruta: s3://{silver_bucket}/{silver_key}")
        print(f"📏 Tamaño: {format_bytes(size_bytes)}")
        print(f"🕒 Última modificación: {response['LastModified']}")
        
        # Leer Parquet
        import io
        obj = storage.get_object(silver_bucket, silver_key)
        parquet_bytes = io.BytesIO(obj.read())
        df_silver = pd.read_parquet(parquet_bytes)
        
        print(f"\n📊 Estructura Transformada:")
        print(f"   - Filas: {len(df_silver)}")
        print(f"   - Columnas: {len(df_silver.columns)}")
        print(f"   - Columnas nuevas (enriquecimiento): is_massive_hit, ingest_latency_ms")
        
        print(f"\n📋 Tipos de Datos en Silver (optimizados):")
        for col in ['timestamp', 'damage_amount', 'is_critical_hit', 'is_massive_hit']:
            if col in df_silver.columns:
                dtype = df_silver[col].dtype
                sample_value = df_silver[col].iloc[0]
                print(f"   - {col}: {dtype} = {sample_value}")
        
        print(f"\n🔍 Muestra de Datos Transformados (primeras 2 filas):")
        cols_to_show = ['event_id', 'event_type', 'timestamp', 'damage_amount', 'is_massive_hit']
        print(df_silver[cols_to_show].head(2).to_string())
        
        return df_silver, size_bytes
        
    except Exception as e:
        print(f"   ❌ ERROR leyendo Silver: {e}")
        return None, 0

def compare_layers(bronze_data, df_bronze, df_silver, bronze_size, silver_size):
    """Compara ambas capas y muestra diferencias"""
    print("\n" + "="*70)
    print("🔬 COMPARACIÓN: TRANSFORMACIONES APLICADAS")
    print("="*70)
    
    # 1. Compresión
    compression_ratio = (1 - silver_size / bronze_size) * 100
    print(f"\n📦 Compresión:")
    print(f"   - Bronze (JSON): {format_bytes(bronze_size)}")
    print(f"   - Silver (Parquet): {format_bytes(silver_size)}")
    print(f"   - Reducción: {compression_ratio:.1f}%")
    
    # 2. Columnas añadidas
    print(f"\n➕ Columnas Enriquecidas (no existen en Bronze):")
    new_cols = set(df_silver.columns) - set(df_bronze.columns)
    for col in sorted(new_cols):
        print(f"   - {col}")
    
    # 3. Conversión de tipos
    print(f"\n🔄 Conversión de Tipos:")
    print(f"   Bronze (JSON): Todo es string/object")
    print(f"   Silver (Parquet):")
    print(f"      - timestamp: {df_silver['timestamp'].dtype} (datetime con nanosegundos)")
    if 'damage_amount' in df_silver.columns:
        print(f"      - damage_amount: {df_silver['damage_amount'].dtype} (float nativo)")
    if 'is_critical_hit' in df_silver.columns:
        print(f"      - is_critical_hit: {df_silver['is_critical_hit'].dtype} (booleano)")
    
    # 4. Ejemplo de enriquecimiento
    if 'is_massive_hit' in df_silver.columns:
        massive_hits = df_silver['is_massive_hit'].sum()
        print(f"\n💥 Enriquecimiento - Massive Hits Detectados:")
        print(f"   - Total de golpes masivos (>10k daño): {massive_hits}")
        print(f"   - Porcentaje: {(massive_hits/len(df_silver)*100):.1f}%")
    
    # 5. Particionamiento
    print(f"\n📂 Particionamiento:")
    print(f"   Bronze: /raidid=X/ingest_date=Y/")
    print(f"   Silver: /raid_id=X/event_date=Y/ (optimizado para queries)")
    print(f"   Ventaja: Las columnas raid_id y event_date ya NO están dentro del Parquet")
    print(f"            (PyArrow las reconstruye desde la ruta = menos bytes por fila)")

def main():
    print("\n" + "🔎 INSPECTOR DE TRANSFORMACIONES: BRONZE → SILVER" + "\n")
    
    # 1. Encontrar el batch más reciente
    print("[1] Buscando el batch más reciente en Bronze...")
    bronze_key = find_latest_batch()
    
    if not bronze_key:
        print("❌ No se encontraron batches en Bronze")
        return
    
    # 2. Inspeccionar Bronze
    bronze_data, df_bronze = inspect_bronze_file(bronze_key)
    bronze_size = len(json.dumps(bronze_data).encode('utf-8'))
    
    # 3. Inspeccionar Silver
    batch_id = bronze_data['batch_id']
    raid_id = bronze_data['events'][0]['raid_id']
    df_silver, silver_size = inspect_silver_file(batch_id, raid_id)
    
    if df_silver is None:
        print("\n⚠️ No se pudo leer Silver. ¿El ETL corrió correctamente?")
        return
    
    # 4. Comparar
    compare_layers(bronze_data, df_bronze, df_silver, bronze_size, silver_size)
    
    print("\n" + "="*70)
    print("✅ INSPECCIÓN COMPLETADA")
    print("="*70)
    print("\n💡 Ahora sabes exactamente qué transformaciones aplicó tu ETL.")
    print("   Los datos pasaron de JSON crudo a Parquet optimizado, limpio y enriquecido.\n")

if __name__ == "__main__":
    main()
