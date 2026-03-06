"""
Script de Prueba FASE 3: ETL Bronze -> Silver
Busca el primer batch real en Bronze y lo transforma.
"""

import os
import pandas as pd

from src.etl.bronze_to_silver import BronzeToSilverETL
from src.storage.minio_client import MinIOStorageClient

def find_any_bronze_file():
    """Busca recursivamente el primer archivo JSON en el bucket Bronze"""
    client = MinIOStorageClient()
    bronze_bucket = os.getenv("S3_BUCKET_BRONZE", "bronze")
    
    print(f"🔍 Buscando archivos en bucket '{bronze_bucket}'...")
    
    # Listamos objetos (paginación automática de boto3)
    paginator = client.s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bronze_bucket, Prefix='wow_raid_events/')
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.json'):
                    return key
    return None

def main():
    print("="*60)
    print("🚀 INICIANDO TEST ETL (BRONZE -> SILVER)")
    print("="*60)
    
    # 1. Encontrar un archivo real para probar
    bronze_key = find_any_bronze_file()
    
    if not bronze_key:
        print("❌ ERROR CRÍTICO: No se encontraron archivos JSON en Bronze.")
        print("   Asegúrate de haber corrido la Fase 2 o inyecta datos manualmente.")
        return

    print(f"✅ Archivo encontrado: {bronze_key}")
    
    # 2. Ejecutar ETL
    print("\n⚡ Ejecutando transformación...")
    etl = BronzeToSilverETL()
    
    try:
        result = etl.run(bronze_key)
        
        if result['status'] == 'success':
            meta = result['metadata']
            store = result['storage']
            
            print("\n🎉 ÉXITO: Transformación completada")
            print(f"   - Eventos procesados: {meta.get('rows_after_validation', 'N/A')}")
            print(f"   - Duplicados eliminados: {meta.get('duplicates_removed', 0)}")
            print(f"   - Ruta Parquet: {store['s3_path']}")
            print(f"   - Tamaño original (JSON): ~{os.path.getsize(bronze_key) if os.path.exists(bronze_key) else 'N/A'} bytes (estimado)")
            print(f"   - Tamaño final (Parquet): {store['bytes']} bytes")
            
            # Verificación rápida del Parquet (lectura de vuelta)
            print("\n🔎 Verificando integridad del archivo Parquet...")
            df_check = pd.read_parquet(
                store['s3_path'].replace("s3://", f"{etl.storage.endpoint_url}/").replace(etl.storage.endpoint_url + "/" + etl.bucket_silver, f"s3://{etl.bucket_silver}"),
                storage_options={
                    "key": etl.storage.access_key,
                    "secret": etl.storage.secret_key,
                    "client_kwargs": {"endpoint_url": etl.storage.endpoint_url}
                }
            )
            print(f"   ✅ Leído correctamente con Pandas. Columnas: {list(df_check.columns)}")
            
        else:
            print(f"\n⚠️ SKIPPED: {result.get('reason')}")
            
    except Exception as e:
        print(f"\n❌ ERROR EN ETL: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
