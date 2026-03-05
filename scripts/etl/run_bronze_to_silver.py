"""
Orquestador ETL: Bronze → Silver
Lee TODOS los archivos JSON de Bronze, transforma a Parquet y escribe en Silver.
"""

from tqdm import tqdm

from src.etl.bronze_to_silver import BronzeToSilverETL
from src.storage.minio_client import MinIOStorageClient


def list_bronze_files(storage: MinIOStorageClient, bucket: str = "bronze") -> list:
    """
    Lista todos los archivos JSON en el bucket Bronze usando boto3.
    
    Returns:
        Lista de keys (paths) de archivos JSON
    """
    print(f"\n🔍 Listando archivos en s3://{bucket}/...")
    
    try:
        # Llamar a list_objects_v2 (API moderna de S3)
        response = storage.s3.list_objects_v2(Bucket=bucket)
        
        # Verificar si el bucket tiene contenido
        if 'Contents' not in response:
            print(f"  ⚠️  El bucket '{bucket}' está vacío")
            return []
        
        json_files = []
        
        # Iterar sobre objetos (cada obj es un dict con 'Key', 'Size', etc.)
        for obj in response['Contents']:
            key = obj['Key']  # ← Acceso por diccionario, no atributo
            
            # Filtrar solo archivos JSON
            if key.endswith('.json'):
                json_files.append(key)
        
        # Manejar paginación (si hay más de 1000 objetos)
        while response.get('IsTruncated', False):
            continuation_token = response['NextContinuationToken']
            response = storage.s3.list_objects_v2(
                Bucket=bucket,
                ContinuationToken=continuation_token
            )
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.endswith('.json'):
                        json_files.append(key)
        
        print(f"  ✅ Encontrados: {len(json_files)} archivos JSON")
        return json_files
    
    except Exception as e:
        print(f"  ❌ Error listando bucket: {e}")
        import traceback
        traceback.print_exc()
        return []


def main():
    print("=" * 70)
    print("ETL Bronze → Silver Pipeline")
    print("=" * 70)
    print()
    
    # Inicializar cliente de storage y ETL
    print("Inicializando clientes MinIO y ETL...")
    storage = MinIOStorageClient()
    etl = BronzeToSilverETL()
    
    # Listar archivos de Bronze
    bronze_files = list_bronze_files(storage, bucket="bronze")
    
    if not bronze_files:
        print()
        print("⚠️  No se encontraron archivos JSON en Bronze.")
        print()
        print("Verifica que:")
        print("  1. MinIO esté corriendo → docker-compose ps")
        print("  2. Hayas ejecutado ingesta HTTP o S3")
        print("  3. Los archivos estén en s3://bronze/")
        print()
        print("Para verificar manualmente:")
        print("  → Abre http://localhost:9001")
        print("  → Login: minio / minio123")
        print("  → Ve a bucket 'bronze'")
        print()
        return
    
    # Mostrar muestra de archivos encontrados
    print()
    print(f"📂 Archivos encontrados (muestra):")
    for i, f in enumerate(bronze_files[:5]):
        print(f"   {i+1}. {f}")
    if len(bronze_files) > 5:
        print(f"   ... y {len(bronze_files) - 5} más")
    print()
    
    # Procesar cada archivo
    successful = 0
    failed = 0
    skipped = 0
    total_rows = 0
    
    print(f"⚡ Iniciando procesamiento de {len(bronze_files)} archivos...")
    print()
    
    with tqdm(total=len(bronze_files), desc="ETL Bronze→Silver", unit="archivo") as pbar:
        for bronze_key in bronze_files:
            try:
                result = etl.run(bronze_key)
                
                if result.get('status') == 'success':
                    successful += 1
                    rows = result.get('storage', {}).get('rows', 0)
                    total_rows += rows
                    
                elif result.get('status') == 'skipped':
                    skipped += 1
                    reason = result.get('reason', 'unknown')
                    tqdm.write(f"⏭️  Omitido: {bronze_key} → {reason}")
                    
                else:
                    failed += 1
                    tqdm.write(f"⚠️  Fallo: {bronze_key}")
                
                pbar.update(1)
                
            except Exception as e:
                failed += 1
                tqdm.write(f"❌ Error en {bronze_key}:")
                tqdm.write(f"   {str(e)}")
                pbar.update(1)
    
    # Reporte final
    print()
    print("=" * 70)
    print("ETL COMPLETADO")
    print("=" * 70)
    print(f"  ✅ Exitosos:      {successful:>6}")
    print(f"  ⏭️  Omitidos:      {skipped:>6}")
    print(f"  ❌ Fallidos:      {failed:>6}")
    print(f"  📊 Filas totales: {total_rows:>6,}")
    print("=" * 70)
    
    if successful > 0:
        print()
        print("✅ Datos transformados disponibles en:")
        print("   s3://silver/wow_raid_events/v1/")
        print()
        print("   Estructura de particiones Hive:")
        print("   raid_id=<id>/event_date=<YYYY-MM-DD>/part-<uuid>.parquet")
        print()
        print("🎯 Listo para Fase 4 - Gold Analytics")
        print()
    elif failed > 0:
        print()
        print("⚠️  Hay errores. Revisa los mensajes arriba.")
        print()


if __name__ == "__main__":
    main()
