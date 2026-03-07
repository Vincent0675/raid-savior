#!/usr/bin/env python3
"""
Ingesta masiva de eventos a Bronze layer.

Modos:
1. HTTP (vía receptor Flask) - Testing completo, lento
2. Direct S3 (boto3 directo) - Producción, rápido

Uso:
    python scripts/ingest_large_dataset.py --mode http
    python scripts/ingest_large_dataset.py --mode s3
"""

import argparse
import json
import time
from datetime import datetime
from typing import Any
import uuid

import requests
import boto3
from tqdm import tqdm

from src.generators.raid_event_generator import WoWEventGenerator


class DatasetIngestor:
    """Orquestador de ingesta masiva con múltiples backends."""
    
    def __init__(
        self,
        mode: str = "http"
    ):
        self.mode = mode
        
        if mode == "http":
            self.api_url = "http://localhost:5000/events"
        elif mode == "s3":
            self.s3_client = boto3.client(
                's3',
                endpoint_url='http://localhost:9000',
                aws_access_key_id='minio',
                aws_secret_access_key='minio123'
            )
            self.bucket = "bronze"
    
    def ingest_via_http(
        self,
        events: list[dict[str, Any]],
        batch_size: int = 1000,
        max_retries: int = 3
    ):
        """
        Ingesta vía receptor Flask (HTTP POST).
        
        Args:
            events: Lista de eventos (dicts)
            batch_size: Eventos por batch (Flask recomienda <1000)
            max_retries: Intentos por batch en caso de error
        """
        total_batches = (len(events) + batch_size - 1) // batch_size
        
        print("\n📡 Ingesta HTTP (Flask Receptor)")
        print(f"  Total eventos: {len(events):,}")
        print(f"  Batch size: {batch_size}")
        print(f"  Total batches: {total_batches}")
        print()
        
        successful = 0
        failed = 0
        
        with tqdm(total=len(events), desc="Enviando eventos", unit="evento") as pbar:
            for i in range(0, len(events), batch_size):
                batch = events[i:i + batch_size]
                
                # Retry logic
                for attempt in range(max_retries):
                    try:
                        response = requests.post(
                            self.api_url,
                            json=batch,
                            headers={'Content-Type': 'application/json'},
                            timeout=30
                        )
                        
                        if response.status_code == 201:
                            successful += len(batch)
                            pbar.update(len(batch))
                            break
                        else:
                            if attempt == max_retries - 1:
                                failed += len(batch)
                                tqdm.write(f"❌ Batch {i//batch_size + 1} falló: {response.status_code}")
                    
                    except requests.exceptions.RequestException as e:
                        if attempt == max_retries - 1:
                            failed += len(batch)
                            tqdm.write(f"❌ Batch {i//batch_size + 1} error de red: {e}")
                        else:
                            time.sleep(1)  # Esperar antes de reintentar
        
        print("\n✅ Ingesta HTTP completada:")
        print(f"  Exitosos: {successful:,} eventos")
        print(f"  Fallidos: {failed:,} eventos")
        print(f"  Tasa de éxito: {(successful / len(events) * 100):.2f}%")
    
    def ingest_via_s3(
            self,
            events: list[dict[str, Any]],
            raid_id: str = "raid001"
    ):
        """
        Ingesta directa a MinIO (S3) sin pasar por Flask.
        
        Args:
            events: Lista de eventos (dicts)
            raid_id: ID de la raid
        """
        print("\n🗄️  Ingesta Directa S3 (MinIO)")
        print(f"  Total eventos: {len(events):,}")
        print(f"  Raid ID: {raid_id}")
        print()
        
        # Agrupar eventos por ingest_date para particionamiento
        events_by_date: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            # Usar timestamp del evento (no ingestion_timestamp)
            event_ts = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            ingest_date = event_ts.strftime('%Y-%m-%d')
            
            if ingest_date not in events_by_date:
                events_by_date[ingest_date] = []
            events_by_date[ingest_date].append(event)
        
        print(f"  Particiones (fechas): {len(events_by_date)}")
        
        # Escribir cada partición
        total_written = 0
        for ingest_date, date_events in tqdm(events_by_date.items(), desc="Escribiendo particiones"):
            # Generar batch_id único
            batch_id = str(uuid.uuid4())
            
            # Path S3 con Hive-style partitioning
            s3_key = f"wow_raid_events/v1/raidid={raid_id}/ingest_date={ingest_date}/batch_{batch_id}.json"
            
            # Serializar eventos
            payload = json.dumps(date_events, ensure_ascii=False, indent=2)
            
            # Upload a S3
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                    Body=payload.encode('utf-8'),
                    ContentType='application/json'
                )
                total_written += len(date_events)
            except Exception as e:
                tqdm.write(f"❌ Error escribiendo partición {ingest_date}: {e}")
        
        print("\n✅ Ingesta S3 completada:")
        print(f"  Eventos escritos: {total_written:,}")
        print(f"  Particiones creadas: {len(events_by_date)}")
        print(f"  Bucket: s3://{self.bucket}/wow_raid_events/v1/raidid={raid_id}/")
    
    def run(
        self,
        num_events: int,
        batch_size: int = 1000
    ):
        """Pipeline completo: Generar → Ingerir."""
        
        print("=" * 70)
        print("WoW Telemetry - Ingesta Masiva de Dataset")
        print("=" * 70)
        print(f"Modo: {self.mode.upper()}")
        print()
        
        # Paso 1: Generar eventos
        print("🎲 Generando eventos sintéticos...")
        gen = WoWEventGenerator(seed=42)
        
        session = gen.generate_raid_session(
            raid_id="raid001",
            num_players=25,
            duration_s=480,
            boss_name="Lich King"
        )
        
        events = gen.generate_events(session=session, num_events=num_events)
        
        # Serializar a dict (Pydantic → JSON-compatible)
        events_dict = [e.model_dump(mode="json") for e in events]
        
        print(f"  ✅ {len(events):,} eventos generados")
        
        # Paso 2: Ingerir según modo
        start_time = time.time()
        
        if self.mode == "http":
            self.ingest_via_http(events_dict, batch_size=batch_size)
        elif self.mode == "s3":
            self.ingest_via_s3(events_dict, raid_id=session.raid_id)
        
        elapsed = time.time() - start_time
        throughput = len(events) / elapsed
        
        print("\n⏱️  Métricas de Rendimiento:")
        print(f"  Tiempo total: {elapsed:.2f}s")
        print(f"  Throughput: {throughput:.0f} eventos/s")
        print()
        print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Ingesta masiva de eventos sintéticos WoW"
    )
    parser.add_argument(
        '--mode',
        choices=['http', 's3'],
        default='http',
        help='Modo de ingesta: http (Flask) o s3 (directo MinIO)'
    )
    parser.add_argument(
        '--events',
        type=int,
        default=10000,
        help='Número de eventos a generar e ingerir'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Tamaño de batch para modo HTTP'
    )
    
    args = parser.parse_args()
    
    # Validación previa
    if args.mode == 'http':
        # Verificar que Flask esté corriendo
        try:
            response = requests.get('http://localhost:5000/health', timeout=2)
            if response.status_code != 200:
                print("⚠️  Flask receptor no responde en http://localhost:5000")
                print("   Ejecuta primero: python src/api/receiver.py")
                return
        except requests.exceptions.RequestException:
            print("❌ Error: Flask receptor no está corriendo")
            print("   Ejecuta en otra terminal: python src/api/receiver.py")
            return
    
    # Ejecutar ingesta
    ingestor = DatasetIngestor(mode=args.mode)
    ingestor.run(num_events=args.events, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
