import pandas as pd
from src.storage.minio_client import MinIOStorageClient
import io

def main():
    storage = MinIOStorageClient()
    
    # Leer raid_summary
    summary_key = "wow_raid_gold/v1/raid_id=raid001/event_date=2026-01-22/raid_summary.parquet"
    summary_bytes = storage.s3.get_object(Bucket="gold", Key=summary_key)['Body'].read()
    raid_summary = pd.read_parquet(io.BytesIO(summary_bytes))
    
    print("=== RAID SUMMARY ===")
    print(raid_summary.to_string())
    print(f"\nColumnas: {raid_summary.columns.tolist()}")
    
    # Leer player_raid_stats
    stats_key = "wow_raid_gold/v1/raid_id=raid001/event_date=2026-01-22/player_raid_stats.parquet"
    stats_bytes = storage.s3.get_object(Bucket="gold", Key=stats_key)['Body'].read()
    player_stats = pd.read_parquet(io.BytesIO(stats_bytes))
    
    print("\n=== PLAYER RAID STATS (primeros 5 jugadores) ===")
    print(player_stats.head().to_string())
    print(f"\nTotal jugadores: {len(player_stats)}")
    print(f"Columnas: {player_stats.columns.tolist()}")

if __name__ == "__main__":
    main()
