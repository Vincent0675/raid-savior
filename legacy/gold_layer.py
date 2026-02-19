import pandas as pd
import io
from typing import Dict, Any, List

from src.storage.minio_client import MinIOStorageClient
from src.config import Config
from src.analytics.aggregators import build_raid_summary, build_player_raid_stats

class GoldLayerETL:
    """
    Orquestador ETL de la capa Gold.
    Lee Parquet de Silver, agrega métricas y escribe tablas Gold en MinIO.
    """

    def __init__(self, config: Config | None = None):
        self.config = config or Config()
        self.storage = MinIOStorageClient()  # reutilizamos cliente S3/MinIO
        self.gold_bucket = self.config.S3_BUCKET_GOLD
        self.silver_bucket = self.config.S3_BUCKET_SILVER

    def read_silver_partition(self, raid_id: str, event_date: str) -> pd.DataFrame:
        prefix = f"wow_raid_events/v1/raid_id={raid_id}/event_date={event_date}/"
        
        # Listar objetos en Silver
        objects = self.storage.list_objects(self.silver_bucket, prefix)
        
        if not objects:
            raise ValueError(f"No Silver data found for {raid_id}/{event_date}")
        
        # Leer y concatenar todos los Parquet
        dfs = []
        for obj in objects:
            parquet_bytes = self.storage.s3.get_object(
                Bucket=self.silver_bucket, Key=obj
            )['Body'].read()
            df = pd.read_parquet(io.BytesIO(parquet_bytes))
            dfs.append(df)
        
        return pd.concat(dfs, ignore_index=True)

    def write_gold_tables(
        self, raid_summary: pd.DataFrame, player_stats: pd.DataFrame
    ) -> Dict[str, Any]:
        raid_id = raid_summary["raid_id"].iloc[0]
        event_date = raid_summary["event_date"].iloc[0]
        
        base_path = f"wow_raid_gold/v1/raid_id={raid_id}/event_date={event_date}/"
        
        # Escribir raid_summary
        summary_key = base_path + "raid_summary.parquet"
        summary_bytes = raid_summary.to_parquet(compression="snappy", index=False)
        self.storage.s3.put_object(
            Bucket=self.gold_bucket, Key=summary_key, Body=summary_bytes
        )
        
        # Escribir player_raid_stats
        stats_key = base_path + "player_raid_stats.parquet"
        stats_bytes = player_stats.to_parquet(compression="snappy", index=False)
        self.storage.s3.put_object(
            Bucket=self.gold_bucket, Key=stats_key, Body=stats_bytes
        )
        
        return {
            "raid_summary_path": f"s3://{self.gold_bucket}/{summary_key}",
            "player_stats_path": f"s3://{self.gold_bucket}/{stats_key}",
            "raid_summary_rows": len(raid_summary),
            "player_stats_rows": len(player_stats),
        }

    def run_for_partition(self, raid_id: str, event_date: str) -> Dict[str, Any]:
        print(f"[Gold ETL] Procesando raid_id={raid_id}, event_date={event_date}")
        
        # 1. Leer Silver
        df_silver = self.read_silver_partition(raid_id, event_date)
        print(f"[Gold ETL] Eventos leídos: {len(df_silver)}")
        
        # Añadir raid_id y event_date como columnas (contexto de partición)
        df_silver["raidid"] = raid_id
        df_silver["eventdate"] = event_date
        
        # Normalizar nombres de columnas para los agregadores
        df_silver = df_silver.rename(columns={
            "event_type": "eventtype",
            "source_player_id": "sourceplayerid",
            "source_player_name": "sourceplayername",
            "source_player_class": "sourceplayerclass",
            "source_player_role": "sourceplayerrole",
            "damage_amount": "damageamount",
            "healing_amount": "healingamount",
            "target_entity_type": "targetentitytype",
            "target_entity_id": "targetentityid",
            "target_entity_health_pct_after": "targetentityhealthpctafter",
            "is_critical_hit": "iscriticalhit",
        })
        
        # 2. Agregar
        raid_summary = build_raid_summary(df_silver)
        player_stats = build_player_raid_stats(df_silver, raid_summary)
        print(f"[Gold ETL] Raids: {len(raid_summary)}, Jugadores: {len(player_stats)}")
        
        # 3. Escribir Gold
        result = self.write_gold_tables(raid_summary, player_stats)
        print(f"[Gold ETL] Escritura completada en bucket '{self.gold_bucket}'")
        
        return result
