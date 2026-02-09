import pandas as pd
from typing import Dict, Any, List

from src.storage.minioclient import MinIOStorageClient
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

    def read_silver_partition(
        self,
        raid_id: str,
        event_date: str
    ) -> pd.DataFrame:
        """
        Lee todos los archivos Parquet de Silver para una partición
        (raid_id, event_date) y los concatena en un único DataFrame.
        """
        ...

    def write_gold_tables(
        self,
        raid_summary: pd.DataFrame,
        player_stats: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Escribe raid_summary y player_raid_stats en el bucket Gold
        usando un layout particionado por raid_id y event_date.
        Devuelve metadatos (paths, filas escritas, etc.).
        """
        ...

    def run_for_partition(
        self,
        raid_id: str,
        event_date: str
    ) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo Gold para una partición
        (raid_id, event_date):
        1. Lee Silver.
        2. Construye raid_summary.
        3. Construye player_raid_stats.
        4. Escribe ambas tablas en Gold.
        """
        ...
