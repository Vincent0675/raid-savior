"""
Transformadores para la capa Silver (Signal Conditioning).
Encapsulan la lógica de limpieza, validación y enriquecimiento.
"""

import pandas as pd
from typing import Dict, List, Tuple

class SilverTransformer:
    """
    Actúa como un circuito acondicionador de señal.
    Entrada: DataFrame 'sucio' (tipos mezclados, posibles duplicados).
    Salida: DataFrame 'limpio' y enriquecido.
    """

    @staticmethod
    def cast_types(df: pd.DataFrame) -> pd.DataFrame:
        """
        Conversión Analógico-Digital (ADC) de los datos.
        Convierte strings genéricos a tipos binarios precisos.
        """
        df = df.copy()
        
        # 1. Timestamps (Time Domain)
        # Convertimos strings ISO a datetime64[ns] (nanosegundos, alta precisión)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        
        if 'ingest_timestamp' in df.columns:
            df['ingest_timestamp'] = pd.to_datetime(df['ingest_timestamp'], utc=True)
        
        # 2. Magnitudes Físicas (Floats)
        # Forzamos a numérico. 'coerce' convierte errores en NaN (valores nulos seguros)
        numeric_cols = [
            'damage_amount', 'healing_amount', 
            'target_entity_health_pct_before', 'target_entity_health_pct_after',
            'resource_amount_before', 'resource_amount_after'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 3. Identificadores (Categorical/String)
        # Usamos el tipo 'string' moderno de Pandas (pd.StringDtype) en lugar de 'object'
        string_cols = [
            'event_id', 'raid_id', 'event_type', 'source_player_name', 
            'target_entity_name', 'ability_name', 'source_system'
        ]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype('string')
        
        return df

    @staticmethod
    def deduplicate(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """
        Filtro de Ruido: Elimina rebotes de señal (eventos duplicados).
        Retorna: (DataFrame limpio, cantidad de eliminados)
        """
        initial_count = len(df)
        # Si llega el mismo event_id dos veces, nos quedamos solo con el primero
        df = df.drop_duplicates(subset=['event_id'], keep='first')
        duplicates_removed = initial_count - len(df)
        
        return df, duplicates_removed

    @staticmethod
    def validate_ranges(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Protección de Sobrevoltaje:
        Descarta valores físicamente imposibles (ej. salud < 0% o > 100%).
        """
        errors = []
        
        # Validación: Porcentajes de salud deben ser 0-100
        # Usamos máscaras booleanas (vectorización) para velocidad
        for col in ['target_entity_health_pct_before', 'target_entity_health_pct_after']:
            if col in df.columns:
                # Detectar inválidos
                mask_invalid = (df[col] < 0) | (df[col] > 100)
                invalid_count = mask_invalid.sum()
                
                if invalid_count > 0:
                    errors.append(f"{col}: {invalid_count} valores fuera de rango [0-100]")
                    # Filtrar: Nos quedamos con los válidos o los nulos (NaN)
                    df = df[~mask_invalid]
        
        return df, errors

    @staticmethod
    def enrich(df: pd.DataFrame) -> pd.DataFrame:
        """
        Etapa de Ganancia/Amplificación:
        Calcula nuevas variables derivadas de la señal original.
        """
        df = df.copy()
        
        # 1. Latencia de Ingesta (Delta T)
        # Tiempo que tardó la señal en viajar desde el juego hasta nuestra nube
        if 'ingest_timestamp' in df.columns and 'timestamp' in df.columns:
            df['ingest_latency_ms'] = (df['ingest_timestamp'] - df['timestamp']).dt.total_seconds() * 1000
            df['ingest_latency_ms'] = df['ingest_latency_ms'].fillna(0).astype('int32')
        
        # 2. Flag de "Massive Hit" (Detector de picos)
        # Marca booleana si el daño supera un umbral crítico (10k)
        if 'damage_amount' in df.columns and 'event_type' in df.columns:
            df['is_massive_hit'] = (
                (df['event_type'] == 'combat_damage') & 
                (df['damage_amount'] > 10000)
            ).astype('bool')
        else:
            df['is_massive_hit'] = False
            
        # 3. Fecha del evento (Para particionado en disco)
        if 'timestamp' in df.columns:
            df['event_date'] = df['timestamp'].dt.date.astype('string')
        
        return df

    @staticmethod
    def transform_pipeline(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, any]]:
        """
        Circuito completo: Tipado -> Deduplicación -> Validación -> Enriquecimiento
        """
        metadata = {}
        
        # Paso 1: Tipado
        df = SilverTransformer.cast_types(df)
        
        # Paso 2: Filtro de duplicados
        df, dup_count = SilverTransformer.deduplicate(df)
        metadata['duplicates_removed'] = dup_count
        
        # Paso 3: Validación de rangos
        df, range_errors = SilverTransformer.validate_ranges(df)
        metadata['validation_errors'] = range_errors
        metadata['rows_after_validation'] = len(df)
        
        # Paso 4: Enriquecimiento
        df = SilverTransformer.enrich(df)
        
        return df, metadata
