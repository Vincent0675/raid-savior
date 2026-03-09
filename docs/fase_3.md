# Fase 3 — ETL Bronze a Silver

## Objetivo

La Fase 3 transforma los datos crudos almacenados en Bronze en un dataset limpio, tipado y optimizado para análisis. En esta etapa se implementó un proceso ETL que lee batches JSON desde MinIO Bronze, aplica transformaciones de calidad y persistencia columnar, y escribe el resultado en la capa Silver en formato Parquet.

## Alcance

Incluye:

- Lectura de batches JSON desde Bronze
- Conversión de eventos a DataFrame
- Casteo correcto de tipos
- Eliminación de duplicados
- Validación básica de rangos
- Enriquecimiento con columnas derivadas
- Escritura en Parquet con compresión Snappy
- Persistencia en bucket Silver de MinIO
- Particionado por `raidid` y `event_date`
- Testing básico del flujo Bronze → Silver

No incluye todavía:

- Modelo dimensional Gold
- Agregaciones analíticas finales
- Procesamiento distribuido con Spark
- Table formats ACID
- Serving o visualización

## Qué resuelve esta fase

La Fase 2 dejó datos válidos pero crudos en Bronze. La Fase 3 añade la primera capa de refinamiento del pipeline:

- limpia ruido estructural
- normaliza tipos
- elimina duplicados
- filtra valores fuera de rango
- enriquece eventos con métricas útiles
- reduce tamaño y mejora la lectura analítica con Parquet

## Transformaciones principales

El pipeline Silver aplica, como mínimo, estas operaciones:

- Parseo de timestamps a tipo fecha/hora real
- Conversión de columnas numéricas a tipos adecuados
- Normalización de cadenas
- Deduplicación por `eventid`
- Validación de porcentajes y magnitudes
- Cálculo de latencia de ingesta
- Derivación de `event_date`
- Detección de eventos destacados como `is_massive_hit`

## Salida de la fase

La salida se guarda en MinIO Silver como archivos Parquet comprimidos, listos para etapas posteriores de análisis y agregación.

Ejemplo conceptual de ruta:

`wowraidevents/v1/raidid=raid001/event_date=2026-01-21/part-<batchid>.parquet`

## Stack

- Python
- Pandas
- PyArrow
- MinIO
- Parquet
- Snappy
