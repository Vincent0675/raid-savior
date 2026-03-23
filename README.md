# WoW Raid Telemetry Pipeline

**Proyecto de Curso:** Big Data e Inteligencia Artificial  
**Autor:** Byron V. Blatch Rodriguez  
**Profesor:** Francisco Javier Ortega  
**Repositorio:** [github.com/Vincent0675/raid-savior](https://github.com/Vincent0675/raid-savior)  
**Estado:** Fase 8 operativa (serving API sobre Iceberg REST Catalog)  
**Ultima actualizacion:** 23 de marzo de 2026.

***

## 1. Vision general

Pipeline de telemetria **event-driven** que simula raids de World of Warcraft
sobre una arquitectura **Medallion** completa (Bronze -> Silver -> Gold), con
almacenamiento en MinIO (S3-compatible), validacion estricta con Pydantic v2,
procesamiento batch y una capa de consumo via API para servir datos Gold.

### Flujo integral del proyecto

1. **Ingesta:** eventos sinteticos se envian por HTTP al receptor Flask y se persisten en Bronze.
2. **Bronze -> Silver:** limpieza, tipado, deduplicacion y enriquecimiento en formato columnar.
3. **Silver -> Gold:** construccion de tablas de hechos y dimensiones para analitica/ML.
4. **Serving y visualizacion:** FastAPI consulta Iceberg REST Catalog y expone endpoints de negocio para consumo externo.

### Volumen de datos procesados (dataset de referencia)

| Capa | Bucket MinIO | Raids | Eventos | Formato |
|------|-------------|-------|---------|---------|
| Bronze | `bronze` | 12 | ~600.000 | JSON (Hive-style) |
| Silver | `silver` | 12 | ~600.000 | Iceberg + Parquet + Snappy |
| Gold | `gold` | 12 | - | Iceberg + Parquet + Snappy |

### Tablas Gold (modelo semidimensional)

| Tabla | Filas | Clave natural | Particion | Tipo | Descripcion |
|-------|-------|---------------|-----------|------|-------------|
| `fact_raid_summary` | 12 | `raid_id` | `event_date` | Hecho | KPIs macro por raid |
| `fact_player_raid_stats` | ~3.744 | `(player_id, raid_id)` | `event_date` | Hecho | KPIs por jugador/raid |
| `dim_player` | 312 | `player_id` | `player_class` | Dimension | Jugadores unicos, upsert ACID |
| `dim_raid` | 12 | `raid_id` | `event_date` | Dimension | Raids unicos, upsert ACID |

### Stack tecnologico por fase

| Fase | Tecnologia principal | Estado |
|------|----------------------|--------|
| 1 - Schema y generador | Pydantic v2, NumPy | ✅ Completada |
| 2 - Ingesta HTTP | Flask, MinIO, Docker | ✅ Completada |
| 3 - ETL Bronze->Silver | Pandas, PyArrow, Parquet | ✅ Completada |
| 4 - ETL Silver->Gold (Pandas) | DuckDB, Pydantic v2 | ✅ Completada |
| 5 - ETL Silver->Gold (Spark) | PySpark 3.5, S3A, MinIO | ✅ Completada |
| 6 - Orquestacion | Dagster | ✅ Completada |
| 7 - Migracion a ACID (Silver/Gold) | Apache Iceberg | ✅ Completada |
| 8 - Serving APIs y operacion catalogo | FastAPI, PyIceberg REST Catalog | ✅ Operativa |

***

## 2. Integracion academica por asignatura

El proyecto sirve como **nucleo comun** para las cuatro asignaturas de la especializacion, usando la arquitectura Medallion como hilo conductor.

### 2.1 Big Data Aplicado (BDA)

- Arquitectura Medallion completa sobre object storage (MinIO) con contratos de escritura por capa.
- Migracion de JSON a Parquet + Snappy para optimizar lectura analitica y costo.
- Modelado semidimensional de Gold con facts y dimensiones listo para BI/ML.

### 2.2 Sistemas de Big Data

- Ingesta event-driven y observabilidad con receptor HTTP, logs y scripts de validacion.
- Infraestructura local contenedorizada con Docker Compose.
- Pipeline preparado para ejecucion/re-ejecucion por fases y pruebas de humo.

### 2.3 Programacion de Inteligencia Artificial

- Capa de consumo via FastAPI para exponer metricas de Gold.
- Base de datos refinada para integracion posterior con dashboards.
- Contratos de salida tipados para desacoplar APIs de la ingesta cruda.

### 2.4 Modelos de Inteligencia Artificial

- Gold como fuente unica de verdad para clasificacion y clustering.
- Features agregadas por raid y por jugador listas para entrenamiento.
- Flujo preparado para evolucionar a tracking de experimentos y consumo GPU.

***

## 3. Arquitectura Medallion

```text
Generador sintetico
        |
        v
   [Bronze]  s3://bronze/wow_raid_events/v1/raid_id={id}/ingest_date={date}/batch_{n}.json
        |        JSON validado por Pydantic (schema-on-write)
        |
   scripts/etl/run_bronze_to_silver.py
        |
        v
   [Silver]  s3://silver/wow_raid_events/v1/raid_id={id}/event_date={date}/part-{n}.parquet
        |        Parquet + Snappy, limpio y tipado
        |
   scripts/etl/run_silver_to_gold.py --all
        |
        v
   [Gold]   s3://gold/
        |- dim_player/
        |- dim_raid/
        |- fact_raid_summary/
        '- fact_player_raid_stats/
        |
        v
   [Serving] FastAPI + PyIceberg (REST Catalog)
```

### 3.1 Capa Bronze - Raw / Ingesta

**Responsabilidad:** almacenar eventos crudos validados en el borde de entrada.

- Formato: JSON.
- Destino: bucket `bronze`.
- Contrato de escritura: `wow_raid_events/v1/raid_id={raid_id}/ingest_date={YYYY-MM-DD}/batch={uuid}.json`.
- Validacion: schema-on-write con Pydantic v2; payload invalido se rechaza con HTTP 400.

### 3.2 Capa Silver - Clean / Refinada

**Responsabilidad:** datos listos para analitica intermedia.

- Formato: Parquet + Snappy.
- Destino: bucket `silver`.
- Particionamiento logico: `raid_id` + `event_date`.
- Transformaciones clave: cast de tipos, deduplicacion por `event_id`, validacion de rangos y enriquecimiento (`ingest_latency_ms`, `event_date`, etc.).

### 3.3 Capa Gold - Modelo semidimensional

**Responsabilidad:** capa de negocio y consumo analitico.

- Dimensiones: `dim_player`, `dim_raid`.
- Hechos: `fact_raid_summary`, `fact_player_raid_stats`.
- Ventaja: lectura eficiente, joins baratos y preparacion directa para BI/ML.

### 3.4 Subfase 7.4 - Dimensiones Gold (Apache Iceberg + MERGE INTO)

Las dimensiones se migraron a tablas Iceberg con escritura ACID e idempotencia via `MERGE INTO`.

| Script | Tabla Iceberg | Grain | Columna inmutable | Particion |
|--------|---------------|-------|-------------------|-----------|
| `src/etl/gold_iceberg_dim_player.py` | `wow.gold.dim_player` | 1 fila / jugador | `first_seen_date` | `player_class` |
| `src/etl/gold_iceberg_dim_raid.py` | `wow.gold.dim_raid` | 1 fila / raid | `event_date` | `event_date` |

***

## 4. Importancia de la validacion en Bronze

La validacion estricta en Bronze es una decision de arquitectura central: evita propagar errores estructurales a Silver/Gold.

- Sin schema-on-write, la deuda de calidad explota aguas abajo.
- Con schema-on-write, solo entra data tipada y coherente.
- Resultado: menos friccion operacional y mejor base para analitica/ML.

***

## 5. Ejecutar el pipeline end-to-end (historico)

### 5.1 Preparacion de entorno

```bash
mamba env create -f environment.yml
mamba activate wow-telemetry
pip install -e .[dev]
```

Opcional con conda:

```bash
conda env create -f environment.yml
conda activate wow-telemetry
```

Descarga de jars Spark (una sola vez):

```bash
chmod +x scripts/download_spark_jars.sh
./scripts/download_spark_jars.sh
```

Levantar infraestructura base:

```bash
docker compose -f infra/minio/docker-compose.yml up -d
```

### 5.2 Ingesta principal (event-driven)

Terminal A:

```bash
python scripts/api/receiver.py
```

Ingesta masiva:

```bash
python scripts/generators/replay_raids.py \
  --num-raids 5 \
  --num-events-per-raid 50000 \
  --batch-size 500
```

### 5.3 ETL Bronze -> Silver

```bash
python scripts/etl/run_bronze_to_silver.py
```

### 5.4 ETL Silver -> Gold

```bash
python scripts/etl/run_silver_to_gold.py --all
python scripts/etl/run_silver_to_gold.py --raid-id raid001 --event-date 2026-02-25
```

### 5.5 Validacion de Gold

```bash
python scripts/analytics/validate_gold_layer.py --all
python scripts/analytics/validate_gold_layer.py --raid-id raid001 --event-date 2026-02-25
```

### 5.6 Tests

```bash
python -m pytest
```

***

## 6. Fase 8 operativa (serving + runbook)

Esta seccion conserva las mejoras operativas actuales sin reemplazar la documentacion historica del pipeline.

### 6.1 Alcance implementado

- API principal: `src/api/main.py` (FastAPI + readiness real en startup).
- Servicio de datos: `src/api/services/iceberg_service.py` (PyIceberg `type=rest`).
- Bootstrap idempotente del catalogo: `scripts/bootstrap/register_tables_rest_catalog.py`.
- Endpoints:
  - `GET /health`
  - `GET /health/readiness`
  - `GET /api/v1/catalog/namespaces`
  - `GET /api/v1/catalog/tables`
  - `GET /raids`
  - `GET /raids/{raid_id}`
  - `GET /raids/{raid_id}/players`
  - `GET /metrics/global`

### 6.2 Pasos manuales verificables (quick-check)

1) Levantar infra:

```bash
docker compose -f infra/minio/docker-compose.yml up -d
curl -sS http://localhost:8181/v1/config
curl -sS http://localhost:9000/minio/health/live
```

2) Registrar tablas Iceberg en REST Catalog:

```bash
mamba run -n wow-telemetry python scripts/bootstrap/register_tables_rest_catalog.py
```

3) Arrancar receptor Flask (precondicion para ciertos smoke tests):

```bash
mamba run -n wow-telemetry python scripts/api/receiver.py
curl -sS http://localhost:5000/health
```

4) Ejecutar pruebas minimas:

```bash
S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry pytest tests/api -q
```

```bash
S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry pytest tests/integration/test_iceberg_rest_catalog_smoke.py -q
```

```bash
S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry pytest tests/integration/test_api_business_endpoints_smoke.py -q
```

5) Arrancar API y validar endpoints:

```bash
S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

En otra terminal:

```bash
curl -sS http://localhost:8000/health
curl -sS http://localhost:8000/health/readiness
curl -sS http://localhost:8000/api/v1/catalog/namespaces
curl -sS http://localhost:8000/api/v1/catalog/tables
curl -sS "http://localhost:8000/raids?limit=2&offset=0"
curl -sS http://localhost:8000/metrics/global
curl -sS http://localhost:8000/raids/raid001
curl -sS http://localhost:8000/raids/raid001/players
curl -sS -i http://localhost:8000/raids/raid404
```

### 6.3 Referencias Fase 8

- Vision y alcance: `docs/fase_8.md`
- Estado operativo resumido: `docs/fase_8_estado_final.md`
- Runbook operativo: `docs/fase_8_runbook_operativo.md`
- Evidencias reproducibles: `docs/evidence/fase_8_evidencias.md`

***

## 7. Estado del proyecto

### Fases completadas

| Fase | Descripcion | Estado |
|------|-------------|--------|
| 1 | Schema Pydantic v2 + generador sintetico | ✅ Completa |
| 2 | Receptor HTTP Flask + ingesta Bronze | ✅ Completa |
| 3 | ETL Bronze -> Silver (Parquet, Snappy, Hive-style) | ✅ Completa |
| 4 | Gold Medallion (dimensiones + facts + validaciones) | ✅ Completa |
| 5 | ETL Silver->Gold con Spark | ✅ Completa |
| 6 | Orquestacion con Dagster | ✅ Completa |
| 7 | Migracion a Apache Iceberg + Time Travel | ✅ Completa |
| 8 | Serving API + REST Catalog + validacion operativa | ✅ Completa |

### Roadmap

| Fase | Descripcion | Tecnologia prevista |
|------|-------------|---------------------|
| 9 | Modelado IA | MLflow + PyCaret + CuDF (RTX 3050) |
| 10 | Datos reales | Warcraft Logs API |

***

## 8. Deuda tecnica documentada

| ID | Descripcion | Severidad | Afecta |
|----|-------------|-----------|--------|
| DT-01 | `MinIOStorageClient.list_objects()` no pagina (limite 1000 objetos) | Media | Silver con >1000 parquets por particion |
| DT-02 | Generador no produce `player_death` en config actual | Baja | `total_deaths` puede quedar en 0 |
| DT-03 | Algunos campos legacy de endpoints (`event_date`, `raid_outcome`, `raid_dps`) pueden venir `null` segun snapshots | Baja | Consumo API en clientes estrictos |

***

## 9. Stack tecnologico

### Implementado

| Capa | Tecnologia |
|------|------------|
| Validacion y schema | Pydantic v2, JSON Schema draft-07 |
| Generacion sintetica | NumPy, UUID v4 |
| API de ingesta | Flask 3.x |
| Serving API | FastAPI, Uvicorn |
| Catalogo tablas | Apache Iceberg REST Catalog + PyIceberg |
| Object storage | MinIO (S3-compatible), boto3 |
| Procesamiento ETL | Pandas, PyArrow, PySpark, DuckDB |
| Formato de almacenamiento | Apache Parquet + Snappy |
| Contenedores | Docker, Docker Compose |
| Testing | pytest |

### Planificado

| Capa | Tecnologia |
|------|------------|
| Visualizacion | Grafana, Apache Superset |
| ML tracking | MLflow, PyCaret |
| GPU computing | RAPIDS/CuDF, CUDA 12.x |

***

**Hardware de desarrollo:**  
ASUS TUF Gaming A15, RTX 3050 4GB (CUDA), Pop!_OS 22.04, 16 GB RAM
