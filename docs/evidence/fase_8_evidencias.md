# Evidencias Fase 8 (ejecucion manual)

Fecha de ejecucion: 2026-03-23  
Entorno: `mamba run -n wow-telemetry` + Docker local

## 1) Infra levantada

Comando:

```bash
docker compose -f infra/minio/docker-compose.yml up -d
```

Salida clave:

```text
Container wow-minio Running
Container wow-iceberg-rest Running
Container wow-minio Healthy
Container wow-minio-init Started
```

Verificacion de REST Catalog:

```bash
curl -sS http://localhost:8181/v1/config
```

Respuesta:

```json
{"defaults":{},"overrides":{}}
```

## 2) Bootstrap de tablas Iceberg en catalogo REST

Comando:

```bash
mamba run -n wow-telemetry python scripts/bootstrap/register_tables_rest_catalog.py
```

Salida clave:

```text
[SKIP] wow.gold.dim_player ya existe en REST Catalog
[SKIP] wow.gold.dim_raid ya existe en REST Catalog
[SKIP] wow.gold.fact_player_raid_stats ya existe en REST Catalog
[SKIP] wow.gold.fact_raid_summary ya existe en REST Catalog
[SKIP] wow.silver.raid_events ya existe en REST Catalog

[INFO] Verificacion final:
+---------+----------------------+-----------+
|namespace|tableName             |isTemporary|
+---------+----------------------+-----------+
|gold     |dim_player            |false      |
|gold     |dim_raid              |false      |
|gold     |fact_player_raid_stats|false      |
|gold     |fact_raid_summary     |false      |
+---------+----------------------+-----------+

+---------+-----------+-----------+
|namespace|tableName  |isTemporary|
+---------+-----------+-----------+
|silver   |raid_events|false      |
+---------+-----------+-----------+
```

## 3) Suite API (obligatoria)

Comando:

```bash
S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry pytest tests/api -q
```

Salida clave:

```text
........................                                                 [100%]
24 passed in 2.15s
```

## 4) Smoke catalogo REST (integracion)

Precondicion detectada: `tests/integration/conftest.py` exige receptor Flask en `localhost:5000`.

Arranque receptor (terminal separada):

```bash
mamba run -n wow-telemetry python scripts/api/receiver.py
```

Health receptor:

```bash
curl -sS http://localhost:5000/health
```

Respuesta:

```json
{
  "service": "wow-telemetry-receiver",
  "status": "healthy",
  "timestamp": "2026-03-23T18:18:42.791497+00:00"
}
```

Comando smoke:

```bash
S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry pytest tests/integration/test_iceberg_rest_catalog_smoke.py -q
```

Salida clave:

```text
..                                                                       [100%]
2 passed in 0.72s
```

## 5) Smoke API negocio con catalogo real

Comando:

```bash
S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry pytest tests/integration/test_api_business_endpoints_smoke.py -q
```

Salida clave:

```text
.                                                                        [100%]
1 passed in 1.92s
```

## 6) Verificacion manual por curl (health/readiness/endpoints de negocio)

Arranque FastAPI:

```bash
S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

Comandos ejecutados:

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

Respuestas clave (extracto):

```json
{"status":"ok"}
```

```json
{"status":"ready","ready":true,"catalog_uri":"http://localhost:8181","expected_tables":["gold.dim_player","gold.dim_raid","gold.fact_player_raid_stats","gold.fact_raid_summary","silver.raid_events"],"catalog_tables":["gold.dim_player","gold.dim_raid","gold.fact_player_raid_stats","gold.fact_raid_summary","silver.raid_events"],"missing_tables":[],"errors":[]}
```

```json
{"namespaces":["gold","silver"]}
```

```json
{"tables":["gold.dim_player","gold.dim_raid","gold.fact_player_raid_stats","gold.fact_raid_summary","silver.raid_events"]}
```

```json
{"total_raids":10,"success_raids":0,"wipe_raids":0,"wipe_rate_pct":0.0,"avg_raid_dps":0.0}
```

404 esperado (`/raids/raid404`):

```json
{"detail":"Raid 'raid404' not found"}
```

Log de FastAPI durante la prueba visual:

```text
API startup completo; readiness inicial OK
GET /health HTTP/1.1" 200 OK
GET /health/readiness HTTP/1.1" 200 OK
GET /api/v1/catalog/namespaces HTTP/1.1" 200 OK
GET /api/v1/catalog/tables HTTP/1.1" 200 OK
GET /raids?limit=2&offset=0 HTTP/1.1" 200 OK
GET /metrics/global HTTP/1.1" 200 OK
GET /raids/raid001 HTTP/1.1" 200 OK
GET /raids/raid001/players HTTP/1.1" 200 OK
GET /raids/raid404 HTTP/1.1" 404 Not Found
```

## 7) Estado reproducible observado

- Readiness API: `ready=true`.
- Catalogo REST: visible y con 5 tablas esperadas.
- Contratos negocio: `200` para casos validos, `404` para raid inexistente.
- Tests obligatorios: todos en verde (`24 passed`, `2 passed`, `1 passed`).
