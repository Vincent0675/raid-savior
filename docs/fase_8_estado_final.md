# Fase 8 - Estado final operativo

Referencia rapida para operar y validar manualmente la Fase 8 (serving APIs sobre Iceberg REST Catalog).
Documento de uso diario: checklist de cierre, comandos copy/paste y criterio de decision Go/No-Go.
Baseado en estado implementado y evidencias actuales; lo planificado se marca explicitamente.

## Stack implementado y alcance actual

**Implementado (real):** FastAPI + Uvicorn, PyIceberg (`type=rest`), Iceberg REST Catalog (`tabulario/iceberg-rest:0.11.0`), MinIO, pytest, entorno `mamba run -n wow-telemetry`.

**Alcance actual (real):**
- Endpoints de salud: `GET /health`, `GET /health/readiness`.
- Endpoints de catalogo: `GET /api/v1/catalog/namespaces`, `GET /api/v1/catalog/tables`.
- Endpoints de negocio: `GET /raids`, `GET /raids/{raid_id}`, `GET /raids/{raid_id}/players`, `GET /metrics/global`.
- Bootstrap idempotente de tablas: `scripts/bootstrap/register_tables_rest_catalog.py`.

**Planificado (no bloqueante para Fase 8):** dashboards Grafana/Infinity o Superset.

## Checklist cierre Fase 8

### P0 (bloqueante)

- [ ] Infra arriba: MinIO + Iceberg REST Catalog disponibles.
- [ ] Bootstrap catalogo ejecutado sin errores.
- [ ] `GET /health/readiness` devuelve `ready=true` y `missing_tables=[]`.
- [ ] Suite `tests/api` en verde.

### P1 (recomendado para GO robusto)

- [ ] Smoke REST Catalog (`tests/integration/test_iceberg_rest_catalog_smoke.py`) en verde.
- [ ] Smoke endpoints negocio con catalogo real (`tests/integration/test_api_business_endpoints_smoke.py`) en verde.
- [ ] Endpoints de negocio validados por curl (`200` validos, `404` inexistente).

### P2 (operacion/documentacion)

- [ ] Evidencias actualizadas en `docs/evidence/fase_8_evidencias.md`.
- [ ] Runbook consistente con ejecucion real (`docs/fase_8_runbook_operativo.md`).

## Comandos clave (copy/paste)

> Ejecutar comandos Python/tests siempre con `mamba run -n wow-telemetry`.

### 1) Levantar infra

```bash
docker compose -f infra/minio/docker-compose.yml up -d
curl -sf http://localhost:8181/v1/config
curl -sf http://localhost:9000/minio/health/live
```

### 2) Bootstrap de tablas

```bash
mamba run -n wow-telemetry python scripts/bootstrap/register_tables_rest_catalog.py
```

### 3) Arrancar API

```bash
S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

### 4) Health/readiness

```bash
curl -sS http://localhost:8000/health
curl -sS http://localhost:8000/health/readiness
```

### 5) Endpoints negocio

```bash
curl -sS "http://localhost:8000/raids?limit=2&offset=0"
curl -sS http://localhost:8000/raids/raid001
curl -sS http://localhost:8000/raids/raid001/players
curl -sS http://localhost:8000/metrics/global
curl -sS -i http://localhost:8000/raids/raid404
```

### 6) Tests API

```bash
S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry pytest tests/api -q
```

### 7) Smoke integracion

```bash
# Precondicion: receptor Flask activo en otra terminal
mamba run -n wow-telemetry python scripts/api/receiver.py

S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry pytest tests/integration/test_iceberg_rest_catalog_smoke.py -q

S3_ENDPOINT_URL=http://localhost:9000 \
S3_ACCESS_KEY=minio \
S3_SECRET_KEY=minio123 \
WAREHOUSE_BUCKET=warehouse \
ICEBERG_REST_URI=http://localhost:8181 \
mamba run -n wow-telemetry pytest tests/integration/test_api_business_endpoints_smoke.py -q
```

## Si falla (5 casos tipicos + accion rapida)

1. **`/health/readiness` devuelve `503` con `missing_tables`:** re-ejecutar bootstrap de tablas y revalidar readiness.
2. **`curl http://localhost:8181/v1/config` falla:** levantar/verificar contenedor `wow-iceberg-rest` y puerto `8181`.
3. **Tests integration en `skip` o error de fixture:** arrancar receptor Flask en `localhost:5000` antes del smoke.
4. **Endpoints negocio responden `503`:** revisar `ICEBERG_REST_URI`, credenciales S3 y que MinIO/Catalog esten vivos.
5. **API no arranca en startup:** confirmar variables de entorno, catalogo accesible y tablas esperadas registradas.

## Decision (Go/No-Go)

**GO** si se cumple todo P0 y, idealmente, P1 completo.

**NO-GO** si ocurre cualquiera de estos:
- Readiness en `503` o `missing_tables` no vacio.
- `tests/api` falla.
- Smokes criticos no ejecutan o fallan por servicios base no disponibles.
- Endpoints de negocio devuelven `503` en escenario nominal.

En caso de duda, priorizar estado de `readiness` + `tests/api` como criterio minimo de liberacion operativa.
