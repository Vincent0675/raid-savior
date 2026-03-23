# Fase 8: Capa de Consumo y Visualizacion

**Estado:** GO condicional (iteracion 2026-03-23)  
**Inicio:** 2026-03-18  
**Referencia ADR:** `docs/fase_8_adr_decisiones-v3.md`

Estado final operativo (1 pagina): `docs/fase_8_estado_final.md`.

---

## Estado final conseguido (segun codigo actual)

Fase 8 queda operativa para serving de datos Gold via FastAPI + Iceberg REST Catalog,
con readiness real en arranque y contratos de respuesta tipados en OpenAPI.

- API principal en `src/api/main.py` con `lifespan` que falla en startup si readiness no pasa.
- Servicio de datos en `src/api/services/iceberg_service.py` usando PyIceberg `type=rest`.
- Registro idempotente de tablas con `scripts/bootstrap/register_tables_rest_catalog.py`.
- Runbook operativo de recuperacion/operacion en `docs/fase_8_runbook_operativo.md`.
- Evidencia reproducible en `docs/evidence/fase_8_evidencias.md`.

## Stack tecnologico de Fase 8 (estado real)

### Implementado

- Serving API: FastAPI + Uvicorn.
- Configuracion: Pydantic Settings (`src/api/settings.py`).
- Datos: PyIceberg (`type=rest`) + Iceberg REST Catalog (`tabulario/iceberg-rest:0.11.0`).
- Storage/infra local: MinIO + Docker Compose.
- Validacion: pytest (`tests/api` e integracion REST Catalog).
- Entorno reproducible: `mamba run -n wow-telemetry`.

### Planificado

- Visualizacion operativa en Grafana (Infinity plugin) o Apache Superset.
- Endurecimiento de despliegue productivo (Gunicorn/workers) como perfil de despliegue, no requisito del flujo manual validado.

---

## Endpoints implementados y readiness real

### Health

- `GET /health` -> `200` con `{"status":"ok"}`.
- `GET /health/readiness`:
  - `200` cuando REST Catalog esta disponible y no hay tablas faltantes.
  - `503` con detalle estructurado cuando falla catalogo o faltan tablas.

Respuesta de readiness (shape real):

```json
{
  "status": "ready",
  "ready": true,
  "catalog_uri": "http://localhost:8181",
  "expected_tables": ["gold.dim_player", "gold.dim_raid", "gold.fact_player_raid_stats", "gold.fact_raid_summary", "silver.raid_events"],
  "catalog_tables": ["gold.dim_player", "gold.dim_raid", "gold.fact_player_raid_stats", "gold.fact_raid_summary", "silver.raid_events"],
  "missing_tables": [],
  "errors": []
}
```

### Catalogo REST

- `GET /api/v1/catalog/namespaces` -> namespaces detectados en catalogo.
- `GET /api/v1/catalog/tables` -> listado completo de tablas visibles.
- Semantica de error: `503` si el catalogo no esta accesible.

### Endpoints de negocio

- `GET /raids?limit=&offset=`
- `GET /raids/{raid_id}`
- `GET /raids/{raid_id}/players`
- `GET /metrics/global`

Comportamiento confirmado:

- `200` en consultas validas.
- `404` para raid inexistente en `/{raid_id}` y `/{raid_id}/players`.
- `503` cuando backend Iceberg/REST Catalog no esta disponible.

---

## Contratos de respuesta (API v1)

Modelos en `src/api/schemas/responses.py`:

- `RaidSummaryResponse`
- `RaidListResponse`
- `RaidPlayerResponse`
- `RaidPlayersResponse`
- `GlobalMetricsResponse`
- `ErrorResponse`

Aplicados como `response_model`/`responses` en `src/api/routes/raids.py` y
`src/api/routes/metrics.py`, desacoplando contrato de salida respecto al schema
de ingesta (alineado con ADR-8-05).

---

## Runbook operativo resumido

1. Levantar infra base:

```bash
docker compose -f infra/minio/docker-compose.yml up -d
```

2. Re-registrar tablas si hay perdida de volumen `iceberg_catalog`:

```bash
mamba run -n wow-telemetry python scripts/bootstrap/register_tables_rest_catalog.py
```

3. Validar disponibilidad:

```bash
curl -sf http://localhost:8181/v1/config
curl -s http://localhost:8000/health/readiness
```

4. Referencia operativa extendida: `docs/fase_8_runbook_operativo.md`.

---

## Limites pendientes / deuda abierta

- Si el volumen de SQLite del REST Catalog se pierde, se requiere bootstrap manual
  (mitigado por script idempotente).
- El smoke de integracion `tests/integration/test_iceberg_rest_catalog_smoke.py`
  depende de que el receptor Flask (`localhost:5000`) este activo por fixture global.
- Campos legacy de dataset en endpoints (`event_date`, `raid_outcome`, `raid_dps`) pueden
  llegar en `null` segun snapshots Gold actuales; no rompe contrato porque son opcionales.

---

## Criterios Go/No-Go (resumen)

**GO** cuando:

- `tests/api` pasa completo.
- smoke de catalogo (`tests/integration/test_iceberg_rest_catalog_smoke.py`) pasa.
- smoke de negocio con catalogo real pasa.
- `/health/readiness` devuelve `ready=true` y `missing_tables=[]`.

**NO-GO** cuando:

- readiness devuelve `503` o faltan tablas en catalogo.
- endpoints de negocio responden `503` por falla de backend.
- smoke tests quedan en `skip` por falta de servicios externos levantados.

Resultados reales de validacion manual: `docs/evidence/fase_8_evidencias.md`.
