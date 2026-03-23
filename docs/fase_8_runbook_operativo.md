# Fase 8 - Runbook Operativo (REST Catalog + API)

## Objetivo

Documentar operacion, recuperacion del catalogo Iceberg REST y validaciones de salud para la Fase 8.

## Precondiciones

- Docker Engine disponible.
- Variables S3 en `.env` coherentes con MinIO local (`minio` / `minio123`).
- Warehouse con tablas Iceberg creadas por Fase 7 (`s3://warehouse/gold/*`, `s3://warehouse/silver/raid_events`).

## Arranque base

```bash
docker compose -f infra/minio/docker-compose.yml up -d
```

Validacion rapida:

```bash
curl -sf http://localhost:8181/v1/config
curl -sf http://localhost:9000/minio/health/live
```

## Recuperacion de catalogo (perdida de volumen iceberg_catalog)

Sintoma tipico: `/health/readiness` devuelve `503` con `missing_tables` no vacio.

1) Verificar metadatos existentes en MinIO:

```bash
python -m scripts.analytics.validate_gold_layer
```

2) Re-registrar tablas en REST Catalog (idempotente):

```bash
python scripts/bootstrap/register_tables_rest_catalog.py
```

3) Revalidar readiness de API:

```bash
curl -s http://localhost:8000/health/readiness
```

Se espera `status=ready`, `missing_tables=[]` y lista completa de tablas esperadas.

## Tabla esperada para readiness

- `gold.dim_player`
- `gold.dim_raid`
- `gold.fact_player_raid_stats`
- `gold.fact_raid_summary`
- `silver.raid_events`

## Troubleshooting rapido

- `REST catalog no accesible`: validar contenedor `wow-iceberg-rest` y puerto 8181.
- `No se pudieron listar tablas`: revisar credenciales S3 y conectividad a MinIO.
- `Tablas faltantes`: ejecutar bootstrap de registro y confirmar metadata en `s3://warehouse/<tabla>/metadata/`.

## Comandos de smoke/integration recomendados

```bash
mamba run -n wow-telemetry pytest -q tests/integration/test_iceberg_rest_catalog_smoke.py
mamba run -n wow-telemetry pytest -q tests/integration/test_api_business_endpoints_smoke.py
mamba run -n wow-telemetry pytest -q tests/api/test_health_readiness.py tests/api/test_catalog_routes.py tests/api/test_raids_routes.py tests/api/test_metrics_routes.py tests/api/test_iceberg_service.py
```
