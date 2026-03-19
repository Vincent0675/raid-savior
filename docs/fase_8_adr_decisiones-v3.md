# Fase 8: Architecture Decision Records (ADR)
**Documento Justificación de Decisiones de Diseño para Producción Sostenible**

**Fecha**: 2026-03-19  
**Versión**: 3.0 (incorpora ADR-8-01 REV.2)  
**Estado**: APROBADO

---

## Resumen Ejecutivo

Fase 8 implementa la capa de consumo del pipeline Medallion con **5 decisiones críticas de arquitectura** que definen **sostenibilidad operativa**, **escalabilidad horizontal** y **cloud-readiness** (portabilidad a AWS S3 real en Fase 9).

**Decisiones documentadas:**
1. **ADR-8-01 REV.2**: REST Catalog + estrategia de MinIO → S3 cloud
2. **ADR-8-02**: BaseSettings propio para FastAPI
3. **ADR-8-03**: Gunicorn + Uvicorn workers
4. **ADR-8-04**: Grafana Infinity Plugin datasource
5. **ADR-8-05**: Modelos de respuesta independientes

**Presupuesto estimado en producción**: €40–50/mes en cloud (AWS t3.medium), **0 servicios adicionales**.

---

## ADR-8-01 REV.2: REST Catalog en lugar de HadoopCatalog directo + Cloud-Readiness Strategy

### Status
**ACEPTADA**  
Sustituye ADR-8-01 v1 (PyIceberg + HadoopCatalog)

### Contexto

#### Problema inicial (Fase 7)
Spark Fase 7 usaba `HadoopCatalog` directamente con `hadoop-aws` (AWS SDK v1) para acceder a MinIO:
```python
# Spark Fase 7 — AWS SDK v1
spark.conf.set("spark.sql.catalog.wow.type", "hadoop")
spark.conf.set("spark.sql.catalog.wow.warehouse", "s3a://warehouse")
```

En Fase 8, **PyIceberg 0.11** no implementa `HadoopCatalog` como `CatalogType` válido: el tipo `hadoop` existe **exclusivamente en la JVM de Spark**. `CatalogType` en Python soporta únicamente: `rest`, `sql`, `hive`, `glue`, `dynamodb`.

**Descubrimiento empírico**: Lo constatamos durante Subfase 8.2 al intentar registrar tablas Gold existentes.

#### Solución adoptada (Fase 8)
Introducir un servidor **REST Catalog** como intermediario único:
- **Spark** habla con REST Catalog (propiedades Iceberg v2)
- **PyIceberg** habla con REST Catalog (Python)
- **REST Catalog** habla con MinIO (AWS SDK v2 internamente)

Beneficio: **un único punto de verdad para metadatos** independiente del cliente (JVM vs Python).

### Decisión

Usar **`tabulario/iceberg-rest:0.11.0`** como servidor REST Catalog central con configuración S3 path-style para MinIO.

**Arquitectura resultante:**
```
┌─────────────────────────────────────────┐
│        Aplicaciones / Jupyter            │
│   (Spark, FastAPI, PyIceberg)           │
└──────────────────┬──────────────────────┘
                   │ HTTP REST (puerto 8181)
         ┌─────────▼──────────┐
         │   Iceberg REST     │
         │  (tabulario 0.11)  │
         │  SQLite → Metadatos│
         └─────────┬──────────┘
                   │ S3 SDK v2
         ┌─────────▼──────────┐
         │      MinIO         │
         │  (warehouse bucket)│
         └────────────────────┘
```

**Cambios sobre componentes:**

| Componente | Antes | Después |
|---|---|---|
| **spark_session.py** | `.config("spark.sql.catalog.wow.type", "hadoop")` `.config("spark.sql.catalog.wow.warehouse", "s3a://warehouse")` | `.config("spark.sql.catalog.wow.type", "rest")` `.config("spark.sql.catalog.wow.uri", "http://localhost:8181")` |
| **docker-compose.yml** | Sin servicio REST | Nuevo servicio `iceberg-rest` (200 MB, puerto 8181) |
| **pyiceberg_service.py** | No implementado | Nuevo: `from pyiceberg.catalog import load_catalog(...)` con `type=rest` |

#### Configuración del servicio iceberg-rest

```yaml
iceberg-rest:
  image: tabulario/iceberg-rest:0.11.0
  container_name: raid-savior-iceberg-rest
  ports:
    - "8181:8181"
  environment:
    - CATALOG_WAREHOUSE=s3://warehouse/
    - CATALOG_IO_IMPL=org.apache.iceberg.aws.s3.S3FileIO
    - CATALOG_S3_ENDPOINT=http://minio:9000
    - CATALOG_S3_PATH_STYLE_ACCESS=true
    - AWS_ACCESS_KEY_ID=minioadmin
    - AWS_SECRET_ACCESS_KEY=minioadmin
    - AWS_REGION=us-east-1
    - JAVA_TOOL_OPTIONS=-Daws.s3.forcePathStyle=true
  networks:
    - raid-network
  depends_on:
    minio:
      condition: service_healthy
  restart: unless-stopped
```

**Nota técnica sobre `-Daws.s3.forcePathStyle=true`**: 
- Resuelve incompatibilidad de AWS SDK v2 interno con MinIO
- Virtual-hosted style (`{bucket}.{host}`) causa `UnknownHostException` en MinIO
- Fuerza path style (`{host}/{bucket}`) que MinIO soporta correctamente
- **Deuda técnica transitoria**: en producción cloud (AWS S3) esta propiedad es innecesaria y debe eliminarse (Fase 9)

---

### Impacto en tablas Gold existentes

**Problema**: Tablas creadas por Spark Fase 7 con `HadoopCatalog` en `s3://warehouse/gold/` **no son visibles automáticamente** para el REST Catalog (su SQLite embebido está vacío).

**Solución**: Registrarlas explícitamente via Spark antes de usar PyIceberg:

```python
# scripts/debug/register_gold_tables.py
spark.sql("""
CREATE TABLE IF NOT EXISTS wow.gold.dim_player
USING ICEBERG
LOCATION 's3a://warehouse/gold/dim_player'
""")
```

Este paso ejecuta **una única vez** al arrancar Subfase 8.2. Después, todas las operaciones (Spark, PyIceberg, FastAPI) ven las tablas desde REST Catalog.

### Matriz de Impacto

| Componente | Antes | Después | Crítico |
|---|---|---|---|
| **spark_session.py** | Tipo hadoop | Tipo rest | No (1 línea) |
| **docker-compose.yml** | Sin REST | +iceberg-rest | Sí (nuevo servicio) |
| **pyiceberg_service.py** | N/A | Nuevo | Sí (nueva funcionalidad) |
| **Tablas Gold existentes** | Visibles en Hadoop | Requieren registro en REST | Sí (script de migración) |
| **Coste operativo** | Ninguno | 200 MB imagen, 1 contenedor | No (0€ adicional) |

### Justificación: ¿Por qué REST Catalog y no alternativas?

#### Alternativa 1: HadoopCatalog direct (rechazada)
- ❌ PyIceberg 0.11 no lo soporta
- ❌ Requeriría esperar PyIceberg v0.13+ para Hadoop Java interop
- ❌ Ralentiza pipeline

#### Alternativa 2: DuckDB Iceberg (rechazada)
- ❌ MinIO no es JDBC-compatible
- ❌ No es el estándar de la industria para data lakes

#### Alternativa 3: REST Catalog (ACEPTADA)
- ✅ PyIceberg 0.11 lo soporta explícitamente
- ✅ Spark lo soporta nativamente
- ✅ Estándar abierto (Apache Iceberg REST API)
- ✅ Un único punto de verdad para metadatos
- ✅ Desacopla cliente de almacenamiento
- ✅ **Cloud-ready**: traslatible a AWS Glue Data Catalog (Fase 9) sin cambios lógicos

### Estrategia de Cloud-Readiness: MinIO → AWS S3 (Fase 9)

Esta arquitectura REST Catalog es **portable a cloud** sin reescribir lógica:

| Componente | Desarrollo (MinIO) | Producción (AWS) |
|---|---|---|
| **REST Catalog** | `tabulario/iceberg-rest` en contenedor | AWS Glue Data Catalog (managed) |
| **Warehouse bucket** | `s3://warehouse/` en MinIO | `s3://raid-savior-data/warehouse/` en AWS S3 |
| **SDK S3** | AWS SDK v2 con path style | AWS SDK v2 estándar (virtual-hosted) |
| **Configuración** | Variables ENV del contenedor | AWS IAM + Secrets Manager |
| **Código Spark/PyIceberg** | `uri=http://localhost:8181` | `warehouse=s3://raid-savior-data/warehouse/` |

**Punto crítico**: En Fase 9, cuando migres a S3 real:
1. Eliminar `-Daws.s3.forcePathStyle=true` (S3 usa virtual-hosted)
2. Cambiar estructura de bucket a `s3://raid-savior-data/` (un único bucket de proyecto)
3. Reemplazar REST Catalog local con Glue Data Catalog
4. Cambiar configuración Spark de `type=rest, uri=http://localhost:8181` a `type=glue, warehouse=s3://...`

**Código no cambia**. Solo configuración.

### Consecuencias

- **Positivas**:
  - Spark + PyIceberg hablan el mismo lenguaje (REST Iceberg API)
  - Metadatos transaccionales para ambos clientes
  - Escalable: soporta múltiples aplicaciones (FastAPI, Grafana, análisis ad-hoc)
  - Cloud-ready para Fase 9
  
- **Negativas**:
  - Servidor REST adicional (pero coste nulo, mismo presupuesto)
  - Complejidad operativa incrementada (nuevo punto de fallo potencial)
  - Deuda técnica con `JAVA_TOOL_OPTIONS` hasta Fase 9

---

## ADR-8-02: Gestión de Configuración — BaseSettings Propio

### Status
**ACEPTADA**

### Contexto

FastAPI necesita configuración **independiente de Flask**:
- Credenciales MinIO
- Puerto API
- Variables de entorno por deployment (dev/staging/prod)

### Decisión

Crear `src/api/settings.py` con `pydantic.settings.BaseSettings` completamente independiente de `src/config.py` (Flask).

```python
# src/api/settings.py
from pydantic.settings import BaseSettings

class Settings(BaseSettings):
    s3_access_key: str  # Requerido, falla si no está
    s3_secret_key: str
    s3_endpoint: str = "http://minio:9000"
    api_port: int = 8000
    workers: int = 2
    
    class Config:
        env_file = ".env"
        case_sensitive = False

settings = Settings()
```

**Razones operativas:**
- **Fail-fast**: Si `S3_ACCESS_KEY` no está definida en producción, el contenedor falla al arrancar (no silenciosamente)
- **Multi-entorno sin rebuilding**: `.env.dev`, `.env.staging`, `.env.prod` cargan variables diferentes sin `docker build`
- **Type-safety**: Pydantic v2 valida tipos automáticamente

### Matriz de decisión

| Aspecto | Extender src/config.py | BaseSettings propio |
|---|---|---|
| **Fail-fast** | Valores por defecto silenciosos ❌ | ValidationError al arrancar ✅ |
| **Multi-entorno** | Sin soporte nativo ❌ | .env por entorno ✅ |
| **Type-safety** | Clase plana, sin validación ❌ | Pydantic v2 con type hints ✅ |
| **Acoplamiento** | Alto (Flask-API) ❌ | Bajo, independiente ✅ |

### Consecuencias
**ACEPTADA** — bajo overhead, alta seguridad, escalabilidad.

---

## ADR-8-03: Modelo de Despliegue — Gunicorn + Uvicorn Workers

### Status
**ACEPTADA**

### Contexto

FastAPI corre sobre Uvicorn (ASGI server). Sin workers, está limitado a **1 core**, lo que causa timeouts cuando Grafana hace polling concurrente (480 requests/hora a 4 paneles).

### Decisión

Usar **Gunicorn como process manager** con `uvicorn.workers.UvicornWorker` workers. Variable `WEBCONCURRENCY` controla workers sin reconstruir imagen.

```bash
gunicorn src.api.main:app \
  --workers 2 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --timeout 120
```

### Métrica de impacto

| Métrica | Uvicorn single | Gunicorn 2w | Gunicorn 4w |
|---|---|---|---|
| **vCPU requerido** | 1 | 2 | 4 |
| **Latencia p99 (Grafana)** | 200–500 ms | 50–100 ms | 10–50 ms |
| **Coste AWS t3.x** | t3.micro (€10) | t3.small (€25) | t3.medium (€40) |

**Para Fase 8**: 2 workers en t3.small (€25/mes). **Para producción**: 4 workers en t3.medium (€40/mes).

### Consecuencias
**ACEPTADA** — permite escalabilidad horizontal sin reescritura.

---

## ADR-8-04: Datasource de Grafana — Infinity Plugin

### Status
**ACEPTADA**

### Contexto

Grafana necesita consumir KPIs desde FastAPI. Alternativas:

1. **Infinity Plugin**: HTTP JSON directamente desde FastAPI (coste: €0)
2. **PostgreSQL sync**: Materializacion en RDS (coste: €50–150/mes)
3. **Athena/BigQuery**: Query directa a Iceberg (coste: €30–100/mes, latencia 5–10s)

### Decisión

**Infinity Plugin** de Grafana (open-source). FastAPI expone endpoints JSON que Infinity consume via HTTP.

```json
{
  "url": "http://fastapi:8000/api/v1/raid-summary",
  "method": "GET"
}
```

### Razón operativa

- **Coste**: €0 adicional (Infinity es open-source)
- **Infraestructura**: FastAPI ya presupuestada
- **Latencia**: 100–500 ms tolerable para observabilidad (refresh 30s)
- **Sin sincronización**: Siempre datos frescos

### Limitación conocida

Infinity **no soporta alertas complejas** (ej.: "si wipe rate > 50% durante 2 ciclos"). Si es crítico, Fase 8.4 evalúa migración a PostgreSQL como breaking change.

### Consecuencias
**ACEPTADA** — bajo overhead, bajo coste.

---

## ADR-8-05: Modelos de Respuesta — Independence de Schemas de Ingesta

### Status
**ACEPTADA**

### Contexto

FastAPI necesita devolver JSON con KPIs. Dos enfoques:

1. **Reutilizar srcschemas**: Modelos Pydantic de ingesta como respuesta (Frágil)
2. **srcapi/schemas/responses.py**: Modelos independientes (Estable)

### Decisión

Crear `src/api/schemas/responses.py` completamente independiente de `src/schemas` (ingesta).

```python
# Frágil — cambios de ingesta rompen API
from src.schemas import RaidEventSchema
app.get("/raids/{raid_id}")
def get_raid(raid_id: UUID):
    return RaidEventSchema(...)  # ❌ Si alguien en Fase 1 añade un campo, API cambia

# Estable — contrato explícito
from src.api.schemas.responses import RaidSummaryResponse
app.get("/raids/{raid_id}")
def get_raid(raid_id: UUID):
    return RaidSummaryResponse(...)  # ✅ Contrato independiente
```

### Razón operativa

- **Contrato de API estable**: Independiente de cambios de ingesta
- **Versionado**: Futuro v1, v2, v3 sin fricciones
- **Breaking changes evitados**: Cambios en `src/schemas` no rompen consumidores

### Matriz de impacto

| Aspecto | Reutilizar | responses.py |
|---|---|---|
| **Contrato API** | Frágil (evoluciona con ingesta) | Estable (controlable) |
| **OpenAPI docs** | Mezcla ingesta/salida | Salida clara |
| **Versionado** | No (1 versión) | Sí (v1, v2, ...) |
| **Consumidores externos** | Frágil (breaking changes) | Protegido |
| **Código nuevo** | 0 lneas | 50–80 lneas |

### Consecuencias
**ACEPTADA** — bajo overhead, alta escalabilidad futura.

---

## Resumen de Decisiones

| ID | Decisión | Coste Nuevo | Latencia | Escalabilidad | Riesgo | Estado |
|---|---|---|---|---|---|---|
| **ADR-8-01 REV.2** | REST Catalog + cloud-readiness | €0 | 100 ms | Alta | Bajo | ✅ ACEPTADA |
| **ADR-8-02** | BaseSettings propio | €0 | N/A | Alta | Bajo | ✅ ACEPTADA |
| **ADR-8-03** | Gunicorn + workers | €15–25 | 50–100 ms | Alta | Bajo | ✅ ACEPTADA |
| **ADR-8-04** | Infinity Plugin | €0 | 100–500 ms | Media | Medio | ✅ ACEPTADA |
| **ADR-8-05** | responses.py independiente | €0 | N/A | Alta | Bajo | ✅ ACEPTADA |

**Coste total estimado en producción**: €40–50/mes en cloud (AWS t3.medium).

---

## Recomendaciones para Auditoria de Presupuesto

Fase 8 implementa visualización de KPIs del pipeline con **coste operativo fijo en €40/mes** y **0 servicios adicionales**.

Las decisiones priorizan **bajo acoplamiento** y **escalabilidad horizontal** para crecimiento futuro sin rediseño arquitectónico.

### Argumentos de Sostenibilidad

| Alternativa | Coste/mes | Mantenimiento | Escalabilidad |
|---|---|---|---|
| **A**: PySpark en cada request | €200 | Alto | Limitada |
| **B**: PostgreSQL sync | €100 | Medio | Media |
| **C**: Nuestra opción (REST + DuckDB) | €40 | Bajo | Alta |

### Roadmap a Producción (Fase 9)

1. **Cambiar estructura de bucket**: `s3://warehouse/` → `s3://raid-savior-data/warehouse/`
2. **Reemplazar REST Catalog**: Tabulario → AWS Glue Data Catalog (managed)
3. **Eliminar `-Daws.s3.forcePathStyle=true`** (S3 usa virtual-hosted)
4. **Migrar configuración**: Variables ENV → AWS Secrets Manager + IAM

**Código Spark/PyIceberg no cambia. Solo configuración.**

---

## Changelog de Decisiones

| Fecha | ADR | Decisión | Razón |
|---|---|---|---|
| 2026-03-18 | 8-01 v1 | PyIceberg + HadoopCatalog | Necesitado inicialmente |
| 2026-03-19 | 8-01 REV.2 | REST Catalog + cloud-readiness | Descubrimiento de incompatibilidad PyIceberg; portabilidad a S3 real |
| 2026-03-18 | 8-02 | BaseSettings propio | Fail-fast, multi-entorno |
| 2026-03-18 | 8-03 | Gunicorn workers | Concurrencia Grafana |
| 2026-03-18 | 8-04 | Infinity Plugin | Coste cero, independencia |
| 2026-03-18 | 8-05 | responses.py independiente | Contrato API estable |

---

## Información de Documento

- **Generado**: 2026-03-19
- **Responsable**: Tutor Senior Pedagógico + Equipo Técnico
- **Revisión próxima**: Al cierre de Fase 8 (estimado 2026-04-15)
- **Repositorio**: [raid-savior](https://github.com/Vincent0675/raid-savior)
