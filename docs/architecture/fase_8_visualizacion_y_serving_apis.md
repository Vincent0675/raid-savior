# Fase 8 — Visualización y Serving APIs

## 1.1. Contexto

La Fase 8 del proyecto **WoW Raid Telemetry Pipeline** aborda la capa de consumo y visualización de datos, construida sobre las capas Bronze, Silver y Gold ya implementadas en fases anteriores. Su objetivo es exponer la capa Gold (tablas Iceberg ACID sobre MinIO) mediante una API REST y paneles de observabilidad para consumo humano y de servicios.

## 1.2. Objetivos

- Proveer una API REST robusta para consultar KPIs de la capa Gold.
- Permitir la visualización de KPIs y estado del pipeline en herramientas industriales (Grafana).
- Diseñar la capa de consumo pensando en escalabilidad, coste y producción sostenible.

## 1.3. Alcance

Incluye:

- Implementación de un servicio FastAPI para lectura de tablas Gold via PyIceberg (REST Catalog).
- Contenedorización del servicio (Docker) y configuración para despliegue con Gunicorn + Uvicorn workers.
- Contratos de respuesta Pydantic independientes.

Planificado dentro de Fase 8.x:

- Configuración de Grafana con Infinity plugin como datasource HTTP JSON.

No incluye:

- SSE para streaming de eventos (se tratará fuera de Fase 8).
- Autenticación/autorización avanzada (planificada para Fase 9+).

---

## 2. Vista Lógica

### 2.1. Componentes Principales

- **API de KPIs (FastAPI):** expone endpoints REST para consulta de KPIs macroscópicos (raid) y microscópicos (jugador) a partir de las tablas Iceberg de Gold.
- **Servicio de Acceso a Datos (PyIceberg):** encapsula la lógica de lectura de datos desde MinIO/Iceberg REST Catalog.
- **Capa de Modelos de Respuesta:** modelos Pydantic específicos de la API que definen el contrato de salida y se generan como JSON Schema / OpenAPI.
- **Dashboards Grafana (planificado):** paneles que consumen la API vía Infinity plugin para mostrar salud del pipeline y rendimiento de raids.

### 2.2. Diagrama Lógico (texto)

- `src/api/main.py` — Punto de entrada FastAPI, configuración de routers y middlewares.
- `src/api/settings.py` — Configuración de entorno basada en Pydantic BaseSettings (endpoints S3, catálogo Iceberg, parámetros de API).
- `src/api/routes/` — Routers de FastAPI:
  - `health.py` — Endpoints de health y readiness.
  - `raids.py` — Consultas por raid (resumen, KPIs, jugadores).
  - `metrics.py` — KPIs globales (wipe rate, kills, etc.).
  - `catalog.py` — Namespaces y tablas visibles en REST Catalog.
- `src/api/schemas/responses.py` — Modelos Pydantic de respuesta (RaidSummaryResponse, GlobalMetricsResponse, etc.).
- `src/api/services/iceberg_service.py` — Servicio de acceso a tablas Iceberg via PyIceberg (REST Catalog).

---

## 3. Vista de Desarrollo

### 3.1. Estructura de Directorios

```text
src/
  api/
    __init__.py
    main.py
    settings.py
    routes/
      __init__.py
      health.py
      raids.py
      metrics.py
      catalog.py
    schemas/
      __init__.py
      responses.py
    services/
      __init__.py
      iceberg_service.py
    exceptions.py
```

### 3.2. Dependencias

- **FastAPI**: framework ASGI para la API REST.
- **Uvicorn**: servidor ASGI para desarrollo; workers en producción bajo Gunicorn.
- **Gunicorn**: process manager en producción con `UvicornWorker`.
- **PyIceberg**: acceso al catálogo Iceberg REST (MinIO) sin JVM.
- **DuckDB**: planificado para consultas SQL in-process en siguientes iteraciones de serving.
- **Pydantic v2 + pydantic-settings**: modelos de datos y configuración basada en entorno.

---

## 4. Vista de Procesos

### 4.1. Flujo de una Request `/raids/{raid_id}`

1. El cliente (Grafana, herramienta externa o script) realiza un `GET /raids/{raid_id}` contra FastAPI.
2. FastAPI valida `raid_id` (UUID) y delega en `IcebergService`.
3. `IcebergService` usa PyIceberg para localizar la tabla `gold.fact_raid_summary` en el catálogo REST sobre MinIO.
4. `IcebergService` carga la tabla como Arrow y la convierte a DataFrame de Pandas.
5. Se aplica filtrado/orden/paginación en memoria según endpoint.
6. Los resultados se transforman en un modelo `RaidSummaryResponse` (Pydantic).
7. FastAPI devuelve la respuesta JSON al cliente.

### 4.2. Concurrencia y Workers

- En desarrollo, se usa `uvicorn` con un único proceso.
- En producción, se contempla `gunicorn` con `UvicornWorker` y `WEB_CONCURRENCY` configurable:
  - `WEB_CONCURRENCY=2` en entornos pequeños (2 vCPU).
  - Permite escalar a 4, 8 workers según necesidades sin cambiar código.

---

## 5. Vista de Implementación

### 5.1. FastAPI y Settings

- `main.py` inicializa FastAPI, carga configuración desde `APISettings` y registra los routers.
- `APISettings` define variables como `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `WAREHOUSE_BUCKET`, `ICEBERG_REST_URI`.
- Se sigue un patrón fail-fast: si una variable obligatoria no está presente, se lanza un ValidationError al arrancar.

### 5.2. Servicio de Acceso Iceberg

- PyIceberg se usa para:
  - Cargar el catálogo REST.
  - Localizar la tabla Iceberg de Gold.
  - Exponer un escaneo como Arrow.
- Las consultas actuales de endpoints se resuelven sobre DataFrames en Pandas.
- DuckDB queda planificado para una siguiente optimización de serving SQL in-process.

### 5.3. Modelos de Respuesta

- Se define `RaidSummaryResponse` con campos como `raid_id`, `raid_date`, `boss_name`, `difficulty`, `total_damage`, `total_healing`, `total_deaths`, `duration_seconds`, `success`.
- Se definen `RaidPlayerResponse` y `RaidPlayersResponse` para exponer métricas por jugador dentro de una raid.
- Los modelos están desacoplados de los schemas de ingesta de eventos (`src/schemas/`).

---

## 6. Vista de Despliegue

### 6.1. Contenedores

- **Servicio API (`raid-api`)**:
  - Imagen basada en Python 3.10 / micromamba.
  - Incluye entorno Conda con FastAPI, PyIceberg, DuckDB.
  - Ejecuta `gunicorn` con `UvicornWorker`.
  - Expone el puerto 8000.
  - Estado: perfil de despliegue planificado; en validación local se ejecuta `uvicorn` desde entorno mamba.
- **MinIO**:
  - Servicio existente con puertos 9000 (API) y 9001 (console).
  - Almacena las capas Bronze, Silver y Gold.
- **Grafana**:
  - Imagen oficial Grafana con Infinity plugin instalado.
  - Se conecta a `raid-api` vía HTTP interno.
  - Estado: planificado para la siguiente iteración operativa.

### 6.2. Red y Health Checks

- En despliegue Docker integrado, los servicios comparten una red dedicada (por ejemplo `raid-network`).
- Health checks:
  - `/health` en FastAPI.
  - `/minio/health/live` en MinIO.
  - `/api/health` en Grafana.

---

## 7. Vista de Datos

### 7.1. Tablas Gold Consumidas

- `wow.gold.fact_raid_summary` — KPIs a nivel raid (una fila por raid).
- `wow.gold.fact_player_raid_stats` — KPIs a nivel jugador/raid.
- Dimensiones `wow.gold.dim_player` y `wow.gold.dim_raid` para enriquecer las respuestas con nombres, clases, dificultades.

### 7.2. Contratos de API (alto nivel)

- `/raids` — listado paginado de raids.
- `/raids/{raid_id}` — resumen de una raid.
- `/raids/{raid_id}/players` — KPIs por jugador dentro de una raid.
- `/metrics/global` — estadísticas globales (total raids, wipe rate, etc.).
- `/health` — estado básico de la API.

---

## 8. Tecnologías y Razonamiento

### 8.1. PyIceberg (implementado) + DuckDB (planificado)

Se adopta PyIceberg para la capa de servicio porque:

- Reduce significativamente la latencia de las consultas.
- Evita la necesidad de arrancar una JVM por proceso.
- Facilita desplegar un único contenedor ligero para la API.

DuckDB queda documentado como optimización planificada para expresividad SQL y potencial mejora de latencia en consultas complejas.

### 8.2. Gunicorn + Uvicorn

Se utiliza Gunicorn con workers Uvicorn en producción para:

- Aprovechar múltiples CPUs en el servidor.
- Gestionar procesos de forma robusta.
- Escalar la concurrencia configurando `WEB_CONCURRENCY` sin modificar el código.

### 8.3. Infinity Plugin en Grafana (planificado)

Se prioriza Infinity plugin como datasource para:

- Evitar desplegar y mantener un PostgreSQL adicional.
- Consumir directamente los endpoints HTTP de FastAPI.
- Mantener el coste de infraestructura bajo.

---

## 9. Riesgos y Deuda Técnica

### 9.1. Riesgos Identificados

- Falta de autenticación/autorización en la API — se debe abordar antes de exposición pública.
- Alerting limitado en Grafana con Infinity — para alertas complejas podría requerirse una base intermedia.

### 9.2. Deuda Técnica

- Integrar SSE como canal de consumo (fuera del alcance de Fase 8).
- Añadir autenticación basada en tokens (OAuth2 / JWT) para Fase 9.
- Evaluar necesidad de PostgreSQL si se incrementa la complejidad de dashboards y alertas.

---

## 10. Criterios de Aceptación de la Fase

La Fase 8 se considera completada cuando:

1. La API FastAPI está desplegada y responde correctamente a `/health`, `/raids`, `/raids/{raid_id}`, `/raids/{raid_id}/players`, `/metrics/global`.
2. Grafana con Infinity plugin está desplegado con al menos dos dashboards funcionales que consultan la API.
3. El despliegue en Docker (MinIO + API + Grafana) se realiza con un solo `docker compose up`.
4. La latencia media de las consultas de API está por debajo de 100 ms en entorno de pruebas.
5. Existen tests automatizados (pytest) que cubren al menos los endpoints principales y la lógica de acceso a datos.
