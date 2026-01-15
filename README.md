# WoW Raid Telemetry Pipeline
**Arquitectura Medallion (Bronze → Silver → Gold)**

## Descripción
Pipeline event-driven para procesar telemetría de raids de World of Warcraft, implementando arquitectura Medallion con validación estricta mediante Pydantic y almacenamiento escalable en MinIO.

## Fases del Proyecto

### Fase 0: Conocimiento Teórico (Completada)
- Data Lakes
- Arquitectura Medallion
- Formatos de almacenamiento (JSON, Parquet)
- S3 API y MinIO
- DuckDB vs Apache Spark

### Fase 1: Schema y Generación de Datos Sintéticos (En progreso)
**Objetivos:**
- [ ] Hito 1: Diseño de schema con Pydantic
- [ ] Hito 2: Generador de eventos sintéticos
- [ ] Hito 3: Validación y exportación a JSON

**Estado actual:** Hito 1 en progreso  
**Última actualización:** 13 de enero de 2026

### Fase 2: Ingesta y Almacenamiento (Planificada)
- Receptor HTTP (Flask)
- Almacenamiento en MinIO (Bronze)
- Dead Letter Queue
- Batching y compresión (JSON → Parquet)

### Fase 3: Transformación (Silver Layer)
- Limpieza de datos
- Normalización
- Enriquecimiento

### Fase 4: Analítica (Gold Layer)
- Agregaciones
- Métricas de rendimiento
- Detección de anomalías

## Stack Tecnológico

| Componente | Tecnología | Versión |
|-----------|------------|---------|
| Validación | Pydantic | 2.10.4 |
| Distribuciones | NumPy | 1.26.4 |
| Ingesta HTTP | Flask | 3.0.0 (Fase 2) |
| Almacenamiento | MinIO | 7.2.0 (Fase 2) |
| Formato Bronze | JSON | - |
| Formato Silver+ | Parquet | - |

## Setup

```bash
# 1. Clonar repositorio
git clone <tu-repo>
cd wow_telemetry_pipeline

# 2. Crear entorno
mamba create -n wow_pipeline python=3.10 -y
mamba activate wow_pipeline

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Ejecutar tests
pytest tests/
```