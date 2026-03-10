# Fase 7 — Table Formats con Apache Iceberg (ACID y Time Travel)

## Objetivo

La Fase 7 introduce Apache Iceberg en las capas Silver y Gold del pipeline para evolucionar desde directorios Parquet tradicionales hacia tablas gestionadas con metadatos transaccionales. El objetivo es añadir capacidades de ACID, time travel, mutaciones controladas y evolución de schema, manteniendo la arquitectura Medallion sobre MinIO.

## Estado actual

La Fase 7 es la fase actual del proyecto. En este momento, el trabajo activo se encuentra en la Subfase 7.2, centrada en construir la capa Silver como tabla Iceberg ACID para eventos limpios.

## Qué problema resuelve

Hasta la Fase 6, Silver y Gold funcionaban como directorios de archivos Parquet. Ese enfoque era válido para lectura y escritura por lotes, pero limitaba la evolución del sistema al no ofrecer transacciones, historial de versiones ni una base sólida para cambios de schema y uso multi-motor.

## Alcance de la fase

Incluye:

- Catálogo Iceberg sobre MinIO
- Migración progresiva de Silver a Iceberg
- Migración progresiva de Gold a Iceberg
- Preparación para time travel y correcciones de negocio
- Base técnica para mutaciones controladas y evolución de schema

No incluye:

- Cambios estructurales en Bronze
- Dashboards finales
- Serving por API
- Modelado de machine learning
- Integración con datos reales externos

## Subfases

- 7.1 Catálogo Iceberg sobre MinIO
- 7.2 Silver ACID (eventos limpios)
- 7.3 Gold ACID: tablas de hechos
- 7.4 Gold ACID: dimensiones
- 7.5 Time travel y correcciones de negocio

## Decisiones técnicas

- PySpark 3.5 como motor principal
- Catálogo tipo Hadoop catalog sobre MinIO
- Namespaces `wow.silver` y `wow.gold`
- Mantener compatibilidad con buenas prácticas de Python 3.10.x
- Posible soporte adicional con Pytest, Ruff y MyPy

## Resultado esperado

La Fase 7 convertirá Silver y Gold en capas más robustas, trazables y preparadas para producción real. Con Iceberg, el pipeline gana una base moderna para crecer en volumen, interoperar con más motores y soportar cambios futuros sin rehacer el layout físico del almacenamiento.

## Estado

**Estado de fase:** actual  
**Subfase activa:** 7.3 Silver ACID (en proceso)
