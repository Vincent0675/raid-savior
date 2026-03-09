# Fase 4 — ETL Silver a Gold

## Objetivo

La Fase 4 transforma los datos refinados de Silver en una capa Gold orientada a negocio. En esta etapa se implementó la analítica Silver → Gold con Pandas y DuckDB, incluyendo la creación inicial de un modelo semidimensional y la consolidación de tablas Gold listas para consumo analítico.

## Alcance

Incluye:

- Lectura de datos limpios desde Silver
- Agregación analítica en memoria con Pandas
- Soporte de consulta y modelado con DuckDB
- Construcción inicial del modelo semidimensional de Gold
- Generación de tablas Gold orientadas a negocio
- Cálculo de KPIs a nivel raid
- Cálculo de KPIs a nivel jugador por raid

No incluye todavía:

- Serving por API en producción
- Dashboards finales
- Procesamiento Gold distribuido con Spark
- Table formats ACID y time travel
- Integración con datos reales de Warcraft Logs

## Salidas principales

La Fase 4 consolida dos tablas principales en Gold:

- `gold.raidsummary`: una fila por encuentro, con visión global de la raid
- `gold.playerraidstats`: una fila por jugador y raid, con métricas individuales de rendimiento

## KPIs principales

Entre las métricas generadas destacan:

- `raidoutcome` con clasificación `Success` o `Wipe`
- DPS y HPS por jugador
- `critrate`
- participación relativa en daño
- muertes individuales

## Resultado de la fase

La Fase 4 deja la capa Gold operativa como fuente analítica principal del proyecto. Desde este punto, el pipeline no solo ingiere y limpia datos, sino que también entrega un modelo analítico inicial con semidimensionalidad suficiente para visualización, APIs y futuros casos de machine learning.

## Stack

- Python
- Pandas
- DuckDB
- MinIO
- Silver
- Gold
