# Fase 6 — Orquestación del Data Pipeline

## Objetivo

La Fase 6 introduce la orquestación automática del pipeline de datos. En esta etapa se incorpora Dagster para coordinar la ejecución de los procesos ETL y automatizar su orden de ejecución.

## Alcance

Incluye:

- Orquestación del pipeline con Dagster
- Modelado de los procesos ETL como `assets`
- Automatización de dependencias entre etapas
- Programación automática mediante `schedules`

No incluye todavía:

- Table formats con ACID y time travel
- Serving por API
- Dashboards finales
- Modelado de machine learning
- Integración con datos reales externos

## Qué resuelve esta fase

Hasta la Fase 5, el pipeline ya ejecutaba transformaciones sobre Bronze, Silver y Gold. La Fase 6 añade una capa de control operacional para ejecutar esas etapas de forma ordenada, repetible y automática.

## Resultado de la fase

La Fase 6 deja el pipeline orquestado con Dagster y preparado para ejecución automática. Con ello, el proyecto gana trazabilidad operativa y una base más profesional para evolucionar hacia gobierno, serving y consumo analítico.

## Stack

- Python
- Dagster
- ETL
- Assets
- Schedules
