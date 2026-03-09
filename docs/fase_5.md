# Fase 5 — ETL Silver a Gold Distribuido con Spark

## Objetivo

La Fase 5 traslada el procesamiento de la capa Gold desde un enfoque local a una ejecución distribuida. En esta etapa se migra el procesamiento Silver → Gold a un motor basado en PySpark 3.5, utilizando conectores S3A para trabajar con el almacenamiento del proyecto.

## Alcance

Incluye:

- Migración del procesamiento Gold a un motor distribuido
- Uso de PySpark 3.5 como framework principal
- Conexión al almacenamiento mediante conectores S3A
- Continuidad del flujo Silver → Gold en un entorno orientado a escalabilidad

No incluye todavía:

- Orquestación del pipeline
- Table formats con ACID y time travel
- Serving por API
- Visualización final
- Integración con datos reales externos

## Qué resuelve esta fase

La Fase 4 dejó una capa Gold funcional en procesamiento local. La Fase 5 aborda la siguiente necesidad natural del pipeline: ejecutar ese procesamiento en una arquitectura más preparada para volumen, paralelismo y crecimiento.

## Resultado de la fase

La Fase 5 deja establecida la transición del procesamiento Gold a un entorno distribuido. Con ello, el proyecto mantiene la misma finalidad analítica de la capa Gold, pero sobre una base técnica más adecuada para escalar.

## Stack

- Python
- PySpark 3.5
- S3A
- Silver
- Gold
