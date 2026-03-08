# scripts/

Scripts de operación, inspección y diagnóstico manual.
No son tests automatizados — se ejecutan directamente con `python`.

| Directorio | Propósito |
|---|---|
| `api/` | Receptor HTTP y endpoints |
| `etl/` | Inspección y diagnóstico del pipeline ETL |
| `analytics/` | Inspección de capas Bronze, Silver y Gold |
| `generators/` | Generación de datasets sintéticos |
| `pipeline/` | Diagnóstico de Spark, Iceberg y orquestación |