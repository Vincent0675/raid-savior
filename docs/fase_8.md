# Fase 8: Capa de Consumo y Visualización

**Estado:** 🟡 En progreso  
**Inicio:** 2026-03-18  

---

## Subfase 8.1 — Infraestructura API

**Estado:** ✅ Completada

- FastAPI arranca sin errores
- `health` devuelve `{"status": "ok"}`
- `src/api/settings.py` con `BaseSettings` independiente de Flask
- OpenAPI docs disponibles en `/docs`

---

## Subfase 8.2 — Registro de tablas en REST Catalog + acceso PyIceberg/DuckDB

**Estado:** 🟡 En progreso — base de infraestructura completada (2026-03-19)

### Problema resuelto: tablas Gold/Silver no visibles en REST Catalog

Las tablas Iceberg creadas por Spark en la Fase 7 usaban `HadoopCatalog` (metadata
en filesystem S3). El REST Catalog arranca con su SQLite **vacío** — no hereda
automáticamente esas tablas. Es necesario registrarlas explícitamente.

### Corrección crítica en `docker-compose.yml`

La imagen `tabulario/iceberg-rest:latest` requiere la variable en notación
SDK v2 con **doble guión bajo** para las propiedades Java anidadas:

```yaml
# ❌ Incorrecto (SDK v1 / no reconocido por latest)
- CATALOG_S3_PATH_STYLE_ACCESS=true

# ✅ Correcto (SDK v2 — notación de propiedad Java)
- CATALOG_S3_PATH__STYLE__ACCESS=true
```

Sin este cambio, el servidor REST no podía contactar MinIO y el
`CALL system.register_table` fallaba con `UnknownHostException`.

### Script de bootstrap creado

`scripts/bootstrap/register_tables_rest_catalog.py`

- Resolución dinámica de versiones via `version-hint.text` (no hardcodea `v2`, `v3`...)
- Compatible con metadata comprimida (`.gz.metadata.json`) y sin comprimir
- Re-ejecutable si se pierde el volumen `iceberg_catalog`


### Tablas registradas

| Namespace | Tabla | Metadata activo | Filas validadas |
| :-- | :-- | :-- | :-- |
| `gold` | `dim_player` | `v3.gz.metadata.json` | ✅ |
| `gold` | `dim_raid` | `v3.gz.metadata.json` | ✅ |
| `gold` | `fact_player_raid_stats` | `v2.gz.metadata.json` | ✅ |
| `gold` | `fact_raid_summary` | `v2.gz.metadata.json` | ✅ (10 filas) |
| `silver` | `raid_events` | `v2.gz.metadata.json` | ✅ |

### Verificación PyIceberg → Arrow

```python
catalog = load_catalog("wow", type="rest", uri="http://localhost:8181", ...)
catalog.list_namespaces()   # [('gold',), ('silver',)]
catalog.list_tables("gold") # 4 tablas
table.scan().to_arrow()     # 10 filas en fact_raid_summary
```

Pipeline completo validado: **MinIO → REST Catalog → PyIceberg → Arrow**

### Deuda técnica registrada

**DT-8.2-01:** Si el volumen `iceberg_catalog` se pierde, re-ejecutar
`scripts/bootstrap/register_tables_rest_catalog.py`. El script resuelve
automáticamente la versión de metadata más reciente — no requiere edición manual.

### Siguiente paso

Implementar `src/api/services/iceberg_service.py` (PyIceberg + DuckDB)
para ser consumido por los endpoints FastAPI.