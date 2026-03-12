# Troubleshooting: Iceberg Vectorized Reader — SIGSEGV en entorno local

**Fase:** 7.4 — Gold ACID: dimensiones  
**Fecha:** 2026-03-12  
**Estado:** Resuelto  

---

## Síntoma

Al ejecutar por primera vez una lectura sobre una tabla Iceberg mediante
`spark.table("wow.silver.raid_events")`, la JVM crashea con `SIGSEGV` durante
el Stage 0 del plan de ejecución:

```text
# A fatal error has been detected by the Java Runtime Environment:
#  SIGSEGV (0xb) at pc=... SymbolTable::do_lookup
ERROR BaseReader: Error reading file(s): s3a://warehouse/silver/raid_events/...
java.lang.NullPointerException
    at org.apache.iceberg.shaded.org.apache.arrow.memory.BaseAllocator.buffer(...)
```

---

## Causa raíz

Iceberg activa por defecto el **lector vectorizado basado en Apache Arrow**
cuando lee tablas Iceberg en Spark. Arrow utiliza memoria **off-heap** (fuera
del heap de la JVM) para alojar sus buffers columnar. En un entorno local sin
configuración explícita de off-heap, el allocator de Arrow lanza un
`NullPointerException` al intentar reservar ese buffer, lo que desestabiliza
la JVM y produce el SIGSEGV.

### Por qué no ocurrió en subfases anteriores

Las subfases 7.1, 7.2 y 7.3 **escribían** hacia tablas Iceberg pero **leían
desde Parquet plano** con `.read.parquet()`. El lector vectorizado Arrow solo
se activa en lecturas de tablas Iceberg registradas en el catálogo. La 7.4 es
la primera subfase que ejecuta `spark.table()` sobre Silver Iceberg, por eso
es el primer punto de fallo.

| Subfase | Lectura fuente | Lector Arrow activado |
|---------|---------------|----------------------|
| 7.1 | — (ping test) | No |
| 7.2 | Parquet plano | No |
| 7.3 | Parquet plano | No |
| **7.4** | **Tabla Iceberg** | **Sí → falla** |

---

## Solución aplicada

Deshabilitar el lector vectorizado en `src/etl/spark_session.py`:

```python
# ── Entorno de desarrollo (laptop 4GB) ────────────────────────────────────────
# Deshabilita el lector vectorizado Arrow de Iceberg.
# Arrow requiere memoria off-heap que no está configurada en local.
# En producción: revertir a True y configurar spark.memory.offHeap.size.
.config("spark.sql.iceberg.vectorization.enabled", "false")
```

Con esta opción, Iceberg usa el lector **row-by-row** en lugar del vectorizado.
Es más lento pero completamente estable sin configuración de memoria off-heap.

---

## Configuración de producción

En un entorno con memoria suficiente, la solución correcta **no es deshabilitar
la vectorización** sino configurar la memoria off-heap que Arrow necesita:

```python
# Producción — revertir la línea anterior a True
.config("spark.sql.iceberg.vectorization.enabled", "true")
.config("spark.memory.offHeap.enabled", "true")
.config("spark.memory.offHeap.size", "1g")  # ajustar según nodo
```

Estas líneas están disponibles como comentario en `spark_session.py` para
facilitar la transición.

---

## Entorno afectado

| Variable | Valor |
|----------|-------|
| Hardware | ASUS TUF Gaming A15 — RTX 3050 Laptop (4 GB VRAM) |
| OS | Pop!\_OS 22.04 LTS |
| JVM | OpenJDK 17.0.18 |
| PySpark | 3.5.x |
| Iceberg runtime | iceberg-spark-runtime-3.5\_2.12-1.7.1 |
| Modo Spark | `local[*]` |

---

## Configuraciones adicionales aplicadas durante el diagnóstico

Las siguientes opciones se añadieron como estabilizadores del entorno local y
se mantienen en `spark_session.py`:

```python
# Sustituye G1 GC por Serial GC — más estable bajo baja memoria
.config("spark.driver.extraJavaOptions",   "-XX:+UseSerialGC -XX:-TieredCompilation")
.config("spark.executor.extraJavaOptions", "-XX:+UseSerialGC -XX:-TieredCompilation")

# Reduce particiones de shuffle para dataset sintético pequeño
.config("spark.sql.shuffle.partitions", "8")
```

`-XX:-TieredCompilation` desactiva la compilación JIT multinivel, reduciendo
la presión sobre la `SymbolTable` de la JVM durante stages de corta duración.

---

## Verificación de la solución

Ejecución exitosa de `gold_iceberg_dim_player.py`:

```text
Jugadores únicos : 312
Filas en Iceberg : 312
Diferencia       : 0

snapshot_id          | committed_at              | operation
---------------------|---------------------------|----------
5336815920032465898  | 2026-03-12 15:45:08.640   | append
3967675925440018982  | 2026-03-12 15:46:09.098   | overwrite
```

Dos snapshots esperados: `append` en la primera ejecución (INSERT masivo),
`overwrite` en la segunda (MERGE con todos los registros ya existentes).
Una tercera ejecución con los mismos datos producirá `overwrite` con 0 filas
nuevas, confirmando idempotencia.

---

## Referencias

- [Apache Iceberg — Spark Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Apache Arrow — Memory Management](https://arrow.apache.org/docs/java/memory.html)
- `src/etl/spark_session.py` — configuración activa del entorno
- `src/etl/gold_iceberg_dim_player.py` — primera subfase afectada
