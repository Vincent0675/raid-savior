from src.etl.spark_session import get_spark_session, stop_spark_session

def main():
    print(">>> SCRIPT INICIADO")
    spark = get_spark_session("IcebergConnectionTest")
    print(">>> SPARK SESSION OK")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS wow.gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS wow.silver")
    print("[OK] Namespaces creados")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS wow.gold.ping_test (
            id   BIGINT,
            msg  STRING
        )
        USING iceberg
    """)
    print("[OK] Tabla wow.gold.ping_test creada")

    spark.sql("INSERT INTO wow.gold.ping_test VALUES (1, 'snapshot_v1')")
    spark.sql("INSERT INTO wow.gold.ping_test VALUES (2, 'snapshot_v2')")
    print("[OK] Datos insertados")

    print("\n── Estado actual ──")
    spark.sql("SELECT * FROM wow.gold.ping_test").show()

    print("\n── Historial de snapshots ──")
    spark.sql("SELECT * FROM wow.gold.ping_test.history").show(truncate=False)

    # ── Time Travel: leer el estado ANTES del segundo INSERT ──────
    snapshot_v1_id = 5516511019615884241  # ← tu snapshot_id del primero

    print("\n── Time Travel → Snapshot 1 (solo primer INSERT) ──")
    spark.read \
        .option("snapshot-id", snapshot_v1_id) \
        .table("wow.gold.ping_test") \
        .show()

    stop_spark_session(spark)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f">>> ERROR: {e}")
        raise