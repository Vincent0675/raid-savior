from src.etl.spark_session import get_spark_session, stop_spark_session


def main() -> None:
    print(">>> SCRIPT INICIADO")
    spark = get_spark_session("IcebergConnectionTest")
    print(">>> SPARK SESSION OK")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS wow.gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS wow.silver")
    print("[OK] Namespaces creados")

    spark.sql("DROP TABLE IF EXISTS wow.gold.ping_test PURGE")
    print("[OK] Tabla wow.gold.ping_test eliminada si existía")

    spark.sql("""
        CREATE TABLE wow.gold.ping_test (
            id BIGINT,
            msg STRING
        )
        USING iceberg
    """)
    print("[OK] Tabla wow.gold.ping_test creada")

    spark.sql("INSERT INTO wow.gold.ping_test VALUES (1, 'snapshot_v1')")
    spark.sql("INSERT INTO wow.gold.ping_test VALUES (2, 'snapshot_v2')")
    print("[OK] Datos insertados")

    print("\n── Estado actual ──")
    spark.sql("SELECT * FROM wow.gold.ping_test ORDER BY id").show()

    print("\n── Historial de snapshots ──")
    history_df = spark.sql("""
        SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor
        FROM wow.gold.ping_test.history
        ORDER BY made_current_at ASC
    """)
    history_df.show(truncate=False)

    first_snapshot_id = history_df.collect()[0]["snapshot_id"]
    print(f"\n[OK] Primer snapshot_id detectado dinámicamente: {first_snapshot_id}")

    print("\n── Time Travel → Snapshot 1 (solo primer INSERT) ──")
    spark.read.option("snapshot-id", first_snapshot_id).table(
        "wow.gold.ping_test"
    ).show()

    stop_spark_session(spark)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f">>> ERROR: {e}")
        raise
