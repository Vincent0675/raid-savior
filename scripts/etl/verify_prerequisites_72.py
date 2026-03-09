from src.etl.spark_session import get_spark_session, stop_spark_session


def main() -> None:
    print(">>> INICIANDO VERIFICACIÓN PREREQUISITOS 7.2")
    spark = get_spark_session("VerifyPrerequisites72")
    print(">>> SPARK SESSION OK\n")

    print("=== CHECK 1: Namespaces en catálogo raíz ===")
    spark.sql("SHOW NAMESPACES").show()

    print("=== CHECK 2: Namespaces dentro de 'wow' ===")
    spark.sql("SHOW NAMESPACES IN wow").show()

    print("=== CHECK 3: Tablas en 'wow.silver' (vacío es OK) ===")
    spark.sql("SHOW TABLES IN wow.silver").show()

    print("=== CHECK 4: Confirmar que 7.1 sigue intacta ===")
    spark.sql("SELECT * FROM wow.gold.ping_test").show()

    stop_spark_session(spark)
    print(">>> VERIFICACIÓN COMPLETADA")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f">>> ERROR: {e}")
        raise
