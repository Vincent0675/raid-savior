print(">>> PASO 1: Python funciona")

try:
    from src.etl.spark_session import get_spark_session

    print(">>> PASO 2: Import OK")
except Exception as e:
    print(f">>> PASO 2 FALLÓ: {e}")
    exit(1)

try:
    spark = get_spark_session("MinimalTest")
    print(">>> PASO 3: SparkSession OK")
except Exception as e:
    print(f">>> PASO 3 FALLÓ: {e}")
    exit(1)

spark.sql("SELECT 1 AS test").show()
print(">>> PASO 4: SQL básico OK")
spark.stop()
