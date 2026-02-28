#!/usr/bin/env bash
# scripts/download_spark_jars.sh
# JARs para PySpark 3.5.x + MinIO via S3A (AWS SDK v1 — compatible con Hadoop 3.3.x)
# DT-04: migración a SDK v2 pendiente en Fase I (requiere PySpark 4.x)

set -e

JAR_DIR="$HOME/spark-jars"
mkdir -p "$JAR_DIR"

MAVEN="https://repo1.maven.org/maven2"

echo "📦 Descargando JARs en $JAR_DIR ..."

[ -f "$JAR_DIR/hadoop-aws-3.3.6.jar" ] || \
    wget -q --show-progress \
    "$MAVEN/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar" \
    -O "$JAR_DIR/hadoop-aws-3.3.6.jar"

[ -f "$JAR_DIR/aws-java-sdk-bundle-1.12.786.jar" ] || \
    wget -q --show-progress \
    "$MAVEN/com/amazonaws/aws-java-sdk-bundle/1.12.786/aws-java-sdk-bundle-1.12.786.jar" \
    -O "$JAR_DIR/aws-java-sdk-bundle-1.12.786.jar"

echo "✅ JARs listos:"
ls -lh "$JAR_DIR"
