#!/usr/bin/env bash
# scripts/download_spark_jars.sh
# JARs para PySpark 3.5.x + MinIO via S3A (AWS SDK v1 — compatible con Hadoop 3.3.x)
# DT-04: migración a SDK v2 pendiente en Fase I (requiere PySpark 4.x)

set -e

JAR_DIR="$HOME/spark-jars"
mkdir -p "$JAR_DIR"

MAVEN="https://repo1.maven.org/maven2"

echo "📦 Descargando JARs en $JAR_DIR ..."

HADOOP_AWS="hadoop-aws-3.3.4.jar"
SDK_BUNDLE="aws-java-sdk-bundle-1.12.262.jar"

[ -f "$JAR_DIR/$HADOOP_AWS" ] || \
    wget -q --show-progress \
    "$MAVEN/org/apache/hadoop/hadoop-aws/3.3.4/$HADOOP_AWS" \
    -O "$JAR_DIR/$HADOOP_AWS"

[ -f "$JAR_DIR/$SDK_BUNDLE" ] || \
    wget -q --show-progress \
    "$MAVEN/com/amazonaws/aws-java-sdk-bundle/1.12.262/$SDK_BUNDLE" \
    -O "$JAR_DIR/$SDK_BUNDLE"

echo "✅ JARs listos:"
ls -lh "$JAR_DIR"
