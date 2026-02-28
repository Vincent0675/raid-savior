#!/usr/bin/env bash
# scripts/download_spark_jars.sh
# Descarga los JARs de Hadoop S3A necesarios para PySpark + MinIO (Fase E)
# ADR-01: hadoop-aws 3.4.1 + AWS SDK v2 (bundle-2.24.6)

set -e  # sale si cualquier comando falla

JAR_DIR="$HOME/spark-jars"
mkdir -p "$JAR_DIR"

HADOOP_AWS="hadoop-aws-3.4.1.jar"
SDK_BUNDLE="bundle-2.24.6.jar"

MAVEN="https://repo1.maven.org/maven2"

echo "📦 Descargando JARs en $JAR_DIR ..."

[ -f "$JAR_DIR/$HADOOP_AWS" ] || \
    wget -q --show-progress \
    "$MAVEN/org/apache/hadoop/hadoop-aws/3.4.1/$HADOOP_AWS" \
    -O "$JAR_DIR/$HADOOP_AWS"

[ -f "$JAR_DIR/$SDK_BUNDLE" ] || \
    wget -q --show-progress \
    "$MAVEN/software/amazon/awssdk/bundle/2.24.6/$SDK_BUNDLE" \
    -O "$JAR_DIR/$SDK_BUNDLE"

echo "✅ JARs listos en $JAR_DIR"
ls -lh "$JAR_DIR"