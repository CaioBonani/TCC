#!/bin/bash
set -euo pipefail

/opt/lambda-scripts/wait-for-tcp.sh kafka 9093 180
/opt/lambda-scripts/wait-for-tcp.sh namenode 9000 180
/opt/lambda-scripts/wait-for-tcp.sh spark-master 7077 180

echo "Iniciando Kafka -> HDFS..."
exec /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
    --total-executor-cores "${SPARK_JOB_CORES:-1}" \
    --executor-memory "${SPARK_JOB_MEMORY:-512m}" \
    --packages "${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0}" \
    /opt/spark-apps/kafka_to_hdfs.py
