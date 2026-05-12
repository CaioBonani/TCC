#!/bin/bash
set -euo pipefail

/opt/lambda-scripts/wait-for-tcp.sh kafka 9093 180
/opt/lambda-scripts/wait-for-tcp.sh cassandra 9042 240
/opt/lambda-scripts/wait-for-tcp.sh spark-master 7077 180

echo "Iniciando Speed Layer..."
exec /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
    --total-executor-cores "${SPARK_JOB_CORES:-1}" \
    --executor-memory "${SPARK_JOB_MEMORY:-512m}" \
    --packages "${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0},${SPARK_CASSANDRA_PACKAGE:-com.datastax.spark:spark-cassandra-connector_2.12:3.4.1}" \
    /opt/spark-apps/speed_layer.py
