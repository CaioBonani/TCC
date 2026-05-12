#!/bin/bash
set -euo pipefail

BATCH_INTERVAL_SECONDS="${BATCH_INTERVAL_SECONDS:-3600}"

/opt/lambda-scripts/wait-for-tcp.sh namenode 9000 180
/opt/lambda-scripts/wait-for-tcp.sh cassandra 9042 240
/opt/lambda-scripts/wait-for-tcp.sh spark-master 7077 180

echo "Instalando dependencias Python da Batch Layer..."
python3 -m pip install --no-cache-dir cassandra-driver==3.29.0

while true; do
    echo "Iniciando execucao da Batch Layer..."
    /opt/spark/bin/spark-submit \
        --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
        --total-executor-cores "${SPARK_JOB_CORES:-1}" \
        --executor-memory "${SPARK_JOB_MEMORY:-512m}" \
        --packages "${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0},${SPARK_CASSANDRA_PACKAGE:-com.datastax.spark:spark-cassandra-connector_2.12:3.4.1}" \
        /opt/spark-apps/batch_layer.py

    echo "Batch Layer finalizada. Proxima execucao em ${BATCH_INTERVAL_SECONDS}s."
    sleep "${BATCH_INTERVAL_SECONDS}"
done
