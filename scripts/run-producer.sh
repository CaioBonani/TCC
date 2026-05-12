#!/bin/bash
set -euo pipefail

/opt/lambda-scripts/wait-for-tcp.sh kafka 9093 180

echo "Instalando dependencias Python do producer..."
python3 -m pip install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    sshtunnel==0.4.0 \
    'paramiko<4'

echo "Iniciando producer DATASUS..."
exec python3 -u /opt/spark-apps/producer.py \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER:-kafka:9093}" \
    --topic "${KAFKA_TOPIC:-datasus-internacoes}" \
    --batch-size "${PRODUCER_BATCH_SIZE:-20}" \
    --interval "${PRODUCER_INTERVAL_SECONDS:-1.0}" \
    --loop \
    ${PRODUCER_EXTRA_ARGS:-}
