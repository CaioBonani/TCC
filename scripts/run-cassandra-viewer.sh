#!/bin/bash
set -euo pipefail

/opt/lambda-scripts/wait-for-tcp.sh cassandra 9042 240

echo "Instalando dependencias do Cassandra Viewer..."
python3 -m pip install --no-cache-dir -r /opt/spark-apps/cassandra_viewer_requirements.txt

echo "Iniciando Cassandra Viewer em http://0.0.0.0:8088..."
exec python3 -m uvicorn cassandra_viewer:app \
    --host 0.0.0.0 \
    --port 8088 \
    --app-dir /opt/spark-apps
