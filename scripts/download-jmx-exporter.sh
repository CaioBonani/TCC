#!/bin/bash
# =============================================================
# Script para download do JMX Prometheus JavaAgent
# Lambda Architecture - TCC DATASUS
# =============================================================
# Baixa o JMX Exporter JAR necessário para expor métricas
# do Kafka para o Prometheus.
# =============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

JMX_EXPORTER_VERSION="0.20.0"
JMX_EXPORTER_JAR="jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"
JMX_EXPORTER_URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/${JMX_EXPORTER_JAR}"

DEST_DIR="${PROJECT_DIR}/kafka"
DEST_FILE="${DEST_DIR}/jmx_prometheus_javaagent.jar"

echo "=================================================="
echo "Download JMX Prometheus JavaAgent v${JMX_EXPORTER_VERSION}"
echo "=================================================="

if [ -f "${DEST_FILE}" ]; then
    echo "✅ JMX Exporter já existe: ${DEST_FILE}"
    echo "   Para forçar re-download, remova o arquivo e execute novamente."
    exit 0
fi

echo "📥 Baixando de: ${JMX_EXPORTER_URL}"
echo "   Destino: ${DEST_FILE}"

if command -v curl &> /dev/null; then
    curl -fSL -o "${DEST_FILE}" "${JMX_EXPORTER_URL}"
elif command -v wget &> /dev/null; then
    wget -q -O "${DEST_FILE}" "${JMX_EXPORTER_URL}"
else
    echo "❌ Erro: nem curl nem wget disponíveis. Instale um deles."
    exit 1
fi

echo "✅ Download concluído: ${DEST_FILE}"
echo "   Tamanho: $(du -h "${DEST_FILE}" | cut -f1)"
