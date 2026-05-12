#!/bin/bash
# =============================================================
# SCRIPT DE INICIALIZAÇÃO - Lambda Architecture
# =============================================================
# Faz o setup completo e sobe todos os containers
# =============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "══════════════════════════════════════════════════════"
echo "  Lambda Architecture - Setup & Start"
echo "  TCC - Arquitetura Lambda (DATASUS)"
echo "══════════════════════════════════════════════════════"

# ─── 1. Download do JMX Exporter ───
echo ""
echo "📋 Etapa 1/4: Verificando JMX Exporter..."
bash "${SCRIPT_DIR}/download-jmx-exporter.sh"

# ─── 2. Criar diretórios do HDFS ───
echo ""
echo "📋 Etapa 2/4: Verificando estrutura de diretórios..."
mkdir -p "${PROJECT_DIR}/kafka"
mkdir -p "${PROJECT_DIR}/hadoop"
mkdir -p "${PROJECT_DIR}/spark"
mkdir -p "${PROJECT_DIR}/cassandra"
mkdir -p "${PROJECT_DIR}/prometheus"
mkdir -p "${PROJECT_DIR}/grafana/provisioning/datasources"
mkdir -p "${PROJECT_DIR}/grafana/provisioning/dashboards"
mkdir -p "${PROJECT_DIR}/grafana/dashboards"
mkdir -p "${PROJECT_DIR}/app"
echo "   ✅ Diretórios OK"

# ─── 3. Validar docker-compose ───
echo ""
echo "📋 Etapa 3/4: Validando docker-compose.yml..."
cd "${PROJECT_DIR}"
docker compose config --quiet 2>/dev/null && echo "   ✅ Docker Compose válido" || {
    echo "   ❌ Erro na validação do docker-compose.yml"
    exit 1
}

# ─── 4. Subir containers ───
echo ""
echo "📋 Etapa 4/4: Subindo containers..."
echo "   (Isso pode demorar na primeira vez por conta do download das imagens)"
echo ""
docker compose up -d

echo ""
echo "══════════════════════════════════════════════════════"
echo "  ✅ Todos os containers foram iniciados!"
echo "══════════════════════════════════════════════════════"
echo ""
echo "  🔗 URLs de Acesso:"
echo "  ─────────────────────────────────────────────"
echo "  Grafana:        http://localhost:3000  (admin/admin)"
echo "  Spark Master:   http://localhost:8080"
echo "  HDFS NameNode:  http://localhost:9870"
echo "  Cassandra UI:   http://localhost:8088"
echo "  Prometheus:     http://localhost:9090"
echo "  Kafka:          localhost:9092"
echo "  Cassandra:      localhost:9042"
echo "  ─────────────────────────────────────────────"
echo ""
echo "  📖 Comandos úteis:"
echo "  ─────────────────────────────────────────────"
echo "  Status:                 docker compose ps"
echo "  Health check:           bash scripts/health-check.sh"
echo "  Logs producer:          docker compose logs -f producer"
echo "  Logs Kafka → HDFS:      docker compose logs -f kafka-to-hdfs"
echo "  Logs Speed Layer:       docker compose logs -f speed-layer"
echo "  Logs Batch Scheduler:   docker compose logs -f batch-layer-scheduler"
echo "  Logs Cassandra UI:      docker compose logs -f cassandra-viewer"
echo "  Parar tudo:             docker compose down"
echo "  Parar + limpar:         docker compose down -v"
echo "  ─────────────────────────────────────────────"
echo ""
echo "  🚀 Próximos passos:"
echo "  1. Aguarde a infraestrutura ficar healthy:"
echo "     watch docker compose ps"
echo "  2. Acompanhe os jobs 24/7 pelos logs acima."
echo "  3. Ajuste PRODUCER_* e BATCH_INTERVAL_SECONDS no .env se quiser mudar o ritmo."
echo ""
