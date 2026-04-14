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
echo "  Prometheus:     http://localhost:9090"
echo "  Kafka:          localhost:9092"
echo "  Cassandra:      localhost:9042"
echo "  ─────────────────────────────────────────────"
echo ""
echo "  📖 Comandos úteis:"
echo "  ─────────────────────────────────────────────"
echo "  Ver logs:          docker compose logs -f [serviço]"
echo "  Status:            docker compose ps"
echo "  Parar tudo:        docker compose down"
echo "  Parar + limpar:    docker compose down -v"
echo "  ─────────────────────────────────────────────"
echo ""
echo "  🚀 Próximos passos:"
echo "  1. Aguarde todos os containers ficarem healthy"
echo "     docker compose ps"
echo "  2. Execute o producer para injetar dados:"
echo "     docker compose exec spark-master pip install kafka-python"
echo "     docker compose exec spark-master python /opt/spark-apps/producer.py --bootstrap-server kafka:9092"
echo "  3. Inicie o consumidor Kafka → HDFS:"
echo "     docker compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/kafka_to_hdfs.py"
echo "  4. Inicie a Speed Layer:"
echo "     docker compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 /opt/spark-apps/speed_layer.py"
echo "  5. Execute o Batch Layer:"
echo "     docker compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 /opt/spark-apps/batch_layer.py"
echo ""
