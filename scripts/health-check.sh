#!/bin/bash
# =============================================================
# Health Check - Verifica status de todos os serviços
# =============================================================

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"

echo "══════════════════════════════════════════════════════"
echo "  Lambda Architecture - Health Check"
echo "══════════════════════════════════════════════════════"
echo ""

SERVICES=("kafka" "namenode" "datanode" "spark-master" "spark-worker" "cassandra" "prometheus" "grafana")
ALL_OK=true

for service in "${SERVICES[@]}"; do
    STATUS=$(docker compose ps --format "{{.Status}}" "$service" 2>/dev/null || echo "NOT RUNNING")

    if echo "$STATUS" | grep -qi "up"; then
        if echo "$STATUS" | grep -qi "healthy"; then
            echo "  ✅ ${service}: HEALTHY"
        else
            echo "  🟡 ${service}: RUNNING (aguardando health check)"
        fi
    else
        echo "  ❌ ${service}: DOWN"
        ALL_OK=false
    fi
done

echo ""
if [ "$ALL_OK" = true ]; then
    echo "  ✅ Todos os serviços estão rodando!"
else
    echo "  ⚠️  Alguns serviços não estão disponíveis."
    echo "  Use 'docker compose logs <serviço>' para investigar."
fi
echo ""

# ─── Verificações de conectividade ───
echo "── Verificações de Conectividade ──"

# Kafka
echo -n "  Kafka (tópicos): "
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9093 --list 2>/dev/null && echo "" || echo "❌ Indisponível"

# HDFS
echo -n "  HDFS (NameNode): "
docker compose exec -T namenode hdfs dfs -ls / 2>/dev/null | head -5 && echo "" || echo "❌ Indisponível"

# Cassandra
echo -n "  Cassandra (keyspaces): "
docker compose exec -T cassandra cqlsh -e "DESCRIBE KEYSPACES;" 2>/dev/null || echo "❌ Indisponível"

echo ""
