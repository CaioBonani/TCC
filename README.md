# Lambda Architecture - DATASUS (TCC)

Infraestrutura Docker completa para a **Arquitetura Lambda** aplicada ao processamento de dados do **DATASUS** (Sistema Único de Saúde), conforme descrito no TCC.

## 🏗️ Arquitetura

```
                    ┌─────────────┐
                    │  Produtor   │
                    │  (DATASUS)  │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │    KAFKA    │  ← Camada de Ingestão
                    │  (Broker)   │
                    └──┬──────┬───┘
                       │      │
            ┌──────────▼─┐  ┌─▼───────────┐
            │    HDFS    │  │    SPARK     │
            │ (NameNode  │  │  Structured  │  ← Speed Layer
            │  DataNode) │  │  Streaming   │
            └──────┬─────┘  └──────┬───────┘
                   │               │
            ┌──────▼─────┐         │
            │   SPARK    │         │
            │   Batch    │         │  ← Batch Layer
            └──────┬─────┘         │
                   │               │
            ┌──────▼───────────────▼──┐
            │      CASSANDRA          │  ← Serving Layer
            │  (batch_view +          │
            │   speed_view +          │
            │   merged_view)          │
            └─────────────────────────┘

            ┌─────────────────────────┐
            │  PROMETHEUS + GRAFANA   │  ← Monitoramento
            └─────────────────────────┘
```

## 📦 Serviços

| Serviço | Imagem | Porta | Propósito |
|---|---|---|---|
| Kafka | `confluentinc/cp-kafka:7.5.0` | 9092, 9093, 9101 | Ingestão e buffer |
| NameNode | `bde2020/hadoop-namenode` | 9870, 9000 | HDFS Master |
| DataNode | `bde2020/hadoop-datanode` | 9864 | HDFS Storage |
| Spark Master | `bitnami/spark:3.5` | 8080, 7077, 4040 | Processamento |
| Spark Worker | `bitnami/spark:3.5` | 8081 | Worker |
| Cassandra | `cassandra:4.1` | 9042, 7199 | Serving Layer |
| Prometheus | `prom/prometheus:v2.48.0` | 9090 | Métricas |
| Grafana | `grafana/grafana:10.2.0` | 3000 | Dashboards |

## 🚀 Quick Start

### 1. Pré-requisitos

- Docker e Docker Compose v2 instalados
- Mínimo 8GB de RAM disponível

### 2. Download do JMX Exporter

```bash
bash scripts/download-jmx-exporter.sh
```

### 3. Subir todos os containers

```bash
# Usando o script automatizado:
bash scripts/start.sh

# Ou manualmente:
docker compose up -d
```

### 4. Verificar status

```bash
# Health check completo:
bash scripts/health-check.sh

# Ou via docker:
docker compose ps
```

### 5. Aguardar todos os serviços ficarem healthy

Isso pode levar 1-2 minutos na primeira vez. O Cassandra é geralmente o último a ficar pronto.

```bash
watch docker compose ps
```

## 📊 Pipeline de Dados

### Passo 1: Injetar dados (Produtor Kafka)

```bash
# Instalar dependencias Python no container Spark
docker compose exec spark-master pip install -r /opt/spark-apps/requirements.txt cassandra-driver

# Rodar o produtor em modo data replay a partir da tabela PostgreSQL public.aih
# Use host.docker.internal se o PostgreSQL estiver rodando no host da maquina.
docker compose exec -e POSTGRES_PASSWORD='<senha>' spark-master python /opt/spark-apps/producer.py \
    --bootstrap-server kafka:9092 \
    --db-host host.docker.internal \
    --db-port 5432 \
    --db-name datasus \
    --db-user postgres \
    --db-schema public \
    --db-table aih \
    --batch-size 10 \
    --interval 1.0 \
    --loop
```

### Passo 2: Consumir Kafka → HDFS (Master Dataset)

```bash
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/spark-apps/kafka_to_hdfs.py
```

### Passo 3: Iniciar Speed Layer (Streaming)

```bash
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
    /opt/spark-apps/speed_layer.py
```

### Passo 4: Executar Batch Layer (Reprocessamento)

```bash
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
    /opt/spark-apps/batch_layer.py
```

## 🔗 URLs de Acesso

> **Nota:** Substitua `localhost` pelo IP da VM quando acessando via VPN.

| Serviço | URL | Credenciais |
|---|---|---|
| **Grafana** | http://localhost:3000 | admin / admin |
| **Spark Master** | http://localhost:8080 | — |
| **HDFS NameNode** | http://localhost:9870 | — |
| **Prometheus** | http://localhost:9090 | — |

## 📁 Estrutura do Projeto

```
├── docker-compose.yml          # Orquestração de todos os serviços
├── .env                        # Variáveis de ambiente
├── kafka/
│   ├── jmx-exporter-config.yml # Métricas JMX do Kafka
│   └── jmx_prometheus_javaagent.jar  # (baixado pelo script)
├── hadoop/
│   ├── core-site.xml           # Config core HDFS
│   └── hdfs-site.xml           # Config HDFS (128MB blocks)
├── spark/
│   └── spark-defaults.conf     # Config Spark + connectors
├── cassandra/
│   └── init.cql                # Schema: batch_view, speed_view, merged_view
├── prometheus/
│   └── prometheus.yml          # Scrape targets
├── grafana/
│   ├── provisioning/           # Auto-config datasources + dashboards
│   └── dashboards/             # Dashboard pré-configurado
├── app/
│   ├── producer.py             # Produtor Kafka (data replay da tabela public.aih)
│   ├── kafka_to_hdfs.py        # Consumidor Kafka → HDFS
│   ├── speed_layer.py          # Spark Structured Streaming
│   ├── batch_layer.py          # Spark Batch Processing
│   └── requirements.txt        # Dependências Python
└── scripts/
    ├── start.sh                # Setup + start completo
    ├── health-check.sh         # Verificação de status
    └── download-jmx-exporter.sh # Download JMX Exporter
```

## 🛑 Parar o Ambiente

```bash
# Parar containers (preserva dados):
docker compose down

# Parar e limpar TUDO (remove volumes):
docker compose down -v
```

## ⚙️ Configuração para Acesso Externo (VPN)

Todos os serviços expõem suas portas no host (`0.0.0.0`), permitindo acesso de qualquer máquina na VPN. Para o Kafka, ao acessar externamente, atualize o `KAFKA_ADVERTISED_LISTENERS` no `docker-compose.yml`:

```yaml
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9093,EXTERNAL://<IP_DA_VM>:9092
```

## 📈 Métricas Monitoradas

| Componente | Métrica | Descrição |
|---|---|---|
| Kafka | Consumer Lag | Atraso dos consumidores por partição |
| Kafka | Throughput | Mensagens/bytes por segundo |
| Spark | Active Tasks | Tarefas em execução |
| Spark | Memory | Uso de memória workers |
| HDFS | DataNode Health | Status dos nós de dados |
| Cassandra | R/W Latency | Latência de leitura/escrita |
