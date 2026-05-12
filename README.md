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
| Spark Master | `apache/spark-py:latest` | 8080, 7077, 4040 | Processamento |
| Spark Worker | `apache/spark-py:latest` | 8081 | Worker |
| Producer | `apache/spark-py:latest` | — | Replay PostgreSQL → Kafka |
| Kafka → HDFS | `apache/spark-py:latest` | — | Master Dataset contínuo |
| Speed Layer | `apache/spark-py:latest` | — | Streaming Kafka → Cassandra |
| Batch Scheduler | `apache/spark-py:latest` | — | Reprocessamento periódico HDFS → Cassandra |
| Cassandra | `cassandra:4.1` | 9042, 7199 | Serving Layer |
| Cassandra Viewer | `python:3.11-slim` | 8088 | Navegação web read-only nas views |
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

## 📊 Pipeline de Dados 24/7

Ao executar `docker compose up -d`, o Compose tambem inicia os processos da aplicacao:

| Serviço Compose | Processo | Política |
|---|---|---|
| `producer` | Le `public.aih` no PostgreSQL via tunel SSH e envia eventos ao Kafka | contínuo com `--loop` |
| `kafka-to-hdfs` | Consome Kafka e grava o Master Dataset no HDFS | streaming contínuo |
| `speed-layer` | Processa Kafka em micro-batches e atualiza `speed_view` no Cassandra | streaming contínuo |
| `batch-layer-scheduler` | Reprocessa o HDFS e atualiza `batch_view`/`merged_view` | roda a cada `BATCH_INTERVAL_SECONDS` |

Configure `app/db_connection_config.py` com os mesmos dados de conexao do DBeaver antes de subir o ambiente. Se o DBeaver precisa de tunelamento SSH, deixe `SSH_TUNNEL["enabled"] = True`.

Para acompanhar os processos:

```bash
docker compose ps
docker compose logs -f producer
docker compose logs -f kafka-to-hdfs
docker compose logs -f speed-layer
docker compose logs -f batch-layer-scheduler
```

Para ver os dados do Cassandra no navegador:

```text
http://localhost:8088
```

O viewer lista as tabelas do keyspace `lambda_arch`, mostra linhas com limite configuravel e aceita apenas consultas `SELECT`.

Parametros principais ficam no `.env`:

```bash
PRODUCER_BATCH_SIZE=20
PRODUCER_INTERVAL_SECONDS=1.0
PRODUCER_EXTRA_ARGS=
BATCH_INTERVAL_SECONDS=3600
```

## 🔗 URLs de Acesso

> **Nota:** Substitua `localhost` pelo IP da VM quando acessando via VPN.

| Serviço | URL | Credenciais |
|---|---|---|
| **Grafana** | http://localhost:3000 | admin / admin |
| **Cassandra Viewer** | http://localhost:8088 | — |
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
