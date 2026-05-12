# 🚀 Arquitetura Lambda - DATASUS: Como Rodar (Passo a Passo)

Este guia prático mostra como rodar o pipeline completo passando por todas as camadas da Arquitetura Lambda (Producer -> Kafka -> HDFS -> Spark Streaming -> Spark Batch -> Cassandra -> Grafana).

## 1. Preparar a conexão PostgreSQL
Certifique-se de estar na pasta raiz do projeto (`/Users/caiobonani/Documents/TCC/CODIGOS`).

Preencha `app/db_connection_config.py` com os mesmos dados de conexao do DBeaver. Se o DBeaver precisa de tunelamento SSH, deixe `SSH_TUNNEL["enabled"] = True`.

---

## 2. Inicializar o Ambiente Docker

```bash
docker compose up -d
```

O Compose sobe a infraestrutura e tambem inicia os processos 24/7:

- `producer`: PostgreSQL -> Kafka
- `kafka-to-hdfs`: Kafka -> HDFS
- `speed-layer`: Kafka -> Cassandra
- `batch-layer-scheduler`: HDFS -> Cassandra periodicamente
- `cassandra-viewer`: interface web read-only para consultar `speed_view`, `batch_view` e `merged_view`

Verifique se os containers estão saudáveis e se os jobs estão rodando:

```bash
watch docker compose ps
```

---

## 3. Acompanhar os processos

```bash
docker compose logs -f producer
docker compose logs -f kafka-to-hdfs
docker compose logs -f speed-layer
docker compose logs -f batch-layer-scheduler
docker compose logs -f cassandra-viewer
```

---

## 4. Ajustar parâmetros

Os principais parametros ficam no `.env`:

```bash
PRODUCER_BATCH_SIZE=20
PRODUCER_INTERVAL_SECONDS=1.0
PRODUCER_EXTRA_ARGS=
BATCH_INTERVAL_SECONDS=3600
```

Para recortar o replay, por exemplo:

```bash
PRODUCER_EXTRA_ARGS=--start-year 2020 --end-year 2020
```

---

## 5. Acessar os Resultados (Serving Layer e Monitoramento)

### Painel do Cassandra (Consultas)
Para ver os dados processados salvos no Cassandra:
```bash
docker compose exec cassandra cqlsh cassandra
```
Aí no prompt `cqlsh>`, execute:
```sql
USE lambda_arch;
SELECT * FROM speed_view LIMIT 10;
SELECT * FROM batch_view LIMIT 10;
SELECT * FROM merged_view LIMIT 10;
```

### Dashboards e Métricas
Agora que os jobs estão rodando, você pode visualizar a saúde do cluster e as métricas de tempo real:

* **Cassandra Viewer:** [http://localhost:8088](http://localhost:8088)
* **Grafana (Dashboards):** [http://localhost:3000](http://localhost:3000) (User/Pass: `admin`/`admin`)
* **Prometheus:** [http://localhost:9090](http://localhost:9090)
* **Spark UI:** [http://localhost:8080](http://localhost:8080)
* **HDFS NameNode UI:** [http://localhost:9870](http://localhost:9870)

---

## 6. Encerrar
Para parar tudo:

```bash
# Parar os containers (preservando os dados)
docker compose down

# Parar e deletar todos os volumes/dados salvos (Reset Total)
docker compose down -v
```
