# 🚀 Arquitetura Lambda - DATASUS: Como Rodar (Passo a Passo)

Este guia prático mostra como rodar o pipeline completo passando por todas as camadas da Arquitetura Lambda (Producer -> Kafka -> HDFS -> Spark Streaming -> Spark Batch -> Cassandra -> Grafana).

## 1. Inicializar o Ambiente Docker
Certifique-se de estar na pasta raiz do projeto (`/Users/caiobonani/Documents/TCC/CODIGOS`).

```bash
docker compose up -d
```
Verifique se todos os containers estão saudáveis (healthy) antes de prosseguir.

```bash
watch docker compose ps
```

---

## 2. Preparar Dependências

O container do Spark precisa dos pacotes Python para se conectar ao Kafka, ao Cassandra e ao PostgreSQL:

```bash
docker compose exec spark-master pip install -r /opt/spark-apps/requirements.txt
```

---

## 3. Iniciar a Ingestão de Dados (Producer)
Abra um terminal dedicado para o **Produtor**. Ele le a tabela real `public.aih` no PostgreSQL e faz um data replay em "conta-gotas", enviando lotes pequenos para o Kafka como se fossem eventos chegando em tempo real.

```bash
# Deixe este terminal aberto rodando:
docker compose exec -e POSTGRES_PASSWORD='<senha>' spark-master python3 /opt/spark-apps/producer.py \
    --bootstrap-server kafka:9092 \
    --db-host host.docker.internal \
    --db-port 5432 \
    --db-name datasus \
    --db-user postgres \
    --db-schema public \
    --db-table aih \
    --batch-size 20 \
    --interval 1.0 \
    --loop
```
Use `--start-year`, `--end-year`, `--start-dt-inter` e `--end-dt-inter` para recortar o replay. Use `--total N` para encerrar depois de N mensagens; sem `--loop`, o producer para ao fim da selecao.

---

## 4. Iniciar a Camada de Retenção (Kafka -> HDFS)
Abra **outro terminal**. Este job joga raw data do Kafka para o HDFS (cria o Master Dataset).

```bash
# Deixe rodando neste terminal:
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --total-executor-cores 1 \
    --executor-memory 512m \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    /opt/spark-apps/kafka_to_hdfs.py
```

---

## 5. Iniciar a Speed Layer (Spark Streaming)
Abra um **terceiro terminal**. Esta é a via rápida: processa dados do Kafka em janelas de 60s e salva agregações temporárias (`speed_view`) no Cassandra em micro-batches.

```bash
# Deixe rodando neste terminal:
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --total-executor-cores 1 \
    --executor-memory 512m \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    /opt/spark-apps/speed_layer.py
```

---

## 6. Rodar a Batch Layer (Spark Batch)
Enquanto a Speed Layer cuida do tempo real, a Batch Layer deve ser executada periodicamente para recalcular a base histórica com precisão, a partir do HDFS.

Abra um **quarto terminal** e execute:

```bash
# O processo roda, atualiza o Cassandra (batch_view e merged_view) e finaliza:
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --total-executor-cores 1 \
    --executor-memory 512m \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    /opt/spark-apps/batch_layer.py
```
> Sempre que quiser consolidar os dados novos que caíram no HDFS, execute este comando novamente.

---

## 7. Acessar os Resultados (Serving Layer e Monitoramento)

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

* **Grafana (Dashboards):** [http://localhost:3000](http://localhost:3000) (User/Pass: `admin`/`admin`)
* **Prometheus:** [http://localhost:9090](http://localhost:9090)
* **Spark UI:** [http://localhost:8080](http://localhost:8080)
* **HDFS NameNode UI:** [http://localhost:9870](http://localhost:9870)

---

## 8. Encerrar
Para parar tudo e limpar, pressione `Ctrl+C` nos terminais abertos e então:

```bash
# Parar os containers (preservando os dados)
docker compose down

# Parar e deletar todos os volumes/dados salvos (Reset Total)
docker compose down -v
```
