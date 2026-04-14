#!/usr/bin/env python3
"""
=============================================================
BATCH LAYER - Spark Batch Processing
=============================================================
Reprocessa o histórico completo do Master Dataset (HDFS),
executa transformações distribuídas via DAGs Spark, e
escreve visões consolidadas no Cassandra (batch_view).

Este é o pipeline que garante precisão máxima e corrige
inconsistências acumuladas pela Speed Layer.

Uso (via spark-submit no container):
    spark-submit --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
        /opt/spark-apps/batch_layer.py
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, year, month,
    current_timestamp, when, lit, collect_list,
    create_map, map_from_arrays
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType
)
from datetime import datetime


# ─── Schema dos dados DATASUS ───
DATASUS_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("municipio_codigo", StringType(), True),
    StructField("cnes", StringType(), True),
    StructField("cid_principal", StringType(), True),
    StructField("cid_descricao", StringType(), True),
    StructField("sexo", StringType(), True),
    StructField("faixa_etaria", StringType(), True),
    StructField("idade", IntegerType(), True),
    StructField("carater_internacao", StringType(), True),
    StructField("data_internacao", StringType(), True),
    StructField("data_alta", StringType(), True),
    StructField("dias_permanencia", IntegerType(), True),
    StructField("valor_aih", DoubleType(), True),
    StructField("obito", BooleanType(), True),
    StructField("uti", BooleanType(), True),
    StructField("procedimento_principal", StringType(), True),
])

# Paths HDFS
HDFS_INPUT_PATH = "hdfs://namenode:9000/datasus/internacoes/"
HDFS_CHECKPOINT_PATH = "hdfs://namenode:9000/datasus/checkpoints/batch/"


def criar_sessao_spark() -> SparkSession:
    """Cria a sessão Spark para processamento batch."""
    return (
        SparkSession.builder
        .appName("BatchLayer-DATASUS")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def main():
    print("=" * 60)
    print("BATCH LAYER - Reprocessamento Completo HDFS")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    spark = criar_sessao_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ─── 1. Leitura completa dos diretórios HDFS ───
    print("\n📂 Etapa 1: Lendo Master Dataset do HDFS...")
    try:
        df_raw = (
            spark.read
            .schema(DATASUS_SCHEMA)
            .json(HDFS_INPUT_PATH)
        )
        total_registros = df_raw.count()
        print(f"   ✅ {total_registros} registros lidos do HDFS")
    except Exception as e:
        print(f"   ⚠️  Nenhum dado encontrado no HDFS ({e})")
        print("   Criando DataFrame vazio para inicialização...")
        df_raw = spark.createDataFrame([], DATASUS_SCHEMA)
        total_registros = 0

    if total_registros == 0:
        print("\n⚠️  Sem dados para processar. Batch Layer inicializado com sucesso.")
        print("   Execute o producer.py para injetar dados no Kafka/HDFS.")
        spark.stop()
        return

    # ─── 2. Transformações distribuídas (DAGs Spark) ───
    print("\n⚙️  Etapa 2: Executando transformações distribuídas...")

    # Flag de óbito para agregação
    df_preparado = df_raw.withColumn(
        "obito_flag", when(col("obito") == True, lit(1)).otherwise(lit(0))
    ).withColumn(
        "ano", year(col("data_internacao"))
    ).withColumn(
        "mes", month(col("data_internacao"))
    )

    # ─── 3. Cálculo preciso - Agregações por UF/Ano/Mês ───
    print("\n📊 Etapa 3: Calculando agregações (UF × Ano × Mês)...")

    df_batch_view = (
        df_preparado
        .groupBy("uf", "ano", "mes")
        .agg(
            count("*").alias("total_internacoes"),
            spark_sum("obito_flag").alias("total_obitos"),
            spark_sum("valor_aih").alias("total_valor"),
            avg("dias_permanencia").alias("tempo_medio_permanencia"),
        )
        .withColumn("processado_em", current_timestamp())
        .withColumn("versao_batch", lit(1))
    )

    batch_view_count = df_batch_view.count()
    print(f"   ✅ {batch_view_count} agregações calculadas")

    # ─── 4. Escrita das visões consolidadas no Cassandra ───
    print("\n💾 Etapa 4: Escrevendo batch_view no Cassandra...")

    (
        df_batch_view
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(table="batch_view", keyspace="lambda_arch")
        .mode("append")
        .save()
    )
    print(f"   ✅ batch_view atualizada: {batch_view_count} registros")

    # ─── 5. Atualizar merged_view (Serving Layer) ───
    print("\n🔄 Etapa 5: Atualizando merged_view (Serving Layer)...")

    df_merged = (
        df_batch_view
        .select(
            col("uf"),
            col("ano"),
            col("mes"),
            lit("batch").alias("fonte"),
            col("total_internacoes"),
            col("total_obitos"),
            col("total_valor"),
            col("tempo_medio_permanencia"),
            current_timestamp().alias("atualizado_em"),
        )
    )

    (
        df_merged
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(table="merged_view", keyspace="lambda_arch")
        .mode("append")
        .save()
    )
    print(f"   ✅ merged_view atualizada")

    # ─── 6. Atualizar tabela de controle ───
    print("\n📋 Etapa 6: Atualizando controle de processamento...")

    from cassandra.cluster import Cluster

    try:
        cluster = Cluster(["cassandra"])
        session = cluster.connect("lambda_arch")
        session.execute("""
            UPDATE controle_processamento
            SET ultima_execucao = toTimestamp(now()),
                status = 'sucesso',
                detalhes = %s
            WHERE tipo_processamento = 'batch'
        """, [f"Processados {total_registros} registros, {batch_view_count} agregações geradas"])
        cluster.shutdown()
        print("   ✅ Tabela de controle atualizada")
    except Exception as e:
        print(f"   ⚠️  Erro ao atualizar controle: {e}")

    # ─── Resumo ───
    print("\n" + "=" * 60)
    print("BATCH LAYER - CONCLUÍDO")
    print("=" * 60)
    print(f"  Registros processados:  {total_registros}")
    print(f"  Agregações geradas:     {batch_view_count}")
    print(f"  Tabelas atualizadas:    batch_view, merged_view")
    print(f"  Conclusão:              {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
