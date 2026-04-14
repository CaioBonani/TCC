#!/usr/bin/env python3
"""
=============================================================
SPEED LAYER - Spark Structured Streaming
=============================================================
Consome dados em tempo real do Kafka (datasus-internacoes),
aplica normalizações, validações e agregações em janelas
móveis, e escreve os resultados no Cassandra (speed_view).

Uso (via spark-submit no container):
    spark-submit --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
        /opt/spark-apps/speed_layer.py
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum,
    avg, current_timestamp, expr, when, lit,
    to_timestamp, collect_list, map_from_arrays
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType
)


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


def criar_sessao_spark() -> SparkSession:
    """Cria a sessão Spark com configurações para streaming."""
    return (
        SparkSession.builder
        .appName("SpeedLayer-DATASUS")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints/speed-layer")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    print("=" * 60)
    print("SPEED LAYER - Spark Structured Streaming")
    print("Processando dados DATASUS do Kafka em tempo real")
    print("=" * 60)

    spark = criar_sessao_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ─── Leitura contínua das partições Kafka ───
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9093")
        .option("subscribe", "datasus-internacoes")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

    # ─── Parse JSON + Normalização ───
    df_parsed = (
        df_kafka
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")
        .select(
            from_json(col("json_str"), DATASUS_SCHEMA).alias("data"),
            col("kafka_timestamp")
        )
        .select("data.*", "kafka_timestamp")
    )

    # ─── Validação dos dados recebidos ───
    df_validado = (
        df_parsed
        .filter(col("uf").isNotNull())
        .filter(col("cid_principal").isNotNull())
        .filter(col("valor_aih") >= 0)
        .withColumn(
            "obito_flag",
            when(col("obito") == True, lit(1)).otherwise(lit(0))
        )
    )

    # ─── Agregações em janelas móveis ───
    # Janela de 60 segundos com slide de 30 segundos
    df_agregado = (
        df_validado
        .withWatermark("kafka_timestamp", "2 minutes")
        .groupBy(
            col("uf"),
            window(col("kafka_timestamp"), "60 seconds", "30 seconds")
        )
        .agg(
            count("*").alias("total_internacoes"),
            spark_sum("obito_flag").alias("total_obitos"),
            spark_sum("valor_aih").alias("total_valor"),
        )
        .select(
            col("uf"),
            col("window.start").alias("janela_inicio"),
            col("window.end").alias("janela_fim"),
            col("total_internacoes"),
            col("total_obitos"),
            col("total_valor"),
            current_timestamp().alias("processado_em"),
        )
    )

    # ─── Escrita incremental no Cassandra (speed_view) ───
    query = (
        df_agregado
        .writeStream
        .outputMode("update")
        .foreachBatch(escrever_cassandra)
        .option("checkpointLocation", "/tmp/spark-checkpoints/speed-layer")
        .trigger(processingTime="1 second")  # Micro-batches de 1 segundo conforme TCC
        .start()
    )

    print("✅ Speed Layer iniciada. Processando micro-batches...")
    print("   Janela: 60s | Slide: 30s | Trigger: 1s")
    print("   Destino: Cassandra (lambda_arch.speed_view)")

    query.awaitTermination()


def escrever_cassandra(batch_df, batch_id):
    """Callback para escrever cada micro-batch no Cassandra."""
    if batch_df.count() > 0:
        print(f"📝 Batch {batch_id}: Escrevendo {batch_df.count()} registros no Cassandra...")
        (
            batch_df
            .write
            .format("org.apache.spark.sql.cassandra")
            .options(table="speed_view", keyspace="lambda_arch")
            .mode("append")
            .save()
        )
        print(f"✅ Batch {batch_id}: Concluído.")
    else:
        print(f"⏭️  Batch {batch_id}: Sem dados para processar.")


if __name__ == "__main__":
    main()
