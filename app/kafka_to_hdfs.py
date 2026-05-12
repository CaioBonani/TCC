#!/usr/bin/env python3
"""
=============================================================
CONSUMIDOR KAFKA → HDFS
=============================================================
Consome mensagens do tópico Kafka 'datasus-internacoes' e
persiste no HDFS como o Master Dataset (dados imutáveis).

Uso (via spark-submit no container):
    spark-submit --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        /opt/spark-apps/kafka_to_hdfs.py
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, LongType
)


# ─── Linha bruta da tabela public.aih (ddl_tabela.sql) ───
AIH_RAW_SCHEMA = StructType([
    StructField("ano_cmpt", IntegerType(), True),
    StructField("mes_cmpt", IntegerType(), True),
    StructField("dt_inter", StringType(), True),
    StructField("dt_saida", StringType(), True),
    StructField("cep", StringType(), True),
    StructField("munic_res", StringType(), True),
    StructField("munic_mov", StringType(), True),
    StructField("cgc_hosp", StringType(), True),
    StructField("cnes", StringType(), True),
    StructField("nasc", StringType(), True),
    StructField("sexo", IntegerType(), True),
    StructField("idade", IntegerType(), True),
    StructField("cod_idade", IntegerType(), True),
    StructField("nacional", StringType(), True),
    StructField("instru", IntegerType(), True),
    StructField("raca_cor", IntegerType(), True),
    StructField("etnia", StringType(), True),
    StructField("cbor", StringType(), True),
    StructField("morte", IntegerType(), True),
    StructField("uti_mes_to", IntegerType(), True),
    StructField("marca_uti", IntegerType(), True),
    StructField("val_uti", DoubleType(), True),
    StructField("proc_solic", StringType(), True),
    StructField("proc_rea", StringType(), True),
    StructField("val_sh", DoubleType(), True),
    StructField("val_sp", DoubleType(), True),
    StructField("n_aih", StringType(), True),
    StructField("val_tot", DoubleType(), True),
    StructField("infehosp", IntegerType(), True),
    StructField("ind_vdrl", IntegerType(), True),
    StructField("diag_princ", StringType(), True),
    StructField("diag_secun", StringType(), True),
    StructField("diagsec1", StringType(), True),
    StructField("diagsec2", StringType(), True),
    StructField("diagsec3", StringType(), True),
    StructField("diagsec4", StringType(), True),
    StructField("diagsec5", StringType(), True),
    StructField("diagsec6", StringType(), True),
    StructField("diagsec7", StringType(), True),
    StructField("diagsec8", StringType(), True),
    StructField("diagsec9", StringType(), True),
    StructField("cid_morte", StringType(), True),
])


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
    StructField("source_table", StringType(), True),
    StructField("replay_sequence", LongType(), True),
    StructField("replay_cycle", IntegerType(), True),
    StructField("raw_aih", AIH_RAW_SCHEMA, True),
])

HDFS_OUTPUT_PATH = "hdfs://namenode:9000/datasus/internacoes/"


def main():
    print("=" * 60)
    print("CONSUMIDOR KAFKA → HDFS (Master Dataset)")
    print("=" * 60)

    spark = (
        SparkSession.builder
        .appName("KafkaToHDFS-DATASUS")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ─── Leitura contínua do Kafka ───
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9093")
        .option("subscribe", "datasus-internacoes")
        .option("startingOffsets", "earliest")  # Lê desde o início para capturar todo histórico
        .option("failOnDataLoss", "false")
        .load()
    )

    # ─── Parse JSON ───
    df_parsed = (
        df_kafka
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), DATASUS_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("_ingestao_timestamp", current_timestamp())
    )

    # ─── Escrita no HDFS particionada por ano/mês/dia ───
    query = (
        df_parsed
        .writeStream
        .format("json")
        .outputMode("append")
        .option("path", HDFS_OUTPUT_PATH)
        .option("checkpointLocation", "hdfs://namenode:9000/datasus/checkpoints/kafka-to-hdfs/")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("✅ Consumidor Kafka → HDFS iniciado")
    print(f"   Destino: {HDFS_OUTPUT_PATH}")
    print("   Trigger: a cada 30 segundos")

    query.awaitTermination()


if __name__ == "__main__":
    main()
