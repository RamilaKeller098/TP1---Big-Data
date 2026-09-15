from pyspark.sql.functions import to_timestamp, count
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ETL Vendas E-commerce")
    .getOrCreate()
)

print("SparkSession criada com sucesso!")

caminho_hdfs = "hdfs://namenode:9000/flume/eventos/20260913"

df = spark.read.json(caminho_hdfs)

print("Dados lidos do HDFS com sucesso!")

df.printSchema()

df.show(truncate=False)

df_limpo = df.dropna()

print("Quantidade antes da limpeza:", df.count())
print("Quantidade depois da limpeza:", df_limpo.count())

df_transformado = df_limpo.withColumn(
    "timestamp",
    to_timestamp("timestamp")
)

resumo_produtos = (
    df_transformado
    .groupBy("produto")
    .agg(count("*").alias("total_eventos"))
    .orderBy("total_eventos", ascending=False)
)

print("Resumo de eventos por produto:")
resumo_produtos.show()

resumo_eventos = (
    df_transformado
    .groupBy("evento")
    .agg(count("*").alias("total"))
    .orderBy("total", ascending=False)
)

print("Resumo por tipo de evento:")
resumo_eventos.show()

resumo_produto_evento = (
    df_transformado
    .groupBy("produto", "evento")
    .agg(count("*").alias("total"))
    .orderBy("produto", "total", ascending=[True, False])
)

print("Resumo por produto e evento:")
resumo_produto_evento.show()


caminho_saida = "hdfs://namenode:9000/resultados/resumo_produto_evento"

resumo_produto_evento.write \
    .mode("overwrite") \
    .parquet(caminho_saida)

print("Resultado salvo no HDFS com sucesso!")

