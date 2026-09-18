from pyspark.sql.functions import to_timestamp, count, current_date
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ETL Vendas E-commerce")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
    .enableHiveSupport()   
    .getOrCreate()
)

print("SparkSession criada com sucesso!")

# --- Leitura do HDFS ---
from datetime import date, timedelta
hoje = date.today().strftime("%Y%m%d")
caminho_hdfs = f"hdfs://namenode:9000/flume/eventos/{hoje}"

df = spark.read.json(caminho_hdfs)
print("Dados lidos do HDFS com sucesso!")
df.printSchema()

# --- Limpeza ---
df_limpo = df.dropna()
print(f"Registros antes/depois da limpeza: {df.count()} / {df_limpo.count()}")

# --- Transformação ---
df_transformado = df_limpo.withColumn("timestamp", to_timestamp("timestamp"))

# --- Aggregations (wide dependencies) ---
resumo_produto_evento = (
    df_transformado
    .groupBy("produto", "evento")
    .agg(count("*").alias("total"))
    .orderBy("produto", "total", ascending=[True, False])
)

resumo_eventos = (
    df_transformado
    .groupBy("evento")
    .agg(count("*").alias("total"))
    .orderBy("total", ascending=False)
)

resumo_produtos = (
    df_transformado
    .groupBy("produto")
    .agg(count("*").alias("total_eventos"))
    .orderBy("total_eventos", ascending=False)
)

resumo_produto_evento.show()
resumo_eventos.show()
resumo_produtos.show()

# --- Gravar no Hive (Data Warehouse) ---
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")

# Tabela principal de resumo
resumo_produto_evento.write \
    .mode("overwrite") \
    .saveAsTable("ecommerce.resumo_produto_evento")

# Tabela de eventos por tipo
resumo_eventos.write \
    .mode("overwrite") \
    .saveAsTable("ecommerce.resumo_eventos")

# Tabela de ranking de produtos
resumo_produtos.write \
    .mode("overwrite") \
    .saveAsTable("ecommerce.ranking_produtos")

print("Dados gravados no Hive com sucesso!")

# --- Backup em Parquet no HDFS (mantém compatibilidade) ---
resumo_produto_evento.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/resultados/resumo_produto_evento")

print("Backup Parquet salvo no HDFS!")
