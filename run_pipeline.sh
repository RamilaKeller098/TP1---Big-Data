#!/bin/bash
set -e

echo "========================================"
echo "  Pipeline Big Data — E-commerce TP1"
echo "========================================"

# 1. Subir serviços
echo ""
echo "[1/6] Subindo todos os serviços com Docker Compose..."
docker compose up -d
echo "Aguardando 60 segundos para inicialização dos serviços"
sleep 60

# 2. Garantir tabela no HBase
echo ""
echo "[2/6] Verificando/Criando tabela 'alertas' no HBase..."
echo "create 'alertas', 'info'" | docker exec -i hbase hbase shell 2>/dev/null || true

# 3. Gerador de eventos
echo ""
echo "[3/6] Iniciando gerador de eventos (30 segundos)..."
timeout 30 python3 gerador.py || true
echo "Gerador finalizado."

# 4. Aguardar Flume ingerir no HDFS
echo ""
echo "[4/6] Aguardando o Flume ingerir os eventos para o HDFS..."
sleep 45
echo "Arquivos gerados no HDFS:"
docker exec namenode hdfs dfs -ls /flume/eventos/

# 5. Executar Job Flink (Streaming e Alertas no HBase)
echo ""
echo "[5/6] Submetendo Job Flink (streaming e alertas)..."
docker exec flink_jobmanager \
  flink run -py /opt/flink/job/flink_job.py

# 6. Executar Job Spark (Batch ETL e escrita no Hive)
echo ""
echo "[6/6] Submetendo Job Spark (batch ETL → Hive)..."
docker exec spark \
  spark-submit \
  --master "local[*]" \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /opt/spark/jobs/etl_vendas.py

# Validações Finais
echo ""
echo "========================================"
echo "  Validação dos Dados"
echo "========================================"

echo ""
echo "[Hive] Consultando Ranking de Produtos no Data Warehouse:"
docker exec hive beeline -u jdbc:hive2://localhost:10000 -n hive -e "USE ecommerce; SELECT * FROM ranking_produtos;"

echo ""
echo "[HBase] Consultando Alertas Gravados no HBase:"
echo "scan 'alertas', {LIMIT => 5}" | docker exec -i hbase hbase shell

echo ""
echo "========================================"
echo "  Pipeline concluído com sucesso!"
echo "========================================"
echo ""
echo "UIs disponíveis:"
echo "  HDFS UI:   http://localhost:9871"
echo "  Flink UI:  http://localhost:8082"
echo "  HBase UI:  http://localhost:16011"
echo "  Hive UI:   http://localhost:10002"
