# TP1 — Arquitetura de Big Data em Tempo Real
**Monitoramento de Vendas e Logística de E-commerce**

## Arquitetura


gerador.py → eventos.log
     ↓
   Flume (exec source → HDFS sink)
     ├→ HDFS /flume/eventos/YYYYMMDD  (logs brutos)
     │     ↓
     │   Spark (ETL batch) → Hive (Data Warehouse)
     │
     └→ Flink (streaming, janelas deslizantes + watermarks)
           ↓
         HBase (alertas em tempo real)


## Tecnologias e Camadas

| Camada | Tecnologia | Função |
|---|---|---|
| **Geração de Dados** | Python 3 | Geração de eventos sintéticos contínuos em formato JSON |
| **Ingestão Contínua** | Apache Flume | Coleta do log em tempo real e escrita particionada no HDFS |
| **Streaming Analytics** | Apache Flink 1.18 | Detecção de anomalias com janelas deslizantes e watermarks |
| **NoSQL / Fast Data** | Apache HBase | Armazenamento de baixa latência para alertas |
| **Processamento Batch** | Apache Spark 3.5 | Pipeline ETL (limpeza, ordenação e agregações com shuffle) |
| **Armazenamento Bruto** | Hadoop HDFS 3.2 | Armazenamento distribuído resiliente |
| **Data Warehouse** | Apache Hive 4.0 | Consultas analíticas SQL estruturadas via HiveServer2 / Beeline |

---

## Guia de Execução Passo a Passo (Terminal)

Abaixo está o roteiro consolidado com todos os comandos para reproduzir e validar cada etapa do pipeline individualmente no terminal:

### 1. Inicializar o Ambiente
```bash
# Constrói as imagens customizadas e sobe todos os 8 serviços distribuídos
docker compose up -d --build

# Verificar se todos os contêineres estão ativos
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### 2. Configurar a Tabela no HBase
```bash
# Acessar o console interativo do HBase
docker exec -it hbase hbase shell
```
*Dentro do HBase Shell:*
```ruby
create 'alertas', 'info'
list
exit
```

### 3. Geração de Dados e Ingestão (Flume ➔ HDFS)
```bash
# 1. Iniciar o gerador de eventos JSON (deixar rodando de 30 a 60 segundos)
python3 gerador.py

# 2. Em outro terminal, validar a criação das pastas e arquivos no HDFS
docker exec namenode hdfs dfs -ls /flume/eventos/
```

### 4. Processamento em Streaming (Flink ➔ HBase)
```bash
# Submeter o job de streaming com janelas deslizantes e detecção de anomalias
docker exec flink_jobmanager flink run -py /opt/flink/job/flink_job.py
```

### 5. Processamento em Lote / ETL (Spark ➔ Hive & Parquet)
```bash
# Executar o job PySpark (limpeza, ordenação, agregações analíticas e salvamento no HDFS)
docker exec spark spark-submit \
  --master local[*] \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /opt/spark/jobs/etl_vendas.py
```

### 6. Validação do Data Warehouse (Apache Hive)
```bash
# Conectar no Hive via Beeline
docker exec -it hive beeline -u jdbc:hive2://localhost:10000 -n hive
```
*Dentro do Beeline (executar os comandos SQL):*
```sql
-- Criar e selecionar o banco de dados
CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

-- Mapear as tabelas externas apontando para o HDFS
CREATE EXTERNAL TABLE IF NOT EXISTS ranking_produtos (
    produto STRING,
    total_eventos BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/ecommerce.db/ranking_produtos';

CREATE EXTERNAL TABLE IF NOT EXISTS resumo_eventos (
    evento STRING,
    total BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/ecommerce.db/resumo_eventos';

CREATE EXTERNAL TABLE IF NOT EXISTS resumo_produto_evento (
    produto STRING,
    evento STRING,
    total BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/ecommerce.db/resumo_produto_evento';

-- Consultas analíticas
SELECT * FROM ranking_produtos;
SELECT * FROM resumo_eventos;
SELECT * FROM resumo_produto_evento;

-- Sair do Beeline
!quit
```

### 7. Validação dos Alertas (Apache HBase)
```bash
# Acessar o HBase shell
docker exec -it hbase hbase shell
```
*Dentro do HBase Shell:*
```ruby
scan 'alertas', {LIMIT => 10}
count 'alertas'
exit
```

---

### ⚡ Execução Automatizada Ponta a Ponta (Opcional)

Se preferir rodar todo o pipeline de uma só vez de forma automatizada:
```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

---

## Interfaces Web (UIs)

- **HDFS NameNode:** [http://localhost:9871](http://localhost:9871)
- **Flink Dashboard:** [http://localhost:8082](http://localhost:8082)
- **HBase Master:** [http://localhost:16011](http://localhost:16011)
- **HiveServer2 Web UI:** [http://localhost:10002](http://localhost:10002)

---

## Estrutura do Repositório

├── README.md
├── gerador.py              # Gerador contínuo de eventos JSON
├── flume.conf              # Configuração do agente Flume (exec source -> HDFS sink)
├── flume-env.sh            # Variáveis de ambiente do Flume
├── eventos.log             # Log de eventos gerados
├── .gitignore
├── docker-compose.yml      # Orquestração dos 8 serviços distribuídos
├── Dockerfile              # Imagem customizada do Flume com Hadoop
├── run_pipeline.sh         # Script de execução automatizada (Bash)
├── flink/
│   ├── Dockerfile          # Imagem customizada PyFlink com Avro
│   ├── flink_job.py        # Job streaming com watermarks e gravação no HBase
│   └── requirements.txt    # Dependências (apache-flink, happybase)
└── spark/
    └── jobs/
        └── etl_vendas.py   # Job Spark ETL com agregações e escrita no Hive/Parquet
