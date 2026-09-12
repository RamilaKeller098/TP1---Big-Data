FROM eclipse-temurin:11-jre-jammy

RUN apt-get update && apt-get install -y wget procps

# Instala o Flume 1.9.0
RUN wget https://archive.apache.org/dist/flume/1.9.0/apache-flume-1.9.0-bin.tar.gz && \
    tar -xzf apache-flume-1.9.0-bin.tar.gz && \
    mv apache-flume-1.9.0-bin /opt/flume && \
    rm apache-flume-1.9.0-bin.tar.gz

# Baixa o Hadoop 3.2.1 (mesma versão do HDFS)
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz && \
    tar -xzf hadoop-3.2.1.tar.gz && \
    mv hadoop-3.2.1 /opt/hadoop && \
    rm hadoop-3.2.1.tar.gz

# --- CORREÇÃO DO CONFLITO DE GUAVA ---
# Remove a versão antiga do Guava que vem no Flume e copia a do Hadoop
RUN rm -f /opt/flume/lib/guava-11.0.2.jar
RUN cp /opt/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar /opt/flume/lib/
# --- FIM DA CORREÇÃO ---

# --- CORREÇÃO DA MEMÓRIA ---
# Copia o arquivo de configuração de memória para dentro do Flume
COPY flume-env.sh /opt/flume/conf/flume-env.sh
# --- FIM DA CORREÇÃO ---

ENV HADOOP_HOME=/opt/hadoop
ENV PATH="/opt/flume/bin:${PATH}"