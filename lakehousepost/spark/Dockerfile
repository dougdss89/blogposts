# Cria Imagem Spark

# Imagem do SO usada como base
FROM python:3.10-bullseye as spark-base

# Atualiza o SO e instala pacotes
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      nano \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Cria usuários spark e airflow e adiciona ao grupo sudo
RUN useradd -m -s /bin/bash spark && \
    useradd -m -s /bin/bash airflow && \
    echo 'spark ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/spark && \
    echo 'airflow ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/airflow

    # Variáveis de ambiente
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

# Cria as pastas
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

RUN chown -R spark:spark ${HADOOP_HOME} && chown -R spark:spark ${SPARK_HOME}
# Download do arquivo de binários do Spark 
RUN curl -fSL https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3-scala2.13.tgz -o spark-3.5.5-bin-hadoop3-scala2.13.tgz \
 && tar -xvzf spark-3.5.5-bin-hadoop3-scala2.13.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.5.5-bin-hadoop3-scala2.13.tgz


# Prepara o ambiente com PySpark
FROM spark-base as pyspark

# Instala as dependências Python
COPY ./requirements.txt .
RUN pip install -r requirements.txt

# Mais variáveis de ambiente
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV PYSPARK_PYTHON=python3

# Copia o arquivo de configuração do Spark para a imagem
COPY conf/spark-defaults.conf "$SPARK_HOME/conf"

# Permissões
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

# Variável PYTHONPATH
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copia o script de inicialização dos serviços para a imagem
COPY entrypoint.sh .

# Ajusta o privilégio
RUN chmod +x entrypoint.sh

# Executa o script quando inicializar um container
ENTRYPOINT ["./entrypoint.sh"]
