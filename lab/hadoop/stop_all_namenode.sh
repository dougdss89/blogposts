#!/bin/bash

# Função para verificar se um serviço está rodando
check_service() {
    local service_name=$1
    local service_status=$(ps aux | grep "$service_name" | grep -v grep)

    if [ -z "$service_status" ]; then
        echo "$service_name não está rodando."
        return 1
    else
        echo "$service_name está rodando."
        return 0
    fi
}

# Para o JournalNode
echo "Parando o JournalNode..."
hdfs --daemon stop journalnode
sleep 5  # Aguarda alguns segundos para o serviço parar

# Para o NameNode
echo "Parando o NameNode..."
hdfs --daemon stop namenode
sleep 5  # Aguarda alguns segundos para o serviço parar

# Para o ZKServer
echo "Parando o ZKServer..."
zkServer.sh stop
sleep 5  # Aguarda alguns segundos para o serviço parar

# Para o ZKFC (Zookeeper Failover Controller)
echo "Parando o ZKFC Controller..."
hdfs --daemon stop zkfc
sleep 5  # Aguarda alguns segundos para o serviço parar

# Verifica se o ZKFC está rodando após a parada
echo "Verificando se o ZKFC Controller está parado..."
check_service "zkfc"

echo "Todos os serviços foram parados com sucesso."
