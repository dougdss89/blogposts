#!/bin/bash

# Caminho para os scripts
ATUALIZAR_HOSTS=/home/hadoop/updatehost.sh
INICIAR_HDFS=/home/hadoop/startall.sh

# Executar script de atualização de hosts
echo "Atualizando lista de hosts..."
if sudo $ATUALIZAR_HOSTS; then
    echo "Lista de hosts atualizada com sucesso."
else
    echo "Erro ao atualizar a lista de hosts."
    exit 1
fi

# Executar script de inicialização do HDFS
echo "Iniciando serviços do HDFS..."
if $INICIAR_HDFS; then
    echo "Serviços do HDFS iniciados com sucesso."
else
    echo "Erro ao iniciar os serviços do HDFS."
    exit 1
fi
