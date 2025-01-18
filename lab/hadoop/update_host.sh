#!/bin/bash

# Captura os IPs dos containers dinamicamente usando getent
NODEMASTER_IP=$(getent hosts nodemaster.hdp.com | awk '{ print $1 }')
SECONDNODE_IP=$(getent hosts secondnode.hdp.com | awk '{ print $1 }')
DATANODE1_IP=$(getent hosts datanode1.hdp.com | awk '{ print $1 }')

DELTAMASTER_IP=$(getent hosts deltamasternode.hdp.com | awk '{ print $1 }')
DELTASECOND_IP=$(getent hosts deltasecondnode.hdp.com | awk '{ print $1 }')
DELTADATANODE_IP=$(getent hosts deltadatanode.hdp.com | awk '{ print $1 }')

# Verifica e atualiza o /etc/hosts apenas se necessário
if ! grep -q "$NODEMASTER_IP    nodemaster.hdp.com" /etc/hosts; then
    echo "$NODEMASTER_IP    nodemaster.hdp.com" >> /etc/hosts
fi

if ! grep -q "$SECONDNODE_IP    secondnode.hdp.com" /etc/hosts; then
    echo "$SECONDNODE_IP    secondnode.hdp.com" >> /etc/hosts
fi

if ! grep -q "$DATANODE1_IP    datanode1.hdp.com" /etc/hosts; then
    echo "$DATANODE1_IP    datanode1.hdp.com" >> /etc/hosts
fi

if ! grep -q "$DELTAMASTER_IP    deltamasternode.hdp.com" /etc/hosts; then
    echo "$DELTAMASTER_IP    deltamasternode.hdp.com" >> /etc/hosts
fi

if ! grep -q "$DELTASECOND_IP    deltasecondnode.hdp.com" /etc/hosts; then
    echo "$DELTASECOND_IP    deltasecondnode.hdp.com" >> /etc/hosts
fi

if ! grep -q "$DELTADATANODE_IP    deltadatanode.hdp.com" /etc/hosts; then
    echo "$DELTADATANODE_IP    deltadatanode.hdp.com" >> /etc/hosts
fi

# Exibir o conteúdo final do /etc/hosts para verificação
cat /etc/hosts
