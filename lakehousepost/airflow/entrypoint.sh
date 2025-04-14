#!/bin/bash
set -e

# Função para aguardar o banco de dados
wait_for_db() {
    airflow db check
    while [[ $? != 0 ]]; do
        echo "Aguardando banco de dados..."
        sleep 5
        airflow db check
    done
}

# Inicializa o banco de dados se necessário
init_airflow() {
    airflow db migrate
    
    if [ ! -e "/opt/airflow/.initialized" ]; then
        echo "Primeira inicialização, criando usuário admin..."
        airflow users create \
            --username admin \
            --firstname admin \
            --lastname admin \
            --role Admin \
            --email admin@example.com \
            --password admin
        
        touch "/opt/airflow/.initialized"
    fi
}

case "$1" in
  webserver)
    wait_for_db
    init_airflow
    exec airflow webserver
    ;;
  scheduler)
    wait_for_db
    init_airflow
    exec airflow scheduler
    ;;
  worker)
    wait_for_db
    exec airflow celery worker
    ;;
  flower)
    wait_for_db
    exec airflow celery flower
    ;;
  version)
    exec airflow version
    ;;
  *)
    # Se nenhum comando específico for fornecido, executa o comando passado
    exec "$@"
    ;;
esac