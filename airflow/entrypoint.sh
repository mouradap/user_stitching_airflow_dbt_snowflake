#!/bin/bash
set -e

# Configuração de variáveis de ambiente
export SNOWFLAKE_ACCOUNT=""
export SNOWFLAKE_USER=""
export SNOWFLAKE_PASSWORD=""
export SNOWFLAKE_ROLE=""
export SNOWFLAKE_WAREHOUSE=""
export SNOWFLAKE_DATABASE=""
export SNOWFLAKE_SCHEMA=""

pip install --no-cache-dir -r /opt/airflow/requirements.txt

echo "Initializing Airflow database..."
airflow db init

echo "Creating admin user..."
airflow users create \
    --username admin \
    --password admin \
    --role Admin \
    --email admin@admin.com \
    --firstname Admin \
    --lastname User

exec airflow webserver &
exec airflow scheduler
