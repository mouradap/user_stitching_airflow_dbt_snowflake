FROM apache/airflow:2.10.0-python3.11

USER root
RUN apt update && apt install -y git && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir dbt-core dbt-snowflake

USER root
ENV AIRFLOW_HOME=/opt/airflow

COPY ./user_identity_stitching /opt/airflow/dbt
RUN mkdir -p /home/airflow/.dbt
COPY ./user_identity_stitching/.dbt/profiles.yml /home/airflow/.dbt/profiles.yml
COPY entrypoint.sh /entrypoint.sh
COPY requirements.txt /opt/airflow/requirements.txt
RUN chmod +x /entrypoint.sh

USER airflow
WORKDIR /opt/airflow

ENTRYPOINT ["/entrypoint.sh"]
