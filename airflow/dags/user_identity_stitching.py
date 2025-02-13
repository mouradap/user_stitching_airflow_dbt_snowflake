from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "transform_events_dbt",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .dbt",
        dag=dag,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .dbt",
        dag=dag,
    )

    dbt_run >> dbt_test
