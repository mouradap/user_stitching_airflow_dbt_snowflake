from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import os

SNOWFLAKE_CONN_ID = "hiring_scratch"
API_CONN_ID = "api_default"


def upload_to_snowflake_stage(file_path):
    """Uploads the Parquet file to Snowflake stage."""
    import snowflake.connector
    
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    cursor = conn.cursor()
    cursor.execute(
        """CREATE STAGE IF NOT EXISTS hiring_dap_stage
            FILE_FORMAT = (TYPE = 'PARQUET');"""
    )
    cursor.execute(f"PUT file://{file_path} @hiring_dap_stage")
    cursor.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "load_historical_data",
    default_args=default_args,
    description="Historical data full load. Run once.",
    schedule_interval=None,
)

upload_stage = PythonOperator(
    task_id="upload_stage",
    python_callable=upload_to_snowflake_stage,
    op_kwargs={"file_path": "/opt/airflow/data/historical_load/events.parquet"},
    dag=dag
)

initiate_table = SnowflakeOperator(
    task_id="initiate_table",
    sql="""
        CREATE TABLE SCRATCH.EVENTS_DAP IF NOT EXISTS (
            ACCOUNT_ID VARCHAR(16777216),
            DEVICE_ID VARCHAR(16777216),
            CREATED_AT TIMESTAMP_NTZ(9),
            EVENT_ID VARCHAR(16777216))
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

merge_table = SnowflakeOperator(
    task_id="merge_table",
    sql="""
        COPY INTO EVENTS_DAP
        FROM (
            SELECT
                $1:"account_id"::varchar as account_id,
                $1:"device_id"::varchar as device_id,
                TO_TIMESTAMP($1:"created_at"::NUMBER / 1000000000) as created_at,
                $1:"event_id"::varchar as event_id
            FROM @HIRING_DAP_STAGE s
        )
        ;
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

initiate_table >> upload_stage >> merge_table
