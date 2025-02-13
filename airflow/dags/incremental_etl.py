from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

import json
import os
import requests
import pandas as pd
from datetime import datetime
import logging

SNOWFLAKE_CONN_ID = "snowflake_default"
API_EXTRACT_ENDPOINT = "https://neonblue--nblearn-hiring-read.modal.run"
LABEL = "denis-test1"
API_EXTRACT_BASE_DATA = {
    "label": LABEL,
    "page_number": 0,
    "row_count": 1000
}
STAGE_NAME = "EVENTS_ETL_DAP"
TABLE_NAME = "EVENTS_DAP"

def extract_from_api(extraction_endpoint, payload): 
    current_timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    output_path = os.path.join(
        os.path.join(os.getcwd(), "output"),
        current_timestamp
    )
    if not os.path.exists(output_path):
        try:
            os.makedirs(output_path)
        except Exception as e:
            raise e

    has_data = True
    page_number = 0

    while has_data:
        payload["page_number"] = page_number
        logging.info(f"Extracting page number: {page_number}")
        res = requests.get(extraction_endpoint,data=json.dumps(payload))
        if res.status_code != 200:
            raise ValueError(f"API request failed with status {res.status_code}: {res.text}")
        data = json.loads(res.json()["data"])
        if len(data) < 1:
            has_data = False
        else:
            save_local_file(data, page_number, output_path)
            # df = pd.DataFrame(data)
            page_number += 1

    return output_path


def save_local_file(data, page_number, output_path):
    df = pd.DataFrame(data)
    file_name = f"events_chunk_{page_number}.parquet"
    df.to_parquet(os.path.join(output_path,file_name))


def upload_to_snowflake_stage(output_path, stage_name):
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
    
    for file in os.listdir(output_path):
        file_path = os.path.join(output_path,file)
        cursor = conn.cursor()
        cursor = conn.cursor()
        cursor.execute(
            f"""CREATE STAGE IF NOT EXISTS {stage_name}
                FILE_FORMAT = (TYPE = 'PARQUET');"""
        )
        cursor.execute(f"PUT file://{file_path} @{stage_name}")
        cursor.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "events_api_incremental_etl",
    default_args=default_args,
    description="ETL to load Parquet files into Snowflake",
    schedule="0 * * * *",
    catchup=False
)


extract_data = PythonOperator(
    task_id="extract_data",
    python_callable=extract_from_api,
    op_kwargs={
        "extraction_endpoint": API_EXTRACT_ENDPOINT,
        "payload": API_EXTRACT_BASE_DATA
    },
    provide_context=True,
    dag=dag
)

# Step 4: Upload to Snowflake stage
upload_stage = PythonOperator(
    task_id="upload_stage",
    python_callable=upload_to_snowflake_stage,
    op_kwargs={
        "output_path": extract_data.output,
        "stage_name": STAGE_NAME
    },
    provide_context=True,
    dag=dag,
)

# # Step 5: Run MERGE into final table
merge_table = SnowflakeOperator(
    task_id="merge_table",
    sql=f"""
        MERGE INTO SCRATCH.{TABLE_NAME} AS target
        USING (
            SELECT
                $1:"account_id"::varchar as account_id,
                $1:"device_id"::varchar as device_id,
                TO_TIMESTAMP($1:"created_at"::NUMBER / 1000000000) as created_at,
                $1:"event_id"::varchar as event_id
            FROM @{STAGE_NAME}
        ) AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN
            INSERT (
                account_id,
                device_id,
                created_at,
                event_id
            )
            VALUES (
                source.account_id,
                source.device_id,
                source.created_at,
                source.event_id
        );
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

trigger_dbt = TriggerDagRunOperator(
    task_id="trigger_dbt_transformations",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    trigger_dag_id="transform_events_dbt",
    dag=dag
)

extract_data >> upload_stage >> merge_table >> trigger_dbt
