{{
    config(
        materialized="incremental",
        unique_key="device_id",
        alias="DEVICE_TO_PERSON_ETL_DAP",
        on_schema_change="sync_all_columns",
        incremental_strategy="merge"
    )
}}

WITH new_persons AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["device_id"]) }} as person_id,
        device_id
    FROM {{ source("SCRATCH", "EVENTS_DAP") }}
    {% if is_incremental() %}
    WHERE device_id NOT IN (SELECT device_id FROM {{ this }})
    {% endif %}
)

SELECT
    device_id,
    person_id
FROM new_persons
