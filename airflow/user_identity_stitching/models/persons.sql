{{
    config(
        materialized="incremental",
        unique_key="person_id",
        alias="PERSONS_ETL_DAP"
    )
}}

WITH agg_devices as (
    SELECT DISTINCT device_id
    FROM {{ source("SCRATCH", "EVENTS_DAP") }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["device_id"]) }} as person_id
FROM
    agg_devices
