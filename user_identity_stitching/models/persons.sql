{{
    config(
        materialized="incremental",
        unique_key="person_id",
        alias="PERSONS_DAP"
    )
}}

WITH agg_devices as (
    SELECT DISTINCT device_id
    FROM {{ source("PUBLIC", "EVENTS") }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["device_id"]) }} as person_id
FROM
    agg_devices
