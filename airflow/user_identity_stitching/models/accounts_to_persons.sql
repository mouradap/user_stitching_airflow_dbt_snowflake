{{
    config(
        materialized="incremental",
        unique_key="account_id",
        alias="ACCOUNT_TO_PERSON_ETL_DAP",
        on_schema_change="sync_all_columns",
        incremental_strategy="merge"
    )
}}

WITH device_persons as (
    SELECT
        device_id,
        person_id
    FROM
        {{ ref("devices_to_persons") }}
),
account_devices AS (
	SELECT
		account_id,
		device_id,
		FIRST_VALUE(e.device_id) OVER (PARTITION BY e.account_id ORDER BY e.created_at) first_device
	FROM
		{{ source("SCRATCH", "EVENTS_DAP") }} e
	WHERE account_id IS NOT null
)

SELECT
    e.account_id,
    e.first_device device_id,
    d.person_id
FROM
    account_devices e
INNER JOIN
    device_persons d
    ON e.first_device = d.device_id
GROUP BY e.account_id, e.first_device, d.person_id

