SELECT
  device_id,
  COUNT(DISTINCT(IFNULL(PERSONS.person_id, '<MISSING>'))) unique_person_check_1,
  COUNT(DISTINCT(PERSONS.person_id)) unique_persons_check_2
FROM {{ source("SCRATCH", "EVENTS_DAP") }} EVENTS
LEFT JOIN {{ ref('accounts_to_persons_first_device') }} USING (account_id)
LEFT JOIN {{ ref('persons') }} PERSONS USING (person_id)
WHERE EVENTS.account_id IS NOT NULL
GROUP BY 1
