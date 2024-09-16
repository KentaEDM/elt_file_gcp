{{ config(
        materialized="view"
) }}

SELECT 
    DISTINCT
    start_date,
    end_date,
    day,
    status,
    discipline AS discipline_schedules,
    discipline_code,
    venue,
    location_code

FROM {{source('edm_data','table_schedules')}}