{{ config(
        materialized="view"
) }}

SELECT
    DISTINCT
    event as event_gender,
    sport ,
    sport_code 
FROM {{source('edm_data','table_events')}}
