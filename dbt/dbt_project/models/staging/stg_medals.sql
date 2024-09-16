{{ config(
        materialized="view"
) }}

SELECT
    DISTINCT
    *
FROM {{source('edm_data','table_medals')}}