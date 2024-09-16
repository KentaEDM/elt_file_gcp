{{ config(
        materialized="view"
) }}

SELECT 
    country_code,
    country,
    Gold_Medal as gold_medal,
    Silver_Medal as silver_medal,
    Bronze_Medal as bronze_medal,
    Total as total_medal
FROM {{ source ('edm_data','table_medals_total') }}