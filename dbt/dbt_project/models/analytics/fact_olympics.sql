WITH athletes AS (
SELECT
    code as athletes_id,
    name,
    name_tv,
    gender,
    function as sport_role,
    country_code,
    country,
    nationality,
    height,
    weight,
    birth_date,
    DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) as age,
    education,
    ROW_NUMBER() OVER (PARTITION BY code) as row_num

FROM {{source('edm_data','table_athletes')}}
),

medals AS(
SELECT
    CAST(REGEXP_EXTRACT(code, r'\d+') AS INT64) as medal_code,
    medal_type,
    medal_date,
    name,
    discipline,
    event,
    ROW_NUMBER() OVER (PARTITION BY code) as row_num_mdl
FROM {{ref('stg_medals')}}
WHERE REGEXP_CONTAINS(code, r'\d+') 
)

SELECT 
    DISTINCT
    ath.athletes_id,
    ath.name,
    ath.name_tv,
    ath.gender,
    ath.sport_role,
    ath.country_code,
    ath.country,
    ath.nationality,
    ath.height,
    ath.weight,
    ath.birth_date,
    ath.age,
    mdl.medal_type,
    mdl.discipline
FROM athletes as ath 
LEFT JOIN medals as mdl on ath.athletes_id = mdl.medal_code 
WHERE row_num = 1 