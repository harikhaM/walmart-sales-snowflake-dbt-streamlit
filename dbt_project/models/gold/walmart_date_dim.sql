{{ config(
    materialized='incremental',
    unique_key='date_id',
    database='MYDB',
    schema='GOLD'
) }}

WITH src AS (
    SELECT
        date_id,
        TO_DATE(TO_VARCHAR(date_id), 'YYYYMMDD') AS date,
        -- if ANY row says TRUE, treat whole date as holiday
        MAX(IFF(isholiday, 1, 0))::BOOLEAN AS isholiday
    FROM MYDB.SILVER.WORK_FACT_TRANSFORM
    WHERE date_id IS NOT NULL
    GROUP BY 1,2
)

{% if is_incremental() %}

SELECT
    s.date_id,
    s.date,
    s.isholiday,
    COALESCE(t.insert_date, CURRENT_TIMESTAMP()) AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
FROM src s
LEFT JOIN {{ this }} t
  ON t.date_id = s.date_id

{% else %}

SELECT
    date_id,
    date,
    isholiday,
    CURRENT_TIMESTAMP() AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
FROM src

{% endif %}