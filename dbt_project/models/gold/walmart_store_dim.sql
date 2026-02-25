{{ config(
    materialized='incremental',
    unique_key=['store_id','dept_id'],
    database='MYDB',
    schema='GOLD'
) }}

WITH src AS (
    SELECT
        store_id,
        dept_id,
        MAX(store_type) AS store_type,
        MAX(store_size) AS store_size
    FROM MYDB.SILVER.WORK_FACT_TRANSFORM
    WHERE store_id IS NOT NULL
      AND dept_id IS NOT NULL
    GROUP BY 1,2
)

{% if is_incremental() %}

SELECT
    s.store_id,
    s.dept_id,
    s.store_type,
    s.store_size,
    COALESCE(t.insert_date, CURRENT_TIMESTAMP()) AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
FROM src s
LEFT JOIN {{ this }} t
  ON t.store_id = s.store_id
 AND t.dept_id  = s.dept_id

{% else %}

SELECT
    store_id,
    dept_id,
    store_type,
    store_size,
    CURRENT_TIMESTAMP() AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
FROM src

{% endif %}