{{ 
    config(materialized='view', 
    schema='GOLD', 
    alias='WALMART_FACT_VIEW') 
}}

WITH CTE AS(
SELECT
  store_id,
  dept_id,
  date_id,
  store_size,
  store_weekly_sales,
  fuel_price,
  temperature,
  unemployment,
  cpi,
  markdown1, markdown2, markdown3, markdown4, markdown5,
  insert_dts, update_dts,
  dbt_valid_from as vrsn_start_date,
  coalesce(dbt_valid_to, '9999-12-31 00:00:00'::timestamp_ntz) as vrsn_end_date
from {{ ref('walmart_fact_snapshot') }}
)

SELECT * FROM CTE