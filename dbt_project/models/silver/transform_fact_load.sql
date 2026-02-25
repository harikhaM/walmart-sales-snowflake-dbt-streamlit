{{ config(
    materialized='table',
    transient=true,
    database='MYDB',
    schema='SILVER',
    alias='WORK_FACT_TRANSFORM',
    pre_hook=[
      copy_into_snowflake_raw('BRONZE_STORES_RAW'),
      copy_into_snowflake_raw('BRONZE_DEPT_RAW'),
      copy_into_snowflake_raw('BRONZE_FACT_RAW')
    ]
) }}

with dept as (
    select
        store_id,
        dept_id,
        dt,
        weekly_sales,
        isholiday
    from {{ source('source', 'BRONZE_DEPT_RAW') }}
),

fact as (
    select
        store_id,
        dt,
        temperature,
        fuel_price,
        markdown1, markdown2, markdown3, markdown4, markdown5,
        cpi,
        unemployment,
        isholiday as fact_isholiday
    from {{ source('source', 'BRONZE_FACT_RAW') }}
),

stores as (
    select
        store_id,
        store_type,
        store_size
    from {{ source('source', 'BRONZE_STORES_RAW') }}
),

finalcte as (
    select
        d.store_id,
        d.dept_id,
        to_number(to_char(d.dt, 'YYYYMMDD')) as date_id,
        s.store_size,
        s.store_type,
        d.weekly_sales as store_weekly_sales,
        f.fuel_price,
        f.temperature,
        f.unemployment,
        f.cpi,
        f.markdown1, f.markdown2, f.markdown3, f.markdown4, f.markdown5,
        d.isholiday as isholiday,
        -- audit columns
        'EST' as time_zone,
        'WALMART' as source_sys_name,
        'STANDARD' as instnc_st_nm,
        current_session() as process_id,
        'TRANSFORM_FACT_LOAD' as process_name,
        current_timestamp() as insert_dts,
        current_timestamp() as update_dts
    from dept d
    left join fact f
      on d.store_id = f.store_id
     and d.dt       = f.dt
    left join stores s
      on d.store_id = s.store_id
)

select *
from finalcte