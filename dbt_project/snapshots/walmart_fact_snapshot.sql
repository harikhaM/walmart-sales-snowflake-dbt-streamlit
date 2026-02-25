{% snapshot walmart_fact_snapshot %}
{{
  config(
    target_database='PC_DBT_DB',
    target_schema='SNAPSHOTS',
    unique_key="STORE_ID || '-' || DEPT_ID || '-' || DATE_ID",
    strategy='check',
    check_cols=[
      'STORE_SIZE','STORE_WEEKLY_SALES','FUEL_PRICE','TEMPERATURE','UNEMPLOYMENT','CPI',
      'MARKDOWN1','MARKDOWN2','MARKDOWN3','MARKDOWN4','MARKDOWN5'
    ]
  )
}}

select * from {{ source('xfm','WORK_FACT_TRANSFORM') }}

{% endsnapshot %}