{% macro copy_into_snowflake_raw(table_nm) %}

delete from {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_nm }};

{% if table_nm == 'BRONZE_STORES_RAW' %}

COPY INTO {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_nm }}
FROM (
  SELECT
    $1::NUMBER  AS store_id,
    $2::STRING  AS store_type,
    $3::NUMBER  AS store_size
  FROM @{{ var('stage_name') }}/stores.csv
)
FILE_FORMAT = (FORMAT_NAME = '{{ var("file_format_json") }}')
PURGE={{ var('purge_status') }}
FORCE=TRUE
;

{% elif table_nm == 'BRONZE_DEPT_RAW' %}

COPY INTO {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_nm }}
FROM (
  SELECT
    $1::NUMBER AS store_id,
    $2::NUMBER AS dept_id,
    TRY_TO_DATE($3, 'MM/DD/YYYY') AS dt,
    $4::NUMBER(18,2) AS weekly_sales,
    IFF($5 = 'TRUE', TRUE, FALSE) AS isholiday
  FROM @{{ var('stage_name') }}/department.csv
)
FILE_FORMAT = (FORMAT_NAME = '{{ var("file_format_json") }}')
PURGE={{ var('purge_status') }}
FORCE=TRUE
;

{% elif table_nm == 'BRONZE_FACT_RAW' %}

COPY INTO {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_nm }}
FROM (
  SELECT
    $1::NUMBER AS store_id,
    TRY_TO_DATE($2, 'MM/DD/YYYY') AS dt,
    $3::NUMBER(10,2)  AS temperature,
    $4::NUMBER(10,3)  AS fuel_price,
    $5::NUMBER(10,2)  AS markdown1,
    $6::NUMBER(10,2)  AS markdown2,
    $7::NUMBER(10,2)  AS markdown3,
    $8::NUMBER(10,2)  AS markdown4,
    $9::NUMBER(10,2)  AS markdown5,
    $10::NUMBER(12,6) AS cpi,
    $11::NUMBER(10,3) AS unemployment,
    IFF($12 = 'TRUE', TRUE, FALSE) AS isholiday
  FROM @{{ var('stage_name') }}/fact.csv
)
FILE_FORMAT = (FORMAT_NAME = '{{ var("file_format_json") }}')
PURGE={{ var('purge_status') }}
FORCE=TRUE
;

{% endif %}

{% endmacro %}