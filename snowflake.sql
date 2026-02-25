USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE DATABASE MYDB;

create or replace schema MYDB.BRONZE;

USE SCHEMA MYDB.BRONZE;
 
CREATE STORAGE INTEGRATION snowflake_storage_s3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xxxxxxxxxxxx'
  STORAGE_ALLOWED_LOCATIONS = ('s3://scd2-data-bucket9/raw_data/');

  DESC INTEGRATION snowflake_storage_s3;

  CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = true;

CREATE STAGE s3_test_stage
  STORAGE_INTEGRATION = snowflake_storage_s3
  URL = 's3://scd2-data-bucket9/raw_data/'
  FILE_FORMAT = MY_CSV_FORMAT;

ls @s3_test_stage;

CREATE OR REPLACE FILE FORMAT MYDB.BRONZE.MY_CSV_FORMAT
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null', 'NA')
  DATE_FORMAT = 'AUTO';

-- 1. Create Database & Schema
CREATE DATABASE IF NOT EXISTS MYDB;
CREATE SCHEMA IF NOT EXISTS MYDB.RAW;
CREATE SCHEMA IF NOT EXISTS MYDB.SILVER;
CREATE SCHEMA IF NOT EXISTS MYDB.GOLD;

-- 2. Create bronze tables
CREATE OR REPLACE TABLE MYDB.BRONZE.BRONZE_FACT_RAW (
  store_id        NUMBER,
  dt              DATE,
  temperature     NUMBER(10,2),
  fuel_price      NUMBER(10,3),
  markdown1       NUMBER(10,2),
  markdown2       NUMBER(10,2),
  markdown3       NUMBER(10,2),
  markdown4       NUMBER(10,2),
  markdown5       NUMBER(10,2),
  cpi             NUMBER(12,6),
  unemployment    NUMBER(10,3),
  isholiday       BOOLEAN
);

CREATE OR REPLACE TABLE MYDB.BRONZE.BRONZE_DEPT_RAW (
  store_id       NUMBER,
  dept_id        NUMBER,
  dt             DATE,
  weekly_sales   NUMBER(18,2),
  isholiday      BOOLEAN
);

CREATE OR REPLACE TABLE MYDB.BRONZE.BRONZE_STORES_RAW (
  store_id      NUMBER,
  store_type    STRING,
  store_size    NUMBER
);

CREATE OR REPLACE TABLE BRONZE.WORK_FACT_COPY (
  store_id           NUMBER,
  dept_id            NUMBER,
  date_id            NUMBER,
  store_size         NUMBER,
  store_weekly_sales NUMBER(18,2),
  fuel_price         NUMBER(10,2),
  temperature        NUMBER(10,2),
  unemployment       NUMBER(10,2),
  cpi                NUMBER(10,2),
  markdown1          NUMBER(10,2),
  markdown2          NUMBER(10,2),
  markdown3          NUMBER(10,2),
  markdown4          NUMBER(10,2),
  markdown5          NUMBER(10,2)
);
