Walmart Analysis Project

Project Overview
This project demonstrates an end-to-end modern data pipeline built using:
• Amazon S3 (Data Lake)
• Snowflake (Cloud Data Warehouse)
• dbt (Transformation & Modeling)
• Streamlit (Analytics & Visualization Layer)
The objective was to design a scalable data platform to analyze Walmart’s weekly sales performance using dimensional modeling and SCD1/SCD2 techniques.
End-to-End Structure

1. CSV files → Amazon S3 (Landing Layer)
2. Snowflake → Bronze (Raw)
3. dbt → Silver (Transform)
4. dbt → Gold (Dimensional Models with SCD1 & SCD2)
5. Streamlit → Dashboard & Reporting
   Architecture follows a Bronze → Silver → Gold layered approach.
   Explanation

1) Source (CSV files)
   Source data is received as structured CSV files containing store, date, and weekly sales information. These files represent batch data extracted from upstream systems.
   Import csv file from local to S3 bucket using Python code.

1. Create AWS IAM user Harikha_manthena and create access key.
2. Configure AWS CLI and provide access key, it will be configured.
3. Create AWS S3 bucket.
4. Write a python code that takes data from local and puts the file to S3 bucket (provide s3 path here and run the code in VS studio).
5. CSV files are loaded to S3 bucket.
6. Each file represents a different subject area and grain (store master, store+dept sales, store+date economics).

2) Amazon S3 (Landing) and Snowflake Setup
   CSV files are uploaded to Amazon S3, which acts as a scalable and durable raw data lake. S3 stores data in its original form for backup, auditability, and reprocessing if needed.

1. Using python code, I uploaded all CSV files to S3 under a folder like raw_data/.
2. S3 keeps raw files “as-is” for audit, durability, and reprocessing.
3. Create IAM policy (snowflake_access9) for accessing Snowflake where I configured the bucket and folder name.
4. Create IAM role.
5. Successfully setup done of S3 with Snowflake using External Stage.

3) Data Ingestion – Snowflake BRONZE Layer (Raw landing tables) — 3 tables
   Reason: 3 different files = 3 different schemas, so 3 landing tables.
   • MYDB.BRONZE.BRONZE_STORES_RAW (from stores.csv)
   o store_id, store_type, store_size
   • MYDB.BRONZE.BRONZE_DEPT_RAW (from department.csv)
   o store_id, dept_id, dt, weekly_sales, isholiday
   • MYDB.BRONZE.BRONZE_FACT_RAW (from fact.csv)
   o store_id, dt, temperature, fuel_price, markdown1..5, cpi, unemployment, isholiday
   What happens here
   • Only ingestion (COPY INTO from S3 stage). Snowflake loads S3 files into RAW tables using external stage + file format + COPY INTO.
   • No joins, no SCD here.
4) dbt TRANSFORM Layer (Cleaning + Standardization + Change Inputs)
   • 1 main table – MYDB.SILVER.WORK_FACT_TRANSFORM.
   This is where we prepare clean datasets that are ready for SCD logic in MART.
   • In this layer I:
   o Cleaned nulls
   o Standardized column names
   o Applied data type casting
   o Performed data quality checks
   o Removed duplicates
   This layer prepares data for business modeling.
   What happens here
   • Join the 3 raw tables into one clean dataset:
   o Base = BRONZE_DEPT_RAW (because it contains Dept + Weekly_Sales)
   o Join BRONZE_FACT_RAW on (store_id, dt) for economic measures
   o Join BRONZE_STORES_RAW on store_id for store_size/type
   • Create date_id (ex: YYYYMMDD)
   • Cast datatypes, handle nulls, standardize values
   • Output becomes the single “clean fact dataset” used for SCD2.
5) dbt GOLD (Mart Layer) — 3 Final Tables
   A) GOLD.WALMART_DATE_DIM (SCD1 – overwrite changes)
   • Key: date_id
   • Upsert latest holiday/date attributes (no history).
   B) GOLD.WALMART_STORE_DIM (SCD1 – overwrite changes)
   • Key: (store_id, dept_id)
   • Upsert latest store_type/store_size for each store+dept (no history).

C) GOLD.WALMART_FACT_TABLE (SCD2 – tracks history)
• Key: (store_id, dept_id, date_id) with versioning
If any measure changes for same key:
• Expire old row (vrsn_end_date)
• Insert new row (vrsn_start_date, vrsn_end_date = 9999-12-31)
SCD2 Logic
If any measure changes:

1. Expire old record (set vrsn_end_date = current_timestamp)
2. Insert new record
3. Set vrsn_end_date = '9999-12-31' for active rows

6) Streamlit Analytics Layer
   • Streamlit connects to Snowflake MART schema and uses the fact + dim tables for dashboards.
   • It calculates KPIs like:
   o Total Sales
   o Average Weekly Sales
   o Store Count
   o Department Count
   o Trend Over Time
   Based on this, business users analyze trends like weekly sales, holiday impact, store performance, and economic factor correlation.
   My dashboard connects to Snowflake using:
   • Secure credentials
   • Parameterized SQL
   • Cached connection
   • Cached queries
   Dashboard Features include:
   • Date Range filter
   • Store filter
   • Department filter
   • Holiday filter
   • KPI metrics
   • Time-series trend
   • Dynamic aggregations
7) Testing
   SCD2 – MYDB.GOLD.WALMART_FACT_VIEW
   I changed MARKDOWN1 and MARKDOWN2 values in csv file and reran the python program. It uploaded the csv file to S3 bucket, after which I reran DBT model and it inserted the new value as a new row and the old one is expired as shown below.
   SCD1 – MYDB.GOLD.WALMART_DATE_DIM
   I changed ISHOLIDAY value in csv file and reran the python program. It uploaded the csv file to S3 bucket, after which I reran DBT model and it replaced the value in DATE_DIM gold table as shown below.
   SCD1 – MYDB.GOLD.WALMART_STORE_DIM
   I changed STORE_SIZE value in csv file and reran the python program. It uploaded the csv file to S3 bucket, after which I reran DBT model and it replaced the value in STORE_DIM gold table as shown below.
