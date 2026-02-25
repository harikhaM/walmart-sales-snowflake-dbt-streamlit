import os
import json
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st
import altair as alt
import snowflake.connector


# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="Walmart Sales Dashboard",
    page_icon="🛒",
    layout="wide",
)

st.title("🛒 Walmart Sales Dashboard")

st.caption(
    "Gold layer dashboard using SCD2 fact view + date/store dimensions. "
    "Only active SCD2 records are displayed."
)


# -----------------------------
# Helpers
# -----------------------------
def _get_secret_or_env(key: str, default: str = "") -> str:
    try:
        secrets = st.secrets
        if "snowflake" in secrets and key in secrets["snowflake"]:
            return str(secrets["snowflake"][key])
    except Exception:
        pass

    return os.getenv(key.upper(), default)


@st.cache_resource(show_spinner=False)
def get_sf_connection(
    account: str,
    user: str,
    password: str,
    warehouse: str,
    database: str,
    schema: str,
    role: str,
):
    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
        client_session_keep_alive=True,
    )


def run_query(_conn, sql: str, params: dict) -> pd.DataFrame:
    with _conn.cursor() as cur:
        cur.execute(sql, params)
        try:
            return cur.fetch_pandas_all()
        except Exception:
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)


@st.cache_data(ttl=300)
def get_distinct_filters(_conn):

    stores_sql = """
        SELECT DISTINCT STORE_ID
        FROM MYDB.GOLD.WALMART_STORE_DIM
        ORDER BY STORE_ID
    """

    depts_sql = """
        SELECT DISTINCT DEPT_ID
        FROM MYDB.GOLD.WALMART_STORE_DIM
        ORDER BY DEPT_ID
    """

    minmax_date_sql = """
        SELECT MIN(DATE) AS MIN_DATE, MAX(DATE) AS MAX_DATE
        FROM MYDB.GOLD.WALMART_DATE_DIM
    """

    stores = run_query(_conn, stores_sql, {}).get("STORE_ID", []).tolist()
    depts = run_query(_conn, depts_sql, {}).get("DEPT_ID", []).tolist()
    mm = run_query(_conn, minmax_date_sql, {})

    min_date = pd.to_datetime(mm.loc[0, "MIN_DATE"]).date()
    max_date = pd.to_datetime(mm.loc[0, "MAX_DATE"]).date()

    return {
        "stores": stores,
        "depts": depts,
        "min_date": min_date,
        "max_date": max_date,
    }


@st.cache_data(ttl=300)
def load_dashboard_data(
    _conn,
    date_start: date,
    date_end: date,
    store_ids: list,
    dept_ids: list,
    holiday_filter: str,
):

    store_clause = ""
    dept_clause = ""
    holiday_clause = ""

    params = {
        "date_start": date_start,
        "date_end": date_end,
    }

    if store_ids:
        params["store_ids_json"] = json.dumps(store_ids)
        store_clause = """
          AND f.store_id IN (
              SELECT value::NUMBER
              FROM TABLE(FLATTEN(input => PARSE_JSON(%(store_ids_json)s)))
          )
        """

    if dept_ids:
        params["dept_ids_json"] = json.dumps(dept_ids)
        dept_clause = """
          AND f.dept_id IN (
              SELECT value::NUMBER
              FROM TABLE(FLATTEN(input => PARSE_JSON(%(dept_ids_json)s)))
          )
        """

    if holiday_filter == "Holiday Only":
        holiday_clause = "AND d.isholiday = TRUE"
    elif holiday_filter == "Non-Holiday Only":
        holiday_clause = "AND d.isholiday = FALSE"

    sql = f"""
        SELECT
            d.date AS SALES_DATE,
            d.isholiday,
            s.store_type,

            f.store_id,
            f.dept_id,
            f.store_weekly_sales,
            f.fuel_price,
            f.temperature,
            f.unemployment,
            f.cpi,
            f.markdown1,
            f.markdown2,
            f.markdown3,
            f.markdown4,
            f.markdown5

        FROM MYDB.GOLD.WALMART_FACT_VIEW f
        JOIN MYDB.GOLD.WALMART_DATE_DIM d
          ON f.date_id = d.date_id
        LEFT JOIN MYDB.GOLD.WALMART_STORE_DIM s
          ON f.store_id = s.store_id
         AND f.dept_id  = s.dept_id

        WHERE
            CURRENT_TIMESTAMP() >= f.vrsn_start_date
            AND CURRENT_TIMESTAMP() < f.vrsn_end_date
            AND d.date BETWEEN %(date_start)s AND %(date_end)s
            {holiday_clause}
            {store_clause}
            {dept_clause}
    """

    df = run_query(_conn, sql, params)

    if not df.empty:
        df["SALES_DATE"] = pd.to_datetime(df["SALES_DATE"])

    return df


def format_money(x):
    return f"${x:,.0f}" if pd.notna(x) else "-"


def format_num(x, d=2):
    return f"{x:,.{d}f}" if pd.notna(x) else "-"


# -----------------------------
# Sidebar: Connection
# -----------------------------
st.sidebar.header("🔐 Snowflake Connection")

account = st.sidebar.text_input("Account", value=_get_secret_or_env("account"))
user = st.sidebar.text_input("Username", value=_get_secret_or_env("user"))
password = st.sidebar.text_input("Password", value=_get_secret_or_env("password"), type="password")

warehouse = st.sidebar.text_input("Warehouse", value=_get_secret_or_env("warehouse", "COMPUTE_WH"))
role = st.sidebar.text_input("Role", value=_get_secret_or_env("role", "ACCOUNTADMIN"))
database = st.sidebar.text_input("Database", value=_get_secret_or_env("database", "MYDB"))
schema = st.sidebar.text_input("Schema", value=_get_secret_or_env("schema", "GOLD"))

if st.sidebar.button("Connect", type="primary"):
    st.session_state.conn = get_sf_connection(
        account, user, password, warehouse, database, schema, role
    )
    st.sidebar.success("Connected ✅")

if "conn" not in st.session_state:
    st.stop()

conn = st.session_state.conn

# -----------------------------
# Filters
# -----------------------------
filters = get_distinct_filters(conn)

st.sidebar.header("🎛️ Filters")

date_range = st.sidebar.date_input(
    "Date Range",
    value=(filters["min_date"], filters["max_date"]),
    min_value=filters["min_date"],
    max_value=filters["max_date"],
)

date_start, date_end = date_range

store_ids = st.sidebar.multiselect("Store", filters["stores"])
dept_ids = st.sidebar.multiselect("Dept", filters["depts"])
holiday_filter = st.sidebar.selectbox("Holiday", ["All", "Holiday Only", "Non-Holiday Only"])

# -----------------------------
# Load Data
# -----------------------------
df = load_dashboard_data(conn, date_start, date_end, store_ids, dept_ids, holiday_filter)

if df.empty:
    st.warning("No data found.")
    st.stop()

# -----------------------------
# KPIs
# -----------------------------
total_sales = df["STORE_WEEKLY_SALES"].sum()
avg_sales = df["STORE_WEEKLY_SALES"].mean()
stores_cnt = df["STORE_ID"].nunique()
depts_cnt = df["DEPT_ID"].nunique()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Sales", format_money(total_sales))
k2.metric("Avg Weekly Sales", format_money(avg_sales))
k3.metric("Stores", stores_cnt)
k4.metric("Departments", depts_cnt)

st.divider()

# -----------------------------
# Chart
# -----------------------------
ts = df.groupby("SALES_DATE", as_index=False)["STORE_WEEKLY_SALES"].sum()

chart = (
    alt.Chart(ts)
    .mark_line()
    .encode(
        x="SALES_DATE:T",
        y="STORE_WEEKLY_SALES:Q",
        tooltip=["SALES_DATE", "STORE_WEEKLY_SALES"]
    )
)

st.altair_chart(chart, use_container_width=True)