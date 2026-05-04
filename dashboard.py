
import streamlit as st
import psycopg2
import pandas as pd
import time

PG_CONN = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "postgres",
    "user":     "postgres",
    "password": "UnigapPostgres@123"
}

st.set_page_config(page_title="Glamira Real-Time Dashboard", page_icon="💎", layout="wide")

def get_conn():
    return psycopg2.connect(**PG_CONN)

def run_query(sql):
    conn = get_conn()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()

st.title("💎 Glamira Real-Time Analytics")
st.caption(f"Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

total = run_query("SELECT COUNT(*) as total FROM fact_product_view")
st.metric("Total Events Today", f"{total['total'][0]:,}")

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("🏆 Top 10 Products by Views")
    df = run_query("""
        SELECT p.product_id, COUNT(*) as views
        FROM fact_product_view f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.product_id
        ORDER BY views DESC
        LIMIT 10
    """)
    st.bar_chart(df.set_index("product_id"))

with col2:
    st.subheader("🌍 Top 10 Countries by Views")
    df = run_query("""
        SELECT l.country_code, COUNT(*) as views
        FROM fact_product_view f
        JOIN dim_location l ON f.location_key = l.location_key
        WHERE l.country_code IS NOT NULL
        GROUP BY l.country_code
        ORDER BY views DESC
        LIMIT 10
    """)
    st.bar_chart(df.set_index("country_code"))

st.divider()

col3, col4 = st.columns(2)

with col3:
    st.subheader("🔗 Top 5 Referrer Domains")
    df = run_query("""
        SELECT r.referrer_domain, COUNT(*) as views
        FROM fact_product_view f
        JOIN dim_referrer r ON f.referrer_key = r.referrer_key
        GROUP BY r.referrer_domain
        ORDER BY views DESC
        LIMIT 5
    """)
    st.bar_chart(df.set_index("referrer_domain"))

with col4:
    st.subheader("🏪 Stores by Views")
    country = st.selectbox("Select country", ["DE", "FR", "UK", "ES", "MX", "IT"])
    df = run_query(f"""
        SELECT s.store_id, COUNT(*) as views
        FROM fact_product_view f
        JOIN dim_location l ON f.location_key = l.location_key
        JOIN dim_store s ON f.store_key = s.store_key
        WHERE l.country_code = '{country}'
        GROUP BY s.store_id
        ORDER BY views DESC
        LIMIT 10
    """)
    st.bar_chart(df.set_index("store_id"))

st.divider()

col5, col6 = st.columns(2)

with col5:
    st.subheader("⏰ Views by Hour for Product")
    product_id = st.text_input("Product ID", value="103324")
    df = run_query(f"""
        SELECT event_hour, COUNT(*) as views
        FROM fact_product_view
        WHERE product_key = (
            SELECT product_key FROM dim_product
            WHERE product_id = '{product_id}'
        )
        GROUP BY event_hour
        ORDER BY event_hour
    """)
    st.bar_chart(df.set_index("event_hour"))

with col6:
    st.subheader("📱 Views by Browser & OS")
    df = run_query("""
        SELECT d.browser, d.os, COUNT(*) as views
        FROM fact_product_view f
        JOIN dim_device d ON f.device_key = d.device_key
        GROUP BY d.browser, d.os
        ORDER BY views DESC
        LIMIT 10
    """)
    st.dataframe(df, use_container_width=True)

st.divider()
st.caption("Auto-refreshes every 30 seconds")
time.sleep(30)
st.rerun()
