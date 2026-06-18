"""
dashboard/app.py

Streamlit observability dashboard.
Shows pipeline health: throughput, success rate, cost, confidence distribution.

Run with: streamlit run dashboard/app.py
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Doc Extraction Pipeline",
    layout="wide",
    page_icon="🔍",
)


# ── Data loading ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)  # refresh every 5 minutes
def load_data():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )
    cursor = conn.cursor()

    clean_df = pd.read_sql("""
        SELECT DATE_TRUNC('day', LOADED_AT) AS DAY,
               COUNT(*) AS RECORDS,
               AVG(CONFIDENCE_SCORE) AS AVG_CONFIDENCE,
               AVG(COMPLETENESS_SCORE) AS AVG_COMPLETENESS,
               SUM(TOKENS_USED) AS TOTAL_TOKENS
        FROM JOB_POSTINGS_RAW
        GROUP BY 1 ORDER BY 1 DESC LIMIT 30
    """, conn)

    review_df = pd.read_sql("""
        SELECT DATE_TRUNC('day', LOADED_AT) AS DAY,
               COUNT(*) AS RECORDS,
               REVIEW_STATUS
        FROM JOB_POSTINGS_REVIEW_QUEUE
        GROUP BY 1, 2 ORDER BY 1 DESC
    """, conn)

    confidence_df = pd.read_sql("""
        SELECT CONFIDENCE_SCORE, SENIORITY_LEVEL, EMPLOYMENT_TYPE,
               COMPLETENESS_SCORE, TOKENS_USED
        FROM JOB_POSTINGS_RAW
        ORDER BY LOADED_AT DESC LIMIT 500
    """, conn)

    cursor.close()
    conn.close()
    return clean_df, review_df, confidence_df


# ── UI ─────────────────────────────────────────────────────────────────────────
st.title("🔍 Document Extraction Pipeline — Observability")

try:
    clean_df, review_df, confidence_df = load_data()

    total_clean = int(clean_df["RECORDS"].sum()) if not clean_df.empty else 0
    total_review = int(review_df["RECORDS"].sum()) if not review_df.empty else 0
    total = total_clean + total_review
    success_rate = round(total_clean / total * 100, 1) if total > 0 else 0
    avg_confidence = round(float(confidence_df["CONFIDENCE_SCORE"].mean()), 2) if not confidence_df.empty else 0
    total_tokens = int(clean_df["TOTAL_TOKENS"].sum()) if not clean_df.empty else 0
    # Approximate cost: Claude Sonnet input ~$3/M tokens, output ~$15/M
    approx_cost = round(total_tokens / 1_000_000 * 5, 4)

    # ── KPI row ────────────────────────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Processed", f"{total:,}")
    col2.metric("Clean Records", f"{total_clean:,}")
    col3.metric("Success Rate", f"{success_rate}%")
    col4.metric("Avg Confidence", f"{avg_confidence}")
    col5.metric("Est. Token Cost", f"${approx_cost}")

    st.divider()

    # ── Throughput chart ───────────────────────────────────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Daily Throughput")
        if not clean_df.empty:
            fig = px.bar(
                clean_df.sort_values("DAY"),
                x="DAY", y="RECORDS",
                color_discrete_sequence=["#4C78A8"],
                labels={"RECORDS": "Records loaded", "DAY": ""},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet — run the pipeline first.")

    with col_right:
        st.subheader("Confidence Score Distribution")
        if not confidence_df.empty:
            fig = px.histogram(
                confidence_df,
                x="CONFIDENCE_SCORE",
                nbins=20,
                color_discrete_sequence=["#72B7B2"],
                labels={"CONFIDENCE_SCORE": "Confidence score"},
            )
            fig.add_vline(x=0.75, line_dash="dash", line_color="red",
                          annotation_text="Review threshold")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet.")

    # ── Review queue ───────────────────────────────────────────────────────────
    st.subheader("Review Queue Status")
    if not review_df.empty:
        pivot = review_df.pivot_table(
            index="DAY", columns="REVIEW_STATUS", values="RECORDS", fill_value=0
        ).reset_index()
        st.dataframe(pivot, use_container_width=True)
    else:
        st.success("Review queue is empty — all records passed quality checks.")

    # ── Completeness by seniority ──────────────────────────────────────────────
    if not confidence_df.empty and "SENIORITY_LEVEL" in confidence_df.columns:
        st.subheader("Avg Completeness by Seniority Level")
        agg = (
            confidence_df.groupby("SENIORITY_LEVEL")["COMPLETENESS_SCORE"]
            .mean()
            .reset_index()
            .sort_values("COMPLETENESS_SCORE", ascending=False)
        )
        fig = px.bar(
            agg,
            x="SENIORITY_LEVEL", y="COMPLETENESS_SCORE",
            color_discrete_sequence=["#F58518"],
            labels={"COMPLETENESS_SCORE": "Avg completeness", "SENIORITY_LEVEL": ""},
            range_y=[0, 1],
        )
        st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("Make sure your `.env` file is configured and Snowflake tables exist.")
    st.code("Run sql/create_tables.sql in your Snowflake worksheet first.")
