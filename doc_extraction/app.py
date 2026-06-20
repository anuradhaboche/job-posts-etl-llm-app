"""
app.py — Pipeline observability dashboard (DuckDB backend)

Run with:
    streamlit run app.py
"""

import sys
import os
import time
import datetime
from pathlib import Path
from collections import Counter

import duckdb
import streamlit as st
import plotly.express as px

sys.path.insert(0, str(Path(__file__).parent))

DB_PATH = os.getenv("DUCKDB_PATH", str(Path(__file__).parent / "pipeline.duckdb"))

st.set_page_config(
    page_title="Job Extraction Pipeline",
    layout="wide",
    page_icon="🔍",
)


# ── Data loading ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_data():
    if not Path(DB_PATH).exists():
        return None, None, None

    for attempt in range(5):
        try:
            conn = duckdb.connect(DB_PATH, read_only=True)
            break
        except Exception:
            if attempt == 4:
                return None, None, None
            time.sleep(2)

    postings = conn.execute("""
        SELECT job_title, company, location, is_remote, seniority_level,
               employment_type, salary_min, salary_max, salary_currency,
               confidence_score, completeness_score, tokens_used,
               required_skills, preferred_skills, source_url, loaded_at
        FROM job_postings
        ORDER BY loaded_at DESC
    """).fetchall()

    cols = ["job_title","company","location","is_remote","seniority_level",
            "employment_type","salary_min","salary_max","salary_currency",
            "confidence_score","completeness_score","tokens_used",
            "required_skills","preferred_skills","source_url","loaded_at"]

    review = conn.execute("""
        SELECT job_title, company, confidence_score, failure_reasons, loaded_at, source_url
        FROM job_postings_review
        ORDER BY loaded_at DESC
    """).fetchall()

    daily = conn.execute("""
        SELECT CAST(loaded_at AS DATE) AS day, COUNT(*) AS records,
               AVG(confidence_score) AS avg_confidence,
               SUM(tokens_used) AS total_tokens
        FROM job_postings
        GROUP BY 1 ORDER BY 1
    """).fetchall()

    conn.close()

    def to_dict(rows, columns):
        result = []
        for row in rows:
            d = {}
            for i, col in enumerate(columns):
                val = row[i]
                if isinstance(val, datetime.datetime):
                    val = val.isoformat()
                d[col] = val
            result.append(d)
        return result

    return (
        to_dict(postings, cols),
        to_dict(review, ["job_title","company","confidence_score","failure_reasons","loaded_at","source_url"]),
        to_dict(daily, ["day","records","avg_confidence","total_tokens"]),
    )


# ── UI ─────────────────────────────────────────────────────────────────────────
st.title("🔍 Job Extraction Pipeline — Dashboard")

col_refresh, col_status = st.columns([1, 5])
with col_refresh:
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()

postings, review, daily = load_data()

if postings is None:
    st.error("Database locked or not found. Disconnect DBeaver/VS Code DB client and refresh.")
    st.stop()

if len(postings) == 0:
    st.warning("No records yet. Trigger the Airflow DAG to load data.")
    st.stop()

total_clean  = len(postings)
total_review = len(review)
total        = total_clean + total_review
success_rate = round(total_clean / total * 100, 1) if total else 0
avg_conf     = round(sum(p["confidence_score"] for p in postings) / total_clean, 2) if total_clean else 0
total_tokens = sum(p["tokens_used"] or 0 for p in postings)
est_cost     = round(total_tokens / 1_000_000 * 3, 4)

# ── KPI row ────────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total Processed", total)
c2.metric("Clean Records",   total_clean)
c3.metric("In Review Queue", total_review)
c4.metric("Pass Rate",       f"{success_rate}%")
c5.metric("Avg Confidence",  avg_conf)
c6.metric("Est. API Cost",   f"${est_cost}")

st.divider()

# ── Row 1: Daily throughput + Confidence distribution ──────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Daily Records Loaded")
    if daily:
        fig = px.bar(
            x=[d["day"] for d in daily],
            y=[d["records"] for d in daily],
            labels={"x": "", "y": "Records"},
            color_discrete_sequence=["#4C78A8"],
        )
        fig.update_layout(margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Confidence Score Distribution")
    scores = [p["confidence_score"] for p in postings]
    fig = px.histogram(x=scores, nbins=10, labels={"x": "Confidence score", "y": "Count"},
                       color_discrete_sequence=["#72B7B2"])
    fig.add_vline(x=0.75, line_dash="dash", line_color="red", annotation_text="Review threshold")
    fig.update_layout(margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

# ── Row 2: Salary ranges + Seniority breakdown ─────────────────────────────────
col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader("Salary Ranges by Company")
    salary_data = [p for p in postings if p["salary_min"] and p["salary_max"]]
    if salary_data:
        fig = px.scatter(
            x=[p["company"] for p in salary_data],
            y=[p["salary_max"] for p in salary_data],
            error_y=[p["salary_max"] - p["salary_min"] for p in salary_data],
            error_y_minus=[0] * len(salary_data),
            labels={"x": "", "y": "Salary (USD)"},
            color_discrete_sequence=["#F58518"],
        )
        fig.update_layout(yaxis_tickformat="$,.0f", margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No salary data available yet.")

with col_right2:
    st.subheader("Records by Seniority Level")
    seniority_counts = Counter(p["seniority_level"] or "unknown" for p in postings)
    fig = px.pie(
        names=list(seniority_counts.keys()),
        values=list(seniority_counts.values()),
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_layout(margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

# ── Row 3: Top skills ──────────────────────────────────────────────────────────
st.subheader("Top Required Skills")
all_skills = []
for p in postings:
    if p["required_skills"]:
        all_skills.extend(p["required_skills"])
if all_skills:
    skill_counts = Counter(all_skills).most_common(15)
    fig = px.bar(
        x=[s[1] for s in skill_counts],
        y=[s[0] for s in skill_counts],
        orientation="h",
        labels={"x": "Count", "y": ""},
        color_discrete_sequence=["#54A24B"],
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Row 4: All postings table ──────────────────────────────────────────────────
st.subheader("All Extracted Postings")
table_rows = []
for p in postings:
    table_rows.append({
        "Job Title":    p["job_title"],
        "Company":      p["company"],
        "Location":     p["location"] or "—",
        "Remote":       "Yes" if p["is_remote"] else "No",
        "Level":        p["seniority_level"] or "—",
        "Salary Min":   f"${p['salary_min']:,.0f}" if p["salary_min"] else "—",
        "Salary Max":   f"${p['salary_max']:,.0f}" if p["salary_max"] else "—",
        "Confidence":   p["confidence_score"],
        "Completeness": p["completeness_score"],
        "LinkedIn URL": p["source_url"] or "—",
    })
st.dataframe(
    table_rows,
    use_container_width=True,
    column_config={
        "LinkedIn URL": st.column_config.LinkColumn("LinkedIn URL"),
        "Confidence":   st.column_config.ProgressColumn("Confidence", min_value=0, max_value=1),
        "Completeness": st.column_config.ProgressColumn("Completeness", min_value=0, max_value=1),
    },
)

# ── Row 5: Review queue ────────────────────────────────────────────────────────
st.divider()
st.subheader(f"Review Queue ({total_review} records)")

if not review:
    st.success("Review queue is empty — all records passed quality checks.")
else:
    st.info("Edit the uncertain fields below, then approve or discard each record.")

    for r in review:
        with st.expander(f"⚠ {r['job_title']} @ {r['company']}  |  confidence: {r['confidence_score']}"):
            st.markdown(f"**Failure reasons:** `{r['failure_reasons']}`")
            if r.get("source_url"):
                st.markdown(f"[View job posting on LinkedIn]({r['source_url']})")

            key = f"{r['job_title']}_{r['company']}".replace(" ", "_")

            col1, col2, col3 = st.columns(3)
            with col1:
                is_remote = st.selectbox("Is Remote", [None, True, False],
                    format_func=lambda x: "Unknown" if x is None else ("Yes" if x else "No"),
                    key=f"remote_{key}")
                seniority = st.selectbox("Seniority Level",
                    [None, "junior", "mid", "senior", "staff", "principal"],
                    format_func=lambda x: "Unknown" if x is None else x,
                    key=f"seniority_{key}")
            with col2:
                salary_min = st.number_input("Salary Min", min_value=0, value=0, step=1000, key=f"sal_min_{key}")
                salary_max = st.number_input("Salary Max", min_value=0, value=0, step=1000, key=f"sal_max_{key}")
            with col3:
                emp_type = st.selectbox("Employment Type",
                    [None, "full-time", "part-time", "contract", "contract-to-hire"],
                    format_func=lambda x: "Unknown" if x is None else x,
                    key=f"emp_{key}")
                yoe_min = st.number_input("Min Years Experience", min_value=0, value=0, step=1, key=f"yoe_{key}")

            col_approve, col_discard, _ = st.columns([1, 1, 4])

            with col_approve:
                if st.button("Approve", key=f"approve_{key}", type="primary"):
                    try:
                        conn = duckdb.connect(DB_PATH)
                        conn.execute("""
                            INSERT INTO job_postings (
                                job_title, company, location, is_remote,
                                salary_min, salary_max, salary_currency,
                                required_skills, preferred_skills,
                                employment_type, seniority_level,
                                years_experience_min,
                                confidence_score, low_confidence_fields,
                                source_file, source_url, extracted_at, model_used, tokens_used,
                                completeness_score
                            )
                            SELECT
                                job_title, company, location,
                                COALESCE(?, is_remote),
                                NULLIF(?, 0), NULLIF(?, 0), salary_currency,
                                required_skills, preferred_skills,
                                COALESCE(?, employment_type), COALESCE(?, seniority_level),
                                NULLIF(?, 0),
                                0.95, [], source_file, source_url, extracted_at, model_used, tokens_used,
                                completeness_score
                            FROM job_postings_review
                            WHERE job_title = ? AND company = ?
                        """, [is_remote, salary_min, salary_max, emp_type, seniority,
                              yoe_min, r['job_title'], r['company']])
                        conn.execute("DELETE FROM job_postings_review WHERE job_title = ? AND company = ?",
                                     [r['job_title'], r['company']])
                        conn.close()
                        st.success("Approved and moved to clean records.")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

            with col_discard:
                if st.button("Discard", key=f"discard_{key}"):
                    try:
                        conn = duckdb.connect(DB_PATH)
                        conn.execute("DELETE FROM job_postings_review WHERE job_title = ? AND company = ?",
                                     [r['job_title'], r['company']])
                        conn.close()
                        st.warning("Record discarded.")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
