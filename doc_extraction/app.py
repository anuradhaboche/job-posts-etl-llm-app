"""
app.py — Pipeline observability dashboard (DuckDB backend)

Shows pipeline health: throughput, confidence, salary ranges, skills, review queue.

Run with:
    streamlit run app.py
"""

import sys
import os
from pathlib import Path
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

    conn = duckdb.connect(DB_PATH, read_only=True)

    postings = conn.execute("""
        SELECT job_title, company, location, is_remote, seniority_level,
               employment_type, salary_min, salary_max, salary_currency,
               confidence_score, completeness_score, tokens_used,
               required_skills, loaded_at
        FROM job_postings
        ORDER BY loaded_at DESC
    """).fetchall()

    cols = ["job_title","company","location","is_remote","seniority_level",
            "employment_type","salary_min","salary_max","salary_currency",
            "confidence_score","completeness_score","tokens_used",
            "required_skills","loaded_at"]

    review = conn.execute("""
        SELECT job_title, company, confidence_score, failure_reasons, loaded_at
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

    import datetime
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

    postings_data = to_dict(postings, cols)
    review_data = to_dict(review, ["job_title","company","confidence_score","failure_reasons","loaded_at"])
    daily_data = to_dict(daily, ["day","records","avg_confidence","total_tokens"])

    return postings_data, review_data, daily_data


# ── UI ─────────────────────────────────────────────────────────────────────────
st.title("🔍 Job Extraction Pipeline — Dashboard")

postings, review, daily = load_data()

if postings is None:
    st.error("Database not found. Run `python run_phase2.py` first to load data.")
    st.stop()

total_clean  = len(postings)
total_review = len(review)
total        = total_clean + total_review
success_rate = round(total_clean / total * 100, 1) if total > 0 else 0
avg_conf     = round(sum(p["confidence_score"] for p in postings) / total_clean, 2) if total_clean else 0
total_tokens = sum(p["tokens_used"] or 0 for p in postings)
est_cost     = round(total_tokens / 1_000_000 * 5, 4)

# ── KPI row ────────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Processed", total)
c2.metric("Clean Records",   total_clean)
c3.metric("In Review Queue", total_review)
c4.metric("Avg Confidence",  avg_conf)
c5.metric("Est. API Cost",   f"${est_cost}")

st.divider()

# ── Row 1: Daily throughput + Confidence distribution ──────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Daily Records Loaded")
    if daily:
        days   = [d["day"] for d in daily]
        counts = [d["records"] for d in daily]
        fig = px.bar(x=days, y=counts, labels={"x": "", "y": "Records"},
                     color_discrete_sequence=["#4C78A8"])
        st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Confidence Score Distribution")
    scores = [p["confidence_score"] for p in postings]
    fig = px.histogram(x=scores, nbins=10, labels={"x": "Confidence score", "y": "Count"},
                       color_discrete_sequence=["#72B7B2"])
    fig.add_vline(x=0.75, line_dash="dash", line_color="red", annotation_text="Review threshold")
    st.plotly_chart(fig, use_container_width=True)

# ── Row 2: Salary ranges + Seniority breakdown ─────────────────────────────────
col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader("Salary Ranges by Company")
    salary_data = [p for p in postings if p["salary_min"] and p["salary_max"]]
    if salary_data:
        companies  = [p["company"] for p in salary_data]
        sal_min    = [p["salary_min"] for p in salary_data]
        sal_max    = [p["salary_max"] for p in salary_data]
        fig = px.scatter(
            x=companies, y=sal_max,
            error_y=[mx - mn for mx, mn in zip(sal_max, sal_min)],
            error_y_minus=[0] * len(sal_min),
            labels={"x": "", "y": "Salary (USD)"},
            color_discrete_sequence=["#F58518"],
        )
        fig.update_layout(yaxis_tickformat="$,.0f")
        st.plotly_chart(fig, use_container_width=True)

with col_right2:
    st.subheader("Records by Seniority Level")
    seniority_counts = {}
    for p in postings:
        lvl = p["seniority_level"] or "unknown"
        seniority_counts[lvl] = seniority_counts.get(lvl, 0) + 1
    if seniority_counts:
        fig = px.pie(
            names=list(seniority_counts.keys()),
            values=list(seniority_counts.values()),
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Row 3: All postings table ──────────────────────────────────────────────────
st.subheader("All Extracted Postings")
table_rows = []
for p in postings:
    table_rows.append({
        "Job Title":       p["job_title"],
        "Company":         p["company"],
        "Location":        p["location"],
        "Remote":          "✓" if p["is_remote"] else "✗",
        "Level":           p["seniority_level"] or "—",
        "Salary Min":      f"${p['salary_min']:,.0f}" if p["salary_min"] else "—",
        "Salary Max":      f"${p['salary_max']:,.0f}" if p["salary_max"] else "—",
        "Confidence":      p["confidence_score"],
        "Completeness":    p["completeness_score"],
        "Tokens":          p["tokens_used"],
    })
st.dataframe(table_rows, use_container_width=True)

# ── Row 4: Review queue ────────────────────────────────────────────────────────
st.divider()
st.subheader(f"Review Queue ({total_review} records)")
if review:
    for r in review:
        with st.expander(f"⚠ {r['job_title']} @ {r['company']}"):
            st.write(f"**Confidence:** {r['confidence_score']}")
            st.write(f"**Failure reasons:** {r['failure_reasons']}")
else:
    st.success("Review queue is empty — all records passed quality checks.")
