"""
Human review form for flagged job postings.

Shows each record in the review queue with uncertain fields highlighted.
Reviewer fills in the missing values and approves or discards the record.
Approved records are saved to Phase 2 Output/clean/.

Run with:
    streamlit run review_form.py
"""

import json
import sys
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))
from models.job_posting import JobPosting
from quality.quality_checker import run_checks

REVIEW_DIR = Path(__file__).parent / "Phase 2 Output" / "review"
CLEAN_DIR  = Path(__file__).parent / "Phase 2 Output" / "clean"
SAMPLE_DIR = Path(__file__).parent / "sample_data"

st.set_page_config(page_title="Review Queue", layout="wide", page_icon="🔎")
st.title("🔎 Human Review Queue")
st.caption("Fields highlighted in orange were flagged as uncertain by the LLM. Fill them in and approve or discard.")

CLEAN_DIR.mkdir(parents=True, exist_ok=True)


def load_review_files():
    return sorted(REVIEW_DIR.glob("job_posting_*.json"))


def get_original_text(source_file: str) -> str:
    path = Path(source_file)
    if path.exists():
        return path.read_text(encoding="utf-8")
    fallback = SAMPLE_DIR / path.name.replace(".json", ".txt")
    if fallback.exists():
        return fallback.read_text(encoding="utf-8")
    return ""


files = load_review_files()

if not files:
    st.success("✅ Review queue is empty — nothing to review!")
    st.stop()

st.info(f"**{len(files)} record(s)** waiting for review")

for file_path in files:
    data = json.loads(file_path.read_text())
    quality = data.pop("_quality", {})
    posting = JobPosting(**data)
    uncertain = set(posting.low_confidence_fields)

    with st.expander(f"📄 {posting.job_title} @ {posting.company} — `{file_path.name}`", expanded=True):

        # ── Original posting text ──────────────────────────────────────────────
        original_text = get_original_text(posting.source_file or "")
        if original_text:
            with st.popover("View original job posting"):
                st.text(original_text[:3000])

        st.markdown(f"**Confidence:** `{posting.confidence_score}` &nbsp;|&nbsp; **Completeness:** `{posting.completeness_score()}`")
        st.markdown(f"**Failure reasons:** {', '.join(quality.get('failure_reasons', []))}")
        st.divider()

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Extracted Fields")
            st.text_input("Job Title", value=posting.job_title, disabled=True, key=f"title_{file_path.name}")
            st.text_input("Company", value=posting.company, disabled=True, key=f"company_{file_path.name}")
            st.text_input("Location", value=posting.location or "", disabled="location" not in uncertain, key=f"loc_{file_path.name}",
                          help="⚠️ Uncertain" if "location" in uncertain else "")
            st.text_input("Salary", value=f"{posting.salary_currency} {posting.salary_min} – {posting.salary_max}", disabled=True, key=f"sal_{file_path.name}")
            st.text_input("Employment Type", value=posting.employment_type or "", disabled=True, key=f"etype_{file_path.name}")

        with col2:
            st.markdown("#### Fields Needing Review")

            is_remote_val = st.selectbox(
                "🟠 Is Remote" if "is_remote" in uncertain else "Is Remote",
                options=["Unknown", "Yes", "No"],
                index={"Unknown": 0, "Yes": 1, "No": 2}.get(
                    "Unknown" if posting.is_remote is None else ("Yes" if posting.is_remote else "No"), 0
                ),
                key=f"remote_{file_path.name}",
            )

            seniority_val = st.selectbox(
                "🟠 Seniority Level" if "seniority_level" in uncertain else "Seniority Level",
                options=["unknown", "junior", "mid", "senior", "staff", "principal"],
                index=["unknown", "junior", "mid", "senior", "staff", "principal"].index(
                    posting.seniority_level if posting.seniority_level in ["junior", "mid", "senior", "staff", "principal"] else "unknown"
                ),
                key=f"seniority_{file_path.name}",
            )

            exp_min = st.number_input(
                "Years Experience (Min)",
                min_value=0, max_value=30,
                value=int(posting.years_experience_min or 0),
                disabled="years_experience_min" not in uncertain,
                key=f"expmin_{file_path.name}",
            )

            exp_max = st.number_input(
                "🟠 Years Experience (Max)" if "years_experience_max" in uncertain else "Years Experience (Max)",
                min_value=0, max_value=30,
                value=int(posting.years_experience_max or 0),
                key=f"expmax_{file_path.name}",
            )

        st.markdown("**Required Skills:** " + ", ".join(posting.required_skills[:8]))

        st.divider()
        approve_col, discard_col, _ = st.columns([1, 1, 4])

        with approve_col:
            if st.button("✅ Approve & Move to Clean", key=f"approve_{file_path.name}", type="primary"):
                updated = data.copy()
                updated["is_remote"] = {"Yes": True, "No": False, "Unknown": None}[is_remote_val]
                updated["seniority_level"] = seniority_val if seniority_val != "unknown" else None
                updated["years_experience_min"] = exp_min if exp_min > 0 else None
                updated["years_experience_max"] = exp_max if exp_max > 0 else None
                updated["low_confidence_fields"] = []
                updated["confidence_score"] = 0.95  # human reviewed

                try:
                    reviewed = JobPosting(**updated)
                    result = run_checks(reviewed)
                    out = json.loads(reviewed.model_dump_json())
                    out["_quality"] = {
                        "passed": True,
                        "completeness_score": result.completeness_score,
                        "failure_reasons": [],
                        "human_reviewed": True,
                    }
                    (CLEAN_DIR / file_path.name).write_text(json.dumps(out, indent=2))
                    file_path.unlink()
                    st.success(f"Moved to clean/ ✓")
                    st.rerun()
                except Exception as e:
                    st.error(f"Validation error: {e}")

        with discard_col:
            if st.button("🗑 Discard", key=f"discard_{file_path.name}"):
                file_path.unlink()
                st.warning("Record discarded.")
                st.rerun()
