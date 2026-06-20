"""
Phase 2 retry — re-extract uncertain fields for review-flagged records.

Reads job_postings_review from DuckDB, retries low-confidence fields via LLM,
re-runs quality checks, and moves passing records to job_postings.
Records that still fail remain in job_postings_review.

Run from doc_extraction/ with:
    python run_phase2_retry.py
"""

import sys
import json
import logging
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

from models.job_posting import JobPosting
from quality.quality_checker import run_checks, QualityResult
from extractors.llm_extractor import retry_uncertain_fields
from loaders.duckdb_loader import get_connection, load_batch

PROCESSED_DIR = Path(__file__).parent / "data" / "processed"
INPUT_DIR     = Path(__file__).parent.parent / "data" / "input"


def get_original_text(source_file: str) -> Optional[str]:
    """Find original posting text by source_file path."""
    path = Path(source_file)
    if path.exists():
        return path.read_text(encoding="utf-8")
    # Check data/processed/
    fallback = Path(__file__).parent.parent / "data" / "processed" / path.name
    if fallback.exists():
        return fallback.read_text(encoding="utf-8")
    return None


def main():
    conn = get_connection()

    rows = conn.execute("""
        SELECT job_title, company, location, is_remote, salary_min, salary_max,
               salary_currency, required_skills, preferred_skills,
               years_experience_min, years_experience_max, employment_type,
               seniority_level, confidence_score, low_confidence_fields,
               source_file, source_url, extracted_at, model_used, tokens_used,
               completeness_score, failure_reasons, id
        FROM job_postings_review
        WHERE review_status = 'pending'
    """).fetchall()
    conn.close()

    if not rows:
        print("No pending records in job_postings_review — nothing to retry.")
        sys.exit(0)

    print(f"Retrying {len(rows)} flagged records\n{'─'*60}")

    still_review, moved_to_clean = [], []

    for row in rows:
        record_id = row[22]
        posting = JobPosting(
            job_title=row[0], company=row[1], location=row[2], is_remote=row[3],
            salary_min=row[4], salary_max=row[5], salary_currency=row[6],
            required_skills=row[7] or [], preferred_skills=row[8] or [],
            years_experience_min=row[9], years_experience_max=row[10],
            employment_type=row[11], seniority_level=row[12],
            confidence_score=row[13], low_confidence_fields=row[14] or [],
            source_file=row[15], source_url=row[16],
            extracted_at=row[17], model_used=row[18], tokens_used=row[19],
        )
        failure_reasons = row[21] or []

        print(f"\n→ {posting.job_title} @ {posting.company}")
        print(f"  Failure reasons: {failure_reasons}")
        print(f"  Uncertain fields: {posting.low_confidence_fields}")

        text = get_original_text(posting.source_file or "")
        if not text:
            print(f"  ✗ Could not find original text — skipping")
            still_review.append(posting.job_title)
            continue

        updated = retry_uncertain_fields(posting, text)
        result = run_checks(updated)

        conn = get_connection()
        if result.passed:
            quality_result = QualityResult(
                posting=updated,
                passed=True,
                failure_reasons=[],
                completeness_score=result.completeness_score,
            )
            load_batch([quality_result], [])
            conn.execute("DELETE FROM job_postings_review WHERE id = ?", [record_id])
            conn.close()
            moved_to_clean.append(posting.job_title)
            print(f"  ✓ PASSED → moved to job_postings")
        else:
            conn.execute(
                "UPDATE job_postings_review SET review_status = 'retry_failed', failure_reasons = ? WHERE id = ?",
                [result.failure_reasons, record_id]
            )
            conn.close()
            still_review.append(posting.job_title)
            print(f"  ⚠ Still needs human review: {result.failure_reasons}")

    print(f"\n{'─'*60}")
    print(f"Retry summary:")
    print(f"  Moved to clean : {len(moved_to_clean)}")
    print(f"  Still in review: {len(still_review)}")


if __name__ == "__main__":
    main()
