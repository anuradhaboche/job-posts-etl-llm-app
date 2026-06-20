"""
loaders/duckdb_loader.py

Loads validated job postings into a local DuckDB database.
Clean records go to job_postings, flagged records to job_postings_review.
"""

import os
import logging
from pathlib import Path
from typing import List

import duckdb
from dotenv import load_dotenv

from models.job_posting import JobPosting
from quality.quality_checker import QualityResult

load_dotenv()
logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DUCKDB_PATH", str(Path(__file__).parent.parent / "pipeline.duckdb"))
SCHEMA_PATH = Path(__file__).parent.parent / "sql" / "create_tables_duckdb.sql"


def get_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(DB_PATH)
    conn.execute(SCHEMA_PATH.read_text())
    return conn


def _posting_to_row(result: QualityResult) -> dict:
    p = result.posting
    return {
        "job_title":             p.job_title,
        "company":               p.company,
        "location":              p.location,
        "is_remote":             p.is_remote,
        "salary_min":            p.salary_min,
        "salary_max":            p.salary_max,
        "salary_currency":       p.salary_currency,
        "required_skills":       p.required_skills,
        "preferred_skills":      p.preferred_skills,
        "years_experience_min":  p.years_experience_min,
        "years_experience_max":  p.years_experience_max,
        "employment_type":       p.employment_type,
        "seniority_level":       p.seniority_level,
        "confidence_score":      p.confidence_score,
        "low_confidence_fields": p.low_confidence_fields,
        "source_file":           p.source_file,
        "source_url":            p.source_url,
        "extracted_at":          p.extracted_at,
        "model_used":            p.model_used,
        "tokens_used":           p.tokens_used,
        "completeness_score":    result.completeness_score,
    }


def load_batch(
    clean: List[QualityResult],
    review: List[QualityResult],
) -> dict:
    conn = get_connection()

    clean_count = 0
    for result in clean:
        row = _posting_to_row(result)
        conn.execute("DELETE FROM job_postings WHERE source_file = $source_file", {"source_file": row["source_file"]})
        conn.execute("""
            INSERT INTO job_postings (
                job_title, company, location, is_remote,
                salary_min, salary_max, salary_currency,
                required_skills, preferred_skills,
                years_experience_min, years_experience_max,
                employment_type, seniority_level,
                confidence_score, low_confidence_fields,
                source_file, source_url, extracted_at, model_used, tokens_used,
                completeness_score
            ) VALUES (
                $job_title, $company, $location, $is_remote,
                $salary_min, $salary_max, $salary_currency,
                $required_skills, $preferred_skills,
                $years_experience_min, $years_experience_max,
                $employment_type, $seniority_level,
                $confidence_score, $low_confidence_fields,
                $source_file, $source_url, $extracted_at, $model_used, $tokens_used,
                $completeness_score
            )
        """, row)
        clean_count += 1
        logger.info(f"Loaded to job_postings: {result.posting.job_title} @ {result.posting.company}")

    review_count = 0
    for result in review:
        row = _posting_to_row(result)
        row["failure_reasons"] = result.failure_reasons
        conn.execute("DELETE FROM job_postings_review WHERE source_file = $source_file", {"source_file": row["source_file"]})
        conn.execute("""
            INSERT INTO job_postings_review (
                job_title, company, location, is_remote,
                salary_min, salary_max, salary_currency,
                required_skills, preferred_skills,
                years_experience_min, years_experience_max,
                employment_type, seniority_level,
                confidence_score, low_confidence_fields,
                source_file, source_url, extracted_at, model_used, tokens_used,
                completeness_score, failure_reasons
            ) VALUES (
                $job_title, $company, $location, $is_remote,
                $salary_min, $salary_max, $salary_currency,
                $required_skills, $preferred_skills,
                $years_experience_min, $years_experience_max,
                $employment_type, $seniority_level,
                $confidence_score, $low_confidence_fields,
                $source_file, $source_url, $extracted_at, $model_used, $tokens_used,
                $completeness_score, $failure_reasons
            )
        """, row)
        review_count += 1
        logger.info(f"Loaded to job_postings_review: {result.posting.job_title} @ {result.posting.company}")

    conn.close()

    summary = {"clean_loaded": clean_count, "review_loaded": review_count, "db": DB_PATH}
    logger.info(f"DuckDB load complete: {summary}")
    return summary
