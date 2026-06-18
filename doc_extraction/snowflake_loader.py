"""
loaders/snowflake_loader.py

Loads validated JobPosting records to Snowflake.
Clean records → JOB_POSTINGS_RAW
Review-flagged records → JOB_POSTINGS_REVIEW_QUEUE
"""

import os
import json
import logging
from datetime import datetime

import snowflake.connector
from dotenv import load_dotenv

from models.job_posting import JobPosting
from quality.quality_checker import QualityResult

load_dotenv()
logger = logging.getLogger(__name__)


def _get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def _posting_to_row(result: QualityResult) -> tuple:
    p = result.posting
    return (
        p.job_title,
        p.company,
        p.location,
        p.is_remote,
        p.salary_min,
        p.salary_max,
        p.salary_currency,
        json.dumps(p.required_skills),
        json.dumps(p.preferred_skills),
        p.years_experience_min,
        p.years_experience_max,
        p.employment_type,
        p.seniority_level,
        p.confidence_score,
        json.dumps(p.low_confidence_fields),
        p.source_file,
        p.extracted_at.isoformat() if isinstance(p.extracted_at, datetime) else p.extracted_at,
        p.model_used,
        p.tokens_used,
        result.completeness_score,
        json.dumps(result.failure_reasons),
    )


UPSERT_SQL = """
INSERT INTO {table} (
    JOB_TITLE, COMPANY, LOCATION, IS_REMOTE,
    SALARY_MIN, SALARY_MAX, SALARY_CURRENCY,
    REQUIRED_SKILLS, PREFERRED_SKILLS,
    YEARS_EXP_MIN, YEARS_EXP_MAX,
    EMPLOYMENT_TYPE, SENIORITY_LEVEL,
    CONFIDENCE_SCORE, LOW_CONFIDENCE_FIELDS,
    SOURCE_FILE, EXTRACTED_AT, MODEL_USED, TOKENS_USED,
    COMPLETENESS_SCORE, QUALITY_FAILURE_REASONS
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s,
    PARSE_JSON(%s), PARSE_JSON(%s),
    %s, %s,
    %s, %s,
    %s, PARSE_JSON(%s),
    %s, %s, %s, %s,
    %s, PARSE_JSON(%s)
)
"""


def load_batch(
    clean: list[QualityResult],
    review: list[QualityResult],
) -> dict:
    """
    Load clean records to JOB_POSTINGS_RAW and
    flagged records to JOB_POSTINGS_REVIEW_QUEUE.
    Returns a summary dict for logging/monitoring.
    """
    conn = _get_connection()
    cursor = conn.cursor()
    loaded_clean = loaded_review = 0

    try:
        if clean:
            rows = [_posting_to_row(r) for r in clean]
            cursor.executemany(UPSERT_SQL.format(table="JOB_POSTINGS_RAW"), rows)
            loaded_clean = len(rows)
            logger.info(f"Loaded {loaded_clean} clean records to JOB_POSTINGS_RAW")

        if review:
            rows = [_posting_to_row(r) for r in review]
            cursor.executemany(UPSERT_SQL.format(table="JOB_POSTINGS_REVIEW_QUEUE"), rows)
            loaded_review = len(rows)
            logger.info(f"Loaded {loaded_review} records to JOB_POSTINGS_REVIEW_QUEUE")

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Snowflake load failed: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

    return {
        "loaded_clean": loaded_clean,
        "loaded_review": loaded_review,
        "total": loaded_clean + loaded_review,
        "loaded_at": datetime.utcnow().isoformat(),
    }
