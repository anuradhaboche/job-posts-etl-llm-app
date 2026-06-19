"""
quality/quality_checker.py

Post-extraction quality checks. Separates clean records from
records that need human review before they hit the main warehouse table.
"""

import logging
from dataclasses import dataclass
from typing import Optional
from models.job_posting import JobPosting

logger = logging.getLogger(__name__)

CONFIDENCE_THRESHOLD = float(__import__("os").getenv("CONFIDENCE_THRESHOLD", 0.75))

# Fields that are expected to be unknown — never flag them as uncertain
EXPECTED_UNKNOWN_FIELDS = {"years_experience_max"}


def infer_seniority(years_min: Optional[int]) -> Optional[str]:
    """Infer seniority level from minimum years of experience."""
    if years_min is None:
        return None
    if years_min <= 2:
        return "junior"
    if years_min <= 5:
        return "mid"
    if years_min <= 8:
        return "senior"
    return "staff"


def post_process(posting: JobPosting) -> JobPosting:
    """
    Apply business rules to fill in fields before quality checks.
    Returns a new JobPosting with inferred values applied.
    """
    data = posting.model_dump()

    # Rule 1: years_experience_max is rarely defined — remove from uncertain fields
    data["low_confidence_fields"] = [
        f for f in data["low_confidence_fields"]
        if f not in EXPECTED_UNKNOWN_FIELDS
    ]

    # Rule 2: if location is present and remote not explicitly stated, assume not remote
    if data["is_remote"] is None and data["location"]:
        data["is_remote"] = False
        data["low_confidence_fields"] = [
            f for f in data["low_confidence_fields"] if f != "is_remote"
        ]
        logger.info(f"Inferred is_remote=False from location: {data['location']}")

    # Rule 3: infer seniority from years_experience_min if missing
    if data["seniority_level"] is None and data["years_experience_min"] is not None:
        data["seniority_level"] = infer_seniority(data["years_experience_min"])
        data["low_confidence_fields"] = [
            f for f in data["low_confidence_fields"] if f != "seniority_level"
        ]
        logger.info(
            f"Inferred seniority_level={data['seniority_level']} "
            f"from years_experience_min={data['years_experience_min']}"
        )

    return JobPosting(**data)


@dataclass
class QualityResult:
    posting: JobPosting
    passed: bool
    failure_reasons: list[str]
    completeness_score: float


def run_checks(posting: JobPosting) -> QualityResult:
    """
    Apply business-rule post-processing, then run quality checks.
    Returns a QualityResult with passed=True if it's clean.
    """
    posting = post_process(posting)
    failures = []

    # 1. Required fields must exist
    if not posting.job_title or posting.job_title.strip() == "":
        failures.append("missing: job_title")
    if not posting.company or posting.company.strip() == "":
        failures.append("missing: company")

    # 2. Confidence below threshold
    if posting.confidence_score < CONFIDENCE_THRESHOLD:
        failures.append(
            f"low_confidence: {posting.confidence_score:.2f} < {CONFIDENCE_THRESHOLD}"
        )

    # 3. Too many uncertain fields
    if len(posting.low_confidence_fields) > 2:
        failures.append(
            f"uncertain_fields: {', '.join(posting.low_confidence_fields)}"
        )

    # 4. Salary sanity check
    if posting.salary_min and posting.salary_max:
        if posting.salary_min > posting.salary_max:
            failures.append(
                f"salary_range_invalid: min {posting.salary_min} > max {posting.salary_max}"
            )

    # 5. Experience sanity check
    if posting.years_experience_min and posting.years_experience_max:
        if posting.years_experience_min > posting.years_experience_max:
            failures.append("experience_range_invalid")

    passed = len(failures) == 0
    if not passed:
        logger.warning(
            f"Quality check failed for '{posting.job_title}' @ '{posting.company}': "
            f"{failures}"
        )

    return QualityResult(
        posting=posting,
        passed=passed,
        failure_reasons=failures,
        completeness_score=posting.completeness_score(),
    )


def split_by_quality(
    postings: list[JobPosting],
) -> tuple[list[QualityResult], list[QualityResult]]:
    """
    Split a batch of postings into (clean, needs_review).
    Use this before loading to Snowflake.
    """
    results = [run_checks(p) for p in postings]
    clean = [r for r in results if r.passed]
    review = [r for r in results if not r.passed]

    logger.info(
        f"Quality split: {len(clean)} clean, {len(review)} flagged for review "
        f"(out of {len(results)} total)"
    )
    return clean, review
