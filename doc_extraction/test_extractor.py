"""
tests/test_extractor.py

Unit tests for the extraction and validation layer.
Run with: pytest tests/
"""

import pytest
from models.job_posting import JobPosting
from quality.quality_checker import run_checks


# ── Fixtures ───────────────────────────────────────────────────────────────────
def make_posting(**overrides) -> JobPosting:
    """Helper: build a valid posting with optional field overrides."""
    base = {
        "job_title": "Senior Data Engineer",
        "company": "Acme Corp",
        "location": "Austin, TX",
        "is_remote": True,
        "salary_min": 140000,
        "salary_max": 180000,
        "required_skills": ["Python", "Spark", "SQL"],
        "years_experience_min": 5,
        "years_experience_max": 8,
        "employment_type": "full-time",
        "seniority_level": "senior",
        "confidence_score": 0.92,
        "low_confidence_fields": [],
    }
    base.update(overrides)
    return JobPosting(**base)


# ── Model tests ────────────────────────────────────────────────────────────────
class TestJobPostingModel:

    def test_valid_posting_creates_successfully(self):
        p = make_posting()
        assert p.job_title == "Senior Data Engineer"
        assert p.company == "Acme Corp"

    def test_employment_type_normalizes(self):
        p = make_posting(employment_type="Full Time")
        assert p.employment_type == "full-time"

    def test_seniority_normalizes_sr(self):
        p = make_posting(seniority_level="Sr.")
        assert p.seniority_level == "senior"

    def test_completeness_score_full(self):
        p = make_posting()
        assert p.completeness_score() == 1.0

    def test_completeness_score_partial(self):
        p = make_posting(salary_min=None, salary_max=None, location=None)
        assert p.completeness_score() < 1.0

    def test_needs_review_low_confidence(self):
        p = make_posting(confidence_score=0.5)
        assert p.needs_human_review() is True

    def test_does_not_need_review_high_confidence(self):
        p = make_posting(confidence_score=0.95, low_confidence_fields=[])
        assert p.needs_human_review() is False

    def test_confidence_score_out_of_range(self):
        with pytest.raises(Exception):
            make_posting(confidence_score=1.5)


# ── Quality checker tests ──────────────────────────────────────────────────────
class TestQualityChecker:

    def test_clean_record_passes(self):
        result = run_checks(make_posting())
        assert result.passed is True
        assert result.failure_reasons == []

    def test_missing_company_fails(self):
        result = run_checks(make_posting(company=""))
        assert result.passed is False
        assert any("company" in r for r in result.failure_reasons)

    def test_low_confidence_fails(self):
        result = run_checks(make_posting(confidence_score=0.6))
        assert result.passed is False
        assert any("low_confidence" in r for r in result.failure_reasons)

    def test_inverted_salary_range_fails(self):
        result = run_checks(make_posting(salary_min=200000, salary_max=100000))
        assert result.passed is False
        assert any("salary_range_invalid" in r for r in result.failure_reasons)

    def test_too_many_uncertain_fields_fails(self):
        result = run_checks(make_posting(
            low_confidence_fields=["salary_min", "location", "years_experience_min"]
        ))
        assert result.passed is False

    def test_completeness_score_surfaced(self):
        p = make_posting(salary_min=None, salary_max=None)
        result = run_checks(p)
        assert result.completeness_score < 1.0
