"""
models/job_posting.py

Pydantic schema for a structured job posting.
Validates the raw JSON returned by the LLM before it touches the warehouse.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class JobPosting(BaseModel):
    # --- Core fields ---
    job_title: str
    company: str
    location: Optional[str] = None
    is_remote: Optional[bool] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: Optional[str] = "USD"
    required_skills: list[str] = Field(default_factory=list)
    preferred_skills: list[str] = Field(default_factory=list)
    years_experience_min: Optional[int] = None
    years_experience_max: Optional[int] = None
    employment_type: Optional[str] = None   # full-time, contract, part-time
    seniority_level: Optional[str] = None  # junior, mid, senior, staff

    # --- Quality signals (LLM self-reports these) ---
    confidence_score: float = Field(
        ge=0.0, le=1.0,
        description="LLM's self-reported confidence across all fields (0-1)"
    )
    low_confidence_fields: list[str] = Field(
        default_factory=list,
        description="Field names the LLM was uncertain about"
    )

    # --- Pipeline metadata (added by our code, not the LLM) ---
    source_file: Optional[str] = None
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    model_used: Optional[str] = None
    tokens_used: Optional[int] = None

    @field_validator("employment_type")
    @classmethod
    def normalize_employment_type(cls, v):
        if v is None:
            return v
        mapping = {
            "full time": "full-time",
            "fulltime": "full-time",
            "part time": "part-time",
            "parttime": "part-time",
            "contract to hire": "contract-to-hire",
        }
        return mapping.get(v.lower(), v.lower())

    @field_validator("seniority_level")
    @classmethod
    def normalize_seniority(cls, v):
        if v is None:
            return v
        mapping = {
            "sr": "senior",
            "sr.": "senior",
            "jr": "junior",
            "jr.": "junior",
            "mid-level": "mid",
            "mid level": "mid",
        }
        return mapping.get(v.lower(), v.lower())

    def needs_human_review(self, threshold: float = 0.75) -> bool:
        """Returns True if this record should go to the human review queue."""
        return (
            self.confidence_score < threshold
            or len(self.low_confidence_fields) > 2
            or not self.job_title
            or not self.company
        )

    def completeness_score(self) -> float:
        """Fraction of key fields that were successfully extracted."""
        key_fields = [
            self.job_title, self.company, self.location,
            self.salary_min, self.required_skills,
            self.years_experience_min, self.employment_type, self.seniority_level
        ]
        filled = sum(1 for f in key_fields if f is not None and f != [] and f != "")
        return round(filled / len(key_fields), 2)
