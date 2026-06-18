"""
extractors/llm_extractor.py

Sends document text to Claude, parses the structured JSON response,
and returns a validated JobPosting model.
"""

import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime

import anthropic
from dotenv import load_dotenv

from models.job_posting import JobPosting

load_dotenv()
logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
MODEL = os.getenv("LLM_MODEL", "claude-opus-4-6")
MAX_TOKENS = int(os.getenv("MAX_TOKENS_PER_CALL", 1000))
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

EXTRACTION_PROMPT = """\
You are a data extraction assistant. Extract structured fields from the job posting below.

Return ONLY a valid JSON object — no explanation, no markdown, no extra text.

Fields to extract:
- job_title (string)
- company (string)
- location (string or null)
- is_remote (boolean or null)
- salary_min (number or null — annual, in currency units, no symbols)
- salary_max (number or null)
- salary_currency (string, default "USD")
- required_skills (list of strings)
- preferred_skills (list of strings)
- years_experience_min (integer or null)
- years_experience_max (integer or null)
- employment_type (string: full-time | part-time | contract | contract-to-hire | null)
- seniority_level (string: junior | mid | senior | staff | principal | null)
- confidence_score (float 0.0–1.0: your overall confidence across all fields)
- low_confidence_fields (list of field names you were uncertain about)

Job posting:
---
{text}
---
"""


def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract raw text from a PDF file using PyMuPDF."""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(pdf_path)
        return "\n".join(page.get_text() for page in doc)
    except ImportError:
        raise ImportError("PyMuPDF not installed. Run: pip install PyMuPDF")


def call_llm(text: str, client: anthropic.Anthropic) -> tuple[dict, int]:
    """
    Call Claude with the extraction prompt.
    Returns (parsed_json_dict, tokens_used).
    Retries up to MAX_RETRIES times on transient failures.
    """
    prompt = EXTRACTION_PROMPT.format(text=text[:8000])  # guard against huge docs

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}]
            )
            raw_text = response.content[0].text.strip()
            tokens_used = response.usage.input_tokens + response.usage.output_tokens

            # Strip markdown fences if the model adds them despite instructions
            if raw_text.startswith("```"):
                raw_text = raw_text.split("```")[1]
                if raw_text.startswith("json"):
                    raw_text = raw_text[4:]

            parsed = json.loads(raw_text)
            return parsed, tokens_used

        except json.JSONDecodeError as e:
            logger.warning(f"Attempt {attempt}: JSON parse failed — {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY_SECONDS)

        except anthropic.APIStatusError as e:
            logger.warning(f"Attempt {attempt}: API error {e.status_code} — {e.message}")
            if attempt == MAX_RETRIES or e.status_code in (400, 401, 403):
                raise
            time.sleep(RETRY_DELAY_SECONDS * attempt)


def extract(
    text: str,
    source_file: str = None,
) -> JobPosting:
    """
    Main entry point. Takes raw document text, calls LLM,
    validates with Pydantic, returns a JobPosting.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    logger.info(f"Extracting fields from: {source_file or 'inline text'}")
    raw_fields, tokens = call_llm(text, client)

    # Inject pipeline metadata before Pydantic validation
    raw_fields["source_file"] = source_file
    raw_fields["extracted_at"] = datetime.utcnow().isoformat()
    raw_fields["model_used"] = MODEL
    raw_fields["tokens_used"] = tokens

    posting = JobPosting(**raw_fields)
    logger.info(
        f"Extracted: {posting.job_title} @ {posting.company} | "
        f"confidence={posting.confidence_score} | "
        f"completeness={posting.completeness_score()} | "
        f"tokens={tokens}"
    )
    return posting


def extract_from_pdf(pdf_path: str) -> JobPosting:
    """Convenience wrapper for PDF files."""
    text = extract_text_from_pdf(pdf_path)
    return extract(text, source_file=str(pdf_path))


# ── Quick smoke test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    sample = """
    Senior Data Engineer — Acme Corp (Austin, TX / Remote OK)

    We're looking for a Senior Data Engineer to join our platform team.
    You'll build and maintain data pipelines that power our ML infrastructure.

    Requirements:
    - 5+ years of data engineering experience
    - Strong Python and SQL skills
    - Experience with Spark, Airflow, and Kafka
    - Familiarity with Snowflake or BigQuery
    - Experience with dbt is a plus

    Nice to have: experience with RAG pipelines, LLM infrastructure, or feature stores.

    Salary: $150,000 – $185,000 per year
    Full-time | Senior level
    """

    result = extract(sample, source_file="sample_inline")
    print(result.model_dump_json(indent=2))
    print(f"\nNeeds human review: {result.needs_human_review()}")
    print(f"Completeness score: {result.completeness_score()}")
