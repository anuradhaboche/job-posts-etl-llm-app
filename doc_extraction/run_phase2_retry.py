"""
Phase 2 retry — re-extract uncertain fields for review-flagged records.

Reads Phase 2 Output/review/, retries low-confidence fields via LLM,
re-runs quality checks, and moves passing records to Phase 2 Output/clean/.
Records that still fail remain in Phase 2 Output/review/ and need human attention.

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
from quality.quality_checker import run_checks
from extractors.llm_extractor import retry_uncertain_fields

REVIEW_DIR  = Path(__file__).parent / "Phase 2 Output" / "review"
CLEAN_DIR   = Path(__file__).parent / "Phase 2 Output" / "clean"
SAMPLE_DIR  = Path(__file__).parent / "sample_data"


def get_original_text(source_file: str) -> Optional[str]:
    """Retrieve original posting text using source_file path stored in JSON."""
    path = Path(source_file)
    if path.exists():
        return path.read_text(encoding="utf-8")
    # Fallback: match by filename in sample_data/
    fallback = SAMPLE_DIR / path.name
    if fallback.exists():
        return fallback.read_text(encoding="utf-8")
    return None


def save_json(data: dict, path: Path):
    path.write_text(json.dumps(data, indent=2))


def main():
    review_files = sorted(REVIEW_DIR.glob("job_posting_*.json"))
    if not review_files:
        print("No files in Phase 2 Output/review/ — nothing to retry.")
        sys.exit(0)

    print(f"Retrying {len(review_files)} flagged records\n{'─'*60}")

    still_review, moved_to_clean = [], []

    for path in review_files:
        data = json.loads(path.read_text())
        quality_meta = data.pop("_quality", {})
        posting = JobPosting(**data)

        print(f"\n→ {path.name}  [{posting.job_title} @ {posting.company}]")
        print(f"  Uncertain fields: {posting.low_confidence_fields}")

        # Get original text for re-extraction
        text = get_original_text(posting.source_file or "")
        if not text:
            print(f"  ✗ Could not find original text — skipping retry")
            still_review.append(path.name)
            continue

        # Retry uncertain fields
        updated = retry_uncertain_fields(posting, text)

        # Re-run quality checks
        result = run_checks(updated)

        out = json.loads(updated.model_dump_json())
        out["_quality"] = {
            "passed": result.passed,
            "completeness_score": result.completeness_score,
            "failure_reasons": result.failure_reasons,
            "retried_fields": posting.low_confidence_fields,
        }

        if result.passed:
            save_json(out, CLEAN_DIR / path.name)
            path.unlink()  # remove from review
            moved_to_clean.append(path.name)
            print(f"  ✓ PASSED after retry → moved to clean/")
            print(f"  New confidence: {updated.confidence_score} | Uncertain: {updated.low_confidence_fields}")
        else:
            save_json(out, path)  # overwrite with updated data
            still_review.append(path.name)
            print(f"  ⚠ Still needs human review")
            print(f"  Remaining issues: {result.failure_reasons}")

    print(f"\n{'─'*60}")
    print(f"Retry summary:")
    print(f"  Moved to clean : {len(moved_to_clean)} — {moved_to_clean}")
    print(f"  Still in review: {len(still_review)} — {still_review}")
    if still_review:
        print(f"\nThese records need human review:")
        for f in still_review:
            rec = json.loads((REVIEW_DIR / f).read_text())
            print(f"  • {f}: {rec.get('_quality', {}).get('failure_reasons', [])}")


if __name__ == "__main__":
    main()
