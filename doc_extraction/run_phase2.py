"""
Phase 2 runner — Pydantic validation + quality checks on Phase 1 output.

Reads JSON files from output/, runs quality checks, and splits into:
  output/clean/    — records ready for warehouse load
  output/review/   — records flagged for human review

Run from doc_extraction/ with:
    python run_phase2.py
"""

import sys
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

from models.job_posting import JobPosting
from quality.quality_checker import run_checks, QualityResult

INPUT_DIR  = Path(__file__).parent / "Phase 1 Output"
CLEAN_DIR  = Path(__file__).parent / "Phase 2 Output" / "clean"
REVIEW_DIR = Path(__file__).parent / "Phase 2 Output" / "review"


def load_postings(input_dir: Path) -> list[tuple[str, JobPosting]]:
    """Load all individual job JSON files (skip subdirs and all_postings.json)."""
    postings = []
    for path in sorted(input_dir.glob("job_posting_*.json")):
        try:
            data = json.loads(path.read_text())
            posting = JobPosting(**data)
            postings.append((path.name, posting))
            logger.info(f"Loaded: {path.name}")
        except Exception as e:
            logger.error(f"Failed to load {path.name}: {e}")
    return postings


def save_result(result: QualityResult, filename: str, dest_dir: Path):
    out = json.loads(result.posting.model_dump_json())
    out["_quality"] = {
        "passed": result.passed,
        "completeness_score": result.completeness_score,
        "failure_reasons": result.failure_reasons,
    }
    (dest_dir / filename).write_text(json.dumps(out, indent=2))


def main():
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    REVIEW_DIR.mkdir(parents=True, exist_ok=True)

    postings = load_postings(INPUT_DIR)
    if not postings:
        print("No Phase 1 output found in output/. Run run_phase1.py first.")
        sys.exit(1)

    print(f"\nRunning quality checks on {len(postings)} postings\n{'─'*60}")

    clean, review = [], []

    for filename, posting in postings:
        result = run_checks(posting)

        if result.passed:
            clean.append((filename, result))
            save_result(result, filename, CLEAN_DIR)
            status = "✓ CLEAN"
        else:
            review.append((filename, result))
            save_result(result, filename, REVIEW_DIR)
            status = "⚠ REVIEW"

        print(f"\n{status}  {filename}")
        print(f"  {posting.job_title} @ {posting.company}")
        print(f"  Confidence: {posting.confidence_score}  |  Completeness: {result.completeness_score}")
        if result.failure_reasons:
            for reason in result.failure_reasons:
                print(f"  ✗ {reason}")

    # Summary
    print(f"\n{'─'*60}")
    print(f"Quality summary: {len(clean)} clean  |  {len(review)} flagged for review")
    print(f"\nClean records  → Phase 2 Output/clean/")
    print(f"Review records → Phase 2 Output/review/")

    # Save combined summary
    summary = {
        "total": len(postings),
        "clean": len(clean),
        "review": len(review),
        "clean_files": [f for f, _ in clean],
        "review_files": [f for f, _ in review],
    }
    summary_path = Path(__file__).parent / "Phase 2 Output" / "phase2_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"Summary        → Phase 2 Output/phase2_summary.json")


if __name__ == "__main__":
    main()
