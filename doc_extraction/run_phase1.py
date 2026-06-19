"""
Phase 1 runner — extract fields from all 5 sample job postings.
Run from doc_extraction/ with:
    python run_phase1.py
"""
import sys
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

sys.path.insert(0, str(Path(__file__).parent))

from extractors.llm_extractor import extract

SAMPLE_DIR = Path(__file__).parent / "sample_data"
OUTPUT_DIR = Path(__file__).parent / "output"

def main():
    postings = sorted(SAMPLE_DIR.glob("*.txt"))
    if not postings:
        print("No sample files found in sample_data/")
        sys.exit(1)

    OUTPUT_DIR.mkdir(exist_ok=True)
    print(f"Found {len(postings)} sample postings\n{'─'*60}")
    results = []

    for path in postings:
        text = path.read_text(encoding="utf-8")
        try:
            result = extract(text, source_file=str(path))
            results.append(result)

            # Save individual JSON
            out_path = OUTPUT_DIR / (path.stem + ".json")
            out_path.write_text(result.model_dump_json(indent=2))

            print(f"\n✓ {path.name}")
            print(f"  Title:       {result.job_title}")
            print(f"  Company:     {result.company}")
            print(f"  Location:    {result.location}  |  Remote: {result.is_remote}")
            print(f"  Salary:      {result.salary_currency} {result.salary_min}–{result.salary_max}")
            print(f"  Type:        {result.employment_type}  |  Level: {result.seniority_level}")
            print(f"  Skills:      {', '.join(result.required_skills[:5])}")
            print(f"  Confidence:  {result.confidence_score}  |  Completeness: {result.completeness_score()}")
            if result.needs_human_review():
                print(f"  ⚠ Flagged for human review: {result.low_confidence_fields}")
            print(f"  → Saved: {out_path}")
        except Exception as e:
            print(f"\n✗ {path.name}: {e}")

    # Save combined output
    if results:
        combined_path = OUTPUT_DIR / "all_postings.json"
        combined = [json.loads(r.model_dump_json()) for r in results]
        combined_path.write_text(json.dumps(combined, indent=2))
        print(f"\n{'─'*60}")
        print(f"Done: {len(results)}/{len(postings)} extracted successfully.")
        print(f"Output saved to: {OUTPUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
