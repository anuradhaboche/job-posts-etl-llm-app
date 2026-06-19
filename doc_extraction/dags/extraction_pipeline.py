"""
dags/extraction_pipeline.py

Airflow DAG: Document Extraction Pipeline
Runs daily. Picks up new .txt/.pdf files from a watched folder,
extracts structured fields via LLM, validates, and loads to Snowflake.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ── DAG defaults ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["your-email@example.com"],
}

WATCH_DIR = Variable.get("DOC_EXTRACTION_WATCH_DIR", default_var="/opt/airflow/data/input")
PROCESSED_DIR = Variable.get("DOC_EXTRACTION_PROCESSED_DIR", default_var="/opt/airflow/data/processed")


# ── Task functions ─────────────────────────────────────────────────────────────
def scan_for_new_files(**context) -> list[str]:
    """Find all unprocessed .txt and .pdf files in the watch directory."""
    watch = Path(WATCH_DIR)
    files = list(watch.glob("*.txt")) + list(watch.glob("*.pdf"))
    file_paths = [str(f) for f in files]

    if not file_paths:
        print("No new files found.")
    else:
        print(f"Found {len(file_paths)} file(s): {file_paths}")

    # Push to XCom so downstream tasks can read it
    context["ti"].xcom_push(key="file_paths", value=file_paths)
    return file_paths


def extract_fields(**context) -> list[dict]:
    """Call LLM extractor on each file. Returns list of serialized postings."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/doc_extraction")

    from extractors.llm_extractor import extract, extract_from_pdf

    file_paths = context["ti"].xcom_pull(key="file_paths", task_ids="scan_for_new_files")
    if not file_paths:
        print("No files to process.")
        context["ti"].xcom_push(key="postings", value=[])
        return []

    postings = []
    failed = []

    for path in file_paths:
        try:
            if path.endswith(".pdf"):
                posting = extract_from_pdf(path)
            else:
                text = Path(path).read_text(encoding="utf-8")
                posting = extract(text, source_file=path)

            postings.append(posting.model_dump(mode="json"))
            print(f"✓ Extracted: {posting.job_title} @ {posting.company}")

        except Exception as e:
            print(f"✗ Failed on {path}: {e}")
            failed.append({"path": path, "error": str(e)})

    print(f"\nExtraction summary: {len(postings)} succeeded, {len(failed)} failed")
    context["ti"].xcom_push(key="postings", value=postings)
    context["ti"].xcom_push(key="failed_files", value=failed)
    return postings


def validate_and_split(**context) -> dict:
    """Run quality checks and split into clean vs. review queue."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/doc_extraction")

    from models.job_posting import JobPosting
    from quality.quality_checker import split_by_quality

    raw_postings = context["ti"].xcom_pull(key="postings", task_ids="extract_fields")
    if not raw_postings:
        context["ti"].xcom_push(key="clean", value=[])
        context["ti"].xcom_push(key="review", value=[])
        return {"clean": 0, "review": 0}

    postings = [JobPosting(**p) for p in raw_postings]
    clean, review = split_by_quality(postings)

    # Serialize for XCom
    context["ti"].xcom_push(
        key="clean",
        value=[{"posting": r.posting.model_dump(mode="json"), "completeness_score": r.completeness_score, "failure_reasons": r.failure_reasons} for r in clean]
    )
    context["ti"].xcom_push(
        key="review",
        value=[{"posting": r.posting.model_dump(mode="json"), "completeness_score": r.completeness_score, "failure_reasons": r.failure_reasons} for r in review]
    )

    return {"clean": len(clean), "review": len(review)}


def load_to_duckdb(**context):
    """Load validated records to DuckDB."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/doc_extraction")

    from models.job_posting import JobPosting
    from quality.quality_checker import QualityResult
    from loaders.duckdb_loader import load_batch

    def deserialize(items):
        results = []
        for item in items:
            posting = JobPosting(**item["posting"])
            results.append(QualityResult(
                posting=posting,
                passed=len(item["failure_reasons"]) == 0,
                failure_reasons=item["failure_reasons"],
                completeness_score=item["completeness_score"],
            ))
        return results

    clean_raw = context["ti"].xcom_pull(key="clean", task_ids="validate_and_split") or []
    review_raw = context["ti"].xcom_pull(key="review", task_ids="validate_and_split") or []

    clean = deserialize(clean_raw)
    review = deserialize(review_raw)

    summary = load_batch(clean, review)
    print(f"Load complete: {summary}")


def move_processed_files(**context):
    """Move successfully processed files to the processed directory."""
    import shutil

    file_paths = context["ti"].xcom_pull(key="file_paths", task_ids="scan_for_new_files") or []
    failed = {f["path"] for f in (context["ti"].xcom_pull(key="failed_files", task_ids="extract_fields") or [])}
    processed = Path(PROCESSED_DIR)
    processed.mkdir(parents=True, exist_ok=True)

    for path in file_paths:
        if path not in failed:
            dest = processed / Path(path).name
            shutil.move(path, dest)
            print(f"Moved: {path} → {dest}")


# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="doc_extraction_pipeline",
    default_args=default_args,
    description="Extract structured fields from documents using LLM",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ai", "extraction", "llm"],
) as dag:

    t1 = PythonOperator(task_id="scan_for_new_files", python_callable=scan_for_new_files)
    t2 = PythonOperator(task_id="extract_fields", python_callable=extract_fields)
    t3 = PythonOperator(task_id="validate_and_split", python_callable=validate_and_split)
    t4 = PythonOperator(task_id="load_to_duckdb", python_callable=load_to_duckdb)
    t5 = PythonOperator(task_id="move_processed_files", python_callable=move_processed_files)

    t1 >> t2 >> t3 >> t4 >> t5
