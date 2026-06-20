"""
dags/extraction_pipeline.py

Airflow DAG: Document Extraction Pipeline
Triggered manually. Scrapes LinkedIn via Apify, deduplicates against DuckDB,
extracts structured fields via LLM, validates, and loads to DuckDB.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ── DAG defaults ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

WATCH_DIR = Variable.get("DOC_EXTRACTION_WATCH_DIR", default_var="/opt/airflow/data/input")
PROCESSED_DIR = Variable.get("DOC_EXTRACTION_PROCESSED_DIR", default_var="/opt/airflow/data/processed")
LINKEDIN_SEARCH_URL = Variable.get("LINKEDIN_SEARCH_URL", default_var="https://www.linkedin.com/jobs/search/?keywords=Data+Engineer&location=Seattle%2C+WA")
APIFY_MAX_RESULTS = int(Variable.get("APIFY_MAX_RESULTS", default_var="20"))


# ── Task functions ─────────────────────────────────────────────────────────────
def scrape_jobs(**context):
    """Call Apify LinkedIn Jobs Scraper, deduplicate against DuckDB, write new postings to data/input/."""
    import time
    import json
    import requests
    import duckdb

    apify_token = os.environ.get("APIFY_API_TOKEN")
    if not apify_token:
        raise ValueError("APIFY_API_TOKEN environment variable not set")

    db_path = os.environ.get("DUCKDB_PATH", "/opt/airflow/doc_extraction/pipeline.duckdb")
    input_dir = Path(WATCH_DIR)
    input_dir.mkdir(parents=True, exist_ok=True)

    # ── Run Apify actor ───────────────────────────────────────────────────────
    print(f"Starting Apify scrape: {LINKEDIN_SEARCH_URL} (max {APIFY_MAX_RESULTS} results)")
    run_resp = requests.post(
        "https://api.apify.com/v2/acts/curious_coder~linkedin-jobs-scraper/runs",
        params={"token": apify_token},
        json={
            "urls": [LINKEDIN_SEARCH_URL],
            "count": APIFY_MAX_RESULTS,
            "scrapeCompany": False,
        },
        timeout=30,
    )
    run_resp.raise_for_status()
    run_id = run_resp.json()["data"]["id"]
    print(f"Apify run started: {run_id}")

    # ── Poll until finished ───────────────────────────────────────────────────
    for _ in range(60):
        time.sleep(10)
        status_resp = requests.get(
            f"https://api.apify.com/v2/acts/curious_coder~linkedin-jobs-scraper/runs/{run_id}",
            params={"token": apify_token},
            timeout=15,
        )
        status = status_resp.json()["data"]["status"]
        print(f"Apify run status: {status}")
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify run {run_id} ended with status: {status}")
    else:
        raise RuntimeError(f"Apify run {run_id} did not finish in time")

    # ── Fetch results ─────────────────────────────────────────────────────────
    dataset_id = status_resp.json()["data"]["defaultDatasetId"]
    items_resp = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"token": apify_token, "format": "json"},
        timeout=30,
    )
    items_resp.raise_for_status()
    jobs = items_resp.json()
    print(f"Apify returned {len(jobs)} jobs")

    # ── Load known URLs from DuckDB ───────────────────────────────────────────
    known_urls = set()
    if Path(db_path).exists():
        conn = duckdb.connect(db_path, read_only=True)
        rows = conn.execute("SELECT source_url FROM job_postings WHERE source_url IS NOT NULL").fetchall()
        known_urls = {r[0] for r in rows}
        rows2 = conn.execute("SELECT source_url FROM job_postings_review WHERE source_url IS NOT NULL").fetchall()
        known_urls |= {r[0] for r in rows2}
        conn.close()
    print(f"Known URLs in DB: {len(known_urls)}")

    # ── Write only new postings to data/input/ ────────────────────────────────
    new_count = 0
    for job in jobs:
        job_url = job.get("link") or job.get("applyUrl") or ""
        if not job_url or job_url in known_urls:
            print(f"Skipping (already seen): {job.get('title', 'unknown')} @ {job.get('company', 'unknown')}")
            continue

        title = job.get("title", "unknown")
        company = job.get("companyName", "unknown")
        location = job.get("location", "")
        description = job.get("descriptionText", "")
        salary = job.get("salary", "")

        content = f"{title} — {company}"
        if location:
            content += f" ({location})"
        content += f"\nSource URL: {job_url}\n"
        if salary:
            content += f"Salary: {salary}\n"
        content += f"\n{description}"

        safe_name = "".join(c if c.isalnum() else "_" for c in f"{company}_{title}")[:60]
        out_file = input_dir / f"{safe_name}_{new_count}.txt"
        out_file.write_text(content, encoding="utf-8")
        print(f"Written: {out_file.name}")
        new_count += 1

    print(f"Scrape complete: {new_count} new postings written to {input_dir}")
    context["ti"].xcom_push(key="new_jobs_count", value=new_count)


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


def retry_uncertain(**context):
    """Auto-retry low-confidence fields via LLM before sending to review queue."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/doc_extraction")

    from models.job_posting import JobPosting
    from quality.quality_checker import run_checks
    from extractors.llm_extractor import retry_uncertain_fields

    review_raw = context["ti"].xcom_pull(key="review", task_ids="validate_and_split") or []
    clean_raw  = context["ti"].xcom_pull(key="clean",  task_ids="validate_and_split") or []

    if not review_raw:
        print("No review records to retry.")
        context["ti"].xcom_push(key="clean",  value=clean_raw)
        context["ti"].xcom_push(key="review", value=[])
        return

    print(f"Retrying {len(review_raw)} review records...")
    still_review = []

    for item in review_raw:
        posting = JobPosting(**item["posting"])
        print(f"  Retrying: {posting.job_title} @ {posting.company} | uncertain: {posting.low_confidence_fields}")

        # Find original text from source_file
        text = None
        if posting.source_file:
            src = Path(posting.source_file)
            if src.exists():
                text = src.read_text(encoding="utf-8")

        if not text:
            print(f"  ✗ Original text not found — sending to review queue")
            still_review.append(item)
            continue

        updated = retry_uncertain_fields(posting, text)
        result  = run_checks(updated)

        if result.passed:
            clean_raw.append({
                "posting": updated.model_dump(mode="json"),
                "completeness_score": result.completeness_score,
                "failure_reasons": [],
            })
            print(f"  ✓ Passed after retry — moved to clean")
        else:
            still_review.append({
                "posting": updated.model_dump(mode="json"),
                "completeness_score": result.completeness_score,
                "failure_reasons": result.failure_reasons,
            })
            print(f"  ⚠ Still failing after retry — sending to review queue: {result.failure_reasons}")

    print(f"Retry complete: {len(clean_raw)} clean, {len(still_review)} still in review")
    context["ti"].xcom_push(key="clean",  value=clean_raw)
    context["ti"].xcom_push(key="review", value=still_review)


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

    clean_raw  = context["ti"].xcom_pull(key="clean",  task_ids="retry_uncertain") or []
    review_raw = context["ti"].xcom_pull(key="review", task_ids="retry_uncertain") or []

    clean  = deserialize(clean_raw)
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
    description="Scrape LinkedIn via Apify, extract structured fields via LLM, load to DuckDB",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ai", "extraction", "llm"],
) as dag:

    t0 = PythonOperator(task_id="scrape_jobs",         python_callable=scrape_jobs)
    t1 = PythonOperator(task_id="scan_for_new_files",  python_callable=scan_for_new_files)
    t2 = PythonOperator(task_id="extract_fields",      python_callable=extract_fields)
    t3 = PythonOperator(task_id="validate_and_split",  python_callable=validate_and_split)
    t4 = PythonOperator(task_id="retry_uncertain",     python_callable=retry_uncertain)
    t5 = PythonOperator(task_id="load_to_duckdb",      python_callable=load_to_duckdb)
    t6 = PythonOperator(task_id="move_processed_files",python_callable=move_processed_files)

    t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6
