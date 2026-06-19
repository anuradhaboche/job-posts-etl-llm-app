# Job Postings ETL — LLM Extraction Pipeline

An end-to-end data engineering pipeline that scrapes real job postings from LinkedIn,
extracts structured fields using Claude (Anthropic), validates output with Pydantic,
applies a quality layer, loads to DuckDB, and surfaces results in a Streamlit dashboard.

---

## Stack

| Layer | Tool |
|---|---|
| Scraping | Apify — LinkedIn Jobs Scraper |
| LLM Extraction | Claude (claude-sonnet-4-6) via Anthropic API |
| Validation | Pydantic v2 |
| Quality checks | Custom rules + business logic |
| Storage | DuckDB |
| Orchestration | Apache Airflow (Docker) |
| Dashboard | Streamlit + Plotly |

---

## Project Structure

```
doc_extraction/
├── dags/                        # Airflow DAGs
│   └── extraction_pipeline.py
├── extractors/                  # LLM extraction logic
│   └── llm_extractor.py
├── models/                      # Pydantic schema
│   └── job_posting.py
├── loaders/                     # Database loaders
│   └── duckdb_loader.py
├── quality/                     # Quality checks + business rules
│   └── quality_checker.py
├── sql/                         # DDL
│   └── create_tables_duckdb.sql
├── sample_data/                 # Real LinkedIn job postings (input)
├── app.py                       # Streamlit observability dashboard
├── review_form.py               # Human review UI for flagged records
├── run_phase1.py                # Phase 1: extract → JSON
├── run_phase2.py                # Phase 2: validate → DuckDB
├── run_phase2_retry.py          # Auto-retry uncertain fields via LLM
├── test_extractor.py            # Unit tests (pytest)
├── requirements.txt
└── .env.example
```

---

## Setup

```bash
# 1. Clone and create virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configure credentials
cp .env.example .env
# Add your ANTHROPIC_API_KEY to .env

# 3. Add job postings to sample_data/
# Paste .txt files manually or scrape LinkedIn via Apify

# 4. Run Phase 1 — LLM extraction
python run_phase1.py
# Output → Phase 1 Output/*.json

# 5. Run Phase 2 — validation + DuckDB load
python run_phase2.py
# Output → Phase 2 Output/clean/ and Phase 2 Output/review/
# Database → pipeline.duckdb

# 6. Launch the dashboard
streamlit run app.py
```

---

## Pipeline Flow

```
LinkedIn (via Apify)
        ↓
sample_data/*.txt
        ↓
llm_extractor.py     ← calls Claude API, returns structured JSON
        ↓
job_posting.py       ← Pydantic validates schema, normalises fields
        ↓
quality_checker.py   ← business rules + completeness checks
        ↓          ↘
job_postings       job_postings_review   ← DuckDB tables
        ↓
Streamlit dashboard  ← KPIs, salary charts, confidence scores
```

---

## Quality Layer

Business rules applied automatically before quality checks:

| Rule | Logic |
|---|---|
| `years_experience_max` | Expected to be unknown — never flagged as uncertain |
| `is_remote` | If location is present and not stated, inferred `False` |
| `seniority_level` | Inferred from `years_experience_min`: 0–2 junior, 3–5 mid, 5–8 senior, 9+ staff |

A record is flagged for human review if:
- `confidence_score < 0.75`
- More than 2 fields in `low_confidence_fields`
- `job_title` or `company` is missing
- Salary or experience range is inverted

---

## Querying DuckDB

```python
import duckdb
conn = duckdb.connect("pipeline.duckdb")

# All clean postings
conn.execute("SELECT job_title, company, salary_min, salary_max FROM job_postings").fetchall()

# Avg salary by seniority
conn.execute("""
    SELECT seniority_level,
           ROUND(AVG(salary_min)) AS avg_min,
           ROUND(AVG(salary_max)) AS avg_max
    FROM job_postings
    GROUP BY 1 ORDER BY avg_max DESC
""").fetchall()
```

Or via DuckDB CLI:
```bash
brew install duckdb
duckdb pipeline.duckdb
```

---

## Running with Airflow (Docker)

```bash
# From the project root
docker-compose up -d

# Open Airflow UI
open http://localhost:8080
# Username: airflow  Password: airflow

# Trigger the DAG manually or wait for the daily schedule
```

---

## Phase Milestones

| Phase | Goal | Status |
|---|---|---|
| 1 | Scrape real LinkedIn postings + LLM extraction | ✅ Done |
| 2 | Pydantic validation + quality checks + DuckDB load | ✅ Done |
| 3 | Airflow DAG for automated scheduling | 🔄 In Progress |
| 4 | Streamlit observability dashboard | ✅ Done |
| 5 | Human review form for flagged records | ✅ Done |
| 6 | dbt models for analytics layer | ⬜ Planned |
