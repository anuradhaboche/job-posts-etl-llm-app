# Document AI Extraction Pipeline

An end-to-end data engineering pipeline that uses an LLM to extract structured fields
from unstructured documents (job postings, invoices, contracts), validates the output,
loads it to Snowflake, and monitors quality over time.

---

## Project Structure

```
doc_extraction/
├── dags/                        # Airflow DAGs
│   └── extraction_pipeline.py   # Main pipeline DAG
├── extractors/                  # LLM extraction logic
│   └── llm_extractor.py
├── models/                      # Pydantic schemas (validation)
│   └── job_posting.py
├── loaders/                     # Snowflake loader
│   └── snowflake_loader.py
├── quality/                     # Data quality checks
│   └── quality_checker.py
├── sql/                         # Snowflake DDL + dbt models
│   ├── create_tables.sql
│   └── dbt_models/
├── dashboard/                   # Streamlit observability dashboard
│   └── app.py
├── tests/                       # Unit tests
│   └── test_extractor.py
├── sample_data/                 # Sample job postings to test with
├── requirements.txt
└── .env.example
```

---

## Setup

```bash
# 1. Clone and install dependencies
pip install -r requirements.txt

# 2. Copy and fill in your credentials
cp .env.example .env

# 3. Create Snowflake tables
# Run sql/create_tables.sql in your Snowflake worksheet

# 4. Run the extractor manually on sample data
python extractors/llm_extractor.py

# 5. Launch the dashboard
streamlit run dashboard/app.py
```

---

## Pipeline Flow

```
Raw text / PDF
      ↓
llm_extractor.py    ← calls Claude API, returns JSON
      ↓
job_posting.py      ← Pydantic validates schema, flags low confidence
      ↓
quality_checker.py  ← checks completeness, flags for human review
      ↓
snowflake_loader.py ← loads to raw + review tables
      ↓
dbt models          ← marts for dashboard consumption
      ↓
Streamlit dashboard ← observability: success rate, cost, confidence
```

---

## Phase Milestones

| Phase | Goal | Done? |
|-------|------|-------|
| 1 | Extract fields from 5 sample postings manually | ☐ |
| 2 | Wrap in Pydantic validation | ☐ |
| 3 | Add Airflow DAG | ☐ |
| 4 | Load to Snowflake | ☐ |
| 5 | Quality layer + review queue | ☐ |
| 6 | Streamlit dashboard | ☐ |
