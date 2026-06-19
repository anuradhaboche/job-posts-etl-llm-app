-- sql/create_tables.sql
-- Run this once in your Snowflake worksheet to set up the schema.

CREATE DATABASE IF NOT EXISTS DOC_EXTRACTION;
USE DATABASE DOC_EXTRACTION;
CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;

-- ── Main table: clean, validated records ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS JOB_POSTINGS_RAW (
    ID                      NUMBER AUTOINCREMENT PRIMARY KEY,
    JOB_TITLE               VARCHAR(500),
    COMPANY                 VARCHAR(500),
    LOCATION                VARCHAR(500),
    IS_REMOTE               BOOLEAN,
    SALARY_MIN              FLOAT,
    SALARY_MAX              FLOAT,
    SALARY_CURRENCY         VARCHAR(10)  DEFAULT 'USD',
    REQUIRED_SKILLS         VARIANT,     -- JSON array
    PREFERRED_SKILLS        VARIANT,     -- JSON array
    YEARS_EXP_MIN           INT,
    YEARS_EXP_MAX           INT,
    EMPLOYMENT_TYPE         VARCHAR(50),
    SENIORITY_LEVEL         VARCHAR(50),
    CONFIDENCE_SCORE        FLOAT,
    LOW_CONFIDENCE_FIELDS   VARIANT,     -- JSON array
    SOURCE_FILE             VARCHAR(1000),
    EXTRACTED_AT            TIMESTAMP_NTZ,
    MODEL_USED              VARCHAR(100),
    TOKENS_USED             INT,
    COMPLETENESS_SCORE      FLOAT,
    QUALITY_FAILURE_REASONS VARIANT,     -- JSON array (empty for clean records)
    LOADED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Review queue: flagged records for human correction ────────────────────────
CREATE TABLE IF NOT EXISTS JOB_POSTINGS_REVIEW_QUEUE (
    ID                      NUMBER AUTOINCREMENT PRIMARY KEY,
    JOB_TITLE               VARCHAR(500),
    COMPANY                 VARCHAR(500),
    LOCATION                VARCHAR(500),
    IS_REMOTE               BOOLEAN,
    SALARY_MIN              FLOAT,
    SALARY_MAX              FLOAT,
    SALARY_CURRENCY         VARCHAR(10),
    REQUIRED_SKILLS         VARIANT,
    PREFERRED_SKILLS        VARIANT,
    YEARS_EXP_MIN           INT,
    YEARS_EXP_MAX           INT,
    EMPLOYMENT_TYPE         VARCHAR(50),
    SENIORITY_LEVEL         VARCHAR(50),
    CONFIDENCE_SCORE        FLOAT,
    LOW_CONFIDENCE_FIELDS   VARIANT,
    SOURCE_FILE             VARCHAR(1000),
    EXTRACTED_AT            TIMESTAMP_NTZ,
    MODEL_USED              VARCHAR(100),
    TOKENS_USED             INT,
    COMPLETENESS_SCORE      FLOAT,
    QUALITY_FAILURE_REASONS VARIANT,
    LOADED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    -- Human review fields
    REVIEW_STATUS           VARCHAR(20) DEFAULT 'pending',  -- pending | approved | rejected | corrected
    REVIEWED_BY             VARCHAR(200),
    REVIEWED_AT             TIMESTAMP_NTZ,
    REVIEW_NOTES            TEXT
);

-- ── Pipeline run log: one row per DAG run ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS PIPELINE_RUN_LOG (
    ID                  NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_DATE            DATE DEFAULT CURRENT_DATE(),
    FILES_SCANNED       INT,
    EXTRACTIONS_OK      INT,
    EXTRACTIONS_FAILED  INT,
    LOADED_CLEAN        INT,
    LOADED_REVIEW       INT,
    TOTAL_TOKENS_USED   INT,
    TOTAL_COST_USD      FLOAT,   -- compute after the run: tokens * rate
    RUN_DURATION_SECS   FLOAT,
    RUN_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
