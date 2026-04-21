# Project 6: Monitored Scheduled Pipeline

## Business Problem
Scripts that "work once" break in production. A real pipeline needs monitoring, run history, failure alerts, and the ability to run automatically on a schedule — not just when you remember to run it manually.

## Solution
The capstone upgrade of Project 3. The same ETL pipeline is rebuilt with production-minded features: run ID tracking, row-count monitoring, failure alerts, a pipeline run history table, and a GitHub Actions workflow for scheduled or push-triggered execution.

## Tools Used
- Python 3.10+
- requests
- pandas
- sqlite3
- logging
- GitHub Actions (CI/CD)

## Workflow
1. **Extract** — Pull API data with timeout and error handling
2. **Transform** — Clean and enrich data, tag with run ID
3. **Load** — Append to database, return row count
4. **Monitor** — Alert if row count drops below threshold
5. **Log Run Summary** — Write run ID, status, row count, and any errors to `pipeline_runs` table
6. **GitHub Actions** — Trigger on push or schedule via `.github/workflows/pipeline.yml`

## Key Features
- Every run gets a unique `run_id` timestamp
- Row-count threshold alert if data volume drops unexpectedly
- `pipeline_runs` table stores full run history
- Full try/catch with FAILED status logged on errors
- GitHub Actions workflow included for scheduling

## Folder Structure
```
06_monitored_pipeline/
   run_pipeline.py              # Main pipeline script
   pipeline.db                  # SQLite DB (generated on run)
   logs/                        # Per-run log files
   .github/workflows/
      pipeline.yml              # GitHub Actions CI config
   README.md
```

## What This Project Proves
- Production-style pipeline thinking
- Run tracking and monitoring
- Error handling and failure logging
- CI/CD with GitHub Actions
- Reliability over just "it runs"

## Growth from Projects 1-5
This project doesn't add a new data source — it adds maturity. The same ETL work from Project 3 is upgraded with the kind of observability, scheduling, and failure tracking that separates a student script from a real data workflow.
