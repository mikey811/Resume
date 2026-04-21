# Project 10: Data Pipeline Orchestrator (CAPSTONE)

## Business Problem

Enterprise data teams manage dozens of standalone scripts — CSV cleaners, API fetchers, KPI reporters, email senders. Each runs in isolation. Dependencies break. Failures go unnoticed. Debugging is painful. There's no single "run everything" button.

## Solution

A lightweight workflow orchestrator that chains all 9 previous projects into a single, self-documenting pipeline. Tasks are defined as functions with explicit dependencies. The orchestrator automatically resolves execution order, retries failed tasks with exponential backoff, logs every step, and provides a summary report showing which tasks succeeded and which failed.

**This capstone project demonstrates the pinnacle of data engineering skills: building infrastructure that ties everything together.**

## Architecture

```
[csv_intake] → [quality_audit] → [excel_dashboard] → ┐
                                                    [email_report]
[api_to_database] → [kpi_reporter] → [monitored_pipeline] → ┘
                  → [db_migration]
[folder_tracker] (no dependencies)
```

## Tools Used

- Python 3.10+ (standard library only)
- Dependency resolution algorithm
- Retry logic with exponential backoff
- Structured logging
- Context sharing between tasks

## Skills Demonstrated

- **Dependency management**: Automatic topological sort of task DAG
- **Error handling**: Retry with backoff, graceful degradation
- **Observability**: Structured logging, execution summary
- **Abstraction**: Task class with pluggable functions
- **Production patterns**: Dry-run mode, context passing, failure isolation
- **System design**: Building orchestration from first principles

## How to Run

```bash
python orchestrator.py
# or dry run:
python orchestrator.py --dry-run
```

## Sample Output

```
========== PIPELINE ORCHESTRATOR START ==========
Total tasks: 9
[TASK 1/9] CSV Intake & Cleaner
[OK] csv_intake: CSV cleaned and validated (0.50s)
[TASK 2/9] Data Quality Audit
[OK] quality_audit: Quality score: 97.3% (0.30s)
[TASK 3/9] API to Database
[OK] api_to_database: API data inserted: 1240 rows (0.60s)
[TASK 4/9] KPI Reporter
[OK] kpi_reporter: KPI report generated (0.40s)
[TASK 5/9] Folder Tracker
[OK] folder_tracker: File inventory logged (42 files tracked) (0.20s)
[TASK 6/9] Monitored Scheduled Pipeline
[OK] monitored_pipeline: Scheduled run logged with monitoring (0.50s)
[TASK 7/9] Excel Dashboard Generator
[OK] excel_dashboard: Dashboard created: 3 sheets, 1 chart (0.70s)
[TASK 8/9] Database Migration
[OK] db_migration: Tables migrated (2 tables, 8452 rows) (0.50s)
[TASK 9/9] Automated Report Emailer
[OK] email_report: Report emailed to 4 recipients (0.40s)

========== PIPELINE SUMMARY ==========
Total time:    4.10 seconds
Tasks succeeded: 9
Tasks failed:    0
=======================================
```

## Why This Is the Capstone

**Projects 1-9 are individual tools.** Each solves a specific problem: cleaning CSV files, generating reports, sending emails, migrating databases.

**Project 10 is the glue.** It demonstrates understanding beyond individual scripts — the ability to design systems that orchestrate multiple components, handle failures gracefully, and provide operational visibility.

This is the difference between a junior analyst with Python scripts and a data engineer who builds production pipelines.

## Growth Arc Across All 10 Projects

1. **Projects 1-3**: Data ingestion fundamentals (CSV, API, database)
2. **Projects 4-5**: Reporting and monitoring
3. **Project 6**: Scheduled automation with failure alerting
4. **Project 7**: Presentation layer (Excel dashboards)
5. **Project 8**: Cross-environment operations (migrations)
6. **Project 9**: Delivery automation (email reports)
7. **Project 10**: System orchestration and production reliability

Together, these 10 projects form a complete modern data engineering portfolio.
