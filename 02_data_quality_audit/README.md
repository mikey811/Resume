# Project 2: Data Quality Audit Tool

## Business Problem
Data pipelines fail silently when datasets have nulls, invalid categories, schema mismatches, or out-of-range values. Without a systematic check, bad data flows downstream and breaks reporting.

## Solution
A reusable data quality auditing tool that scans any CSV dataset against a defined rule set and produces a structured JSON report flagging every issue found.

## Tools Used
- Python 3.10+
- pandas
- json
- logging
- pathlib

## Workflow
1. Load CSV from `data/` folder
2. Run five independent checks: null rate, duplicate rate, dtype validation, category validation, numeric range validation
3. Aggregate results into a structured report
4. Save JSON report to `reports/` with timestamp
5. Print console summary of all failed checks

## Key Features
- Config-driven rules (easy to update thresholds)
- Five check types covering most common data quality failures
- JSON output for easy downstream use
- Works on any CSV, no schema hardcoding

## Folder Structure
```
02_data_quality_audit/
   data/           # CSV files to audit
   reports/        # JSON audit reports (timestamped)
   audit.py        # Main script
   README.md
```

## Sample Output
```
=== Audit Report: sales_data.csv ===
  Rows: 5000  |  Columns: 12
  Passed: 3   |  Failed: 2
  Issues:
    [FAIL] null_rate on 'customer_email': 0.12
    [FAIL] numeric_range on 'price': 14 rows out of range
```

## What This Project Proves
- Rule-based validation logic
- Structured reporting and JSON output
- Systematic data trust measurement
- Config-driven architecture

## Growth from Project 1
Project 1 cleaned data reactively. This project defines quality rules proactively and measures whether data meets them — a step up from cleanup to governance.
