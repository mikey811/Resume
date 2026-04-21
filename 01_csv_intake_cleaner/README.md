# Project 1: CSV Intake Cleaner

## Business Problem
Raw CSV exports from different sources are inconsistent — mismatched column names, duplicate rows, missing values, and wrong data types make them unusable for analysis without manual cleanup.

## Solution
Automated multi-file CSV cleaning pipeline that ingests raw files, standardizes column names, removes duplicates, fixes data types, and exports clean files plus a detailed error log.

## Tools Used
- Python 3.10+
- pandas
- logging
- pathlib
- os

## Workflow
1. Scan `input/` folder for all `.csv` files
2. Standardize column names (lowercase, strip spaces, replace spaces with underscores)
3. Remove duplicate rows
4. Fill or flag missing values
5. Fix data type mismatches
6. Export cleaned files to `output/`
7. Write error/change log to `logs/`

## Key Features
- Handles multiple files in one run
- Tracks rows dropped, columns renamed, nulls found
- Produces a clean audit log per file
- Config-driven — easy to adjust rules

## Folder Structure
```
01_csv_intake_cleaner/
   input/          # Raw CSV files go here
   output/         # Cleaned CSV files
   logs/           # Audit logs per run
   cleaner.py      # Main script
   config.py       # Column rules and settings
   README.md
```

## What This Project Proves
- File handling with pathlib and os
- Data manipulation with pandas
- Logging and error tracking
- Repeatable, automated data preparation

## Growth from Previous Projects
This is Project 1 — it establishes the foundation. Every project after this builds on these core skills.
