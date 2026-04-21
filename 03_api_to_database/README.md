# Project 3: API to Database Pipeline

## Business Problem
Manual data collection from external sources is slow, inconsistent, and not repeatable. There's no structured storage, no history, and no way to build reporting on top of it.

## Solution
An automated ETL pipeline that pulls hourly weather data from a public REST API, transforms it into raw and clean tables, and loads both into a local SQLite database with load timestamps for incremental tracking.

## Tools Used
- Python 3.10+
- requests
- pandas
- sqlite3
- logging
- pathlib

## Workflow
1. **Extract** — Pull hourly forecast data from Open-Meteo API (free, no key needed)
2. **Transform** — Build raw DataFrame, then cleaned version with renamed columns, nulls dropped, and temp converted to Fahrenheit
3. **Load** — Append both raw and clean tables to SQLite database with load timestamp
4. **Log** — Every step logged with timestamps to `logs/`

## Key Features
- Separate raw and clean tables (data lineage)
- Load timestamp on every record (incremental tracking)
- Error handling with retry-friendly structure
- No external database needed — runs locally with SQLite
- Modular extract / transform / load functions

## Folder Structure
```
03_api_to_database/
   pipeline.py     # Main ETL script
   weather.db      # SQLite database (generated on run)
   logs/           # Pipeline logs
   README.md
```

## What This Project Proves
- End-to-end ETL pipeline construction
- API ingestion with error handling
- Separate raw and clean data storage
- SQLite database management
- Production-style logging

## Growth from Project 2
Projects 1 and 2 worked with files. This project introduces a live data source, a real database, and a full ETL structure — the core pattern used in analyst and data engineering roles.
