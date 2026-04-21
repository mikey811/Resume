# Project 4: Automated Weekly KPI Reporter

## Business Problem
Managers and teams need regular performance summaries, but building them manually from raw data every week wastes time and introduces inconsistency.

## Solution
An automated Python script that reads sales data, calculates key business KPIs, and generates weekly reports with regional and product breakdowns exported as CSV and Markdown files.

## Tools Used
- Python 3.10+
- pandas
- pathlib
- logging

## Workflow
1. Load CSV files from `data/` folder (or auto-generate sample data for demo)
2. Calculate top-line KPIs: revenue, cost, profit, margin, units sold, top product, top region
3. Build regional and product breakdown tables
4. Export CSV summaries and a Markdown report to `output/` with date timestamp

## Key Features
- Handles real or sample data automatically
- Regional and product-level breakdowns
- Markdown report ready to share or paste
- Timestamped output files
- Zero manual steps after initial setup

## Folder Structure
```
04_kpi_reporter/
   data/           # Input CSV files (sales data)
   output/         # Generated reports (CSV + Markdown)
   reporter.py     # Main script
   README.md
```

## Sample Output
```
=== Weekly KPI Report ===
  Report Date: 2026-04-21
  Total Revenue: 24850.32
  Total Profit: 8921.14
  Avg Profit Margin: 0.3591
  Total Units Sold: 2841
  Top Product: Gadget X
  Top Region: Northeast
```

## What This Project Proves
- Business metrics calculation with pandas
- Grouped aggregations and summaries
- Multi-format report export
- Connecting raw data to business-readable output

## Growth from Project 3
Project 3 stored data in a database. This project turns that data into a business report — the step that bridges data work to actual business value.
