# Project 7: Excel/CSV Dashboard Generator

## Business Problem

Analysts waste hours manually copying data into Excel and formatting reports. Stakeholders need consistent, branded deliverables — not raw CSV dumps.

## Solution

A Python script that ingests any number of cleaned CSV files from a `data/` folder and auto-generates a polished, multi-tab Excel workbook with a branded summary page, per-file data sheets with conditional formatting, frozen headers, and an auto-generated bar chart showing row counts across all sources.

## Tools Used

- Python 3.10+
- pandas
- openpyxl (workbook creation, styling, charts)

## Skills Demonstrated

- Excel automation without touching Excel
- Dynamic multi-sheet workbook generation
- Reusable styling functions (header fill, row banding, borders)
- Chart embedding with `BarChart` + `Reference`
- Clean separation of concerns: ingestion, styling, summary, charting

## How to Run

```bash
pip install pandas openpyxl
python generate_dashboard.py
```

Place any `.csv` files in a `data/` folder. If none exist, a sample sales CSV is created automatically.

## Output

`dashboard.xlsx` with tabs:
- **Summary** — file inventory with row counts, column counts, null value totals
- **Row Counts** — bar chart visualizing data volume per source
- **[One tab per CSV]** — styled, freeze-paned data sheet

## Growth from Previous Projects

Projects 1-6 wrote data *into* databases and files. Project 7 surfaces that data into presentation-ready Excel reports — closing the loop from raw ingestion to stakeholder-facing deliverable.
