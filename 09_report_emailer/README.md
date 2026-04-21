# Project 9: Automated Report Emailer

## Business Problem

Businesses run daily/weekly reports but someone always has to manually export, format, and email them. That person is a bottleneck. Reports go out late, inconsistently, or not at all.

## Solution

A Python script that pulls KPI data from a SQLite database, renders a styled HTML email with a summary block and full data table, optionally attaches Excel files, and sends it via SMTP (Gmail/Outlook/any relay). Safe to run via `--dry-run` for testing without sending.

## Tools Used

- Python 3.10+
- pandas
- smtplib + email.mime (stdlib)
- sqlite3 (stdlib)
- argparse + logging (stdlib)

## Skills Demonstrated

- HTML email templating with inline CSS (cross-client compatible)
- MIME multipart messages with binary attachments
- Environment variable config pattern (no credentials in code)
- SMTP with STARTTLS for secure delivery
- `--dry-run` flag for safe testing in CI/CD pipelines
- Cron / GitHub Actions scheduling pattern

## How to Run

```bash
pip install pandas

# Dry run (no actual email sent)
python send_report.py --dry-run

# Real send via Gmail App Password
export SMTP_USER=you@gmail.com
export SMTP_PASS=your_app_password
python send_report.py --to manager@company.com --attach dashboard.xlsx
```

A demo database with regional KPI data is auto-created if `report.db` is missing.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SMTP_HOST` | smtp.gmail.com | SMTP server |
| `SMTP_PORT` | 587 | SMTP port (TLS) |
| `SMTP_USER` | (empty) | Sender email |
| `SMTP_PASS` | (empty) | App password |
| `REPORT_DB` | report.db | Source database |

## Growth from Previous Projects

Project 4 generated KPI reports as files. Project 9 delivers those reports directly to stakeholders' inboxes — fully automated, no manual steps.
