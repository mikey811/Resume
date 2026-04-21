# Project 8: Database Migration Tool

## Business Problem

Data teams constantly move data between environments — dev to staging, staging to production, legacy to new schema. Manual migrations break, lose rows, and leave no audit trail.

## Solution

A CLI tool that migrates one or more tables between SQLite databases (architecture mirrors PostgreSQL/MySQL via SQLAlchemy swap). It streams rows in configurable batches, validates row counts and MD5 checksums post-migration, and writes a full audit log to a separate `migration_log.db`.

## Tools Used

- Python 3.10+
- sqlite3 (stdlib)
- hashlib (stdlib)
- argparse (stdlib)
- logging (stdlib)

## Skills Demonstrated

- Batched streaming inserts for memory efficiency
- Row-count + checksum validation before committing
- DDL-preserving table copy (schema + data)
- Structured audit logging to a separate database
- CLI design with argparse (--src, --dst, --tables, --drop)
- `PRAGMA journal_mode=WAL` for concurrent read safety

## How to Run

```bash
python migrate.py
# or with options:
python migrate.py --src old.db --dst new.db --tables employees --drop
```

A demo source database with `employees` and `products` tables is created automatically on first run.

## CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--src` | source.db | Source database |
| `--dst` | dest.db | Destination database |
| `--tables` | (all) | Space-separated list of tables to migrate |
| `--drop` | False | Drop and recreate tables in destination |
| `--log-db` | migration_log.db | Audit log database |

## Growth from Previous Projects

Projects 3 and 6 inserted data into a single database. Project 8 moves data *between* databases with full validation — a production DevOps skill used in every data engineering team.
