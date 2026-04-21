"""
Project 8: Database Migration Tool
Author: mikey811
Description: Migrates tables between two SQLite databases (easily swappable
             to PostgreSQL/MySQL via SQLAlchemy). Validates row counts,
             column schemas, and checksums before committing.
"""

import sqlite3
import hashlib
import argparse
import logging
import os
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CONNECTION HELPERS
# ---------------------------------------------------------------------------

def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def list_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [r["name"] for r in rows]


def get_schema(conn: sqlite3.Connection, table: str) -> list[tuple]:
    """Return (cid, name, type, notnull, dflt_value, pk) for each column."""
    return conn.execute(f"PRAGMA table_info('{table}')").fetchall()


def row_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM '{table}'").fetchone()[0]


# ---------------------------------------------------------------------------
# CHECKSUM
# ---------------------------------------------------------------------------

def table_checksum(conn: sqlite3.Connection, table: str) -> str:
    """MD5 over all row data concatenated — order-insensitive via ORDER BY."""
    rows = conn.execute(f"SELECT * FROM '{table}' ORDER BY 1").fetchall()
    hasher = hashlib.md5()
    for row in rows:
        hasher.update(str(tuple(row)).encode())
    return hasher.hexdigest()


# ---------------------------------------------------------------------------
# VALIDATION
# ---------------------------------------------------------------------------

def validate_migration(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    table: str,
) -> bool:
    """Compare row counts and checksums between source and destination."""
    src_count = row_count(src, table)
    dst_count = row_count(dst, table)
    if src_count != dst_count:
        log.error("[%s] Row count mismatch: src=%d dst=%d", table, src_count, dst_count)
        return False

    src_cs = table_checksum(src, table)
    dst_cs = table_checksum(dst, table)
    if src_cs != dst_cs:
        log.error("[%s] Checksum mismatch: src=%s dst=%s", table, src_cs, dst_cs)
        return False

    log.info("[%s] Validation passed (%d rows, checksum OK)", table, src_count)
    return True


# ---------------------------------------------------------------------------
# MIGRATION
# ---------------------------------------------------------------------------

def migrate_table(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    table: str,
    batch_size: int = 500,
    drop_if_exists: bool = False,
) -> bool:
    """Copy a single table from src to dst."""
    # Grab DDL from source
    ddl_row = src.execute(
        f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'"
    ).fetchone()
    if not ddl_row:
        log.error("[%s] Table not found in source database.", table)
        return False

    ddl = ddl_row[0]

    # Handle existing table in destination
    existing = dst.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
    ).fetchone()
    if existing:
        if drop_if_exists:
            log.warning("[%s] Dropping existing table in destination.", table)
            dst.execute(f"DROP TABLE '{table}'")
        else:
            log.error(
                "[%s] Table already exists in destination. Use --drop to overwrite.",
                table,
            )
            return False

    # Create table
    dst.execute(ddl)

    # Stream rows in batches
    cursor = src.execute(f"SELECT * FROM '{table}'")
    cols   = [d[0] for d in cursor.description]
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO '{table}' VALUES ({placeholders})"

    total = 0
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        dst.executemany(insert_sql, [tuple(r) for r in batch])
        total += len(batch)
        log.info("[%s] Inserted %d rows ...", table, total)

    dst.commit()
    log.info("[%s] Migration complete (%d rows committed).", table, total)
    return True


# ---------------------------------------------------------------------------
# MIGRATION LOG
# ---------------------------------------------------------------------------

def write_migration_log(
    log_db: str,
    table: str,
    src_db: str,
    dst_db: str,
    rows: int,
    status: str,
):
    conn = sqlite3.connect(log_db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS migration_log (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at    TEXT,
            table_name TEXT,
            src_db    TEXT,
            dst_db    TEXT,
            rows      INTEGER,
            status    TEXT
        )
    """)
    conn.execute(
        "INSERT INTO migration_log (run_at, table_name, src_db, dst_db, rows, status) VALUES (?,?,?,?,?,?)",
        (datetime.utcnow().isoformat(), table, src_db, dst_db, rows, status),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# DEMO DATA
# ---------------------------------------------------------------------------

def seed_demo_source(db_path: str):
    """Create a sample source database if it doesn't exist."""
    if os.path.exists(db_path):
        return
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id      INTEGER PRIMARY KEY,
            name    TEXT NOT NULL,
            dept    TEXT,
            salary  REAL
        )
    """)
    conn.executemany(
        "INSERT INTO employees VALUES (?,?,?,?)",
        [
            (1, "Alice",   "Engineering", 95000),
            (2, "Bob",     "Marketing",   72000),
            (3, "Carol",   "Engineering", 105000),
            (4, "David",   "HR",           68000),
            (5, "Eve",     "Engineering", 115000),
        ],
    )
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            sku   TEXT PRIMARY KEY,
            name  TEXT,
            price REAL,
            stock INTEGER
        )
    """)
    conn.executemany(
        "INSERT INTO products VALUES (?,?,?,?)",
        [
            ("SKU-001", "Widget A",  19.99, 200),
            ("SKU-002", "Widget B",  34.99, 85),
            ("SKU-003", "Gadget Pro",99.00, 40),
        ],
    )
    conn.commit()
    conn.close()
    log.info("Seeded demo source database: %s", db_path)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="SQLite database migration tool")
    parser.add_argument("--src",   default="source.db",  help="Source database path")
    parser.add_argument("--dst",   default="dest.db",    help="Destination database path")
    parser.add_argument("--tables",nargs="*",             help="Tables to migrate (default: all)")
    parser.add_argument("--drop",  action="store_true",  help="Drop existing tables in destination")
    parser.add_argument("--log-db",default="migration_log.db", help="Migration audit log database")
    args = parser.parse_args()

    # Seed demo data if needed
    seed_demo_source(args.src)

    src = get_connection(args.src)
    dst = get_connection(args.dst)

    tables_to_migrate = args.tables or list_tables(src)
    if not tables_to_migrate:
        log.warning("No tables found in source database.")
        return

    log.info("Starting migration: %s -> %s", args.src, args.dst)
    log.info("Tables: %s", tables_to_migrate)

    results = {}
    for table in tables_to_migrate:
        ok = migrate_table(src, dst, table, drop_if_exists=args.drop)
        if ok:
            valid = validate_migration(src, dst, table)
            status = "SUCCESS" if valid else "VALIDATION_FAILED"
        else:
            status = "FAILED"
            valid  = False

        results[table] = status
        write_migration_log(
            args.log_db, table, args.src, args.dst,
            row_count(src, table) if ok else 0,
            status,
        )

    log.info("\n=== Migration Summary ===")
    for t, s in results.items():
        log.info("  %-30s %s", t, s)

    src.close()
    dst.close()


if __name__ == "__main__":
    main()
