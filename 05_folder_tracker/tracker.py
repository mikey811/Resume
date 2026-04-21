import os
import sqlite3
import hashlib
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
WATCH_DIR = Path("watch_folder")
DB_PATH   = Path("file_tracker.db")
LOG_DIR   = Path("logs")

WATCH_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / f"tracker_{datetime.now().strftime('%Y%m%d')}.log",
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)


# ── Database Setup ─────────────────────────────────────────────────────────────
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS file_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                filename    TEXT NOT NULL,
                extension   TEXT,
                size_bytes  INTEGER,
                created_ts  TEXT,
                modified_ts TEXT,
                file_hash   TEXT,
                scan_ts     TEXT NOT NULL
            )
        """)
    logging.info("Database initialized")


# ── File Metadata Extraction ──────────────────────────────────────────────────
def get_file_hash(path: Path) -> str:
    """MD5 hash of file contents for change detection."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_metadata(path: Path) -> dict:
    stat = path.stat()
    return {
        "filename":    path.name,
        "extension":   path.suffix.lower(),
        "size_bytes":  stat.st_size,
        "created_ts":  datetime.fromtimestamp(stat.st_ctime).isoformat(),
        "modified_ts": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "file_hash":   get_file_hash(path),
        "scan_ts":     datetime.now().isoformat(),
    }


# ── Scan & Load ────────────────────────────────────────────────────────────────
def scan_folder() -> list:
    files = [f for f in WATCH_DIR.rglob("*") if f.is_file()]
    records = []
    for f in files:
        try:
            records.append(extract_metadata(f))
        except Exception as e:
            logging.error(f"Failed to process {f.name}: {e}")
    logging.info(f"Scanned {len(records)} file(s)")
    return records


def load_to_db(records: list) -> None:
    df = pd.DataFrame(records)
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("file_log", conn, if_exists="append", index=False)
    logging.info(f"Loaded {len(df)} records to database")


def query_summary() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql("""
            SELECT extension,
                   COUNT(*)              AS file_count,
                   SUM(size_bytes)       AS total_bytes,
                   AVG(size_bytes)       AS avg_bytes
            FROM file_log
            WHERE scan_ts = (SELECT MAX(scan_ts) FROM file_log)
            GROUP BY extension
            ORDER BY file_count DESC
        """, conn)


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("Starting folder tracker...")
    init_db()
    records = scan_folder()

    if not records:
        print("No files found in watch_folder/. Add some files and re-run.")
        return

    load_to_db(records)
    summary = query_summary()

    print(f"\nScanned {len(records)} file(s). Summary by type:")
    print(summary.to_string(index=False))
    print(f"\nAll records stored in {DB_PATH}")


if __name__ == "__main__":
    main()
