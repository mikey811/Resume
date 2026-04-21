"""
Project 6: Monitored Scheduled Pipeline

This is the capstone project. It takes the ETL pattern from Project 3
and upgrades it with modular structure, monitoring, row-count validation,
alert logging, and a GitHub Actions-ready design.
"""
import sqlite3
import requests
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
API_URL      = "https://api.open-meteo.com/v1/forecast"
PARAMS       = {
    "latitude": 40.7128,
    "longitude": -74.0060,
    "hourly": "temperature_2m,precipitation,windspeed_10m",
    "timezone": "America/New_York",
    "forecast_days": 1,
}
DB_PATH      = Path("pipeline.db")
LOG_DIR      = Path("logs")
MIN_ROWS     = 10    # Alert if fewer rows than this are loaded

LOG_DIR.mkdir(exist_ok=True)
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

logging.basicConfig(
    filename=LOG_DIR / f"run_{RUN_ID}.log",
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)


# ── Pipeline Steps ─────────────────────────────────────────────────────────────
def extract() -> dict:
    logging.info("[EXTRACT] Starting")
    resp = requests.get(API_URL, params=PARAMS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    logging.info("[EXTRACT] Success")
    return data


def transform(raw: dict) -> pd.DataFrame:
    logging.info("[TRANSFORM] Starting")
    hourly = raw["hourly"]
    df = pd.DataFrame(hourly)
    df.rename(columns={"time": "datetime"}, inplace=True)
    df["temperature_f"] = (df["temperature_2m"] * 9 / 5 + 32).round(1)
    df["run_id"]        = RUN_ID
    df["load_ts"]       = datetime.now().isoformat()
    df = df.dropna()
    logging.info(f"[TRANSFORM] {len(df)} rows produced")
    return df


def load(df: pd.DataFrame) -> int:
    logging.info(f"[LOAD] Writing {len(df)} rows to {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("weather_pipeline", conn, if_exists="append", index=False)
    logging.info("[LOAD] Complete")
    return len(df)


# ── Monitoring ──────────────────────────────────────────────────────────────────
def monitor(rows_loaded: int) -> None:
    if rows_loaded < MIN_ROWS:
        msg = f"[ALERT] Only {rows_loaded} rows loaded — below threshold of {MIN_ROWS}"
        logging.warning(msg)
        print(msg)
    else:
        logging.info(f"[MONITOR] Row count OK: {rows_loaded} rows")


def log_run_summary(status: str, rows: int, error: str = "") -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id   TEXT,
                status   TEXT,
                rows     INTEGER,
                error    TEXT,
                run_ts   TEXT
            )
        """)
        conn.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?)",
            (RUN_ID, status, rows, error, datetime.now().isoformat())
        )
    logging.info(f"[SUMMARY] run_id={RUN_ID} status={status} rows={rows}")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print(f"Pipeline run: {RUN_ID}")
    rows = 0
    try:
        raw  = extract()
        df   = transform(raw)
        rows = load(df)
        monitor(rows)
        log_run_summary("SUCCESS", rows)
        print(f"Pipeline complete. {rows} rows loaded.")
    except Exception as e:
        logging.error(f"[PIPELINE FAILED] {e}")
        log_run_summary("FAILED", rows, str(e))
        print(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()
