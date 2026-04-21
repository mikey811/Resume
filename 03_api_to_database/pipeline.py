import sqlite3
import requests
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
API_URL  = "https://api.open-meteo.com/v1/forecast"
PARAMS   = {
    "latitude": 40.7128,
    "longitude": -74.0060,
    "hourly": "temperature_2m,precipitation,windspeed_10m",
    "timezone": "America/New_York",
    "forecast_days": 1,
}
DB_PATH  = Path("weather.db")
LOG_DIR  = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log",
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)


# ── Extract ────────────────────────────────────────────────────────────────────
def extract() -> dict:
    logging.info("Extracting data from API")
    try:
        resp = requests.get(API_URL, params=PARAMS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        logging.info("Extract successful")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Extract failed: {e}")
        raise


# ── Transform ──────────────────────────────────────────────────────────────────
def transform(raw: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    logging.info("Transforming data")
    hourly = raw["hourly"]

    # Raw table — keep everything
    raw_df = pd.DataFrame(hourly)
    raw_df["load_ts"] = datetime.now().isoformat()
    raw_df.rename(columns={"time": "datetime"}, inplace=True)

    # Cleaned table — drop nulls, rename, round
    clean_df = raw_df.copy()
    clean_df = clean_df.dropna()
    clean_df["temperature_f"] = (clean_df["temperature_2m"] * 9 / 5 + 32).round(1)
    clean_df = clean_df.rename(columns={
        "precipitation": "precip_mm",
        "windspeed_10m": "windspeed_mph",
    })

    logging.info(f"Transform complete: {len(clean_df)} rows")
    return raw_df, clean_df


# ── Load ──────────────────────────────────────────────────────────────────────
def load(raw_df: pd.DataFrame, clean_df: pd.DataFrame) -> None:
    logging.info(f"Loading to {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        raw_df.to_sql("raw_weather", conn, if_exists="append", index=False)
        clean_df.to_sql("clean_weather", conn, if_exists="append", index=False)
    logging.info("Load complete")
    print(f"Loaded {len(raw_df)} raw rows and {len(clean_df)} clean rows to {DB_PATH}")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("Starting weather data pipeline...")
    raw_data = extract()
    raw_df, clean_df = transform(raw_data)
    load(raw_df, clean_df)
    print("Pipeline complete.")


if __name__ == "__main__":
    main()
