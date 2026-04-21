import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
INPUT_DIR  = Path("input")
OUTPUT_DIR = Path("output")
LOG_DIR    = Path("logs")

for d in [INPUT_DIR, OUTPUT_DIR, LOG_DIR]:
    d.mkdir(exist_ok=True)

log_file = LOG_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)


# ── Helpers ──────────────────────────────────────────────────────────────────
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names, strip whitespace, replace spaces with underscores."""
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(r"\s+", "_", regex=True)
                  .str.replace(r"[^\w]", "", regex=True)
    )
    return df


def remove_duplicates(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    if dropped:
        logging.info(f"{filename}: removed {dropped} duplicate row(s)")
    return df


def flag_nulls(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    null_counts = df.isnull().sum()
    for col, count in null_counts.items():
        if count:
            logging.warning(f"{filename}: column '{col}' has {count} null value(s)")
    return df


def fix_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Try to coerce object columns that look numeric to float."""
    for col in df.select_dtypes(include="object").columns:
        converted = pd.to_numeric(df[col], errors="coerce")
        if converted.notna().mean() > 0.8:   # >80% parseable → convert
            df[col] = converted
    return df


def clean_file(filepath: Path) -> None:
    filename = filepath.name
    logging.info(f"Processing: {filename}")
    print(f"  Cleaning {filename} ...")

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        logging.error(f"{filename}: failed to read — {e}")
        print(f"  ERROR reading {filename}: {e}")
        return

    original_rows = len(df)
    df = standardize_columns(df)
    df = remove_duplicates(df, filename)
    df = flag_nulls(df, filename)
    df = fix_numeric_columns(df)

    out_path = OUTPUT_DIR / filename
    df.to_csv(out_path, index=False)
    logging.info(
        f"{filename}: done — {original_rows} rows in, {len(df)} rows out, "
        f"saved to {out_path}"
    )
    print(f"  Done  {filename}: {original_rows} -> {len(df)} rows")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    csv_files = list(INPUT_DIR.glob("*.csv"))
    if not csv_files:
        print("No CSV files found in input/. Add some and re-run.")
        return

    print(f"Found {len(csv_files)} file(s) to clean.")
    for f in csv_files:
        clean_file(f)

    print(f"\nAll done. Log saved to {log_file}")


if __name__ == "__main__":
    main()
