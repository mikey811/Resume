import pandas as pd
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timedelta

# ── Config ──────────────────────────────────────────────────────────────────
DATA_DIR   = Path("data")
OUTPUT_DIR = Path("output")
for d in [DATA_DIR, OUTPUT_DIR]:
    d.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


# ── Sample data generator (for demo purposes) ───────────────────────────────────
def generate_sample_data() -> pd.DataFrame:
    """Create a fake sales dataset if no real data found."""
    import random
    random.seed(42)
    regions  = ["Northeast", "Southeast", "Midwest", "West"]
    products = ["Widget A", "Widget B", "Gadget X", "Gadget Y"]
    rows = []
    for i in range(200):
        date = datetime.now() - timedelta(days=random.randint(0, 6))
        rows.append({
            "date": date.strftime("%Y-%m-%d"),
            "region": random.choice(regions),
            "product": random.choice(products),
            "units_sold": random.randint(1, 50),
            "revenue": round(random.uniform(20, 500), 2),
            "cost": round(random.uniform(10, 300), 2),
        })
    return pd.DataFrame(rows)


# ── KPI Calculations ────────────────────────────────────────────────────────────
def calculate_kpis(df: pd.DataFrame) -> dict:
    df["profit"] = df["revenue"] - df["cost"]
    df["profit_margin"] = (df["profit"] / df["revenue"]).round(4)

    summary = {
        "report_date": datetime.now().strftime("%Y-%m-%d"),
        "total_revenue": round(df["revenue"].sum(), 2),
        "total_cost": round(df["cost"].sum(), 2),
        "total_profit": round(df["profit"].sum(), 2),
        "avg_profit_margin": round(df["profit_margin"].mean(), 4),
        "total_units_sold": int(df["units_sold"].sum()),
        "top_product": df.groupby("product")["revenue"].sum().idxmax(),
        "top_region": df.groupby("region")["revenue"].sum().idxmax(),
    }
    return summary, df


def regional_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("region").agg(
        total_revenue=("revenue", "sum"),
        total_units=("units_sold", "sum"),
        avg_margin=("profit_margin", "mean"),
    ).round(2).reset_index()


def product_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("product").agg(
        total_revenue=("revenue", "sum"),
        total_units=("units_sold", "sum"),
        avg_margin=("profit_margin", "mean"),
    ).round(2).reset_index()


# ── Report Export ───────────────────────────────────────────────────────────────
def export_report(summary: dict, regional: pd.DataFrame, product: pd.DataFrame):
    ts = datetime.now().strftime("%Y%m%d")

    # CSV exports
    regional.to_csv(OUTPUT_DIR / f"regional_kpis_{ts}.csv", index=False)
    product.to_csv(OUTPUT_DIR / f"product_kpis_{ts}.csv", index=False)

    # Markdown summary
    md_path = OUTPUT_DIR / f"summary_{ts}.md"
    with open(md_path, "w") as f:
        f.write(f"# Weekly KPI Report\n")
        f.write(f"**Report Date:** {summary['report_date']}\n\n")
        f.write(f"## Top-Line KPIs\n")
        f.write(f"| Metric | Value |\n|--------|-------|\n")
        for k, v in summary.items():
            if k != "report_date":
                f.write(f"| {k.replace('_', ' ').title()} | {v} |\n")

    logging.info(f"Report exported to {OUTPUT_DIR}")
    return md_path


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    csv_files = list(DATA_DIR.glob("*.csv"))
    if csv_files:
        df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
        logging.info(f"Loaded {len(df)} rows from {len(csv_files)} file(s)")
    else:
        logging.info("No data files found — using generated sample data")
        df = generate_sample_data()

    summary, enriched_df = calculate_kpis(df)
    regional = regional_breakdown(enriched_df)
    product  = product_breakdown(enriched_df)
    md_path  = export_report(summary, regional, product)

    print("\n=== Weekly KPI Report ===")
    for k, v in summary.items():
        print(f"  {k.replace('_', ' ').title()}: {v}")
    print(f"\nFull report saved to: {md_path}")


if __name__ == "__main__":
    main()
