import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
DATA_DIR   = Path("data")
REPORT_DIR = Path("reports")
for d in [DATA_DIR, REPORT_DIR]:
    d.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# ── Validation Rules (config-driven) ─────────────────────────────────────────
RULES = {
    "null_threshold": 0.05,          # Flag columns with >5% nulls
    "duplicate_threshold": 0.01,     # Flag if >1% rows are duplicates
    "expected_dtypes": {},           # e.g. {"age": "int64", "name": "object"}
    "valid_categories": {},          # e.g. {"status": ["active", "inactive"]}
    "numeric_ranges": {},            # e.g. {"age": {"min": 0, "max": 120}}
}


# ── Checks ─────────────────────────────────────────────────────────────────────
def check_nulls(df, rules):
    issues = []
    threshold = rules["null_threshold"]
    for col in df.columns:
        rate = df[col].isnull().mean()
        if rate > threshold:
            issues.append({
                "check": "null_rate",
                "column": col,
                "value": round(rate, 4),
                "threshold": threshold,
                "status": "FAIL",
            })
    return issues


def check_duplicates(df, rules):
    dup_rate = df.duplicated().mean()
    status = "FAIL" if dup_rate > rules["duplicate_threshold"] else "PASS"
    return [{
        "check": "duplicate_rate",
        "column": "ALL",
        "value": round(dup_rate, 4),
        "threshold": rules["duplicate_threshold"],
        "status": status,
    }]


def check_dtypes(df, rules):
    issues = []
    for col, expected in rules["expected_dtypes"].items():
        if col not in df.columns:
            continue
        actual = str(df[col].dtype)
        status = "PASS" if actual == expected else "FAIL"
        issues.append({
            "check": "dtype",
            "column": col,
            "value": actual,
            "threshold": expected,
            "status": status,
        })
    return issues


def check_categories(df, rules):
    issues = []
    for col, valid in rules["valid_categories"].items():
        if col not in df.columns:
            continue
        invalid = df[~df[col].isin(valid)][col].dropna().unique().tolist()
        if invalid:
            issues.append({
                "check": "invalid_category",
                "column": col,
                "value": invalid[:5],
                "threshold": valid,
                "status": "FAIL",
            })
    return issues


def check_ranges(df, rules):
    issues = []
    for col, bounds in rules["numeric_ranges"].items():
        if col not in df.columns:
            continue
        out_of_range = df[
            (df[col] < bounds.get("min", float("-inf"))) |
            (df[col] > bounds.get("max", float("inf")))
        ]
        if not out_of_range.empty:
            issues.append({
                "check": "numeric_range",
                "column": col,
                "value": len(out_of_range),
                "threshold": bounds,
                "status": "FAIL",
            })
    return issues


# ── Report ────────────────────────────────────────────────────────────────────
def run_audit(filepath: Path) -> dict:
    logging.info(f"Auditing {filepath.name}")
    df = pd.read_csv(filepath)

    issues = []
    issues += check_nulls(df, RULES)
    issues += check_duplicates(df, RULES)
    issues += check_dtypes(df, RULES)
    issues += check_categories(df, RULES)
    issues += check_ranges(df, RULES)

    passed = sum(1 for i in issues if i["status"] == "PASS")
    failed = sum(1 for i in issues if i["status"] == "FAIL")

    report = {
        "file": filepath.name,
        "rows": len(df),
        "columns": len(df.columns),
        "checks_passed": passed,
        "checks_failed": failed,
        "timestamp": datetime.now().isoformat(),
        "issues": issues,
    }
    return report


def save_report(report: dict, filename: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = REPORT_DIR / f"audit_{filename}_{ts}.json"
    with open(out, "w") as f:
        json.dump(report, f, indent=2)
    logging.info(f"Report saved: {out}")
    return out


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    csv_files = list(DATA_DIR.glob("*.csv"))
    if not csv_files:
        print("No CSV files found in data/. Add some and re-run.")
        return

    for f in csv_files:
        report = run_audit(f)
        save_report(report, f.stem)

        print(f"\n=== Audit Report: {f.name} ===")
        print(f"  Rows: {report['rows']}  |  Columns: {report['columns']}")
        print(f"  Passed: {report['checks_passed']}  |  Failed: {report['checks_failed']}")
        if report["issues"]:
            print("  Issues:")
            for issue in report["issues"]:
                if issue["status"] == "FAIL":
                    print(f"    [FAIL] {issue['check']} on '{issue['column']}': {issue['value']}")


if __name__ == "__main__":
    main()
