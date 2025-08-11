import re
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

BRONZE_DIR = "messages_bronze_injected"


def detect_sql_injection(columns: list[str], patterns: list[str], dataset_path=BRONZE_DIR):
    ds_bronze = ds.dataset(dataset_path, format="parquet", partitioning="hive")
    df = ds_bronze.to_table().to_pandas()

    compiled_patterns = [re.compile(p, re.IGNORECASE) for p in patterns]
    violations = []

    for col in columns:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in dataset")
        for pattern in compiled_patterns:
            mask = df[col].astype(str).str.contains(pattern, regex=True, na=False)
            viol_rows = df[mask][[col]]
            for value in viol_rows[col]:
                violations.append({"column": col, "value": value, "pattern": pattern.pattern})

    if violations:
        report = pd.DataFrame(violations)
        Path("reports").mkdir(exist_ok=True)
        report_path = "reports/sql_injection_report.csv"
        report.to_csv(report_path, index=False)
        print(f"[INFO] Report saved to {report_path} with {len(report)} violations.")
        return report
    else:
        print("[INFO] No violations found.")


def main():
    columns = ["vin"]
    patterns = [r"(?:'(?:''|[^'])*')|(?:;)|(?:\b(?:ALTER|CREATE|DELETE|DROP|EXEC(?:UTE)?|INSERT(?: +INTO)?|MERGE|SELECT|UPDATE|UNION(?: +ALL)?)\b)"]
    detect_sql_injection(columns, patterns)


def example_injection_detection():
    columns = ["vin"]
    patterns = [r"(?:'(?:''|[^'])*')|(?:;)|(?:\b(?:ALTER|CREATE|DELETE|DROP|EXEC(?:UTE)?|INSERT(?: +INTO)?|MERGE|SELECT|UPDATE|UNION(?: +ALL)?)\b)"]
    detect_sql_injection(columns, patterns, "messages_bronze_injected")


if __name__ == "__main__":
    main()

