"""
Test: Ensure there are no duplicate (vin, timestamp) rows in Silver.

Why:
- Gold's last-state calculation relies on having only one row per VIN/timestamp for determinism.
- Duplicate VIN/timestamp entries could cause non-deterministic or incorrect last-state values.

How:
- Load the Silver dataset from `messages_silver/`.
- Count duplicates using DataFrame.duplicated(subset=["vin","timestamp"]).
- Assert the count is zero; fail the test with the count if not.
"""

import pandas as pd
from pathlib import Path

SILVER_DIR = Path("messages_silver")

def test_no_duplicate_vin_timestamp():
    df = pd.read_parquet(SILVER_DIR)
    dup_count = df.duplicated(subset=["vin", "timestamp"]).sum()
    assert dup_count == 0, f"Found {dup_count} duplicate (vin, timestamp) rows in Silver"

