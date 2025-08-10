"""
Test: Verify Gold `vin_last_state` parquet matches the latest data from Silver.

Why:
- Gold must reflect the most recent timestamp per VIN and the last non-null door/wiper states.
- Any mismatch indicates incorrect aggregation or data loss between Silver and Gold.

How:
1. Load Silver and Gold datasets.
2. For each VIN, calculate the max ts_utc in Silver and ensure it equals Gold's last_reported_timestamp.
3. For door/wiper states, take the last non-null value in Silver (sorted by ts_utc) and compare to Gold.
4. Fail if any mismatch is found.
"""


import pandas as pd
from pathlib import Path

SILVER = Path("messages_silver")
GOLD = Path("messages_gold") / "vin_last_state.parquet"

def test_gold_last_timestamp_and_states():
    silver = pd.read_parquet(SILVER)
    gold = pd.read_parquet(GOLD)

    # last_reported_timestamp = max ts_utc per VIN
    max_ts = (silver.groupby("vin", as_index=False)["ts_utc"].max()
              .rename(columns={"ts_utc": "last_reported_timestamp"}))
    merged = gold.merge(max_ts, on="vin", suffixes=("_gold", "_calc"))
    assert (merged["last_reported_timestamp_gold"] == merged["last_reported_timestamp_calc"]).all()

    # door/wiper = last non-null by ts_utc per VIN
    silver_sorted = silver.sort_values(["vin","ts_utc"])
    last_nonnull = (silver_sorted
        .dropna(subset=["frontLeftDoorState","wipersState"], how="all")
        .groupby("vin")[["frontLeftDoorState","wipersState"]]
        .last()
        .reset_index())
    gold2 = gold.merge(last_nonnull, on="vin", how="left")
    if "front_left_door_state" in gold2:
        assert (gold2["front_left_door_state"].fillna(pd.NA).astype("string")
                == gold2["frontLeftDoorState"].fillna(pd.NA).astype("string")).all()
    if "wipers_state" in gold2:
        assert (gold2["wipers_state"].fillna(pd.NA).astype("string")
                == gold2["wipersState"].fillna(pd.NA).astype("string")).all()

