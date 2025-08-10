"""
Test: Validate the 'top 10 fastest vehicles per hour' report is correct and sorted.

Why:
- The report must contain at most 10 VINs per hour, sorted by top_velocity descending.
- top_velocity values must match the maximum velocity seen in Silver for that VIN/hour.

How:
1. Load Silver and the generated report CSV.
2. Group the report by date_hour, ensure each group has ≤10 rows.
3. Check that top_velocity is non-increasing within each hour.
4. Calculate per-VIN/hour max velocity from Silver, compare to reported top_velocity.
5. Fail if any ordering or value mismatches are found.
"""


import pandas as pd
from pathlib import Path

SILVER = Path("messages_silver")
REPORT = Path("reports/top_fastest_per_hour.csv")

def test_report_top10_sorted_and_correct():
    silver = pd.read_parquet(SILVER)
    silver["ts_utc"]   = pd.to_datetime(silver["ts_utc"], utc=True)
    silver["velocity"] = pd.to_numeric(silver["velocity"], errors="coerce")
    silver = silver.dropna(subset=["vin","velocity"]).copy()
    silver["date_hour"] = silver["ts_utc"].dt.floor("h")

    rep = pd.read_csv(REPORT, parse_dates=["date_hour"])
    # ≤ 10 rows per hour and sorted desc by top_velocity
    sizes = rep.groupby("date_hour").size()
    assert (sizes <= 10).all(), "Found hours with more than 10 rows"

    def is_desc(s): 
        return (s.shift(-1).fillna(s.iloc[-1]) <= s).all()

    assert rep.groupby("date_hour")["top_velocity"].apply(is_desc).all(), \
        "Within-hour rows not sorted by top_velocity desc"

    # Validate values: reported top_velocity == silver per-VIN-per-hour max
    per_vin_hour = (silver.groupby(["date_hour","vin"], as_index=False)["velocity"]
                    .max().rename(columns={"velocity":"calc_top_velocity"}))
    merged = rep.merge(per_vin_hour, on=["date_hour","vin"], how="left")
    mismatches = (merged["top_velocity"] != merged["calc_top_velocity"]).sum()
    assert mismatches == 0, f"{mismatches} rows have mismatched top_velocity vs Silver"

