"""
Reads messages_silver/ parquet files and creates messages_gold/ parquet
with the last-known state per VIN:
- vin
- last_reported_timestamp (UTC)
- frontLeftDoorState (last non-null)
- wipersState (last non-null)
"""

from pathlib import Path
import pandas as pd

SILVER_DIR = "messages_silver"
GOLD_DIR = Path("messages_gold")

def vin_last_state(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df = df.sort_values(["vin", "ts_utc"])

    last_ts = (
        df.groupby("vin", as_index=False)["ts_utc"]
        .max()
        .rename(columns={"ts_utc": "last_reported_timestamp"})
    )

    last_door = (
        df.dropna(subset=["frontLeftDoorState"])
        .groupby("vin", as_index=False)["frontLeftDoorState"]
        .last()
    )

    last_wiper = (
        df.dropna(subset=["wipersState"])
        .groupby("vin", as_index=False)["wipersState"]
        .last()
    )

    out = last_ts.merge(last_door, on="vin", how="left").merge(last_wiper, on="vin", how="left")
    out = out.rename(columns={
        "frontLeftDoorState": "front_left_door_state",
        "wipersState": "wipers_state"})
    return out[["vin", "last_reported_timestamp", "front_left_door_state", "wipers_state"]]


def main():
    GOLD_DIR.mkdir(exist_ok=True)
    df = pd.read_parquet(SILVER_DIR)
    out = vin_last_state(df)
    out.to_parquet(GOLD_DIR / "vin_last_state.parquet", index=False)
    print(f"[INFO] Wrote {len(out)} rows → {GOLD_DIR}/vin_last_state.parquet")

if __name__ == "__main__":
    main()
