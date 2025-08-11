"""
vehicle_messages_ingest.py

Fetches a batch of vehicle messages from the upstream API and writes them to
Parquet files partitioned by date and hour (Hive-style).

Critical cleaning applied during ingestion:
- Drops any records missing or having invalid `timestamp` (required for partitioning).
- Converts `timestamp` from milliseconds to UTC datetime.
Logs the number of dropped records for visibility.
"""

import requests
import os

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from dotenv import load_dotenv

load_dotenv()

API = "http://localhost:9900/upstream/vehicle_messages"
OUT_DIR = "messages_bronze"
AMOUNT = int(os.getenv("BRONZE_AMOUNT", "10000"))


def fetch_messages(amount: int):
    """Fetch a batch of vehicle messages from the API."""
    r = requests.get(API, params={"amount": amount}, timeout=15)
    r.raise_for_status()
    return r.json()


def clean_and_partition(df: pd.DataFrame) -> pd.DataFrame:
    """Drop invalid timestamps, add date/hour fields."""
    before_count = len(df)
    df = df.dropna(subset=["timestamp"])
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    dropped_count = before_count - len(df)
    if dropped_count > 0:
        print(f"[INFO] Dropped {dropped_count} records due to missing/invalid timestamp.")

    df["ts_utc"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["date"] = df["ts_utc"].dt.strftime("%Y-%m-%d")
    df["hour"] = df["ts_utc"].dt.hour.astype("int16")
    return df


def write_parquet(df: pd.DataFrame, out_dir: str):
    """Write DataFrame to partitioned Parquet dataset."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    ds.write_dataset(
        table,
        base_dir=out_dir,
        format="parquet",
        partitioning=["date", "hour"],
        partitioning_flavor="hive",
    )
    print(f"[INFO] Wrote {len(df)} rows to {out_dir}/ partitioned by date/hour.")


def main():
    data = fetch_messages(AMOUNT)
    df = pd.DataFrame(data)
    df = clean_and_partition(df)
    write_parquet(df, OUT_DIR)


if __name__ == "__main__":
    main()
