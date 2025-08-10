"""
sanitize_messages_silver.py

Reads the bronze dataset (Hive-partitioned Parquet) from `messages_bronze/`,
applies minimal cleaning, and writes a silver dataset to `messages_silver/`.

Cleaning steps:
1) Strip whitespace from manufacturer names.
2) Drop rows with null/empty VIN.
3) Standardize gearPosition: NEUTRAL→0, REVERSE→-1; keep numeric gears; drop others.
Logs row changes at each step.
"""


import os

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from dotenv import load_dotenv

load_dotenv()

BRONZE_DIR = "messages_bronze"
SILVER_DIR = "messages_silver"


def load_bronze() -> pd.DataFrame:
    """
    Load the full Bronze dataset into a Pandas DataFrame.

    NOTE: In the current architecture, all Bronze entries are loaded at once.
    If Bronze grows large, consider using `pyarrow.dataset` to filter partitions
    before converting to Pandas to reduce memory usage.
    """
    return pd.read_parquet(BRONZE_DIR)


def fix_manufacturer_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    if "manufacturer" in df:
        before = df["manufacturer"].copy()
        df["manufacturer"] = df["manufacturer"].astype("string").str.strip()
        fixed = (before != df["manufacturer"]).sum()
        if fixed:
            print(f"[INFO] Manufacturer whitespace fixed in {fixed} rows.")
            changed = before[before != df["manufacturer"]].unique()
            print(f"[INFO] Changed manufacturer names: {list(changed)}")
    return df


def drop_null_vin(df: pd.DataFrame) -> pd.DataFrame:
    if "vin" in df:
        before = len(df)
        df = df.dropna(subset=["vin"])
        df = df[df["vin"].astype("string").str.len() > 0]
        dropped = before - len(df)
        if dropped:
            print(f"[INFO] Dropped {dropped} rows with null/empty VIN.")
    return df


def standardize_gear_position(df: pd.DataFrame) -> pd.DataFrame:
    if "gearPosition" in df:
        # Map NEUTRAL → 0, REVERSE → -1
        GEAR_MAP = {"NEUTRAL": 0, "REVERSE": -1}
        neutral_count = (df["gearPosition"] == "NEUTRAL").sum()
        reverse_count = (df["gearPosition"] == "REVERSE").sum()
        df["gearPosition"] = df["gearPosition"].replace(GEAR_MAP)

        if neutral_count:
            print(f"[INFO] Replaced {neutral_count} 'NEUTRAL' entries with 0.")
        if reverse_count:
            print(f"[INFO] Replaced {reverse_count} 'REVERSE' entries with -1.")

        # Convert to numeric; invalid → NaN
        gp_numeric = pd.to_numeric(df["gearPosition"], errors="coerce")

        # Keep only rows that converted successfully
        keep_mask = gp_numeric.notna()
        dropped_values = df.loc[~keep_mask, "gearPosition"].unique()
        dropped_gp = (~keep_mask).sum()
        df = df[keep_mask].copy()
        df["gearPosition"] = gp_numeric[keep_mask].astype("int16")

        if dropped_gp:
            print(f"[INFO] Dropped {dropped_gp} rows with invalid gearPosition values: {list(dropped_values)}")
    return df


def write_silver(df: pd.DataFrame, out_dir: str) -> None:
    required = {"date", "hour"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing partition columns: {missing}")

    table = pa.Table.from_pandas(df, preserve_index=False)
    ds.write_dataset(
        table,
        base_dir=out_dir,
        format="parquet",
        partitioning=["date", "hour"],
        partitioning_flavor="hive",
        existing_data_behavior="overwrite_or_ignore",
    )
    print(f"[INFO] Wrote {len(df)} rows to {out_dir}/ (partitioned by date/hour).")


def remove_velocity_outliers(df: pd.DataFrame, min_v: int = 0, max_v: int = 300) -> pd.DataFrame:
    """Keep velocities within [min_v, max_v]; drop negatives/implausible highs."""
    if "velocity" in df:
        v = pd.to_numeric(df["velocity"], errors="coerce")
        before = len(df)
        mask = v.between(min_v, max_v, inclusive="both")
        dropped = before - int(mask.sum())
        if dropped:
            print(f"[INFO] Dropped {dropped} rows with velocity outside [{min_v}, {max_v}].")
        df = df[mask].copy()
        df["velocity"] = v[mask].astype("int16")
    return df


def fix_collisions(df: pd.DataFrame) -> pd.DataFrame:
    if {"vin", "timestamp"}.issubset(df.columns):
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        before = len(df)
        df = df.sort_values(["vin", "ts_utc", "timestamp"], kind="mergesort")
        df = df.drop_duplicates(subset=["vin", "timestamp"], keep="last")
        dropped = before - len(df)
        if dropped:
            print(f"[INFO] Dropped {dropped} duplicate rows with same (vin, timestamp).")
    return df


def main():
    df = load_bronze()
    df = fix_manufacturer_whitespace(df)
    df = drop_null_vin(df)
    df = standardize_gear_position(df)
    if os.getenv("APPLY_VELOCITY_FILTER", "0") == "1":
        max_v = int(os.getenv("MAX_VELOCITY", "300"))
        print(f"[INFO] Velocity filter enabled (0–{max_v}).")
        df = remove_velocity_outliers(df, max_v=max_v)
    else:
        print("[INFO] Velocity filter disabled.")
    df = fix_collisions(df)
    write_silver(df, SILVER_DIR)


if __name__ == "__main__":
    main()

