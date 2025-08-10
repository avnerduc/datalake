"""
Generate a CSV report of the top 10 fastest vehicles per hour from SILVER.

Fastest vehicle = highest velocity per VIN within each hour.
Output columns: vin, date_hour, top_velocity

Reads:   messages_silver/  (Hive-partitioned Parquet)
Writes:  reports/top_fastest_per_hour.csv
"""


from pathlib import Path
import pandas as pd


SILVER_PATH = "messages_silver"
OUTPUT_PATH = "reports/top_fastest_per_hour.csv"


def build_top_speeds_from_silver(
    silver_path: str = SILVER_PATH,
    output_path: str = OUTPUT_PATH,
) -> str:
    df = pd.read_parquet(silver_path)
    df["ts_utc"]   = pd.to_datetime(df["ts_utc"], utc=True)
    df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce")
    df = df.dropna(subset=["vin","velocity"]).copy()
    df["date_hour"] = df["ts_utc"].dt.floor("h")

    # per VIN per hour top speed
    per_vin_hour = (
        df.groupby(["date_hour","vin"], as_index=False)["velocity"]
          .max()
          .rename(columns={"velocity":"top_velocity"})
    )

    # top 10 per hour
    out = (per_vin_hour
           .sort_values(["date_hour","top_velocity"], ascending=[True,False])
           .groupby("date_hour", group_keys=False)
           .head(10))

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)
    return output_path


if __name__ == "__main__":
    output_path = build_top_speeds_from_silver()
    print(f"[INFO] Top 10 fastest vehicles per hour report saved → {output_path}")

