import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def main():
    bronze_path = "messages_bronze"
    dataset = ds.dataset(bronze_path, format="parquet", partitioning="hive")
    df = dataset.to_table().to_pandas()

    df.loc[len(df)] = df.iloc[0]  # duplicate a real row so schema matches
    df.iloc[-1, df.columns.get_loc("vin")] = "DROP TABLE users;"

    df.to_parquet("messages_bronze_injected")


if __name__ == "__main__":
    main()

