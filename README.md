### About
This repo is an exercise in working with Datalakes.
It loads telematics from connected vehicles via API.
Collected data is saved as BRONZE level data.
A subsequent step cleans and standardizes the data into a SILVER level.
The last state of each connected vehicle is extracted into a GOLD level.
Additionally, the top 10 velocities per hour are recorded in a report.

### Prerequisits
- You will have to gain access to Docker image tarball, load it to docker, and run the server with port forwarding 9900:9900.
- Use Python 3.11.8 (virtual env encouraged)
- Update `pip` with `python -m pip install --upgrade pip`
- Install dependencies with `pip install -r requirement.txt`

### Usage
- Grant permission to the script with `chmod +x run_pipeline.sh`
- Edit the `.env` file according to your preferences (see below)
- Run the pipeline ./run_pipeline.sh
- Re-run the Jupyter Notebook to see graphs and statistics.
- Open the top-speeds report from `reports/`

### `.env` file
- `BRONZE_AMOUNT`: Number of entries to ingest into bronze
- `APPLY_VELOCITY_FILTER`: Set to `1` to filter ourliers. Set to `0` to disable filtering.
- `MAX_VELOCITY`: Set to the maximum velocity allowed without being considered an outlier.

### Clear outputs
- To clear all outputs and start from scratch: `rm -rf messages_bronze messages_silver messages_gold reports`

### Component
- `run_pipeline.sh`: A script that builds Bronze -> Silver -> Gold + Top Velocities Report
- `ingest_messages_bronze.py`: Script for ingesting vehicles messages from API to a bronze parquet file with partitions
- `sanitize_messages_silver.py`: Script for clearning and preprocessing the data.
- `build_last_state_gold.py`: Extract the last state for each VIN
- `report_top_speeds.py`: Produces a report of the 10 fastest vehicles per hour.
- `EDA.ipynb`: A Jupyter Notebook for EDA
- `.env` (hidden): Config file, mainly for configuring velocity outlier filtering

### Further consideration
- Current architecture is batch refresh.
    - Run is triggered manually by running a script.
    - No state, we start from a clean slate.
    - IO reads _all_ partitions each time
    - Simple, but not very effecient
    - Ephermal logs are only printed and not saved
- For a real system we would switch to an incremental, partition-aware approach.
    - Triggered periodically by AirFlow/Prefect
    - Process only touched partitions, not all data
    - Stateful - use a high-water mark (hwm) to indicate last timestamp of ingestion
    - Keep logs persistent
- Key differences:
    - Full refresh -> Incremental + high-water mark and lookback
    - Load all -> load only relevant partitions
    - Save all -> Save only changed partitions
    - Manual run -> Scheduled with logs and monitoring
- Next steps:
    - Make bronze incremental using HWM+lookback
    - Silver incremental - accepted changed partitions from Bronze, change only those
    - Gold/Report incremental - Recompute only affected hours/VINs

### Tests
- Run unittests with `pytest -q`

