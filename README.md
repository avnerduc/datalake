### About
This repo is an exercise in working with Datalakes.
It loads telematics from connected vehicles via API.
Collected data is saved as BRONZE level data.
A subsequent step cleans and standardizes the data into a SILVER level.
The last state of each connected vehicle is extracted into a GOLD level.
Additionally, the top 10 velocities per hour are recorded in a report.

The repo also includes a SQL injection detection script, as well as a script that simulates such an injection.

There are a couple of other scripts, like a script that proves that the API responses are unsorted. For a full list, see the Components section below.

### Prerequisites
- You will have to gain access to the Docker image tarball, load it into Docker, and run the server with port forwarding 9900:9900.
- Use Python 3.11.8 (virtual env encouraged)
- Update `pip` with `python -m pip install --upgrade pip`
- Install dependencies with `pip install -r requirements.txt`

### Usage
- Grant permission to the script with `chmod +x run_pipeline.sh`
- Edit the `.env` file according to your preferences (see below)
- Run the pipeline with `./run_pipeline.sh`
- Re-run the Jupyter Notebook to see graphs and statistics.
- Open the top-speeds report from `reports/`

### `.env` file
- `BRONZE_AMOUNT`: Number of entries to ingest into bronze level dataset
- `APPLY_VELOCITY_FILTER`: Set to `1` to filter outliers. Set to `0` to disable filtering.
- `MAX_VELOCITY`: Set to the maximum velocity allowed without being considered an outlier.

### Clear outputs
- To clear all outputs and start from scratch: `rm -rf messages_bronze messages_silver messages_gold reports api_sort_check.json messages_bronze_injected`

### Components
- `run_pipeline.sh`: A script that builds Bronze -> Silver -> Gold + Top Velocities Report
- `ingest_messages_bronze.py`: Script for ingesting vehicle messages from API to a bronze parquet file with partitions
- `sanitize_messages_silver.py`: Script for cleaning and preprocessing the data.
- `build_last_state_gold.py`: Extract the last state for each VIN
- `report_top_speeds.py`: Produces a report of the 10 fastest vehicles per hour.
- `EDA.ipynb`: A Jupyter Notebook for EDA
- `.env` (hidden): Config file, mainly for configuring velocity outlier filtering
- `simulate_injection.py`: Creates a copy of the Bronze level, and simulates an SQL injection query
- `sql_injection.py`: Script for detecting SQL injections in bronze data. (See below)
- `check_api_sorting.sh`: A script that verifies that the API returns unsorted entries.

### Further consideration
- Current architecture is batch refresh.
    - Run is triggered manually by running a script.
    - No state, we start from a clean slate.
    - IO reads _all_ partitions each time
    - Simple, but not very efficient
    - Ephemeral logs are only printed and not saved
- For a real system we would switch to an incremental, partition-aware approach.
    - Triggered periodically by Airflow/Prefect
    - Process only touched partitions, not all data
    - Stateful - use a high-water mark (hwm) to indicate last timestamp of ingestion
    - Keep logs persistent
- Key differences:
    - Full refresh -> Incremental + high-water mark and lookback
    - Load all -> load only relevant partitions
    - Save all -> Save only changed partitions
    - Manual run -> Scheduled with logs and monitoring
- Server is random
    - Since the server returns unsorted, random responses, using a HWM is irrelevant
    - Therefore we skip such implementation
- Next steps:
    - These steps are relevant only for a server that returns sorted data
    - Make bronze incremental using HWM+lookback
    - Silver incremental - accepted changed partitions from Bronze, change only those
    - Gold/Report incremental - Recompute only affected hours/VINs

### Tests
- Run unittests with `pytest -q`

### SQL Injection detection
- An SQL injection detection module is included: `sql_injection.py`
- You can test it by:
    - Run the whole pipeline, or at least make sure you created the `messages_bronze` dir.
    - Run `simulate_injection.py` (or run all tests `pytest -q`)
    - Then run `python -c "from sql_injection import example_injection_detection; example_injection_detection()"`
    - You can then view the report under `reports/`

