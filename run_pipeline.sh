#!/usr/bin/env bash
set -euo pipefail

python ingest_messages_bronze.py
python sanitize_messages_silver.py
python build_last_state_gold.py
python report_top_speeds.py

echo "[DONE] bronze → silver → gold → reports"
