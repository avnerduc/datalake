import importlib
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
import simulate_injection
import sql_injection


def test_sql_injection_detection(tmp_path):
    # 1. Run the injection simulation to create dataset
    simulate_injection.main()  # you may need to wrap your simulate_injection in a main()

    # 2. Run detection
    columns = ["vin"]
    patterns = [
        r"(?:'(?:''|[^'])*')|(?:;)|(?:\b(?:ALTER|CREATE|DELETE|DROP|EXEC(?:UTE)?|INSERT(?: +INTO)?|MERGE|SELECT|UPDATE|UNION(?: +ALL)?)\b)"
    ]
    report = sql_injection.detect_sql_injection(columns, patterns, dataset_path="messages_bronze_injected")

    # 3. Verify detection
    assert isinstance(report, pd.DataFrame)
    assert not report.empty
    assert any("DROP TABLE users;" in str(v) for v in report["value"])

