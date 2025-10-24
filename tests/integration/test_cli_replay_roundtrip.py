from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
import pytest

from order_shipping_status import cli
from order_shipping_status.io.paths import derive_output_paths


def run_cli(args):
    return cli.main(args)


def test_cli_replay_roundtrip_creates_processed_and_indicators(tmp_path: Path):
    """
    Run the CLI in replay mode using the 10-20 capture Excel and the
    corresponding combined JSON bodies file. Assert that the processed
    workbook exists, that indicator columns are present, and that at least
    one indicator value is non-zero (i.e., the run produced non-trivial
    indicator results).
    """
    repo_root = Path(__file__).resolve().parents[3]

    excel = repo_root / "tests" / "data" / "RAW_TransitIssues_10-20-2025.xlsx"
    json_bodies = repo_root / "tests" / "data" / \
        "RAW_TransitIssues_10-20-2025-json-bodies.json"

    if not excel.exists() or not json_bodies.exists():
        pytest.skip(
            "Required test fixtures (10-20 excel + json bodies) not present under tests/data")

    # Run CLI in replay mode; no console output and a fixed reference date to keep results stable
    rc = run_cli([
        str(excel),
        "--no-console",
        "--reference-date",
        "2025-10-22",
        "--replay-dir",
        str(json_bodies),
    ])

    assert rc == 0

    processed, _log = derive_output_paths(excel)
    assert processed.exists(), "Processed workbook not created"

    # Read the 'All Shipments' or 'All Issues' sheet depending on output; prefer All Shipments
    try:
        df = pd.read_excel(
            processed, sheet_name="All Shipments", engine="openpyxl")
    except Exception:
        df = pd.read_excel(
            processed, sheet_name="All Issues", engine="openpyxl")

    # Ensure there are rows
    assert len(df) > 0, "Processed workbook contains no rows"

    # Indicator columns we care about
    indicators = ["IsPreTransit", "IsDelivered", "IsStalled"]
    for col in indicators:
        assert col in df.columns, f"Missing indicator column: {col}"

    # At least one indicator column should have a non-zero value across the frame
    # Convert to numeric, treat NaN as 0
    total = 0
    for col in indicators:
        vals = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        total += int(vals.sum())

    assert total > 0, "All indicator values are zero — expected at least one non-zero indicator"
