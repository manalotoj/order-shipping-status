from pathlib import Path
import sys
import types
import pytest
import os
import pandas as pd

from order_shipping_status import cli


def run_cli(args):
    # Invoke main() with a custom argv list; don’t spawn a subprocess
    return cli.main(args)


def test_cli_happy_path(tmp_path: Path, monkeypatch):
    # Create a fake "xlsx" path (contents won't be read thanks to our patch)
    src = tmp_path / "abc.xlsx"
    src.write_text("x")

    # ---- Minimal but realistic single-row input ----
    fake_df = pd.DataFrame(
        [{
            # core status fields
            "code": "DL",                 # delivered
            "derivedCode": "DL",
            "statusByLocale": "Delivered",
            "description": "Package delivered",

            # boolean flags your pipeline expects to already exist
            "IsPreTransit": False,
            "IsDelivered": True,
            "HasException": False,
            "IsRTS": False,
            "IsStalled": False,
            "Damaged": False,

            # calculated fields (seed with something harmless)
            "CalculatedStatus": "Delivered",
            "CalculatedReasons": "",
            "DaysSinceLatestEvent": 0,

            # columns you said were required
            "Tracking Number": "123456789012",
            "latestStatusDetail": "Delivered"
        }]
    )

    # ---- Patch where pandas is looked up by your CLI code ----
    # If cli.py does `import pandas as pd`, patch cli.pd.read_excel.
    monkeypatch.setattr(cli.pd, "read_excel", lambda *a, **k: fake_df)

    # Always no-op writes
    monkeypatch.setattr(cli.pd.DataFrame, "to_excel", lambda *a, **k: None)

    # If your pipeline tries to hit FedEx / external enrichment, safely no-op it.
    # These patches won't raise if the attribute doesn't exist.
    monkeypatch.setattr("order_shipping_status.tracking.fetch_latest_statuses",
                        lambda df, *a, **k: df, raising=False)
    monkeypatch.setattr("order_shipping_status.tracking.update_latest_status_details",
                        lambda df, *a, **k: df, raising=False)
    monkeypatch.setattr("order_shipping_status.fedex.fetch_latest_statuses",
                        lambda df, *a, **k: df, raising=False)

    # Keep console quiet in test runner
    code = run_cli([str(src), "--no-console", "--log-level=DEBUG"])
    assert code == 0


def test_cli_missing_input_returns_2(tmp_path: Path):
    code = run_cli([str(tmp_path / "nope.xlsx"), "--no-console"])
    assert code == 2


def test_cli_strict_env_missing_returns_2(tmp_path, monkeypatch):
    src = tmp_path / "abc.xlsx"
    src.write_text("x")
    # ensure credentials are not present
    monkeypatch.delenv("SHIPPING_CLIENT_ID", raising=False)
    monkeypatch.delenv("SHIPPING_CLIENT_SECRET", raising=False)

    # Run CLI from an isolated directory so it cannot see project .env
    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        code = run_cli([str(src), "--no-console", "--strict-env"])
    finally:
        os.chdir(cwd)

    assert code == 2
