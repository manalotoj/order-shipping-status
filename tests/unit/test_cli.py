from pathlib import Path
import types
import sys
import os
import pandas as pd

from order_shipping_status import cli


def run_cli(args):
    return cli.main(args)


def _install_stub_module(monkeypatch, fullname: str, **attrs):
    mod = types.ModuleType(fullname)
    for k, v in attrs.items():
        setattr(mod, k, v)
    monkeypatch.setitem(sys.modules, fullname, mod)
    if "." in fullname:
        parent_name, child_name = fullname.rsplit(".", 1)
        parent = sys.modules.get(parent_name)
        if parent is not None:
            setattr(parent, child_name, mod)


def test_cli_happy_path(tmp_path: Path, monkeypatch):
    src = tmp_path / "abc.xlsx"
    src.write_text("x")  # exists so CLI path checks pass

    fake_df = pd.DataFrame([{
        "code": "DL",
        "derivedCode": "DL",
        "statusByLocale": "Delivered",
        "description": "Package delivered",
        "IsPreTransit": False,
        "IsDelivered": True,
        "HasException": False,
        "IsRTS": False,
        "IsStalled": False,
        "Damaged": False,
        "CalculatedStatus": "Delivered",
        "CalculatedReasons": "",
        "DaysSinceLatestEvent": 0,
        "Tracking Number": "123456789012",
        "latestStatusDetail": "Delivered",
    }])

    # Read/Write stubs
    monkeypatch.setattr("pandas.read_excel", lambda *a, **k: fake_df)
    monkeypatch.setattr(pd.DataFrame, "to_excel", lambda *a, **k: None)

    # Optional imports the CLI might touch: stub them so imports succeed.
    _install_stub_module(
        monkeypatch, "order_shipping_status.tracking",
        fetch_latest_statuses=lambda df, *a, **k: df,
        update_latest_status_details=lambda df, *a, **k: df,
    )
    _install_stub_module(
        monkeypatch, "order_shipping_status.fedex",
        fetch_latest_statuses=lambda df, *a, **k: df,
    )

    # CRUCIAL: bypass the real pipeline to avoid side-effect errors in a unit test
    monkeypatch.setattr(
        "order_shipping_status.pipelines.workbook_processor.WorkbookProcessor.process",
        lambda self, *a, **k: None,
        raising=True,
    )

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
