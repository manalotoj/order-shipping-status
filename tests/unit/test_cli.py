from pathlib import Path
import sys
import types
import pytest
import os

from order_shipping_status import cli


def run_cli(args):
    # Invoke main() with a custom argv list; don’t spawn a subprocess
    return cli.main(args)


def test_cli_happy_path(tmp_path: Path, monkeypatch):
    print("tmp_path:", tmp_path)
    # create fake input xlsx
    src = tmp_path / "abc.xlsx"
    src.write_text("x")

    # keep console quiet in test runner
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
