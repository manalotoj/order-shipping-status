# tests/config/test_env.py

import os
import pytest
from pathlib import Path

from order_shipping_status.config.env import (
    load_project_dotenv,
    load_env,
    get_app_env,
    env as env_get,
)


def _write_env_file(dirpath, text=""):
    f = dirpath / ".env"
    f.write_text(text)
    return f


def _clear_keys(monkeypatch, *names):
    for n in names:
        monkeypatch.delenv(n, raising=False)


def test_load_env_reads_file_and_sets_process_env_when_missing(tmp_path, monkeypatch):
    """
    When the variables are not already in the environment,
    load_env should populate os.environ from the file (override doesn't matter).
    """
    _clear_keys(monkeypatch, "SHIPPING_CLIENT_ID", "SHIPPING_CLIENT_SECRET")

    f = _write_env_file(
        tmp_path,
        "SHIPPING_CLIENT_ID=file_id\nSHIPPING_CLIENT_SECRET=file_secret\n",
    )

    loaded = load_env(f, override=False, strict=True)
    # Dict reflects what was in the file
    assert loaded["SHIPPING_CLIENT_ID"] == "file_id"
    assert loaded["SHIPPING_CLIENT_SECRET"] == "file_secret"

    # os.environ is also populated
    assert os.environ["SHIPPING_CLIENT_ID"] == "file_id"
    assert os.environ["SHIPPING_CLIENT_SECRET"] == "file_secret"


def test_env_overrides_dotenv_env_wins_with_get_app_env(tmp_path, monkeypatch):
    """
    get_app_env() should respect existing process env values over .env file
    (it calls load_env with override=False under the hood).
    """
    _clear_keys(monkeypatch, "SHIPPING_CLIENT_ID", "SHIPPING_CLIENT_SECRET")

    f = _write_env_file(
        tmp_path,
        "SHIPPING_CLIENT_ID=file_id\nSHIPPING_CLIENT_SECRET=file_secret\n",
    )

    # Pre-set one var in the environment — this should win over the file.
    monkeypatch.setenv("SHIPPING_CLIENT_ID", "env_id")

    cfg = get_app_env(f)

    assert cfg.SHIPPING_CLIENT_ID == "env_id"         # env wins
    assert cfg.SHIPPING_CLIENT_SECRET == "file_secret"  # came from file


def test_load_env_override_true_file_wins(tmp_path, monkeypatch):
    """
    If override=True, values from the .env file should clobber process env.
    """
    _clear_keys(monkeypatch, "SHIPPING_CLIENT_ID", "SHIPPING_CLIENT_SECRET")

    # Pre-set something we expect to be overwritten
    monkeypatch.setenv("SHIPPING_CLIENT_ID", "env_id")

    f = _write_env_file(
        tmp_path,
        "SHIPPING_CLIENT_ID=file_id\nSHIPPING_CLIENT_SECRET=file_secret\n",
    )

    load_env(f, override=True, strict=True)

    # File should win because override=True
    assert os.environ["SHIPPING_CLIENT_ID"] == "file_id"
    assert os.environ["SHIPPING_CLIENT_SECRET"] == "file_secret"


def test_env_required_flag_raises(monkeypatch):
    """
    The low-level env() accessor should raise KeyError when required=True and missing.
    """
    _clear_keys(monkeypatch, "SOME_MISSING_VAR")

    with pytest.raises(KeyError):
        env_get("SOME_MISSING_VAR", required=True)


def test_env_default_is_used_when_missing(monkeypatch):
    """
    The low-level env() accessor should return the default when the variable is missing.
    """
    _clear_keys(monkeypatch, "OPTIONAL_VAR")

    assert env_get("OPTIONAL_VAR", default="fallback") == "fallback"


def test_get_app_env_strict_raises_when_missing_isolated(monkeypatch, tmp_path: Path):
    # Point to a .env file that does NOT exist
    env_file = tmp_path / ".env"
    assert not env_file.exists()

    # Ensure nothing is coming from process env
    for k in ("SHIPPING_CLIENT_ID", "SHIPPING_CLIENT_SECRET"):
        monkeypatch.delenv(k, raising=False)

    # Ask get_app_env to load from a non-existent .env (and be strict)
    with pytest.raises(RuntimeError) as e:
        get_app_env(dotenv_path=env_file, strict=True)

    msg = str(e.value).lower()
    assert "shipping_client_id" in msg or "missing" in msg


# Resolve repo root as the directory that contains pyproject.toml (adjust if needed)
REPO_ROOT = Path(__file__).resolve().parents[1]
print("REPO_ROOT:", REPO_ROOT)
DOTENV = REPO_ROOT / ".env"


def test_project_dotenv_loaded_when_present(monkeypatch):
    monkeypatch.delenv("SHIPPING_CLIENT_ID", raising=False)
    monkeypatch.delenv("SHIPPING_CLIENT_SECRET", raising=False)
    os.chdir(REPO_ROOT)

    found = load_project_dotenv(start=REPO_ROOT, override=False)
    expected = REPO_ROOT / ".env"
    assert found.resolve() == expected.resolve()

    cfg = get_app_env(strict=True)
    assert cfg.SHIPPING_CLIENT_ID and cfg.SHIPPING_CLIENT_SECRET


def test_project_dotenv_values_are_used_when_env_missing(monkeypatch):
    monkeypatch.delenv("SHIPPING_CLIENT_ID", raising=False)
    monkeypatch.delenv("SHIPPING_CLIENT_SECRET", raising=False)
    os.chdir(REPO_ROOT)

    cfg = get_app_env(dotenv_path=None, strict=True)
    # You can assert exact values if you want, but just asserting presence is safer
    assert isinstance(cfg.SHIPPING_CLIENT_ID, str)
    assert isinstance(cfg.SHIPPING_CLIENT_SECRET, str)
