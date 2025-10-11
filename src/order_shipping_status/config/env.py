# src/order_shipping_status/config/env.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, Dict

from order_shipping_status.models import EnvCfg

try:
    # De facto standard for .env files
    from dotenv import load_dotenv, find_dotenv  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "Missing dependency 'python-dotenv'. Install it with:\n"
        "  pip install python-dotenv"
    ) from e


# --- Public contract ---------------------------------------------------------

class EnvError(RuntimeError):
    """Raised when required environment variables are missing."""


# Expand as your app grows
REQUIRED_KEYS: Tuple[str, ...] = (
    "SHIPPING_CLIENT_ID",
    "SHIPPING_CLIENT_SECRET",
)


def load_project_dotenv(start: Optional[Path] = None, *, override: bool = False) -> Path:
    """
    Load variables from the nearest `.env` file (searching upward from `start` or CWD).
    Does NOT override existing env vars unless `override=True`.
    Returns the resolved Path to the .env file if found; otherwise Path().
    """
    start_path = Path.cwd() if start is None else Path(start)

    # Use python-dotenv's search first
    dotenv_str = find_dotenv(filename=".env", usecwd=True)
    dotenv_path = Path(dotenv_str) if dotenv_str else Path()

    # If python-dotenv didn’t find anything, do a manual upward search from `start`
    if not dotenv_str:
        for p in (start_path, *start_path.parents):
            candidate = p / ".env"
            if candidate.exists():
                dotenv_path = candidate
                break

    # If we still didn’t find a file, bail
    if not dotenv_path.exists() or dotenv_path.is_dir():
        return Path()

    # Now load the actual file
    load_dotenv(dotenv_path=dotenv_path, override=override)
    return dotenv_path.resolve()


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Convenience accessor mirroring os.getenv."""
    return os.getenv(name, default)


def get_required_env(name: str) -> str:
    """Fetch a required env var or raise a helpful error."""
    value = os.getenv(name)
    if not value:
        raise EnvError(f"Missing required environment variable: {name}")
    return value


def env(name: str, *, default: Optional[str] = None, required: bool = False, cast=None):
    """
    Test-friendly accessor.

    - If `required=True` and var is missing, raise KeyError(name).
    - If `cast` is provided, apply it to the raw string and propagate cast errors.
    - Returns `default` when missing and not required.
    """
    raw = os.getenv(name)
    if raw is None:
        if required:
            raise KeyError(name)
        return default

    if cast is not None:
        return cast(raw)
    return raw


# --- Internal helpers --------------------------------------------------------

def _parse_dotenv_lines(text: str) -> Dict[str, str]:
    """
    Parse .env-style content into a dict. Supports:
    - leading 'export '
    - inline comments after a value ('value # comment')
    - quoted values
    - blank lines and full-line comments
    """
    out: Dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        # Drop inline comments (only when preceded by a space to avoid hashes in secrets)
        if " #" in v:
            v = v.split(" #", 1)[0]
        v = v.strip().strip('"').strip("'")
        out[k.strip()] = v
    return out


# --- Main loader APIs --------------------------------------------------------

def load_env(
    dotenv_path: Optional[Path] = None,
    *,
    override: bool = False,
    required_keys: Tuple[str, ...] = (),
    strict: bool = False,
) -> Dict[str, str]:
    """
    Load env vars from a .env file into the process environment and return a dict
    of key/value pairs found in that file.

    - If `dotenv_path` is provided, load exactly that file.
    - Otherwise, auto-discover the nearest .env via `load_project_dotenv`.
    - If `strict=True` and `required_keys` are provided, ensure they are present
      in `os.environ` after loading; otherwise raise EnvError.
    - `override` controls whether .env values replace existing process env values.
    """
    loaded: Dict[str, str] = {}

    if dotenv_path:
        path = Path(dotenv_path)
        if path.exists():
            load_dotenv(dotenv_path=path, override=override)
            loaded = _parse_dotenv_lines(path.read_text(encoding="utf-8"))
    else:
        path = load_project_dotenv(override=override)
        if path and path.exists():
            # Already loaded into os.environ by python-dotenv
            loaded = _parse_dotenv_lines(path.read_text(encoding="utf-8"))

    if strict and required_keys:
        missing = [k for k in required_keys if not os.getenv(k)]
        if missing:
            raise EnvError(
                f"Missing required environment variable(s): {', '.join(missing)}")

    return loaded


@dataclass(frozen=True)
class AppEnv:
    SHIPPING_CLIENT_ID: str
    SHIPPING_CLIENT_SECRET: str


def get_app_env(dotenv_path: Path | str | None = ".env", *, strict: bool = True) -> EnvCfg:
    """
    Load application-required variables and return a typed config object.

    - `dotenv_path` may be a Path/str pointing to a specific .env file or None to
      disable file loading (useful for tests).
    - By default does not override existing process env (prefers CI/host settings).
    - When `strict=True` this validates REQUIRED_KEYS and raises on missing values.
    """
    load_env(
        Path(dotenv_path) if dotenv_path else None,
        override=False,
        required_keys=REQUIRED_KEYS,
        strict=strict,
    )

    # At this point, values must exist in the environment.
    return EnvCfg(
        SHIPPING_CLIENT_ID=os.environ["SHIPPING_CLIENT_ID"],
        SHIPPING_CLIENT_SECRET=os.environ["SHIPPING_CLIENT_SECRET"],
    )


__all__ = [
    "EnvError",
    "REQUIRED_KEYS",
    "load_project_dotenv",
    "load_env",
    "get_env",
    "get_required_env",
    "env",
    "AppEnv",
    "get_app_env",
]
