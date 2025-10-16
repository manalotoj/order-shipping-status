from __future__ import annotations

from pathlib import Path
import tempfile
import os
import time


def mk_run_tempdir(prefix: str | None = None, base: str | None = None) -> Path:
    """Create and return a per-run temporary directory.

    - prefix: optional prefix for the directory name (e.g., project name)
    - base: optional base directory to place the temp dir (defaults to tempfile.gettempdir())

    Returns a Path that exists. Caller is responsible for cleanup if desired.
    """
    b = Path(base or os.getenv("OSS_TMP_DIR") or tempfile.gettempdir())
    name = (prefix or "order_shipping_status") + f"_run_{int(time.time())}"
    out = b / name
    out.mkdir(parents=True, exist_ok=True)
    return out
