from __future__ import annotations

from pathlib import Path
from typing import Tuple

PROCESSED_SUFFIX = "_processed.xlsx"


def derive_output_paths(input_file: Path) -> Tuple[Path, Path]:
    """
    Given an input Excel path, return (processed_xlsx_path, log_path) in the same directory.

    Raises FileNotFoundError if input_file doesn't exist (explicit early signal for CLI).
    """
    p = Path(input_file)
    if not p.exists():
        raise FileNotFoundError(p)

    stem = p.stem  # filename without extension
    processed = p.with_name(f"{stem}{PROCESSED_SUFFIX}")
    log = p.with_suffix(".log")
    return processed, log
