from __future__ import annotations

from pathlib import Path
from typing import Any, Optional
import datetime as dt

import pandas as pd

from order_shipping_status.models import EnvCfg


def process_workbook(
    input_path: Path,
    processed_path: Path,
    logger,  # typing: logging.Logger, but keep loose here to avoid import cycle in stub
    env_cfg: Optional[EnvCfg] = None,
) -> dict[str, Any]:
    """
    STUB IMPLEMENTATION:
      - Reads the input Excel just to prove we can open it.
      - Writes a new Excel at `processed_path` containing a single 'Marker' sheet
        with minimal metadata. No classification rules yet.

    Returns a small dict with a few facts for test assertions.
    """
    input_path = Path(input_path)
    processed_path = Path(processed_path)

    if not input_path.exists():
        logger.error("Input file does not exist: %s", input_path)
        raise FileNotFoundError(input_path)

    # Try to read (no dependency on actual columns/sheets);
    # if it fails, we still continue to prove write-path works.
    # Reading is best-effort so we don't block e2e tests on input shape.
    try:
        # Read only first sheet quickly; if empty, this will still succeed with an empty DF.
        _ = pd.read_excel(input_path, sheet_name=0, engine="openpyxl")
        logger.debug("Opened input workbook: %s", input_path.name)
    except Exception as ex:
        logger.warning("Could not read input workbook (%s): %s",
                       input_path.name, ex)

    # Build a tiny marker dataframe
    now_utc = dt.datetime.now(dt.timezone.utc).isoformat()
    has_creds = bool(
        getattr(env_cfg, "SHIPPING_CLIENT_ID", "") and
        getattr(env_cfg, "SHIPPING_CLIENT_SECRET", "")
    )

    marker = pd.DataFrame(
        [{
            "_oss_marker": "ok",
            "input_name": input_path.name,
            "input_dir": str(input_path.parent),
            "output_name": processed_path.name,
            "timestamp_utc": now_utc,
            "env_has_creds": has_creds,
        }]
    )

    # Write one-sheet Excel with our marker
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(processed_path, engine="openpyxl", mode="w") as xw:
        marker.to_excel(xw, sheet_name="Marker", index=False)

    logger.info("Wrote stub processed workbook → %s", processed_path)

    return {
        "output_path": str(processed_path),
        "env_has_creds": has_creds,
        "timestamp_utc": now_utc,
    }
