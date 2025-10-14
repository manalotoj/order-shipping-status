from __future__ import annotations
import datetime as dt
from pathlib import Path

import pandas as pd

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor


class _QuietLogger:
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


def test_days_since_latest_event_computed(tmp_path: Path):
    """
    WorkbookProcessor should compute DaysSinceLatestEvent from LatestEventTimestampUtc.
    We disable date filtering so rows aren't dropped by the preprocessor.
    """
    # Build deterministic timestamps
    now_utc = dt.datetime.now(dt.timezone.utc)
    five_days_ago = (now_utc - dt.timedelta(days=5)).isoformat()

    # Minimal input with first column to be dropped by preprocessor
    df = pd.DataFrame([
        {"X": "drop", "LatestEventTimestampUtc": five_days_ago},  # -> 5
        {"X": "drop", "LatestEventTimestampUtc": "not-a-date"},   # -> 0 fallback
        {"X": "drop", "LatestEventTimestampUtc": None},           # -> 0 fallback
    ])

    src = tmp_path / "in.xlsx"
    out = tmp_path / "in_processed.xlsx"
    df.to_excel(src, index=False)

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=None,
        normalizer=None,
        reference_date=None,
        enable_date_filter=False,  # <— important for test stability
    )
    proc.process(src, out, env_cfg=None)

    result = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    assert "DaysSinceLatestEvent" in result.columns

    # Row 0 ≈ 5 days; integer days should be exact
    assert int(result.loc[0, "DaysSinceLatestEvent"]) == 5
    # Invalid/missing -> 0 by contract
    assert int(result.loc[1, "DaysSinceLatestEvent"]) == 0
    assert int(result.loc[2, "DaysSinceLatestEvent"]) == 0
