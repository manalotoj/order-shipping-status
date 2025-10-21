from __future__ import annotations
import pandas as pd
from pathlib import Path

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor


class _QuietLogger:
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


def test_stalled_synthetic_end_to_end(tmp_path: Path):
    src = tmp_path / "in.xlsx"
    out = tmp_path / "in_processed.xlsx"
    # Use LatestEventTimestampUtc far in the past; processor computes DaysSinceLatestEvent
    df = pd.DataFrame(
        [{"X": "drop", "LatestEventTimestampUtc": "2000-01-01T00:00:00Z"}])
    df.to_excel(src, index=False)

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=None,
        normalizer=None,
        reference_date=None,
        enable_date_filter=False,
        stalled_threshold_days=4,
    )
    proc.process(src, out, env_cfg=None)

    # Reconstruct processed DataFrame via WorkbookProcessor helper
    df_input = pd.read_excel(
        out, sheet_name="All Shipments", engine="openpyxl")
    df_final = proc._prepare_and_enrich(df_input)

    assert int(df_final.loc[0, "IsStalled"]) == 1
    assert df_final.loc[0, "CalculatedStatus"] in (
        "Stalled", "Exception", "ReturnedToSender", "Delivered", "PreTransit")
