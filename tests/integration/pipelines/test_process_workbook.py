from pathlib import Path
import pandas as pd
import datetime as dt
from types import SimpleNamespace

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor


class QuietLogger:
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


def test_workbook_processor_end_to_end(tmp_path: Path):
    src = tmp_path / "abc.xlsx"
    pd.DataFrame([{
        "X": "drop",
        "Promised Delivery Date": "2025-01-06",
        "Delivery Tracking Status": "in transit",
        "A": 1,
        "Tracking Number": "123456789012",
        "latestStatusDetail": "{}",
    }]).to_excel(src, index=False)
    out = tmp_path / "abc_processed.xlsx"

    env = SimpleNamespace(SHIPPING_CLIENT_ID="", SHIPPING_CLIENT_SECRET="")
    proc = WorkbookProcessor(
        QuietLogger(), reference_date=dt.date(2025, 1, 15))
    result = proc.process(src, out, env)

    assert out.exists()
    # Reconstruct processed DataFrame from input to validate preprocessor effects
    df_in = pd.read_excel(out, sheet_name="All Shipments", engine="openpyxl")
    df_final = proc._prepare_and_enrich(df_in)
    # first column dropped; others preserved
    assert "X" not in df_final.columns and "A" in df_final.columns
    # basic payload sanity
    assert result["output_path"].endswith("abc_processed.xlsx")
