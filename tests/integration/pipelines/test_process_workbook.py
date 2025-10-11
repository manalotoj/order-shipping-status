from pathlib import Path
import pandas as pd

from order_shipping_status.pipelines.process_workbook import process_workbook
from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS, OUTPUT_STATUS_COLUMN


class QuietLogger:
    def info(self, *a, **k): pass
    def debug(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


def test_pipeline_writes_processed_and_marker(tmp_path: Path):
    src = tmp_path / "abc.xlsx"
    pd.DataFrame([{"x": 1}]).to_excel(src, index=False)
    out = tmp_path / "abc_processed.xlsx"

    result = process_workbook(src, out, QuietLogger(), None)

    assert out.exists()
    # Marker sheet still present (backwards compat with earlier tests)
    marker = pd.read_excel(out, sheet_name="Marker", engine="openpyxl")
    assert marker.loc[0, "_oss_marker"] == "ok"
    assert result["output_path"].endswith("abc_processed.xlsx")

    # Processed sheet has required new columns
    processed = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    for col in OUTPUT_FEDEX_COLUMNS + [OUTPUT_STATUS_COLUMN]:
        assert col in processed.columns


def test_pipeline_handles_representative_columns(tmp_path: Path):
    cols = [
        "Order ID", "Order Line ID", "Item ID", "Item Short Description",
        "Ship From Location ID", "Ship to Location ID", "Status",
        "Order Created Time", "Released Time", "Fulfilled Time",
        "Release Line Updated Ts Time", "Processing Sla Dt Time", "Store Picked Time",
        "Promised Delivery Date", "Delivered Time",
        "Tracking Number", "Carrier Code", "Delivery Tracking Status",
        "Total Ordered Amt", "Total Ordered Units",
    ]
    src = tmp_path / "rep.xlsx"
    pd.DataFrame([{c: None for c in cols}]).to_excel(src, index=False)
    out = tmp_path / "rep_processed.xlsx"

    process_workbook(src, out, QuietLogger(), None)

    assert out.exists()
    processed = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    # Original columns preserved
    for c in cols:
        assert c in processed.columns
