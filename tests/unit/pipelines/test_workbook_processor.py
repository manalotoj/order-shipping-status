from pathlib import Path
import pandas as pd
import datetime as dt
from types import SimpleNamespace

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor


class Logger:
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


def test_replay_enrichment_populates_fedex_columns(tmp_path: Path):
    import json
    from order_shipping_status.api.client import ReplayClient, normalize_status

    tn = "123456789012"
    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()
    (replay_dir / f"{tn}.json").write_text(json.dumps({
        "code": "DLV",
        "statusByLocale": "Delivered",
        "description": "Left at front door",
    }), encoding="utf-8")

    src = tmp_path / "in.xlsx"
    pd.DataFrame([{
        "X": "drop",
        "Promised Delivery Date": "2025-01-06",
        "Delivery Tracking Status": "in transit",
        "Tracking Number": tn,
        "Carrier Code": "FDX",
        "A": 1,
    }]).to_excel(src, index=False)
    out = tmp_path / "in_processed.xlsx"

    env = SimpleNamespace(SHIPPING_CLIENT_ID="", SHIPPING_CLIENT_SECRET="")
    proc = WorkbookProcessor(Logger(), client=ReplayClient(replay_dir), normalizer=normalize_status,
                             reference_date=dt.date(2025, 1, 15))
    proc.process(src, out, env)

    df = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    # read_excel may return NaN for empty cells; coerce to str for robust assertions
    assert str(df.loc[0, "code"]) == "DLV"
    assert str(df.loc[0, "derivedCode"]) == "DLV"
    assert str(df.loc[0, "statusByLocale"]) == "Delivered"
    assert str(df.loc[0, "description"]) == "Left at front door"


class QL:
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


def test_processor_handles_empty_input(tmp_path: Path):
    src = tmp_path / "empty.xlsx"
    # write an empty sheet
    pd.DataFrame([]).to_excel(src, index=False)
    out = tmp_path / "empty_processed.xlsx"
    env = SimpleNamespace(SHIPPING_CLIENT_ID="", SHIPPING_CLIENT_SECRET="")
    WorkbookProcessor(QL()).process(src, out, env)
    assert out.exists()
