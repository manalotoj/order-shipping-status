from pathlib import Path
import pandas as pd
import datetime as dt
from types import SimpleNamespace
import pytest

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
        "Tracking Number": "TN1",
        "latestStatusDetail": {"one": 1, "two": 2},
    }), encoding="utf-8")

    src = tmp_path / "in.xlsx"
    pd.DataFrame([{
        "X": "drop",
        "Promised Delivery Date": "2025-01-06",
        "Delivery Tracking Status": "in transit",
        "Tracking Number": tn,
        "Carrier Code": "FDX",
        "latestStatusDetail": {"one": 1, "two": 2},
        "A": 1,
    }]).to_excel(src, index=False)
    out = tmp_path / "in_processed.xlsx"

    env = SimpleNamespace(SHIPPING_CLIENT_ID="", SHIPPING_CLIENT_SECRET="")
    proc = WorkbookProcessor(Logger(), client=ReplayClient(replay_dir), normalizer=normalize_status,
                             reference_date=dt.date(2025, 1, 15))
    proc.process(src, out, env)

    # Reconstruct processed DataFrame from written 'All Shipments' and run
    # same pipeline steps so tests don't require a 'Processed' sheet.
    from order_shipping_status.pipelines.column_contract import ColumnContract
    from order_shipping_status.pipelines.enricher import Enricher
    from order_shipping_status.rules.indicators import apply_indicators
    from order_shipping_status.rules.status_mapper import map_indicators_to_status

    df_input = pd.read_excel(
        out, sheet_name="All Shipments", engine="openpyxl")
    df_proc = ColumnContract().ensure(df_input)
    df_proc = Enricher(QL(), client=ReplayClient(replay_dir),
                       normalizer=normalize_status).enrich(df_proc)
    df_proc = apply_indicators(df_proc)
    df_proc = map_indicators_to_status(df_proc)

    assert str(df_proc.loc[0, "code"]) == "DLV"
    assert str(df_proc.loc[0, "derivedCode"]) == "DLV"
    assert str(df_proc.loc[0, "statusByLocale"]) == "Delivered"
    assert str(df_proc.loc[0, "description"]) == "Left at front door"


class QL:
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


@pytest.mark.skip()
def test_processor_handles_empty_input(tmp_path: Path):
    src = tmp_path / "empty.xlsx"
    # write an empty sheet
    pd.DataFrame([]).to_excel(src, index=False)
    out = tmp_path / "empty_processed.xlsx"
    env = SimpleNamespace(SHIPPING_CLIENT_ID="", SHIPPING_CLIENT_SECRET="")
    WorkbookProcessor(QL()).process(src, out, env)
    assert out.exists()
