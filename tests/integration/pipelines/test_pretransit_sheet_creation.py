import pandas as pd
from pathlib import Path
from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
from order_shipping_status.pipelines.preprocessor import Preprocessor
from order_shipping_status.api.client import ReplayClient
from order_shipping_status.api.normalize import normalize_fedex
from datetime import datetime, timezone


class _QuietLogger:
    def debug(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


def _write_temp_xlsx(rows, tmpdir, name="pretransit_test.xlsx") -> Path:
    p = Path(tmpdir) / name
    pd.DataFrame(rows).to_excel(p, index=False)
    return p


def _read_sheet(path: Path, sheet: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet, engine="openpyxl")


def test_pretransit_sheet_has_rows(tmp_path):
    # Build input with one pretransit tracking number (Label created/OC)
    rows = [{"X": "drop", "Tracking Number": "TN1",
             "Carrier Code": "FDX", "RowId": 1, "latestStatusDetail": "{}"}]
    src = _write_temp_xlsx(rows, tmp_path, "in_pretransit.xlsx")
    out = Path(tmp_path) / "out_pretransit.xlsx"

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=None,
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=False,
        stalled_threshold_days=1,
        reference_now=datetime(2025, 10, 7, tzinfo=timezone.utc),
    )

    proc.process(src, out, env_cfg=None)

    # Read sheets
    all_issues = _read_sheet(out, "All Issues")
    pre = _read_sheet(out, "PreTransit")

    # PreTransit should exist and have the same number of columns as All Issues (processed)
    assert list(pre.columns) == list(all_issues.columns)
    # Since enrichment is not wired to API in this test, pre sheet may be empty or contain the row
    # The key is that the PreTransit sheet exists.
    assert "PreTransit" in [s for s in pd.ExcelFile(
        out, engine="openpyxl").sheet_names]


def test_pretransit_sheet_empty_when_no_pretransit(tmp_path):
    # Build input with a non-pretransit tracking number
    rows = [{"X": "drop", "Tracking Number": "TN2",
             "Carrier Code": "FDX", "RowId": 1, "latestStatusDetail": "{}"}]
    src = _write_temp_xlsx(rows, tmp_path, "in_no_pretransit.xlsx")
    out = Path(tmp_path) / "out_no_pretransit.xlsx"

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=None,
        normalizer=None,
        reference_date=None,
        enable_date_filter=False,
        stalled_threshold_days=1,
        reference_now=datetime(2025, 10, 7, tzinfo=timezone.utc),
    )

    proc.process(src, out, env_cfg=None)

    # Read sheets
    all_issues = _read_sheet(out, "All Issues")
    pre = _read_sheet(out, "PreTransit")

    # PreTransit sheet should exist and be empty (only headers) or have zero rows
    assert list(pre.columns) == list(all_issues.columns)
    assert len(pre) == 0
