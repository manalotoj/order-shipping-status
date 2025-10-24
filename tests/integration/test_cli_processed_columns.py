from pathlib import Path
import pandas as pd

from order_shipping_status import cli
from order_shipping_status.io.paths import derive_output_paths
from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS, OUTPUT_STATUS_COLUMN


def run_cli(args):
    return cli.main(args)


# Minimal logger stub used by tests
class Logger:
    def debug(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


def test_cli_creates_processed_with_expected_columns(tmp_path: Path):
    src = tmp_path / "abc.xlsx"
    # Include a disposable first column so the preprocessor can drop it
    pd.DataFrame([{"X": "drop", "A": 1, "B": 2, "latestStatusDetail": {
                 "one": 1, "two": 2}, "Tracking Number": "123456789012"}]).to_excel(src, index=False)

    # Relax date filtering so we don't care about the date window here
    code = run_cli([str(src), "--no-console",
                   "--log-level=DEBUG", "--skip-date-filter"])
    assert code == 0

    processed, _log = derive_output_paths(src)
    assert processed.exists()

    # Reconstruct processed DataFrame from All Shipments so test is independent
    from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
    df_in = pd.read_excel(
        processed, sheet_name="All Shipments", engine="openpyxl")
    proc = WorkbookProcessor(Logger(), reference_date=None)
    df_final = proc._prepare_and_enrich(df_in)

    # original (non-dropped) columns preserved
    for col in ["A", "B"]:
        assert col in df_final.columns

    # new FedEx columns present
    for col in OUTPUT_FEDEX_COLUMNS:
        assert col in df_final.columns

    # CalculatedStatus present
    assert OUTPUT_STATUS_COLUMN in df_final.columns


def test_replay_enrichment_populates_fedex_columns(tmp_path: Path):
    import json
    from types import SimpleNamespace
    from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
    from order_shipping_status.api.client import ReplayClient
    from order_shipping_status.api.normalize import normalize_fedex
    from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS

    # --- Arrange: replay body for a tracking number ---
    tracking = "123456789012"
    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()
    (replay_dir / f"{tracking}.json").write_text(json.dumps({
        "code": "DLV",
        "statusByLocale": "Delivered",
        "description": "Left at front door",
        "latestStatusDetail": {"one": 1, "two": 2},
        "Tracking Number": tracking,
    }), encoding="utf-8")

    # --- Arrange: input workbook with that tracking number (include disposable first column) ---
    src = tmp_path / "in.xlsx"
    pd.DataFrame([{
        "X": "drop",
        "Tracking Number": tracking,
        "Carrier Code": "FDX",
        "A": 1,
        "latestStatusDetail": {"one": 1, "two": 2},
    }]).to_excel(src, index=False)
    out = tmp_path / "in_processed.xlsx"

    # --- Arrange: quiet logger + env ---
    class Logger:
        def debug(self, *a, **k): pass
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
        def error(self, *a, **k): pass

    env = SimpleNamespace(SHIPPING_CLIENT_ID="", SHIPPING_CLIENT_SECRET="")

    # --- Act: run with ReplayClient + normalizer, skip date filter to avoid window logic ---
    proc = WorkbookProcessor(
        Logger(),
        client=ReplayClient(replay_dir),
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=False,
    )
    proc.process(src, out, env)

    # --- Assert: processed sheet has populated FedEx columns for that row ---
    # Reconstruct processed DataFrame and assert enrichment
    df_in = pd.read_excel(out, sheet_name="All Shipments", engine="openpyxl")
    df_proc = proc._prepare_and_enrich(df_in)
    assert df_proc.iloc[0]["statusByLocale"] == "Delivered"
    assert df_proc.iloc[0]["description"] == "Left at front door"

    # And: all expected FedEx columns exist
    for col in OUTPUT_FEDEX_COLUMNS:
        assert col in df_proc.columns
