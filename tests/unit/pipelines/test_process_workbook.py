from pathlib import Path
from types import SimpleNamespace
import pandas as pd
import pytest

from order_shipping_status.pipelines.process_workbook import process_workbook
from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS, OUTPUT_STATUS_COLUMN


class DummyLogger:
    def __init__(self): self.messages = []
    def debug(self, *a, **k): self.messages.append(("DEBUG", a, k))
    def info(self, *a, **k): self.messages.append(("INFO", a, k))
    def warning(self, *a, **k): self.messages.append(("WARNING", a, k))
    def error(self, *a, **k): self.messages.append(("ERROR", a, k))


def test_pass_through_adds_new_columns(tmp_path: Path):
    src = tmp_path / "in.xlsx"
    pd.DataFrame([{"A": 1, "B": 2}]).to_excel(src, index=False)
    out = tmp_path / "in_processed.xlsx"

    result = process_workbook(
        src, out, DummyLogger(),
        SimpleNamespace(SHIPPING_CLIENT_ID="", SHIPPING_CLIENT_SECRET="")
    )

    assert out.exists()

    # Ensure 'Processed' sheet has original + new columns
    df = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    for col in ["A", "B"]:
        assert col in df.columns
    for col in OUTPUT_FEDEX_COLUMNS:
        assert col in df.columns
    assert OUTPUT_STATUS_COLUMN in df.columns

    # Return payload includes columns/shape
    assert "output_cols" in result and "output_shape" in result


def test_non_xlsx_still_writes_processed_and_marker(tmp_path: Path):
    src = tmp_path / "weird.xlsx"
    src.write_text("not actually an xlsx")
    out = tmp_path / "weird_processed.xlsx"

    logger = DummyLogger()
    result = process_workbook(
        src, out, logger,
        SimpleNamespace(SHIPPING_CLIENT_ID="id",
                        SHIPPING_CLIENT_SECRET="secret")
    )

    assert out.exists()
    dfp = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    dfm = pd.read_excel(out, sheet_name="Marker", engine="openpyxl")
    # New columns exist even when input couldn't be read
    for col in OUTPUT_FEDEX_COLUMNS + [OUTPUT_STATUS_COLUMN]:
        assert col in dfp.columns
    # Warned about read failure
    assert any(level == "WARNING" for level, *_ in logger.messages)


def test_missing_input_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        process_workbook(
            tmp_path / "nope.xlsx", tmp_path / "nope_processed.xlsx",
            DummyLogger(), None
        )


def test_replay_enrichment_populates_fedex_columns(tmp_path):
    import json
    from types import SimpleNamespace
    import pandas as pd
    from order_shipping_status.pipelines.process_workbook import process_workbook
    from order_shipping_status.api.client import ReplayClient, normalize_status
    from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS

    # --- Arrange: replay body for a tracking number ---
    tracking = "123456789012"
    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()
    (replay_dir / f"{tracking}.json").write_text(json.dumps({
        "code": "DLV",
        "statusByLocale": "Delivered",
        "description": "Left at front door",
    }), encoding="utf-8")

    # --- Arrange: input workbook with that tracking number ---
    src = tmp_path / "in.xlsx"
    pd.DataFrame([{
        "A": 1,
        "Tracking Number": tracking,
        "Carrier Code": "FDX",
    }]).to_excel(src, index=False)
    out = tmp_path / "in_processed.xlsx"

    # --- Arrange: quiet logger + env ---
    class Logger:
        def debug(self, *a, **k): pass
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
        def error(self, *a, **k): pass
    env = SimpleNamespace(SHIPPING_CLIENT_ID="", SHIPPING_CLIENT_SECRET="")

    # --- Act: run with ReplayClient + normalizer ---
    process_workbook(
        src, out, Logger(), env,
        client=ReplayClient(replay_dir),
        normalizer=normalize_status,
    )

    # --- Assert: processed sheet has populated FedEx columns for that row ---
    df = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    assert df.iloc[0]["code"] == "DLV"
    # mirrors code in current normalizer
    assert df.iloc[0]["derivedCode"] == "DLV"
    assert df.iloc[0]["statusByLocale"] == "Delivered"
    assert df.iloc[0]["description"] == "Left at front door"

    # And: all expected FedEx columns exist
    for col in OUTPUT_FEDEX_COLUMNS:
        assert col in df.columns
