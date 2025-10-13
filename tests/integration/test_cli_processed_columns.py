from pathlib import Path
import pandas as pd

from order_shipping_status import cli
from order_shipping_status.io.paths import derive_output_paths
from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS, OUTPUT_STATUS_COLUMN


def run_cli(args):
    return cli.main(args)


def test_cli_creates_processed_with_expected_columns(tmp_path: Path):
    src = tmp_path / "abc.xlsx"
    # Include a disposable first column so the preprocessor can drop it
    pd.DataFrame([{"X": "drop", "A": 1, "B": 2}]).to_excel(src, index=False)

    code = run_cli([str(src), "--no-console", "--log-level=DEBUG"])
    assert code == 0

    processed, _log = derive_output_paths(src)
    assert processed.exists()

    out = pd.read_excel(processed, sheet_name="Processed", engine="openpyxl")

    # first column was dropped
    assert "X" not in out.columns

    # original (non-first) columns preserved
    for col in ["A", "B"]:
        assert col in out.columns

    # new FedEx columns present
    for col in OUTPUT_FEDEX_COLUMNS:
        assert col in out.columns

    # CalculatedStatus present
    assert OUTPUT_STATUS_COLUMN in out.columns
