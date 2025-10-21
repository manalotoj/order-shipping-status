from pathlib import Path
import pandas as pd
import json
from order_shipping_status import cli
from order_shipping_status.io.paths import derive_output_paths


def run_cli(args): return cli.main(args)


def test_cli_reference_date_filters(tmp_path: Path):
    src = tmp_path / "d.xlsx"
    pd.DataFrame([
        {"X": "x", "Promised Delivery Date": "2025-01-05",
            "Delivery Tracking Status": "in transit"},  # keep
        {"X": "x", "Promised Delivery Date": "2025-01-12",
            "Delivery Tracking Status": "in transit"},  # drop
    ]).to_excel(src, index=False)
    code = run_cli([str(src), "--no-console",
                   "--reference-date", "2025-01-15"])
    assert code == 0
    processed, _ = derive_output_paths(src)
    # The pipeline no longer writes a full 'Processed' sheet; check the
    # filtered 'All Issues' sheet which reflects the post-processed subset.
    out = pd.read_excel(processed, sheet_name="All Issues", engine="openpyxl")
    assert len(out) == 1


def test_cli_invalid_reference_date_returns_2(tmp_path: Path):
    src = tmp_path / "d.xlsx"
    pd.DataFrame([{"A": 1}]).to_excel(src, index=False)
    code = run_cli([str(src), "--no-console",
                   "--reference-date", "not-a-date"])
    assert code == 2


def test_cli_replay_populates_columns(tmp_path: Path):
    tn = "123"
    src = tmp_path / "in.xlsx"
    pd.DataFrame([{"X": "x", "Promised Delivery Date": "2025-01-06", "Delivery Tracking Status": "in transit",
                   "Tracking Number": tn, "Carrier Code": "FDX"}]).to_excel(src, index=False)
    rdir = tmp_path / "replay"
    rdir.mkdir()
    # Use a non-delivered code so the enriched row appears in the 'All Issues' sheet
    (rdir / f"{tn}.json").write_text(json.dumps({"code": "OC",
                                                 "statusByLocale": "Label created", "description": "ok"}), encoding="utf-8")
    code = run_cli([str(src), "--no-console", "--reference-date",
                   "2025-01-15", "--replay-dir", str(rdir)])
    assert code == 0
    processed, _ = derive_output_paths(src)
    out = pd.read_excel(processed, sheet_name="All Issues", engine="openpyxl")
    assert out.loc[0, "code"] == "OC"
