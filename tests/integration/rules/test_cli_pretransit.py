from pathlib import Path
import json
import pandas as pd
from order_shipping_status import cli
from order_shipping_status.io.paths import derive_output_paths


def run_cli(args): return cli.main(args)


def test_cli_sets_pretransit(tmp_path: Path):
    # input row that should survive preprocessing (prior-week range)
    src = tmp_path / "in.xlsx"
    pd.DataFrame([{
        "X": "drop",
        "Promised Delivery Date": "2025-01-06",
        "Delivery Tracking Status": "in transit",
        "Tracking Number": "123",
        "Carrier Code": "FDX",
    }]).to_excel(src, index=False)

    # replay with label-created vibe
    rdir = tmp_path / "replay"
    rdir.mkdir()
    (rdir / "123.json").write_text(json.dumps({
        "statusByLocale": "Label created",
        "description": "Shipment information sent to FedEx"
    }), encoding="utf-8")

    code = run_cli([str(src), "--no-console", "--reference-date",
                   "2025-01-15", "--replay-dir", str(rdir)])
    assert code == 0

    processed, _ = derive_output_paths(src)
    out = pd.read_excel(processed, sheet_name="Processed", engine="openpyxl")
    assert out.loc[0, "CalculatedStatus"] == "PreTransit"
