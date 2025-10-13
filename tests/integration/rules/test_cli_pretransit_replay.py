from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
from order_shipping_status.api.client import ReplayClient
from order_shipping_status.api.normalize import normalize_fedex
from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS


# ---- Pre-transit tracking numbers provided by you ----
PRETRANSIT_TNS = [
    "393845597098",
    "393793611690",
    "393716079317",
    "393685676789",
    "393674135078",
    "393674951216",
    "393670134330",
    "393670064845",
    "393670030950",
    "393673433397",
    "393640202357",
    "393580256910",
]


@pytest.fixture(scope="module")
def raw_capture_path() -> Path:
    """
    Where to find the combined capture. We try repo path first, then /mnt/data as a convenience.
    """
    candidates = [
        Path("tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json"),
        Path("/mnt/data/RAW_TransitIssues_10-7-2025_api_bodies.json"),
    ]
    for p in candidates:
        if p.exists():
            return p
    pytest.skip(
        "FedEx capture JSON not found. Place it at tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json"
    )


def _extract_response_body(env: dict) -> dict:
    """
    Return the raw FedEx response body dict that contains `completeTrackResults`,
    unwrapping common capture envelope keys like 'output', 'body', 'response', or 'data'.
    """
    cand = env
    for key in ("output", "body", "response", "data"):
        if isinstance(cand, dict) and key in cand and isinstance(cand[key], dict):
            cand = cand[key]
            break
    return cand


def _map_tracking_to_body(combined_records: list[dict]) -> dict[str, dict]:
    """
    Build a dict: tracking_number -> FedEx response body (NOT the outer envelope).
    Expects the body to have: body["completeTrackResults"][*]["trackingNumber"]
    """
    mapping: dict[str, dict] = {}
    for env in combined_records:
        try:
            body = _extract_response_body(env)
            ctr = body.get("completeTrackResults")
            if not (isinstance(ctr, list) and ctr):
                continue
            for r in ctr:
                tn = str(r.get("trackingNumber", "")).strip()
                if tn:
                    mapping[tn] = body  # store the unwrapped body
        except Exception:
            # Skip malformed records quietly
            continue
    return mapping


@pytest.fixture()
def replay_dir_with_pretransit(tmp_path: Path, raw_capture_path: Path) -> Path:
    """
    Creates tmp_path/replay containing <tn>.json for each TN in PRETRANSIT_TNS,
    writing out the *unwrapped* FedEx body so normalize_fedex can parse it.
    """
    data = json.loads(raw_capture_path.read_text(encoding="utf-8"))
    tn_to_body = _map_tracking_to_body(data)

    if not any(tn in tn_to_body for tn in PRETRANSIT_TNS):
        pytest.skip(
            "None of the listed pre-transit TNs were found in the capture file.")

    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()

    missing = []
    for tn in PRETRANSIT_TNS:
        body = tn_to_body.get(tn)
        if body is None:
            missing.append(tn)
            continue
        (replay_dir / f"{tn}.json").write_text(json.dumps(body),
                                               encoding="utf-8")

    if missing:
        print(
            f"[warn] Missing {len(missing)} pretransit TNs not present in capture: "
            f"{missing[:5]}{'...' if len(missing) > 5 else ''}"
        )

    return replay_dir


class _QuietLogger:
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


@pytest.mark.parametrize("batch_size", [3, 6, 12])
def test_pretransit_rows_classified(tmp_path: Path, replay_dir_with_pretransit: Path, batch_size: int):
    """
    Build an input workbook using real capture bodies via ReplayClient.
    We RELAX date filtering at the processor level (enable_date_filter=False), so dates don't matter here.
    Then assert CalculatedStatus == 'PreTransit' (rule output).
    """
    avail = [
        tn for tn in PRETRANSIT_TNS
        if (replay_dir_with_pretransit / f"{tn}.json").exists()
    ]
    if not avail:
        pytest.skip(
            "No pretransit TN files were materialized in replay_dir; skipping.")
    subset = avail[:batch_size]

    # Build input workbook (include disposable first column 'X' so preprocessor drops it)
    rows = [{
        "X": "drop-me",
        "Promised Delivery Date": "N/A",  # ignored because we disable the date filter
        "Delivery Tracking Status": "in transit",
        "Tracking Number": tn,
        "Carrier Code": "FDX",
        "A": idx + 1,
    } for idx, tn in enumerate(subset)]

    src = tmp_path / "in.xlsx"
    pd.DataFrame(rows).to_excel(src, index=False)
    out = tmp_path / "in_processed.xlsx"

    # Run the processor with replay client + normalizer, and DISABLE date filter
    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=ReplayClient(replay_dir_with_pretransit),
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=False,
    )
    proc.process(src, out, env_cfg=None)

    # Assertions
    df = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    for col in OUTPUT_FEDEX_COLUMNS + ["CalculatedStatus"]:
        assert col in df.columns

    # Expect classification result (rules) rather than a specific FedEx code
    for i in range(len(subset)):
        assert df.loc[i, "CalculatedStatus"] == "PreTransit"
        # sanity: enriched columns exist; allow description to be empty
        assert isinstance(df.loc[i, "statusByLocale"], str)


def test_single_pretransit_smoke(tmp_path: Path, replay_dir_with_pretransit: Path):
    tn = "393845597098"
    if not (replay_dir_with_pretransit / f"{tn}.json").exists():
        pytest.skip(
            f"{tn} not present in replay_dir (capture may not include it).")

    src = tmp_path / "in.xlsx"
    pd.DataFrame([{
        "X": "drop",
        "Promised Delivery Date": "N/A",  # ignored because we disable the date filter
        "Delivery Tracking Status": "In Transit",
        "Tracking Number": tn,
        "Carrier Code": "FDX",
    }]).to_excel(src, index=False)
    out = tmp_path / "out.xlsx"

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=ReplayClient(replay_dir_with_pretransit),
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=False,
    )
    proc.process(src, out, env_cfg=None)

    df = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    assert df.loc[0, "CalculatedStatus"] == "PreTransit"
