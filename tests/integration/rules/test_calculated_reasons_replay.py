from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pytest

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
from order_shipping_status.api.client import ReplayClient
from order_shipping_status.api.normalize import normalize_fedex


@pytest.fixture(scope="module")
def raw_capture_path() -> Path:
    """
    Locate capture file or SKIP (don't fail the suite).
    You can override with:
      OSS_FEDEX_CAPTURE=/abs/path/to/RAW_TransitIssues_10-7-2025_api_bodies.json
    """
    env_path = os.getenv("OSS_FEDEX_CAPTURE")
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p

    repo_candidates = [
        Path("tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json"),
        Path(__file__).resolve().parent.parent.parent
        / "data"
        / "RAW_TransitIssues_10-7-2025_api_bodies.json",
    ]
    external_candidates = [
        Path("/mnt/data/RAW_TransitIssues_10-7-2025_api_bodies.json"),
    ]

    for p in repo_candidates + external_candidates:
        if p.exists():
            return p

    pytest.skip(
        "FedEx capture JSON not found. Set OSS_FEDEX_CAPTURE or place the file at tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json"
    )


def _extract_response_body(env: dict) -> dict:
    """Unwrap common envelope keys to the body containing completeTrackResults."""
    cand = env
    for key in ("output", "body", "response", "data"):
        if isinstance(cand, dict) and key in cand and isinstance(cand[key], dict):
            cand = cand[key]
            break
    return cand


def _map_tracking_to_body(combined_records: list[dict]) -> dict[str, dict]:
    """Build: tracking_number -> unwrapped FedEx response body."""
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
                    mapping[tn] = body
        except Exception:
            continue
    return mapping


class _QuietLogger:
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


def _expected_reasons_row(row: pd.Series) -> str:
    active = []
    # Order must match the mapper's order: PreTransit;Delivered;Exception;ReturnedToSender
    if int(row.get("IsPreTransit", 0)) == 1:
        active.append("PreTransit")
    if int(row.get("IsDelivered", 0)) == 1:
        active.append("Delivered")
    if int(row.get("HasException", 0)) == 1:
        active.append("Exception")
    if int(row.get("IsRTS", 0)) == 1:
        active.append("ReturnedToSender")
    return ";".join(active)


def test_calculated_reasons_matches_indicators(tmp_path: Path, raw_capture_path: Path):
    """
    Run WorkbookProcessor in replay mode (date filter disabled).
    Assert:
      - CalculatedReasons is present and non-empty for at least one row
      - For every processed row, CalculatedReasons exactly equals the join of active indicators
        in the expected order (PreTransit;Delivered;Exception;ReturnedToSender).
    """
    data = json.loads(raw_capture_path.read_text(encoding="utf-8"))
    mapping = _map_tracking_to_body(data)
    if not mapping:
        pytest.skip("Capture did not contain any completeTrackResults entries.")

    # Materialize a small replay set (up to 8 tracking numbers for speed)
    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()
    subset = list(mapping.keys())[:8]
    for tn in subset:
        (replay_dir /
         f"{tn}.json").write_text(json.dumps(mapping[tn]), encoding="utf-8")

    # Build minimal input workbook with those TNs
    rows = [{
        "X": "drop-me",
        "Promised Delivery Date": "N/A",   # ignored; date filter disabled
        "Delivery Tracking Status": "in transit",
        "Tracking Number": tn,
        "Carrier Code": "FDX",
        "RowId": i + 1,
    } for i, tn in enumerate(subset)]
    src = tmp_path / "in.xlsx"
    pd.DataFrame(rows).to_excel(src, index=False)
    out = tmp_path / "in_processed.xlsx"

    # Run processor
    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=ReplayClient(replay_dir),
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=False,
    )
    proc.process(src, out, env_cfg=None)

    # Validate output
    df = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")

    assert "CalculatedReasons" in df.columns, "Missing CalculatedReasons"
    # at least one non-empty
    assert (df["CalculatedReasons"].astype(str) !=
            "").any(), "All CalculatedReasons are empty"

    # Exact match row-by-row against indicators
    for i, row in df.iterrows():
        expected = _expected_reasons_row(row)
        got = str(row["CalculatedReasons"])
        assert got == expected, (
            f"Row {i}: CalculatedReasons mismatch. got={got!r} expected={expected!r} "
            f"Indicators: IsPreTransit={row.get('IsPreTransit')}, "
            f"IsDelivered={row.get('IsDelivered')}, HasException={row.get('HasException')}, "
            f"IsRTS={row.get('IsRTS')}"
        )
