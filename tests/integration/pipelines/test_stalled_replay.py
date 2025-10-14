# tests/integration/pipelines/test_stalled_replay.py
from __future__ import annotations

from pathlib import Path
import json
import pytest
import pandas as pd
import datetime as dt

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
from order_shipping_status.api.client import ReplayClient
from order_shipping_status.api.normalize import normalize_fedex
from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS


class _QuietLogger:
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


# One TN we know is stale in the supplied capture (last scan on 2025-10-03 local)
KNOWN_STALLED_TN = "394090475411"


# -------------------- Capture helpers --------------------

@pytest.fixture(scope="module")
def raw_capture_path() -> Path:
    """
    Provide path to the big capture file. Try project-local first, then /mnt/data.
    Fail fast (assert) so we don’t silently skip when you expect replays to run.
    """
    candidates = [
        Path("tests/data/RAW_TransitIssues_10-13-2025_api_bodies.json"),
        # Path("tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json"),
        # Path("/mnt/data/RAW_TransitIssues_10-7-2025_api_bodies.json"),
        # Path("/mnt/data/RAW_TransitIssues_10-13-2025_api_bodies.json")
    ]
    for p in candidates:
        if p.exists():
            return p
    raise AssertionError(
        "FedEx capture JSON not found. Place it at tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json"
    )


def _iter_capture_items(capture_json: Path) -> list[dict]:
    with capture_json.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return [x for x in data if isinstance(x, dict)] if isinstance(data, list) else []


def _find_output_for_tn(capture_json: Path, tn: str) -> dict | None:
    """
    Return the FedEx `output` node for a given tracking number.
    Searches all completeTrackResults[*] and their trackResults[*].
    """
    target = str(tn).strip()

    for item in _iter_capture_items(capture_json):
        out = item.get("output")
        if not isinstance(out, dict):
            continue

        ctr = out.get("completeTrackResults")
        if not isinstance(ctr, list) or not ctr:
            continue

        # Search every completeTrackResults entry
        for entry in ctr:
            # 1) Top-level trackingNumber (common path)
            top_tn = str(entry.get("trackingNumber") or "").strip()
            if top_tn == target:
                return out

            # 2) Inner: trackResults[*].trackingNumberInfo.trackingNumber
            tr = entry.get("trackResults")
            if isinstance(tr, list):
                for res in tr:
                    tni = (res.get("trackingNumberInfo") or {})
                    inner_tn = str(tni.get("trackingNumber") or "").strip()
                    if inner_tn == target:
                        return out

    return None


def _write_replay_payload(dst_dir: Path, tn: str, output_node: dict) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    (dst_dir / f"{tn}.json").write_text(json.dumps(output_node),
                                        encoding="utf-8")


# -------------------- Fixtures --------------------

@pytest.fixture()
def replay_dir_with_known_stalled(tmp_path: Path, raw_capture_path: Path) -> Path:
    """
    Create a fresh tmp replay dir containing KNOWN_STALLED_TN (must exist in capture).
    """
    out = _find_output_for_tn(raw_capture_path, KNOWN_STALLED_TN)
    assert out is not None, f"TN {KNOWN_STALLED_TN} not found in capture."
    replay_dir = tmp_path / "replay"
    _write_replay_payload(replay_dir, KNOWN_STALLED_TN, out)
    return replay_dir


# -------------------- Tests --------------------

def test_stalled_rows_set_indicator(tmp_path: Path, replay_dir_with_known_stalled: Path):
    """
    End-to-end: run the processor in replay mode and assert IsStalled == 1
    for the known stalled TN as of 2025-10-06Z with a lenient threshold.
    """
    src = tmp_path / "in.xlsx"
    out = tmp_path / "in_processed.xlsx"

    pd.DataFrame([{
        "X": "drop-me",
        "Promised Delivery Date": "N/A",  # ignored; date filter disabled
        "Delivery Tracking Status": "in transit",
        "Tracking Number": KNOWN_STALLED_TN,
        "Carrier Code": "FDX",
        "RowId": 1,
    }]).to_excel(src, index=False)

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=ReplayClient(replay_dir_with_known_stalled),
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=False,
        stalled_threshold_days=1,   # very forgiving
        reference_now=dt.datetime(2025, 10, 13, tzinfo=dt.timezone.utc),
    )
    proc.process(src, out, env_cfg=None)

    df = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")

    # Contract columns present
    for col in OUTPUT_FEDEX_COLUMNS + ["CalculatedStatus", "CalculatedReasons"]:
        assert col in df.columns
    for indicator in ("IsPreTransit", "IsDelivered", "HasException", "IsRTS", "IsStalled"):
        assert indicator in df.columns

    # Metrics
    assert "LatestEventTimestampUtc" in df.columns
    assert "DaysSinceLatestEvent" in df.columns

    # Should be stalled given reference_now and threshold
    assert int(df.loc[0, "IsStalled"]) == 1


def test_single_stalled_smoke(tmp_path: Path, replay_dir_with_known_stalled: Path):
    """
    Single-row smoke test using the same known stalled TN.
    """
    src = tmp_path / "in.xlsx"
    out = tmp_path / "in_processed.xlsx"

    pd.DataFrame([{
        "X": "drop-me",
        "Promised Delivery Date": "N/A",
        "Delivery Tracking Status": "in transit",
        "Tracking Number": KNOWN_STALLED_TN,
        "Carrier Code": "FDX",
    }]).to_excel(src, index=False)

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=ReplayClient(replay_dir_with_known_stalled),
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=False,
        stalled_threshold_days=1,
        reference_now=dt.datetime(2025, 10, 13, tzinfo=dt.timezone.utc),
    )
    proc.process(src, out, env_cfg=None)

    df = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    assert "IsStalled" in df.columns
    assert int(df.loc[0, "IsStalled"]) == 1
