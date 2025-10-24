from __future__ import annotations

from datetime import date
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
    Prefer the 10-13 capture file for stalled-replay tests. Allow an override
    via OSS_FEDEX_CAPTURE. If not present, fail the test so missing fixtures
    are visible instead of silently skipping.
    """
    env_path = os.getenv("OSS_FEDEX_CAPTURE")
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p
        # Env var was explicitly set but file missing: fail hard so config issues are visible
        raise AssertionError(
            f"OSS_FEDEX_CAPTURE is set but file not found: {env_path}")

    # Resolve candidates relative to the repository root (this file lives under
    # tests/integration/pipelines). This makes the fixture robust when pytest
    # changes the current working directory during collection/execution.
    repo_root = Path(__file__).resolve().parents[3]
    candidates = [
        repo_root / "tests" / "data" / "RAW_TransitIssues_10-13-2025_api_bodies.json",
        Path("/mnt/data/RAW_TransitIssues_10-13-2025_api_bodies.json"),
    ]
    for p in candidates:
        if p.exists():
            return p

    # Also look for any RAW_TransitIssues_*.json under tests/data as a last resort
    data_dir = repo_root / "tests" / "data"
    if data_dir.exists() and data_dir.is_dir():
        for match in sorted(data_dir.glob("RAW_TransitIssues_*.json")):
            if match.exists():
                return match

    # No env var set and no local captures found: fail so missing fixtures are visible
    raise AssertionError(
        "FedEx capture JSON not found. Place one of the RAW_TransitIssues JSONs under tests/data/ or set OSS_FEDEX_CAPTURE to point to the file."
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
    def debug(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


def test_stalled_replay_end_to_end(tmp_path: Path, raw_capture_path: Path):
    """
    Run WorkbookProcessor in replay mode for the known stalled TN (394090475411)
    from the 10-13 capture file. Assert the TN is present and marked Stalled.
    """
    # Use a TN from the capture that actually has its latest event >= 1 day ago
    STALLED_TN = "393911277600"

    data = json.loads(raw_capture_path.read_text(encoding="utf-8"))
    mapping = _map_tracking_to_body(data)
    if STALLED_TN not in mapping:
        raise AssertionError(f"Stalled TN {STALLED_TN} not present in capture")

    # Materialize a single-file combined replay containing the stalled TN
    replay_file = tmp_path / "replay.json"
    replay_file.write_text(json.dumps([mapping[STALLED_TN]]), encoding="utf-8")

    # Build minimal input workbook with that TN
    rows = [{
        "X": "drop-me",
        "Promised Delivery Date": "N/A",
        "Delivery Tracking Status": "in transit",
        "Tracking Number": STALLED_TN,
        "Carrier Code": "FDX",
        "RowId": 1,
        "latestStatusDetail": {"one": 1, "two": 2},
    }]
    src = tmp_path / "in.xlsx"
    pd.DataFrame(rows).to_excel(src, index=False)
    out = tmp_path / "in_processed.xlsx"

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=ReplayClient(replay_file),
        normalizer=normalize_fedex,
        reference_date=date(2025, 10, 13),
        enable_date_filter=False,
        stalled_threshold_days=1,
    )
    proc.process(src, out, env_cfg=None)

    # Reconstruct processed DataFrame via WorkbookProcessor helper
    df_input = pd.read_excel(
        out, sheet_name="All Shipments", engine="openpyxl")
    df_final = proc._prepare_and_enrich(df_input)

    # Find the row for the TN
    df_final["Tracking Number"] = df_final["Tracking Number"].astype(str)
    assert STALLED_TN in list(
        df_final["Tracking Number"]), "Expected TN not in processed output"
    row = df_final[df_final["Tracking Number"] == STALLED_TN].iloc[0]

    # Indicators/status assertions
    assert int(row.get("IsStalled", 0)
               ) == 1, "Expected IsStalled==1 for stalled TN"
    # CalculatedReasons should include 'Stalled' as the first reason
    reasons = str(row.get("CalculatedReasons", "")).split(";")
    assert reasons[
        0] == "Stalled", f"CalculatedReasons did not start with 'Stalled': {reasons!r}"


# end of file
