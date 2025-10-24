from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
from order_shipping_status.api.client import ReplayClient
from order_shipping_status.api.normalize import normalize_fedex


import json as _json

_fixtures_path = Path(__file__).resolve().parent / "stalled_snapshots.json"
SNAPSHOTS = _json.loads(_fixtures_path.read_text(encoding="utf-8"))


def _extract_response_body(env: dict) -> dict:
    cand = env
    for key in ("output", "body", "response", "data"):
        if isinstance(cand, dict) and key in cand and isinstance(cand[key], dict):
            cand = cand[key]
            break
    return cand


def _map_tracking_to_body(combined_records: list[dict]) -> dict[str, dict]:
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


@pytest.mark.parametrize("tn", list(SNAPSHOTS.keys()))
def test_tn_snapshot_matches(tmp_path: Path, tn: str):
    candidate = Path("tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json")
    alt = Path(__file__).resolve().parent.parent.parent / "data" / \
        "RAW_TransitIssues_10-7-2025_api_bodies.json"
    if not candidate.exists() and not alt.exists():
        raise AssertionError(
            "Capture not present; add tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json to run regression snapshot test"
        )
    if not candidate.exists():
        candidate = alt

    data = json.loads(candidate.read_text(encoding="utf-8"))
    mapping = _map_tracking_to_body(data)
    if tn not in mapping:
        pytest.fail(f"TN {tn} not present in capture; cannot assert snapshot")

    # materialize a single-file combined replay and input workbook
    replay_file = tmp_path / "replay.json"
    replay_file.write_text(json.dumps([mapping[tn]]), encoding="utf-8")

    rows = [{"X": "drop-me", "Tracking Number": tn,
             "Carrier Code": "FDX", "RowId": 1, "latestStatusDetail": {"one": 1, "two": 2}, }]
    src = tmp_path / "in.xlsx"
    pd.DataFrame(rows).to_excel(src, index=False)
    out = tmp_path / "out.xlsx"

    class _QuietLogger:
        def debug(self, *a, **k): pass
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
        def error(self, *a, **k): pass

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=ReplayClient(replay_file),
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=False,
        stalled_threshold_days=1,
        reference_now=datetime(2025, 10, 7, tzinfo=timezone.utc),
    )

    proc.process(src, out, env_cfg=None)

    # The processor no longer writes a 'Processed' sheet; use 'All Issues'
    df = pd.read_excel(out, sheet_name="All Issues", engine="openpyxl")
    df["Tracking Number"] = df["Tracking Number"].astype(str)
    row = df[df["Tracking Number"] == tn].iloc[0]

    expected = SNAPSHOTS[tn]
    got_days = None if pd.isna(row.get("DaysSinceLatestEvent")) else float(
        row.get("DaysSinceLatestEvent"))
    got_reasons = str(row.get("CalculatedReasons", ""))

    assert got_days == expected["DaysSinceLatestEvent"], f"TN {tn}: DaysSinceLatestEvent mismatch: got={got_days} expected={expected['DaysSinceLatestEvent']}"
    assert got_reasons == expected["CalculatedReasons"], f"TN {tn}: CalculatedReasons mismatch: got={got_reasons} expected={expected['CalculatedReasons']}"
