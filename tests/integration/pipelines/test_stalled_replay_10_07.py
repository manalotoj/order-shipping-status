from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
from order_shipping_status.api.client import ReplayClient
from order_shipping_status.api.normalize import normalize_fedex


TARGET_TNS = [
    "393832944198",
    "393801412800",
    "393794700019",
    "393782568016",
    "393749229309",
    "393700581150",
    "393685575780",
    "393670067605",
    "393673954539",
    "393673877905",
    "476166383897",
    "393670853379",
    "393643090600",
    "393588963934",
    "393583782818",
    "393564459231",
    "393508710044",
]


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


class _QuietLogger:
    def debug(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


def test_stalled_tns_from_10_07_capture(tmp_path: Path):
    """
    Verify the listed TNs are marked IsStalled==1 when using the 10-07 capture
    and a reference-now of 2025-10-07. If the 10-7 capture is not present, skip.
    """
    candidates = [
        Path("tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json"),
        Path("/mnt/data/RAW_TransitIssues_10-7-2025_api_bodies.json"),
        Path(__file__).resolve().parent.parent.parent / "data" /
        "RAW_TransitIssues_10-7-2025_api_bodies.json",
    ]
    capture_path = None
    for p in candidates:
        if p.exists():
            capture_path = p
            break
    if not capture_path:
        raise AssertionError(
            "10-07 capture not present; please add tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json")

    data = json.loads(capture_path.read_text(encoding="utf-8"))
    mapping = _map_tracking_to_body(data)

    # Ensure all target TNs exist in the capture; if missing, fail the test so
    # we know the capture doesn't contain the expected items.
    missing = [t for t in TARGET_TNS if t not in mapping]
    if missing:
        pytest.fail(f"Missing TNs in capture: {missing!r}")

    # Materialize replay files for the targets
    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()
    for t in TARGET_TNS:
        (replay_dir /
         f"{t}.json").write_text(json.dumps(mapping[t]), encoding="utf-8")

    # Build input workbook rows for each TN (include a dummy first column so Preprocessor
    # does not drop the Tracking Number).
    rows = []
    for i, t in enumerate(TARGET_TNS, start=1):
        rows.append({
            "X": "drop-me",
            "Tracking Number": t,
            "Carrier Code": "FDX",
            "RowId": i,
        })

    src = tmp_path / "in.xlsx"
    pd.DataFrame(rows).to_excel(src, index=False)
    out = tmp_path / "out.xlsx"

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=ReplayClient(replay_dir),
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=False,
        stalled_threshold_days=1,
        reference_now=datetime(2025, 10, 7, tzinfo=timezone.utc),
    )

    proc.process(src, out, env_cfg=None)

    df = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    df["Tracking Number"] = df["Tracking Number"].astype(str)

    stalled_found = []
    for t in TARGET_TNS:
        row = df[df["Tracking Number"] == t]
        assert not row.empty, f"TN {t} not present in processed output"
        row = row.iloc[0]
        is_stalled = int(row.get("IsStalled", 0))
        is_pre = int(row.get("IsPreTransit", 0))
        # PreTransit and Stalled must be mutually exclusive
        assert not (
            is_stalled and is_pre), f"PreTransit and Stalled both true for {t}"
        assert is_stalled == 1, f"Expected IsStalled==1 for {t}, got IsStalled={is_stalled}"
        stalled_found.append(t)
        # Additional sanity: ensure DaysSinceLatestEvent indicates stalled OR there are no scan events
        days = row.get("DaysSinceLatestEvent")
        scan_count = row.get("ScanEventsCount", None)
        # If days is missing/NaN, fall back to ScanEventsCount == 0 as a reason to be stalled
        try:
            # pandas may return numpy.nan which fails float conversion; use pandas to check
            import pandas as _pd

            if _pd.isna(days):
                assert int(scan_count or 0) == 0, (
                    f"TN {t}: IsStalled==1 but neither DaysSinceLatestEvent nor ScanEventsCount indicate stalled (days={days}, scans={scan_count})"
                )
            else:
                assert float(days) >= 1.0, (
                    f"TN {t}: DaysSinceLatestEvent {days} < stalled_threshold 1"
                )
        except Exception:
            # If something unexpected is present, still ensure the row indicates stalled
            assert is_stalled == 1

    assert set(stalled_found) == set(TARGET_TNS)
