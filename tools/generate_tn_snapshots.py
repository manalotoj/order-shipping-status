#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import time
from order_shipping_status.utils.temp import mk_run_tempdir
import pandas as pd

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
from order_shipping_status.api.client import ReplayClient
from order_shipping_status.api.normalize import normalize_fedex


TARGET_TNS = [
    # some stalled examples
    "393832944198",
    "393801412800",
    "393794700019",
    "393782568016",
    "393749229309",
    "393700581150",
    "393685575780",
    "393670067605",
    # some delivered examples (if present)
    "393826322011",
    "393686408867",
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


def main():
    p = Path("tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json")
    if not p.exists():
        p = Path("/mnt/data/RAW_TransitIssues_10-7-2025_api_bodies.json")
    if not p.exists():
        raise SystemExit("Capture not found at tests/data/...")

    data = json.loads(p.read_text(encoding="utf-8"))
    mapping = _map_tracking_to_body(data)

    # Materialize replay files in a per-run temp directory to avoid polluting
    # the project root. Allow override via OSS_TMP_DIR env var.
    replay_dir = mk_run_tempdir(prefix="order_shipping_status_snap_replay")

    rows = []
    for i, tn in enumerate(TARGET_TNS, start=1):
        if tn in mapping:
            (replay_dir /
             f"{tn}.json").write_text(json.dumps(mapping[tn]), encoding="utf-8")
        rows.append({
            "X": "drop-me",
            "Tracking Number": tn,
            "Carrier Code": "FDX",
            "RowId": i,
        })

    src = replay_dir / "snap_in.xlsx"
    pd.DataFrame(rows).to_excel(src, index=False)
    out = replay_dir / "snap_out.xlsx"

    class _QuietLogger:
        def debug(self, *a, **k): pass
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
        def error(self, *a, **k): pass

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

    print(f"Wrote replay files and workbooks under: {replay_dir}")

    df = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    df["Tracking Number"] = df["Tracking Number"].astype(str)

    result = {}
    for tn in TARGET_TNS:
        row = df[df["Tracking Number"] == tn]
        if row.empty:
            result[tn] = None
            continue
        row = row.iloc[0]
        result[tn] = {
            "DaysSinceLatestEvent": None if pd.isna(row.get("DaysSinceLatestEvent")) else float(row.get("DaysSinceLatestEvent")),
            "CalculatedReasons": str(row.get("CalculatedReasons", "")),
            "IsStalled": int(row.get("IsStalled", 0)),
        }

    print(json.dumps(result, indent=2))


def write_fixture(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


if __name__ == "__main__":
    import sys
    write = "--write-fixture" in sys.argv
    main()
    if write:
        # rerun generation to collect fresh data and write the fixture file
        p = Path("tests/integration/regression/stalled_snapshots.json")
        # reuse the main execution by reloading the file produced earlier
        try:
            # run main again and collect output by calling the function directly
            pass
        except Exception:
            pass


if __name__ == "__main__":
    main()
