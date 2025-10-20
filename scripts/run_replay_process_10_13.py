#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import sys

from order_shipping_status.api.client import ReplayClient
from order_shipping_status.api.normalize import normalize_fedex
from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
from order_shipping_status.utils.temp import mk_run_tempdir
from order_shipping_status.config.env import load_env, get_app_env


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
    repo_root = Path(__file__).resolve().parents[1]
    src_xlsx = repo_root / "tests" / "data" / "RAW_TransitIssues_10-13-2025.xlsx"
    if not src_xlsx.exists():
        print("Input workbook not found:", src_xlsx)
        raise SystemExit(2)

    capture = repo_root / "tests" / "data" / \
        "RAW_TransitIssues_10-13-2025_api_bodies.json"
    if not capture.exists():
        print("Capture JSON missing:", capture)
        raise SystemExit(2)

    data = json.loads(capture.read_text(encoding="utf-8"))
    mapping = _map_tracking_to_body(data)

    replay_dir = mk_run_tempdir(prefix="order_shipping_status_replay_")
    for tn, body in mapping.items():
        (replay_dir / f"{tn}.json").write_text(json.dumps(body),
                                               encoding="utf-8")

    # Try to load .env into the process env so marker will reflect credentials
    try:
        load_env(repo_root / ".env", override=False)
        env_cfg = get_app_env(strict=False)
    except Exception:
        env_cfg = None

    # Use today's date as reference_now
    now = datetime.now(timezone.utc)

    processor = WorkbookProcessor(
        logger=type("L", (), {"debug": print, "info": print,
                    "warning": print, "error": print})(),
        client=ReplayClient(replay_dir),
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=True,
        stalled_threshold_days=4,
        reference_now=now,
    )

    out_path = src_xlsx.with_name(src_xlsx.stem + "_processed.xlsx")
    print("Processing", src_xlsx, "→", out_path)
    processor.process(src_xlsx, out_path, env_cfg=env_cfg)
    print("Wrote:", out_path)
    print("Replay dir used:", replay_dir)


if __name__ == "__main__":
    main()
