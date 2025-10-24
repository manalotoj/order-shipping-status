# tests/integration/rules/test_cli_pretransit_replay.py
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pytest

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
from order_shipping_status.api.client import ReplayClient
from order_shipping_status.api.normalize import normalize_fedex
from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS
from order_shipping_status.rules.classifier import classify_row_pretransit

# Provided pre-transit tracking numbers (your list)
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
    Locate capture file or SKIP (don't fail suite).
    Override with env OSS_FEDEX_CAPTURE=/abs/path/to/RAW_TransitIssues_10-7-2025_api_bodies.json
    """
    # 1) Env override
    env_path = os.getenv("OSS_FEDEX_CAPTURE")
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p

    # 2) Repo-local candidates
    repo_candidates = [
        Path("tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json"),
        Path(__file__).resolve().parent.parent.parent
        / "data"
        / "RAW_TransitIssues_10-7-2025_api_bodies.json",
    ]

    # 3) External mount (when available)
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
    """Unwrap common envelope keys to return the FedEx body that contains completeTrackResults."""
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


@pytest.fixture()
def replay_dir_with_listed_tns(tmp_path: Path, raw_capture_path: Path) -> tuple[Path, list[str]]:
    """Write replay files for each TN from the provided list that exists in the capture."""
    data = json.loads(raw_capture_path.read_text(encoding="utf-8"))
    mapping = _map_tracking_to_body(data)

    present = [tn for tn in PRETRANSIT_TNS if tn in mapping]
    if not present:
        pytest.skip(
            "None of the provided pretransit TNs were present in the capture file.")

    combined = [mapping[tn] for tn in present]
    replay_file = tmp_path / "replay.json"
    replay_file.write_text(json.dumps(combined), encoding="utf-8")

    return replay_file, present


class _QuietLogger:
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


@pytest.mark.parametrize("batch_size", [3, 6, 12])
def test_pretransit_rows_classified(tmp_path: Path, replay_dir_with_listed_tns: tuple[Path, list[str]], batch_size: int):
    """
    Deterministic integration:
      - Materialize replays for TNs in the provided list that are present in the capture.
      - Run processor (date filter disabled).
      - For each processed row belonging to our subset, assert:
          IsPreTransit == classify_row_pretransit(code, derivedCode, statusByLocale, description)
    """
    replay_dir, present_tns = replay_dir_with_listed_tns
    subset = present_tns[:max(1, min(batch_size, len(present_tns)))]

    # Build input workbook (include disposable first column so preprocessor drops it)
    rows = [{
        "X": "drop-me",
        "Promised Delivery Date": "N/A",   # ignored; date filter disabled
        "Delivery Tracking Status": "in transit",
        "Tracking Number": tn,
        "Carrier Code": "FDX",
        "RowId": idx + 1,
        "latestStatusDetail": {"one": 1, "two": 2},
    } for idx, tn in enumerate(subset)]

    src = tmp_path / "in.xlsx"
    pd.DataFrame(rows).to_excel(src, index=False)
    out = tmp_path / "in_processed.xlsx"

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=ReplayClient(replay_dir),
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=False,
    )
    proc.process(src, out, env_cfg=None)

    # The pipeline no longer writes a full 'Processed' sheet; check 'All Issues'
    df = pd.read_excel(out, sheet_name="All Issues", engine="openpyxl")

    # Contract columns present
    for col in OUTPUT_FEDEX_COLUMNS + ["CalculatedStatus"]:
        assert col in df.columns, f"Missing column: {col}"

    # Indicators present
    for col in ("IsPreTransit", "IsDelivered", "HasException"):
        assert col in df.columns, f"Missing indicator column: {col}"

    # Index rows by Tracking Number for robust matching
    df["Tracking Number"] = df["Tracking Number"].astype(str)
    idx_by_tn = {str(tn): i for i, tn in enumerate(df["Tracking Number"])}

    # For each TN in subset, compute expected via classifier on ENRICHED fields and compare
    for tn in subset:
        key = str(tn)
        assert key in idx_by_tn, f"Expected TN {tn} not found in processed output."
        i = idx_by_tn[key]
        code = str(df.loc[i, "code"])
        derived = str(df.loc[i, "derivedCode"])
        status = str(df.loc[i, "statusByLocale"])
        desc = str(df.loc[i, "description"])

        expected = 1 if classify_row_pretransit(
            code, derived, status, desc) else 0
        got = int(df.loc[i, "IsPreTransit"])
        assert got == expected, (
            f"TN {tn}: IsPreTransit={got}, expected={expected} "
            f"(code={code!r}, derived={derived!r}, status={status!r}, desc={desc!r})"
        )
