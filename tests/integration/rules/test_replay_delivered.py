# tests/integration/pipelines/test_replay_delivered.py
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor
from order_shipping_status.api.client import ReplayClient
from order_shipping_status.api.normalize import normalize_fedex
from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS


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


def _latest_status_fields(body: dict) -> tuple[str, str, str]:
    # (code, statusByLocale, description)
    try:
        tr = body["completeTrackResults"][0]["trackResults"][0]
        det = tr.get("latestStatusDetail", {}) or {}
        return (
            str(det.get("code", "")),
            str(det.get("statusByLocale", "")),
            str(det.get("description", "")),
        )
    except Exception:
        # flat fallback
        det = body.get("latestStatusDetail", {}) or {}
        if det:
            return (
                str(det.get("code", "")),
                str(det.get("statusByLocale", "")),
                str(det.get("description", "")),
            )
        return (
            str(body.get("code", "")),
            str(body.get("statusByLocale", "")),
            str(body.get("description", "")),
        )


def _find_delivered(mapping: dict[str, dict], limit: int) -> list[str]:
    found: list[str] = []
    for tn, body in mapping.items():
        # Use the same normalization logic the pipeline uses to pick delivered
        try:
            nd = normalize_fedex(body, tracking_number=tn,
                                 carrier_code="FDX", source="api")
            code = (nd.code or "").upper()
        except Exception:
            code = ""
        if code == "DL":
            found.append(tn)
            if len(found) >= limit:
                break
    return found


@pytest.fixture(scope="module")
def raw_capture_path() -> Path:
    candidates = [
        Path("tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json"),
        Path("/mnt/data/RAW_TransitIssues_10-7-2025_api_bodies.json"),
        # also try relative to this test file (robust when cwd is different)
        Path(__file__).resolve().parent.parent.parent / "data" /
        "RAW_TransitIssues_10-7-2025_api_bodies.json",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise AssertionError(
        "FedEx capture JSON not found at tests/data/RAW_TransitIssues_10-7-2025_api_bodies.json"
    )


class _QuietLogger:
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


@pytest.mark.parametrize("batch_size", [3, 6])
def test_replay_delivered_classified(tmp_path: Path, raw_capture_path: Path, batch_size: int):
    data = json.loads(raw_capture_path.read_text(encoding="utf-8"))
    mapping = _map_tracking_to_body(data)
    subset = _find_delivered(mapping, batch_size)

    if not subset:
        raise AssertionError(
            "No delivered rows found in capture; cannot run delivered classification test")

    # Write replays
    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()
    for tn in subset:
        (replay_dir /
         f"{tn}.json").write_text(json.dumps(mapping[tn]), encoding="utf-8")

    # Build input workbook (date filter is disabled, so dates don't matter)
    rows = [{
        "X": "drop-me",
        "Promised Delivery Date": "N/A",
        "Delivery Tracking Status": "in transit",
        "Tracking Number": tn,
        "Carrier Code": "FDX",
        "latestStatusDetail": {"one": 1, "two": 2},
    } for tn in subset]

    src = tmp_path / "in.xlsx"
    pd.DataFrame(rows).to_excel(src, index=False)
    out = tmp_path / "out.xlsx"

    proc = WorkbookProcessor(
        logger=_QuietLogger(),
        client=ReplayClient(replay_dir),
        normalizer=normalize_fedex,
        reference_date=None,
        enable_date_filter=False,
    )
    proc.process(src, out, env_cfg=None)

    # Reconstruct processed output: read original input sheet and run the
    # same pipeline steps (ColumnContract + Enricher + indicators) so tests
    # don't depend on a 'Processed' sheet being written.
    from order_shipping_status.pipelines.column_contract import ColumnContract
    from order_shipping_status.pipelines.enricher import Enricher
    from order_shipping_status.rules.indicators import apply_indicators
    from order_shipping_status.rules.status_mapper import map_indicators_to_status

    df_input = pd.read_excel(
        out, sheet_name="All Shipments", engine="openpyxl")
    df_proc = ColumnContract().ensure(df_input)
    df_proc = Enricher(_QuietLogger(), client=ReplayClient(
        replay_dir), normalizer=normalize_fedex).enrich(df_proc)
    df_proc = apply_indicators(df_proc)
    df_proc = map_indicators_to_status(df_proc)

    # Contract columns present
    for col in OUTPUT_FEDEX_COLUMNS + ["CalculatedStatus"]:
        assert col in df_proc.columns

    # Expect Delivered indicator to be set for these rows
    for i in range(len(subset)):
        assert int(df_proc.loc[i, "IsDelivered"]
                   ) == 1, f"Row {i} expected IsDelivered==1, got CalculatedStatus={df_proc.loc[i, 'CalculatedStatus']}"
