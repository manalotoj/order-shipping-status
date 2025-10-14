# src/order_shipping_status/api/normalize.py
from __future__ import annotations

from typing import Any, Dict, Tuple


def _from_latest_status_detail(payload: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    Try to extract (code, derivedCode, statusByLocale, description) from
    output.completeTrackResults[*].trackResults[*].latestStatusDetail.
    """
    out = payload.get("output", payload)
    if not isinstance(out, dict):
        return "", "", "", ""

    ctr = out.get("completeTrackResults")
    if not isinstance(ctr, list) or not ctr:
        return "", "", "", ""

    tr_list = ctr[0].get("trackResults")
    if not isinstance(tr_list, list) or not tr_list:
        return "", "", "", ""

    lsd = tr_list[0].get("latestStatusDetail") or {}
    if not isinstance(lsd, dict):
        return "", "", "", ""

    code = str(lsd.get("code") or "")
    derived = str(lsd.get("derivedCode") or code)
    status = str(lsd.get("statusByLocale") or "")
    desc = str(lsd.get("description") or "")
    return code, derived, status, desc


def _from_scan_events(payload: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    Fallback to scanEvents (either top-level or nested) when latestStatusDetail
    is not available. Use the FIRST event present (unit tests only require a sane fallback).
    """
    candidates = []

    # top-level scanEvents
    se = payload.get("scanEvents")
    if isinstance(se, list) and se:
        candidates.append(se)

    # nested under output.completeTrackResults[*].trackResults[*].scanEvents
    out = payload.get("output", payload)
    if isinstance(out, dict):
        ctr = out.get("completeTrackResults")
        if isinstance(ctr, list):
            for cr in ctr:
                if not isinstance(cr, dict):
                    continue
                tr_list = cr.get("trackResults")
                if isinstance(tr_list, list):
                    for tr in tr_list:
                        if not isinstance(tr, dict):
                            continue
                        se2 = tr.get("scanEvents")
                        if isinstance(se2, list) and se2:
                            candidates.append(se2)

    for events in candidates:
        ev = events[0]
        if isinstance(ev, dict):
            code = str(ev.get("derivedStatusCode")
                       or ev.get("eventType") or "")
            derived = str(ev.get("derivedStatusCode") or code)
            status = str(ev.get("derivedStatus")
                         or ev.get("eventDescription") or "")
            desc = str(ev.get("eventDescription") or "")
            return code, derived, status, desc

    return "", "", "", ""


def _from_flat(payload: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    Final fallback: respect flat shapes used in unit tests.
    """
    code = str(payload.get("code") or "")
    derived = str(payload.get("derivedCode") or code)
    status = str(payload.get("statusByLocale") or "")
    desc = str(payload.get("description") or "")
    return code, derived, status, desc


def _carrier_from_code(carrier_code: str) -> str:
    """
    Light mapping just so required 'carrier' field is non-empty.
    """
    cc = (carrier_code or "").upper()
    if cc.startswith("FDX") or cc.startswith("FEDEX"):
        return "FedEx"
    return cc or "Unknown"


def normalize_fedex(
    payload: Dict[str, Any],
    *,
    tracking_number: str,
    carrier_code: str,
    source: str,  # kept for signature parity; not used in core fields
):
    """
    Produce a NormalizedShippingData object with the core status fields populated:
      - code
      - derivedCode
      - statusByLocale
      - description

    Other required fields in NormalizedShippingData are filled with neutral defaults.
    Timestamp/backfill (LatestEventTimestampUtc) is intentionally NOT performed here
    to keep unit tests deterministic; that happens later during enrichment/processing.
    """
    # 1) Deep, official path
    code, derived, status, desc = _from_latest_status_detail(payload)

    # 2) Fallback: scanEvents (top-level or nested)
    if not code and not status:
        code, derived, status, desc = _from_scan_events(payload)

    # 3) Fallback: flat shape (unit tests rely on this)
    if not code and not status:
        code, derived, status, desc = _from_flat(payload)

    # Import here to avoid circulars on module import
    from order_shipping_status.models import NormalizedShippingData

    # Construct with required fields + safe defaults
    return NormalizedShippingData(
        code=code,
        derivedCode=derived,
        statusByLocale=status,
        description=desc,

        # Required by your model (use safe defaults)
        carrier=_carrier_from_code(carrier_code),
        tracking_number=str(tracking_number or ""),
        carrier_code=str(carrier_code or ""),
        actual_delivery_dt="",
        possession_status=False,
        service_type="",
        service_desc="",
        origin_city="",
        origin_state="",
        dest_city="",
        dest_state="",
        received_by_name="",

        # Keep the raw payload attached
        raw=payload,
    )
