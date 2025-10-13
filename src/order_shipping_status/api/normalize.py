# src/order_shipping_status/api/normalize.py
from __future__ import annotations
from typing import Any, Dict, Optional
from datetime import datetime

try:
    from dateutil import parser as dtp  # optional; used when date strings present
except Exception:  # pragma: no cover
    dtp = None

from order_shipping_status.models import NormalizedShippingData


def _safe_parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s or not dtp:
        return None
    try:
        return dtp.parse(s)
    except Exception:
        return None


def _get(d: Dict[str, Any], *path: str, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def _select_track_result(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Accept full FedEx response OR a single trackResult OR a flat captured body."""
    if not isinstance(payload, dict):
        return {}

    # Full response shape: completeTrackResults -> trackResults[0]
    ctr = payload.get("completeTrackResults")
    if isinstance(ctr, list) and ctr:
        tr = ctr[0].get("trackResults")
        if isinstance(tr, list) and tr:
            return tr[0]

    # Mid-level shape: trackResults[0]
    tr = payload.get("trackResults")
    if isinstance(tr, list) and tr:
        return tr[0]

    # Already a trackResult (or a flat replay body)
    return payload


def _find_date(track_result: Dict[str, Any], want_type: str) -> Optional[datetime]:
    for item in (track_result.get("dateAndTimes") or []):
        if item.get("type") == want_type:
            return _safe_parse_dt(item.get("dateTime"))
    return None


def normalize_fedex(
    payload: Dict[str, Any],
    *,
    tracking_number: str | None,
    carrier_code: str | None,
    source: str,
) -> NormalizedShippingData:
    tr = _select_track_result(payload)

    # Prefer latestStatusDetail if present
    lsd = tr.get("latestStatusDetail") or {}

    # Fallback: if lsd missing, accept flat bodies with top-level fields
    if not lsd and any(k in tr for k in ("code", "statusByLocale", "description", "derivedCode")):
        lsd = {
            "code": tr.get("code"),
            "derivedCode": tr.get("derivedCode"),
            "statusByLocale": tr.get("statusByLocale"),
            "description": tr.get("description"),
        }

    code = (lsd.get("code") or "").strip()
    derived = (lsd.get("derivedCode") or "").strip()
    status = (lsd.get("statusByLocale") or "").strip()
    desc = (lsd.get("description") or "").strip()

    # Fallback to last scanEvent for status/code/desc if still empty
    if not (code or status or desc):
        scans = tr.get("scanEvents") or []
        if scans:
            last = sorted(scans, key=lambda e: e.get("date") or "")[-1]
            code = code or (last.get("derivedStatusCode")
                            or last.get("eventType") or "")
            status = status or (last.get("derivedStatus")
                                or last.get("eventDescription") or "")
            desc = desc or (last.get("eventDescription") or "")

    actual_delivery_dt = _find_date(tr, "ACTUAL_DELIVERY")
    possession = _get(tr, "shipmentDetails", "possessionStatus")
    svc_type = _get(tr, "serviceDetail", "type")
    svc_desc = _get(tr, "serviceDetail", "description")

    def addr(side: str):
        a = _get(tr, f"{side}Location",
                 "locationContactAndAddress", "address", default={}) or {}
        return (a.get("city"), a.get("stateOrProvinceCode"))

    o_city, o_state = addr("origin")
    d_city, d_state = addr("destination")
    received_by = _get(tr, "deliveryDetails", "receivedByName")

    raw = {
        "scanEvents": tr.get("scanEvents") or [],
        "dateAndTimes": tr.get("dateAndTimes") or [],
        "latestStatusDetail": lsd or {},
        "shipmentDetails": tr.get("shipmentDetails"),
        "serviceDetail": tr.get("serviceDetail"),
        "deliveryDetails": tr.get("deliveryDetails"),
        "originLocation": tr.get("originLocation"),
        "destinationLocation": tr.get("destinationLocation"),
        "packageDetails": tr.get("packageDetails"),
        "serviceCommitMessage": tr.get("serviceCommitMessage"),
        "error": tr.get("error"),
    }

    return NormalizedShippingData(
        carrier="FEDEX",
        tracking_number=tracking_number,
        carrier_code=carrier_code,
        code=code or "",
        derivedCode=(derived or code or ""),
        statusByLocale=status or "",
        description=desc or "",
        actual_delivery_dt=actual_delivery_dt,
        possession_status=possession,
        service_type=svc_type,
        service_desc=svc_desc,
        origin_city=o_city,
        origin_state=o_state,
        dest_city=d_city,
        dest_state=d_state,
        received_by_name=received_by,
        raw=raw,
        source=source,
        captured_at=None,
    )
