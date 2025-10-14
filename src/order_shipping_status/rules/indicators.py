# src/order_shipping_status/rules/indicators.py
from __future__ import annotations

import re
import pandas as pd

# Public, test-visible constants
_PRETRANSIT_CODES = {"OC", "LP"}          # Label-created / pre-transit codes
_DELIVERED_CODES = {"DL"}                 # Delivered
_EXCEPTION_CODES = {"DE", "SE"}           # Delivery/Shipment exception
_RTS_CODES = {"RS", "RT"}                 # Return to Shipper / Returned

_INDICATOR_COLS = ("IsPreTransit", "IsDelivered",
                   "HasException", "IsRTS", "IsStalled")


def _ensure_series_str_upper(x) -> pd.Series:
    """Return a pandas Series of uppercase strings, robust to scalar/missing."""
    if isinstance(x, pd.Series):
        return x.astype("string").fillna("").str.upper()
    # scalar or missing: broadcast to a 1-length series, but callers always pass Series
    return pd.Series([str(x or "")], dtype="string").str.upper()


def _or_contains(series: pd.Series, needles: list[str]) -> pd.Series:
    """
    Case-insensitive substring search without regex pitfalls/warnings.
    ORs across all 'needles'.
    """
    s = series.astype("string").fillna("")
    acc = pd.Series(False, index=s.index)
    for n in needles:
        acc |= s.str.contains(n, case=False, regex=False, na=False)
    return acc


def _get_text_cols(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Return (code, status, desc) as uppercase string Series (empty when missing/NaN).
    """
    code = _ensure_series_str_upper(df.get("code", ""))
    status = _ensure_series_str_upper(df.get("statusByLocale", ""))
    desc = _ensure_series_str_upper(df.get("description", ""))
    return code, status, desc


def _is_pretransit(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    # code-based
    by_code = code.isin(_PRETRANSIT_CODES)
    # text-based (label created etc.)
    by_text = _or_contains(status, ["label created"]) | _or_contains(
        desc, ["label created"])
    return (by_code | by_text).astype("int64")


def _is_delivered(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    by_code = code.isin(_DELIVERED_CODES)
    by_text = _or_contains(status, ["delivered"]) | _or_contains(
        desc, ["delivered"])
    return (by_code | by_text).astype("int64")


def _has_exception(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    by_code = code.isin(_EXCEPTION_CODES)
    by_text = (
        _or_contains(
            status, ["exception", "delivery exception", "shipment exception"])
        | _or_contains(desc, ["exception", "delivery exception", "shipment exception"])
    )
    return (by_code | by_text).astype("int64")


def _is_rts(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    by_code = code.isin(_RTS_CODES)
    by_text = _or_contains(status, ["returning to shipper", "returned to sender", "return to shipper"]) | \
        _or_contains(desc,   ["returning to shipper",
                     "returned to sender", "return to shipper"])
    return (by_code | by_text).astype("int64")


def _is_terminal(is_delivered: pd.Series, is_rts: pd.Series) -> pd.Series:
    """
    Terminal means the shipment is complete/closed for our purposes.
    **Exception IS NOT terminal.**
    Only Delivered or ReturnedToSender should count as terminal.
    """
    return ((is_delivered.astype("int64") == 1) | (is_rts.astype("int64") == 1))


def _as_int(series) -> pd.Series:
    try:
        return pd.to_numeric(series, errors="coerce").fillna(0).astype("int64")
    except Exception:
        return pd.Series([0] * len(series), index=series.index, dtype="int64")


def apply_indicators(df: pd.DataFrame, *, stalled_threshold_days: int = 4) -> pd.DataFrame:
    """
    Adds indicator columns:
      - IsPreTransit, IsDelivered, HasException, IsRTS, IsStalled
    Stalled definition:
      DaysSinceLatestEvent >= stalled_threshold_days AND NOT terminal (Delivered/RTS).
      Having an Exception does NOT suppress stalled.
    """
    out = df.copy()

    # Ensure skeleton columns exist so vectorized logic is stable
    for col in _INDICATOR_COLS:
        if col not in out.columns:
            out[col] = 0
    if "DaysSinceLatestEvent" not in out.columns:
        out["DaysSinceLatestEvent"] = 0

    code, status, desc = _get_text_cols(out)

    # Primary indicators
    pre = _is_pretransit(code, status, desc)
    dlv = _is_delivered(code, status, desc)
    exc = _has_exception(code, status, desc)
    rts = _is_rts(code, status, desc)

    out["IsPreTransit"] = _as_int(pre)
    out["IsDelivered"] = _as_int(dlv)
    out["HasException"] = _as_int(exc)
    out["IsRTS"] = _as_int(rts)

    # Stalled: days >= threshold and not terminal (Delivered or RTS)
    days = _as_int(out["DaysSinceLatestEvent"])
    terminal = _is_terminal(out["IsDelivered"], out["IsRTS"])
    stalled = (days >= int(stalled_threshold_days)) & (~terminal)
    out["IsStalled"] = stalled.astype("int64")

    return out
