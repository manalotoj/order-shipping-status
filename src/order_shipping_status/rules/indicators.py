from __future__ import annotations

import re
import pandas as pd

# Public indicator column names (used by tests and ColumnContract)
INDICATOR_COLS: tuple[str, ...] = (
    "IsPreTransit",
    "IsDelivered",
    "IsDamaged",
    "HasException",
    "IsRTS",
    "IsStalled",
)

# ---- Helpers -----------------------------------------------------------------


def _series_of_strings(df: pd.DataFrame, name: str) -> pd.Series:
    """
    Return an uppercase string Series for column `name`.
    If missing, return a same-length empty string Series.
    """
    if name in df.columns:
        return df[name].astype("string").fillna("").str.upper()
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _as_int_series(s, *, length: int) -> pd.Series:
    """
    Coerce any input (Series or scalar) to int64 via numeric with NaN->0.
    If scalar, broadcast to a Series of given length.
    """
    if not isinstance(s, pd.Series):
        s = pd.Series([s] * length)
    return pd.to_numeric(s, errors="coerce").fillna(0).astype("int64")


def _get_text_cols(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Return (code, status, desc) as uppercase string Series.
    Missing columns are treated as empty strings.
    """
    code = _series_of_strings(df, "code")
    status = _series_of_strings(df, "statusByLocale")
    desc = _series_of_strings(df, "description")
    return code, status, desc


def _is_terminal(is_delivered: pd.Series, is_rts: pd.Series) -> pd.Series:
    """Terminal means Delivered OR RTS."""
    d = _as_int_series(is_delivered, length=len(is_delivered)) == 1
    r = _as_int_series(is_rts, length=len(is_rts)) == 1
    return d | r


def _compute_stalled(days_since, *, threshold_days: int, length: int) -> pd.Series:
    """Stalled if DaysSinceLatestEvent >= threshold_days."""
    days = _as_int_series(days_since, length=length)
    return (days >= int(threshold_days)).astype("int64")

# ---- PreTransit / Delivered / Exception / RTS rules --------------------------


_PRETRANSIT_CODES = {"OC"}
_DELIVERED_CODES = {"DL"}
_EXCEPTION_CODES = {"DE", "SE", "EX", "EXC"}
_RTS_CODES = {"RS", "RTS"}

_PRETRANSIT_TEXTS = (
    "LABEL CREATED",
    "SHIPMENT INFORMATION SENT TO FEDEX",
    "LABEL HAS BEEN CREATED",
)
_DELIVERED_TEXTS = ("DELIVERED",)
_EXCEPTIONS_TEXT = (
    "EXCEPTION",
    "DELIVERY EXCEPTION",
    "DAMAGED",
    "UNABLE TO DELIVER",
)

_RTS_REGEX = re.compile(
    r"RETURN(?:ING)?\s+PACKAGE\s+TO\s+SHIPPER|RETURN\s+TO\s+SHIPPER",
    re.IGNORECASE,
)


def _is_pretransit(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    # Pre-transit is only when the latest status code is exactly 'OC'.
    # Do not rely on status/description text heuristics here.
    return code == "OC"


def _is_delivered_text(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    # Only treat as delivered when the latest status code is exactly 'DL'.
    # Do not rely on status/description text heuristics.
    return code == "DL"


def _has_exception(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    c = code.isin(_EXCEPTION_CODES)
    patt = "|".join(map(re.escape, _EXCEPTIONS_TEXT))
    s = status.str.contains(patt, case=False, regex=True)
    d = desc.str.contains(patt, case=False, regex=True)
    return c | s | d


def _is_rts_text(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    c = code.isin(_RTS_CODES)
    s = status.str_contains(_RTS_REGEX) if hasattr(
        status, "str_contains") else status.str.contains(_RTS_REGEX)
    d = desc.str_contains(_RTS_REGEX) if hasattr(
        desc, "str_contains") else desc.str.contains(_RTS_REGEX)
    return c | s | d


def _is_damaged(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    """Damaged if there is an exception and status/description contains 'DAMAGED' or 'MULTIPLE TRACKING LABELS'.

    The function expects uppercase status/desc series (as produced by _get_text_cols).
    Only consider records that have exception-like codes/texts.
    """
    # Base exception detection from codes/texts
    has_exc = code.isin(_EXCEPTION_CODES)

    # Look for keywords in status/description
    patt = r"DAMAGED|MULTIPLE\s+TRACKING\s+LABELS"
    s = status.str.contains(patt, case=False, regex=True)
    d = desc.str.contains(patt, case=False, regex=True)

    return (has_exc & (s | d))


def apply_indicators(df: pd.DataFrame, *, stalled_threshold_days: int = 4) -> pd.DataFrame:
    """
    Create/overwrite indicator columns:
      - IsPreTransit, IsDelivered, HasException, IsRTS, IsStalled

    Stalled rule:
      - Stalled if DaysSinceLatestEvent >= threshold
      - Delivered OR RTS => not stalled (forced 0)
      - Exception does NOT block stalled

    If incoming DataFrame already has IsDelivered/IsRTS, we RESPECT those flags.
    """
    out = df.copy()

    # Ensure all indicator columns exist (stable pipeline)
    for col in INDICATOR_COLS:
        if col not in out.columns:
            out[col] = 0

    code, status, desc = _get_text_cols(out)

    # PreTransit always computed from text/code
    pre = _is_pretransit(code, status, desc)

    # Delivered/RTS: combine pre-seeded flags with text-based detection.
    # This allows ColumnContract (which may add zero-filled indicator columns
    # before enrichment) to coexist with normalized text produced later.
    dlv_text = _is_delivered_text(code, status, desc)
    if "IsDelivered" in df.columns:
        existing_dlv = _as_int_series(df["IsDelivered"], length=len(df)) == 1
        dlv = dlv_text | existing_dlv
    else:
        dlv = dlv_text

    rts_text = _is_rts_text(code, status, desc)
    if "IsRTS" in df.columns:
        existing_rts = _as_int_series(df["IsRTS"], length=len(df)) == 1
        rts = rts_text | existing_rts
    else:
        # use text-based RTS if not preseeded
        rts = rts_text

    exc = _has_exception(code, status, desc)

    # Damaged detection: must be an exception and mention DAMAGED or MULTIPLE TRACKING LABELS
    damaged = _is_damaged(code, status, desc)

    # Materialize as 0/1 int64, robust to NaN
    out["IsPreTransit"] = pre.astype("int64")
    out["IsDelivered"] = dlv.astype("int64")
    out["HasException"] = exc.astype("int64")
    out["IsDamaged"] = damaged.astype("int64")
    out["IsRTS"] = rts.astype("int64")

    # Terminal (Delivered or RTS)
    terminal = _is_terminal(out.get("IsDelivered", 0), out.get("IsRTS", 0))

    # Stalled: Exception does NOT block stalled; only Delivered/RTS force 0
    if "DaysSinceLatestEvent" in out.columns:
        days = out["DaysSinceLatestEvent"]
    else:
        days = pd.Series([0] * len(out), index=out.index)
    stalled_raw = _compute_stalled(
        days, threshold_days=stalled_threshold_days, length=len(out))

    # Primary stalled condition: DaysSinceLatestEvent >= threshold
    stalled_cond = stalled_raw.astype("bool")

    # Also consider records with zero scan events at all (no scans in history)
    if "ScanEventsCount" in out.columns:
        scan_ct = _as_int_series(out["ScanEventsCount"], length=len(out))
        no_scans = (scan_ct == 0)
        stalled_cond = stalled_cond | no_scans

    # Exclude terminal states (Delivered or RTS) and PRE-TRANSIT
    pre = _as_int_series(out.get("IsPreTransit", 0), length=len(out)) == 1
    terminal = _is_terminal(out.get("IsDelivered", 0), out.get("IsRTS", 0))

    final_stalled = stalled_cond & (~terminal) & (~pre)

    out["IsStalled"] = _as_int_series(
        final_stalled.astype(int), length=len(out))

    return out
