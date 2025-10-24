from __future__ import annotations

import re
import pandas as pd

# Public indicator column names (used by tests and ColumnContract)
INDICATOR_COLS: tuple[str, ...] = (
    "IsPreTransit",
    "IsDelivered",
    "HasException",
    "IsRTS",
    "IsStalled",
    "Damaged",
    "UnableToDeliver",
)

# ---- Helpers -----------------------------------------------------------------


def _extract_ancillary_from_latest_status_detail(v) -> str:
    """Flatten ancillaryDetails text from a latestStatusDetail dict."""
    if not isinstance(v, dict):
        return ""
    details = v.get("ancillaryDetails") or []
    parts = []
    if isinstance(details, list):
        for d in details:
            if isinstance(d, dict):
                for k in ("reasonDescription", "actionDescription", "reason", "action"):
                    val = d.get(k)
                    if val:
                        parts.append(str(val))
    return " ".join(parts)


def _extract_ancillary_series(df: pd.DataFrame) -> pd.Series:
    """
    Return a lowercase string Series of ancillary text.
    Priority:
      1) df['LatestAncillaryText'] if present (already flattened)
      2) df['latestStatusDetail'] if present and dict-like
      3) df['raw'] (normalized payload) → walk down to ...trackResults[*].latestStatusDetail
    """
    n = len(df)
    texts = pd.Series([""] * n, index=df.index, dtype="string")

    # 1) Pre-flattened text takes precedence
    if "LatestAncillaryText" in df.columns:
        return df["LatestAncillaryText"].astype("string").fillna("").str.lower()

    # 2) Direct dict column
    if "latestStatusDetail" in df.columns:
        direct = df["latestStatusDetail"].apply(
            _extract_ancillary_from_latest_status_detail)
        texts = direct.astype("string")

    # 3) Fallback to raw payload traversal (only fill where still empty)
    def _from_raw_payload(v) -> str:
        if not isinstance(v, dict):
            return ""
        out = v.get("output", v)
        if not isinstance(out, dict):
            return ""
        ctr = out.get("completeTrackResults")
        if not isinstance(ctr, list) or not ctr:
            return ""
        # Take first trackResults entry
        tr_list = ctr[0].get("trackResults")
        if not isinstance(tr_list, list) or not tr_list:
            return ""
        lsd = tr_list[0].get("latestStatusDetail")
        return _extract_ancillary_from_latest_status_detail(lsd)

    if "raw" in df.columns:
        fallback = df["raw"].apply(_from_raw_payload).astype("string")
        # Only replace empties
        mask_empty = texts.fillna("") == ""
        texts = texts.where(~mask_empty, fallback)

    return texts.fillna("").str.lower()


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
    Return (latest_code, status, desc) as uppercase string Series.
    Missing columns are treated as empty strings.
    """
    # Prefer derivedCode (your pipeline's "latest" code), fallback to code.
    if "derivedCode" in df.columns:
        code = _series_of_strings(df, "derivedCode")
    else:
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
    r"\bRETURN(?:ING)?\s(?:PACKAGE\sTO\sSHIPPER|TO\s(?:SHIPPER|SENDER))\b",
    re.IGNORECASE,
)


def _is_pretransit(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    # Pre-transit is only when the latest status code is exactly 'OC'.
    return code == "OC"


def _is_delivered_text(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    # Only treat as delivered when the latest status code is exactly 'DL'.
    return code == "DL"


def _has_exception(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    c = code.isin(_EXCEPTION_CODES)
    patt = "|".join(map(re.escape, _EXCEPTIONS_TEXT))
    s = status.str.contains(patt, case=False, regex=True)
    d = desc.str.contains(patt, case=False, regex=True)
    return c | s | d


def _is_rts_text(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    c = code.isin(_RTS_CODES)
    s = status.str.contains(_RTS_REGEX, regex=True)
    d = desc.str_contains(_RTS_REGEX) if hasattr(
        status, "str_contains") else desc.str.contains(_RTS_REGEX, regex=True)
    return c | s | d


# Phrase we care about (case-insensitive)
_UNABLE_TO_DELIVER_RE = r"\bunable\s+to\s+deliver\b"


def _compute_unable_to_deliver(df: pd.DataFrame) -> pd.Series:
    """
    UnableToDeliver = 1 IFF:
      (a) row is an exception, AND
      (b) 'unable to deliver' appears in ancillaryDetails OR status/description.
    """
    n = len(df)

    # (a) Exception?
    if "HasException" in df.columns:
        is_exc = pd.to_numeric(
            df["HasException"], errors="coerce").fillna(0).astype(int) == 1
    else:
        status_lc = _as_lower_str_series(
            df.get("statusByLocale", pd.Series([""] * n)))
        is_exc = status_lc.str.contains("exception", na=False)

    # (b) Gather from ancillary + status/description (same pattern as Damaged)
    # already lowercased by helper
    ancillary = _extract_ancillary_series(df)
    status = _as_lower_str_series(
        df.get("statusByLocale", pd.Series([""] * n)))
    desc = _as_lower_str_series(df.get("description", pd.Series([""] * n)))

    combined = (ancillary.fillna("") + " " +
                status.fillna("") + " " + desc.fillna(""))
    has_phrase = combined.str.contains(
        _UNABLE_TO_DELIVER_RE, regex=True, na=False)

    return (is_exc & has_phrase).astype(int)


def _as_lower_str_series(s: pd.Series) -> pd.Series:
    try:
        return s.astype("string").fillna("").str.strip().str.lower()
    except Exception:
        return pd.Series([""] * len(s), index=s.index, dtype="string")


def _compute_damaged(df: pd.DataFrame) -> pd.Series:
    """
    Damaged = 1 IFF:
      (a) row is an exception (HasException==1 or 'exception' in statusByLocale), AND
      (b) 'damaged' appears (case-insensitive) in latestStatusDetail.ancillaryDetails[*]
          fields: reasonDescription, actionDescription, reason, or action.
    """
    n = len(df)

    # (a) Exception?
    if "HasException" in df.columns:
        is_exc = pd.to_numeric(
            df["HasException"], errors="coerce").fillna(0).astype(int) == 1
    else:
        status_lc = _as_lower_str_series(
            df.get("statusByLocale", pd.Series([""] * n)))
        is_exc = status_lc.str.contains("exception", na=False)

    # (b) Extract ancillary text from latestStatusDetail/raw ONLY
    ancillary = _extract_ancillary_series(df)
    # series already lowercased; pattern is lowercase
    has_damaged = ancillary.str.contains(r"\bdamaged\b", regex=True, na=False)

    return (is_exc & has_damaged).astype(int)


def apply_indicators(df: pd.DataFrame, *, stalled_threshold_days: int = 4) -> pd.DataFrame:
    """
    Create/overwrite indicator columns:
      - IsPreTransit, IsDelivered, HasException, IsRTS, IsStalled
      - Damaged (0/1)
    """
    out = df.copy()

    # Ensure all indicator columns exist (stable pipeline)
    for col in INDICATOR_COLS:
        if col not in out.columns:
            out[col] = 0

    code, status, desc = _get_text_cols(out)

    # PreTransit always computed from code
    pre = _is_pretransit(code, status, desc)

    # Delivered/RTS: combine pre-seeded flags with code/text-based detection.
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
        rts = rts_text

    exc = _has_exception(code, status, desc)

    # Materialize as 0/1 int64, robust to NaN
    out["IsPreTransit"] = pre.astype("int64")
    out["IsDelivered"] = dlv.astype("int64")
    out["HasException"] = exc.astype("int64")
    out["IsRTS"] = rts.astype("int64")

    # Stalled: Exception does NOT block stalled; only Delivered/RTS force 0
    days = out["DaysSinceLatestEvent"] if "DaysSinceLatestEvent" in out.columns else pd.Series([
                                                                                               0] * len(out), index=out.index)
    stalled_raw = _compute_stalled(
        days, threshold_days=stalled_threshold_days, length=len(out))
    stalled_cond = stalled_raw.astype("bool")

    if "ScanEventsCount" in out.columns:
        scan_ct = _as_int_series(out["ScanEventsCount"], length=len(out))
        no_scans = scan_ct == 0
        stalled_cond = stalled_cond | no_scans

    pre_flag = _as_int_series(out.get("IsPreTransit", 0), length=len(out)) == 1
    terminal = _is_terminal(out.get("IsDelivered", 0), out.get("IsRTS", 0))
    final_stalled = stalled_cond & (~terminal) & (~pre_flag)
    out["IsStalled"] = _as_int_series(
        final_stalled.astype(int), length=len(out))

    try:
        out["UnableToDeliver"] = _compute_unable_to_deliver(out)
    except Exception:
        out["UnableToDeliver"] = 0

    # Damaged — strictly from ancillaryDetails under latestStatusDetail/raw
    try:
        out["Damaged"] = _compute_damaged(out)
    except Exception:
        out["Damaged"] = 0

    return out
