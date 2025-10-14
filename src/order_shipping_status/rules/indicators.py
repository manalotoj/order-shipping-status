from __future__ import annotations

from typing import Iterable
import numpy as np
import pandas as pd

# Public contract: these are created/finalized by apply_indicators
INDICATOR_COLS: tuple[str, ...] = (
    "IsPreTransit",
    "IsDelivered",
    "HasException",
    "IsRTS",
    "IsStalled",
)

# ---- helpers ----------------------------------------------------------------


def _empty_like(df: pd.DataFrame) -> pd.Series:
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _get_text_cols(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Return (code, status, desc) as uppercase strings with empty-string fallback.
    Robust to missing columns and non-string values.
    """
    code = df["code"] if "code" in df.columns else _empty_like(df)
    status = df["statusByLocale"] if "statusByLocale" in df.columns else _empty_like(
        df)
    desc = df["description"] if "description" in df.columns else _empty_like(
        df)

    code = code.astype("string").fillna("").str.upper()
    status = status.astype("string").fillna("").str.upper()
    desc = desc.astype("string").fillna("").str.upper()
    return code, status, desc


def _as_int(s: pd.Series) -> pd.Series:
    """
    Coerce any Series to clean integer 0/1 (or 0 for invalid), avoiding NaN -> int errors.
    """
    return pd.to_numeric(s, errors="coerce").fillna(0).astype("int64")


def _any_text_contains(series: pd.Series, patterns: Iterable[str]) -> pd.Series:
    """
    Case: series is already uppercased. Perform OR over literal substrings (no regex).
    """
    if not patterns:
        return pd.Series([False] * len(series), index=series.index)
    out = pd.Series([False] * len(series), index=series.index)
    for p in patterns:
        out = out | series.str.contains(p, case=False, regex=False, na=False)
    return out


# ---- indicator predicates ----------------------------------------------------


def _pretransit_mask(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    # Code-based
    code_hit = code.isin({"OC", "LP", "IN"})
    # Text hints
    patt = [
        "LABEL CREATED",
        "SHIPMENT INFORMATION SENT TO FEDEX",
        "LABEL",
    ]
    text_hit = _any_text_contains(
        status, patt) | _any_text_contains(desc, patt)
    return code_hit | text_hit


def _delivered_mask(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    code_hit = code.isin({"DL", "DLV"})
    patt = ["DELIVERED"]
    text_hit = _any_text_contains(
        status, patt) | _any_text_contains(desc, patt)
    return code_hit | text_hit


def _exception_mask(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    code_hit = code.isin({"EX", "SE", "DE"})
    patt = ["EXCEPTION", "UNABLE TO DELIVER", "ADDRESS CORRECT", "DAMAGED"]
    text_hit = _any_text_contains(
        status, patt) | _any_text_contains(desc, patt)
    return code_hit | text_hit


def _rts_mask(code: pd.Series, status: pd.Series, desc: pd.Series) -> pd.Series:
    code_hit = code.isin({"RS", "RTS", "RD"})
    patt = ["RETURN TO SENDER",
            "RETURNING PACKAGE TO SHIPPER", "RETURNED TO SHIPPER"]
    text_hit = _any_text_contains(
        status, patt) | _any_text_contains(desc, patt)
    return code_hit | text_hit


def _is_terminal(is_delivered: pd.Series, is_rts: pd.Series) -> pd.Series:
    """
    Terminal means Delivered OR RTS. Exception is NOT terminal for stalled logic.
    """
    d = _as_int(is_delivered) == 1
    r = _as_int(is_rts) == 1
    return d | r


def _stalled_mask(
    days_since: pd.Series,
    is_delivered: pd.Series,
    is_rts: pd.Series,
    *,
    threshold_days: int,
) -> pd.Series:
    """
    Stalled if (DaysSinceLatestEvent >= threshold) AND NOT terminal(Delivered|RTS).
    Exception does NOT block stalled.
    """
    days = _as_int(days_since)  # invalid/missing -> 0
    terminal = _is_terminal(is_delivered, is_rts)
    return (days >= int(threshold_days)) & (~terminal)


# ---- main entry --------------------------------------------------------------


def apply_indicators(
    df: pd.DataFrame,
    *,
    stalled_threshold_days: int = 4,
) -> pd.DataFrame:
    """
    Compute and (re)write the indicator columns:
      - IsPreTransit
      - IsDelivered
      - HasException
      - IsRTS
      - IsStalled   (uses DaysSinceLatestEvent and ignores Exception as a blocker)

    IMPORTANT: If the input already contains any of the indicator columns, we
    **merge** with the derived values (logical OR / max), instead of overwriting.
    This preserves explicitly provided indicators (e.g., from tests or upstream steps).
    """
    out = df.copy()

    # Ensure indicator columns exist (so downstream dtype coercions are stable)
    for col in INDICATOR_COLS:
        if col not in out.columns:
            out[col] = 0

    # Text fields
    code, status, desc = _get_text_cols(out)

    # Derived (from text) masks as 0/1
    derived_pre = _pretransit_mask(code, status, desc).astype("int64")
    derived_dlv = _delivered_mask(code, status, desc).astype("int64")
    derived_exc = _exception_mask(code, status, desc).astype("int64")
    derived_rts = _rts_mask(code, status, desc).astype("int64")

    # Merge with any pre-existing indicator values (max == logical OR for 0/1)
    out["IsPreTransit"] = np.maximum(_as_int(out["IsPreTransit"]), derived_pre)
    out["IsDelivered"] = np.maximum(_as_int(out["IsDelivered"]), derived_dlv)
    out["HasException"] = np.maximum(_as_int(out["HasException"]), derived_exc)
    out["IsRTS"] = np.maximum(_as_int(out["IsRTS"]), derived_rts)

    # Stalled (Exception does NOT block stalled)
    if "DaysSinceLatestEvent" not in out.columns:
        out["DaysSinceLatestEvent"] = 0
    stalled = _stalled_mask(
        out["DaysSinceLatestEvent"],
        out["IsDelivered"],
        out["IsRTS"],
        threshold_days=stalled_threshold_days,
    ).astype("int64")
    out["IsStalled"] = stalled

    return out
