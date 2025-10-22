# src/order_shipping_status/rules/classifier.py
from __future__ import annotations

from typing import Iterable
import pandas as pd

# Output statuses we set
PRETRANSIT = "PreTransit"
DELIVERED = "Delivered"
EXCEPTION = "Exception"
DAMAGED = "Damaged"

# -------- Text hints (lowercased) --------
_PRETRANSIT_HINTS: tuple[str, ...] = (
    "label created",
    "shipment information sent",
    "shipment info sent",
    "order created",
    "pending pickup",
    "awaiting pickup",
    "waiting for carrier pickup",
)

_DELIVERED_HINTS: tuple[str, ...] = ("delivered",)

_EXCEPTION_HINTS: tuple[str, ...] = (
    "exception",
    "delivery exception",
    "address correction",
    "damage",
)

# Hints specifically used to detect damaged/multiple-labels conditions
_DAMAGED_HINTS: tuple[str, ...] = (
    "damage",
    "damaged",
    "multiple tracking labels",
)

# -------- FedEx (commonly observed) codes --------
# Pre-transit / label created
# observed for "label created" / pre-advice
_PRETRANSIT_CODES: set[str] = {"OC", "LP"}

# Delivered
# Some captures show "DL", others "DLV" (be permissive to avoid false negatives)
_DELIVERED_CODES: set[str] = {"DLV", "DL", "DEL"}

# Exception
_EXCEPTION_CODES: set[str] = {"DE", "SE",
                              "EX", "EXC"}  # common exception codes


def _any_in(text: str, phrases: Iterable[str]) -> bool:
    t = (text or "").casefold()
    return any(p in t for p in phrases)


def classify_row_pretransit(
    code: str, derived: str, status_by_locale: str, description: str
) -> bool:
    """
    Independent indicator: detect pre-transit / label-created signals.
    Do NOT exclude when delivered/exception text is present; indicators are non-exclusive.
    """
    cu, du = (code or "").upper(), (derived or "").upper()
    if cu in _PRETRANSIT_CODES or du in _PRETRANSIT_CODES:
        return True
    # text fallback
    s = status_by_locale or ""
    d = description or ""
    return _any_in(s, _PRETRANSIT_HINTS) or _any_in(d, _PRETRANSIT_HINTS)


def classify_row_delivered(
    code: str, derived: str, status_by_locale: str, description: str
) -> bool:
    cu, du = (code or "").upper(), (derived or "").upper()
    if cu in _DELIVERED_CODES or du in _DELIVERED_CODES:
        return True
    return _any_in(status_by_locale, _DELIVERED_HINTS) or _any_in(description, _DELIVERED_HINTS)


def classify_row_exception(
    code: str, derived: str, status_by_locale: str, description: str
) -> bool:
    cu, du = (code or "").upper(), (derived or "").upper()
    if cu in _EXCEPTION_CODES or du in _EXCEPTION_CODES:
        return True
    return _any_in(status_by_locale, _EXCEPTION_HINTS) or _any_in(description, _EXCEPTION_HINTS)


def classify_row_damaged(
    code: str, derived: str, status_by_locale: str, description: str
) -> bool:
    """Detect damaged shipments: require an exception-like code/text AND damaged/multiple-labels hints.

    This mirrors the indicators logic where IsDamaged requires both an exception signal and
    a textual hint (e.g. 'damaged' or 'multiple tracking labels').
    """
    cu, du = (code or "").upper(), (derived or "").upper()
    # Conservative: require an exception code/derivedCode AND damaged/multiple-labels text
    if cu in _EXCEPTION_CODES or du in _EXCEPTION_CODES:
        return _any_in(status_by_locale, _DAMAGED_HINTS) or _any_in(description, _DAMAGED_HINTS)
    return False


def apply_rules(df: pd.DataFrame, *, status_col: str = "CalculatedStatus") -> pd.DataFrame:
    """
    Apply simple precedence:
      1) Delivered
      2) Exception
      3) PreTransit
    Only sets status when current value is empty.
    """
    out = df.copy()

    # Ensure required columns exist (avoid KeyError / NaN in Excel)
    for col in ("code", "derivedCode", "statusByLocale", "description", status_col):
        if col not in out.columns:
            out[col] = ""

    # Helper to read + coerce to str
    def _fields(r: pd.Series) -> tuple[str, str, str, str]:
        return (
            str(r.get("code", "")),
            str(r.get("derivedCode", "")),
            str(r.get("statusByLocale", "")),
            str(r.get("description", "")),
        )

    # Only fill when empty
    empty = out[status_col].astype("string").fillna("") == ""

    # 1) Delivered
    deliv_mask = out.apply(
        lambda r: classify_row_delivered(*_fields(r)), axis=1)
    out.loc[empty & deliv_mask, status_col] = DELIVERED
    empty = out[status_col].astype("string").fillna("") == ""

    # 2) Exception
    exc_mask = out.apply(lambda r: classify_row_exception(*_fields(r)), axis=1)
    out.loc[empty & exc_mask, status_col] = EXCEPTION
    empty = out[status_col].astype("string").fillna("") == ""

    # 3) PreTransit
    pre_mask = out.apply(
        lambda r: classify_row_pretransit(*_fields(r)), axis=1)
    out.loc[empty & pre_mask, status_col] = PRETRANSIT

    # Ensure dtype is string for the status col
    out[status_col] = out[status_col].astype("string").fillna("")
    return out
