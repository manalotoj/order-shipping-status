# src/order_shipping_status/rules/classifier.py
from __future__ import annotations
import re
import pandas as pd
from typing import Iterable

# Status we set in the output workbook
PRETRANSIT = "PreTransit"

# simple keyword banks (lowercased)
_PRETRANSIT_HINTS: tuple[str, ...] = (
    "label created",
    "shipment information sent",
    "shipment info sent",
    "order created",
    "pending pickup",
    "awaiting pickup",
    "waiting for carrier pickup",
)

_DELIVERED_HINTS: tuple[str, ...] = (
    "delivered",
)

_EXCEPTION_HINTS: tuple[str, ...] = (
    "exception",
    "delivery exception",
    "address correction",
    "damage",
)


def _any_in(text: str, phrases: Iterable[str]) -> bool:
    t = (text or "").casefold()
    return any(p in t for p in phrases)


def classify_row_pretransit(status_by_locale: str, description: str) -> bool:
    """
    Returns True if text suggests the shipment hasn't entered transit yet.
    Conservative: only label-created / pre-advice phrases, and not delivered/exception.
    """
    s = (status_by_locale or "").casefold()
    d = (description or "").casefold()
    if _any_in(s, _DELIVERED_HINTS) or _any_in(d, _DELIVERED_HINTS):
        return False
    if _any_in(s, _EXCEPTION_HINTS) or _any_in(d, _EXCEPTION_HINTS):
        return False
    return _any_in(s, _PRETRANSIT_HINTS) or _any_in(d, _PRETRANSIT_HINTS)


def apply_rules(df: pd.DataFrame, *, status_col: str = "CalculatedStatus") -> pd.DataFrame:
    """
    In-place-friendly: returns a DataFrame where CalculatedStatus is set for pre-transit rows,
    without clobbering existing non-empty values.
    """
    out = df.copy()
    # guard columns
    for col in ("statusByLocale", "description", status_col):
        if col not in out.columns:
            out[col] = "" if col != status_col else ""
    # Only set when empty (don’t override future rules you’ll add with precedence)
    mask_empty = out[status_col].astype("string").fillna("") == ""
    # pre-transit candidates
    pretransit_mask = out.apply(
        lambda r: classify_row_pretransit(
            str(r.get("statusByLocale", "")),
            str(r.get("description", "")),
        ),
        axis=1,
    )
    out.loc[mask_empty & pretransit_mask, status_col] = PRETRANSIT
    # ensure dtype
    out[status_col] = out[status_col].astype("string").fillna("")
    return out
