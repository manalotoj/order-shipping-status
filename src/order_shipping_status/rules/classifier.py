# src/order_shipping_status/rules/classifier.py
from __future__ import annotations
import pandas as pd
from typing import Iterable

PRETRANSIT = "PreTransit"

# lowercased text hints
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
    "exception", "delivery exception", "address correction", "damage")

# FedEx codes commonly used for pre-transit / label-created states
_PRETRANSIT_CODES: set[str] = {"OC", "LP"}  # extend later if needed


def _any_in(text: str, phrases: Iterable[str]) -> bool:
    t = (text or "").casefold()
    return any(p in t for p in phrases)


def classify_row_pretransit(code: str, derived: str, status_by_locale: str, description: str) -> bool:
    s = (status_by_locale or "")
    d = (description or "")
    if _any_in(s, _DELIVERED_HINTS) or _any_in(d, _DELIVERED_HINTS):
        return False
    if _any_in(s, _EXCEPTION_HINTS) or _any_in(d, _EXCEPTION_HINTS):
        return False
    # code-based signal first
    if (code or "").upper() in _PRETRANSIT_CODES or (derived or "").upper() in _PRETRANSIT_CODES:
        return True
    # then text-based
    return _any_in(s, _PRETRANSIT_HINTS) or _any_in(d, _PRETRANSIT_HINTS)


def apply_rules(df: pd.DataFrame, *, status_col: str = "CalculatedStatus") -> pd.DataFrame:
    out = df.copy()
    # guards
    for col in ("code", "derivedCode", "statusByLocale", "description", status_col):
        if col not in out.columns:
            out[col] = ""  # keep as empty string to avoid NaN in Excel

    # Only fill when empty
    mask_empty = out[status_col].astype("string").fillna("") == ""
    pretransit_mask = out.apply(
        lambda r: classify_row_pretransit(
            str(r.get("code", "")),
            str(r.get("derivedCode", "")),
            str(r.get("statusByLocale", "")),
            str(r.get("description", "")),
        ),
        axis=1,
    )
    out.loc[mask_empty & pretransit_mask, status_col] = PRETRANSIT
    out[status_col] = out[status_col].astype("string").fillna("")
    return out
