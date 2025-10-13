# src/order_shipping_status/rules/indicators.py
from __future__ import annotations
import pandas as pd
from order_shipping_status.rules.classifier import (
    classify_row_pretransit,
    classify_row_delivered,
    classify_row_exception,
)

# Public, testable list if you want to assert presence
INDICATOR_COLUMNS = ("IsPreTransit", "IsDelivered", "HasException")


def _fields(r: pd.Series) -> tuple[str, str, str, str]:
    return (
        str(r.get("code", "")),
        str(r.get("derivedCode", "")),
        str(r.get("statusByLocale", "")),
        str(r.get("description", "")),
    )


def apply_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Compute independent shipment indicators (0/1). No precedence or collapsing."""
    out = df.copy()

    for col in ("code", "derivedCode", "statusByLocale", "description"):
        if col not in out.columns:
            out[col] = ""

    out["IsPreTransit"] = out.apply(
        lambda r: 1 if classify_row_pretransit(*_fields(r)) else 0, axis=1)
    out["IsDelivered"] = out.apply(
        lambda r: 1 if classify_row_delivered(*_fields(r)) else 0, axis=1)
    out["HasException"] = out.apply(
        lambda r: 1 if classify_row_exception(*_fields(r)) else 0, axis=1)
    return out
