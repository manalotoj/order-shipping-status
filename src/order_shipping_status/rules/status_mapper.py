# src/order_shipping_status/rules/status_mapper.py
from __future__ import annotations

import pandas as pd

# We treat these as independent boolean-like indicators (0/1). Some may be missing.
# IsStalled is optional; if absent, it is treated as 0 everywhere.
_BASE_INDICATOR_COLS = ("IsPreTransit", "IsDelivered", "HasException", "IsRTS")
_OPTIONAL_INDICATOR_COLS = ("IsStalled",)  # treated as 0 if missing


def _as_int(series: pd.Series) -> pd.Series:
    """
    Coerce a Series to integer 0/1 robustly, handling NaN/strings.
    """
    try:
        return pd.to_numeric(series, errors="coerce").fillna(0).astype("int64")
    except Exception:
        # ultra-defensive fallback
        return series.fillna(0).astype("int64", errors="ignore")


def _reasons_from_row(pre: int, dlv: int, exc: int, rts: int, stalled: int) -> str:
    reasons: list[str] = []
    if pre == 1:
        reasons.append("PreTransit")
    if dlv == 1:
        reasons.append("Delivered")
    if exc == 1:
        reasons.append("Exception")
    if rts == 1:
        reasons.append("ReturnedToSender")
    if stalled == 1:
        reasons.append("Stalled")
    return ";".join(reasons)


def map_indicators_to_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts independent boolean-like indicator columns into:
      - CalculatedStatus (primary label with deterministic precedence)
      - CalculatedReasons (semicolon-joined list of active indicators)

    Precedence (top to bottom):
        ReturnedToSender
        DeliveredWithIssue (Delivered & Exception together)
        Delivered
        Exception
        Stalled            (only if not Delivered/Exception/RTS)
        PreTransit
        ""  (no status)

    Notes:
    - Missing indicators are treated as 0.
    - Does not mutate input; returns a new DataFrame.
    """
    out = df.copy()

    # Ensure required indicator columns exist (default 0)
    for col in _BASE_INDICATOR_COLS:
        if col not in out.columns:
            out[col] = 0
    # Optional indicators (e.g., IsStalled) → treat as 0 if absent
    for col in _OPTIONAL_INDICATOR_COLS:
        if col not in out.columns:
            out[col] = 0

    # Coerce to int 0/1 for vectorized logic
    pre = _as_int(out["IsPreTransit"])
    dlv = _as_int(out["IsDelivered"])
    exc = _as_int(out["HasException"])
    rts = _as_int(out["IsRTS"])
    stalled = _as_int(out.get("IsStalled", 0))

    # Build reasons string
    out["CalculatedReasons"] = [
        _reasons_from_row(int(pre.iloc[i]), int(dlv.iloc[i]), int(exc.iloc[i]),
                          int(rts.iloc[i]), int(stalled.iloc[i]))
        for i in range(len(out))
    ]
    out["CalculatedReasons"] = out["CalculatedReasons"].astype("string")

    # Ensure CalculatedStatus exists
    if "CalculatedStatus" not in out.columns:
        out["CalculatedStatus"] = ""

    # Vectorized precedence mapping
    # 1) Returned to sender
    out.loc[rts == 1, "CalculatedStatus"] = "ReturnedToSender"

    # 2) Delivered with an exception (co-exist). You may rename later if desired.
    out.loc[(rts == 0) & (dlv == 1) & (exc == 1),
            "CalculatedStatus"] = "DeliveredWithIssue"

    # 3) Delivered
    out.loc[(rts == 0) & (dlv == 1) & (exc == 0),
            "CalculatedStatus"] = "Delivered"

    # 4) Exception
    out.loc[(rts == 0) & (dlv == 0) & (exc == 1),
            "CalculatedStatus"] = "Exception"

    # 5) Stalled (only if not RTS/Delivered/Exception)
    out.loc[(rts == 0) & (dlv == 0) & (exc == 0) & (
        stalled == 1), "CalculatedStatus"] = "Stalled"

    # 6) PreTransit
    out.loc[(rts == 0) & (dlv == 0) & (exc == 0) & (stalled == 0)
            & (pre == 1), "CalculatedStatus"] = "PreTransit"

    # Normalize dtype
    out["CalculatedStatus"] = out["CalculatedStatus"].astype("string")

    return out
