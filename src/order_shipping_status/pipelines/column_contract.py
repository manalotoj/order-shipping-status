# src/order_shipping_status/pipelines/column_contract.py
from __future__ import annotations

from typing import Iterable
import pandas as pd

from order_shipping_status.io.schema import (
    # tuple like ("code", "derivedCode", "statusByLocale", "description")
    OUTPUT_FEDEX_COLUMNS,
    OUTPUT_STATUS_COLUMN,      # "CalculatedStatus"
)

# Exported so tests can import it directly
INDICATOR_COLS: tuple[str, ...] = (
    "IsPreTransit",
    "IsDelivered",
    "HasException",
    "IsRTS",
    "IsStalled",
)


def _as_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype("int64")


class ColumnContract:
    """
    Ensures the processed DataFrame contains:
      - OUTPUT_FEDEX_COLUMNS (string dtype)
      - indicator columns (int 0/1)
      - CalculatedStatus (string)
      - CalculatedReasons (string)
    and orders columns as:
      [<originals in original order>] +
      list(OUTPUT_FEDEX_COLUMNS) +
      list(INDICATOR_COLS) +
      [OUTPUT_STATUS_COLUMN, "CalculatedReasons"] +
      [<any other unexpected columns>]
    """

    def ensure(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        # Ensure presence of contract columns with sensible defaults
        for col in OUTPUT_FEDEX_COLUMNS:
            if col not in out.columns:
                out[col] = ""

        for col in INDICATOR_COLS:
            if col not in out.columns:
                out[col] = 0

        if OUTPUT_STATUS_COLUMN not in out.columns:
            out[OUTPUT_STATUS_COLUMN] = ""

        if "CalculatedReasons" not in out.columns:
            out["CalculatedReasons"] = ""

        # Cast dtypes
        for col in list(OUTPUT_FEDEX_COLUMNS) + [OUTPUT_STATUS_COLUMN, "CalculatedReasons"]:
            out[col] = out[col].astype("string").fillna("")

        for col in INDICATOR_COLS:
            out[col] = _as_int(out[col])

        # Build desired order:
        originals: list[str] = list(df.columns)
        suffix_order: list[str] = list(OUTPUT_FEDEX_COLUMNS) + list(INDICATOR_COLS) + [
            OUTPUT_STATUS_COLUMN,
            "CalculatedReasons",
        ]

        # Keep suffix entries only if they aren't already in the originals (avoid duplication)
        suffix_unique = [c for c in suffix_order if c not in originals]

        # Any other columns that were added during normalization/enrichment but not in suffix
        # should go after the suffix (rare, but keeps behavior stable).
        extras = [
            c for c in out.columns if c not in originals and c not in suffix_order]

        ordered = originals + suffix_unique + extras
        out = out.reindex(columns=ordered)

        return out
