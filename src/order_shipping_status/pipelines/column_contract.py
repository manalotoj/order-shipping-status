from __future__ import annotations
import pandas as pd
from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS, OUTPUT_STATUS_COLUMN


class ColumnContract:
    """Ensures output schema columns exist without mutating originals."""

    def ensure(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for col in OUTPUT_FEDEX_COLUMNS:
            if col not in out.columns:
                out[col] = ""
        if OUTPUT_STATUS_COLUMN not in out.columns:
            out[OUTPUT_STATUS_COLUMN] = ""
        for col in OUTPUT_FEDEX_COLUMNS + [OUTPUT_STATUS_COLUMN]:
            out[col] = out[col].astype("string").fillna("")
        return out
