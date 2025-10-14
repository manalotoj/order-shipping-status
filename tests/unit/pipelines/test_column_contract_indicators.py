from __future__ import annotations
import pandas as pd

from order_shipping_status.pipelines.column_contract import ColumnContract


def test_ensure_adds_indicator_and_reason_columns():
    df = pd.DataFrame([{"A": 1}])
    out = ColumnContract().ensure(df)

    # Presence
    for col in ("IsPreTransit", "IsDelivered", "HasException", "IsRTS", "CalculatedReasons"):
        assert col in out.columns

    # Basic type sanity: must be representable as ints or strings
    # We tolerate either int dtype 0/1 or string "0"/"1".
    for col in ("IsPreTransit", "IsDelivered", "HasException", "IsRTS"):
        s = out[col]
        ok_kind = s.dtype.kind in ("i", "u") or str(s.dtype) == "string"
        assert ok_kind, f"{col} should be int-like or pandas 'string' dtype"

    assert str(out["CalculatedReasons"].dtype) == "string"
