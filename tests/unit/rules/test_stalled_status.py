from __future__ import annotations
import pandas as pd

from order_shipping_status.rules.status_mapper import map_indicators_to_status


def test_stalled_maps_to_status_when_no_higher_precedence():
    df = pd.DataFrame([
        {"IsStalled": 1, "IsDelivered": 0, "HasException": 0,
            "IsRTS": 0, "IsPreTransit": 0},
        {"IsStalled": 1, "IsDelivered": 1, "HasException": 0,
            "IsRTS": 0, "IsPreTransit": 0},  # delivered wins
        {"IsStalled": 1, "IsDelivered": 0, "HasException": 1,
            "IsRTS": 0, "IsPreTransit": 0},  # exception wins
        {"IsStalled": 1, "IsDelivered": 0, "HasException": 0,
            "IsRTS": 1, "IsPreTransit": 0},  # RTS wins
        {"IsStalled": 0, "IsDelivered": 0, "HasException": 0,
            "IsRTS": 0, "IsPreTransit": 1},  # falls back to PreTransit
    ])
    out = map_indicators_to_status(df)
    assert list(out["CalculatedStatus"]) == ["Stalled",
                                             "Delivered", "Exception", "ReturnedToSender", "PreTransit"]
    # reasons include Stalled where appropriate
    assert "CalculatedReasons" in out.columns
    assert out.loc[0, "CalculatedReasons"].split(";")[0] == "Stalled"
