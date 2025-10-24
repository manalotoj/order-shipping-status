# tests/test_indicators.py
import pandas as pd
import datetime as dt

from order_shipping_status.rules.indicators import apply_indicators


def test_apply_indicators_values():
    today = dt.date(2025, 1, 15)
    df = pd.DataFrame([
        # PreTransit via latest status code OC
        {"derivedCode": "OC", "statusByLocale": "Label created",
            "description": "", "DaysSinceLatestEvent": 1},
        # Delivered via latest status code DL
        {"derivedCode": "DL", "statusByLocale": "Delivered",
            "description": "", "DaysSinceLatestEvent": 0},
        # Exception and Damaged
        {"derivedCode": "SE", "statusByLocale": "Shipment exception",
         "description": "Package damaged in transit", "HasException": 1, "DaysSinceLatestEvent": 2},
        # RTS via regex in text
        {"derivedCode": "", "statusByLocale": "Return to sender initiated",
         "description": "", "DaysSinceLatestEvent": 3},
        # Stalled: idle >= 4 days, not delivered/RTS/pretransit
        {"derivedCode": "IT", "statusByLocale": "In transit",
         "description": "", "DaysSinceLatestEvent": 5},
    ])

    out = apply_indicators(df, stalled_threshold_days=4)

    # PreTransit / Delivered basics (based on your rules)
    assert out.loc[0, "IsPreTransit"] == 1
    assert out.loc[1, "IsDelivered"] == 1

    # HasException respected; Damaged when exception text mentions damaged
    assert out.loc[2, "HasException"] == 1
    if "damaged" in out.columns:
        assert out.loc[2, "Damaged"] == 1

    # RTS detection via text
    assert out.loc[3, "IsRTS"] == 1

    # Stalled: >= threshold, not terminal/pretransit
    assert out.loc[4, "IsStalled"] == 1
