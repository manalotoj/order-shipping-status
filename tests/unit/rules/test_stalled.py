# tests/unit/rules/test_stalled.py
from __future__ import annotations

import pandas as pd

from order_shipping_status.rules.indicators import apply_indicators


def test_stalled_sets_indicator_when_threshold_met_and_not_terminal():
    # New behavior: Exception does NOT block stalled.
    df = pd.DataFrame([
        {"DaysSinceLatestEvent": 5,  "IsDelivered": 0,
            "HasException": 0, "IsRTS": 0},  # -> stalled
        {"DaysSinceLatestEvent": 3,  "IsDelivered": 0,
            "HasException": 0, "IsRTS": 0},  # -> not stalled
        {"DaysSinceLatestEvent": 10, "IsDelivered": 1,
            "HasException": 0, "IsRTS": 0},  # delivered blocks stalled
        {"DaysSinceLatestEvent": 10, "IsDelivered": 0, "HasException": 1,
            "IsRTS": 0},  # exception does NOT block stalled
        {"DaysSinceLatestEvent": 10, "IsDelivered": 0,
            "HasException": 0, "IsRTS": 1},  # RTS blocks stalled
        {"DaysSinceLatestEvent": "bad", "IsDelivered": 0,
            "HasException": 0, "IsRTS": 0},  # bad -> 0
    ])
    out = apply_indicators(df, stalled_threshold_days=4)
    # Expected: [1,0,0,1,0,0]
    assert list(out["IsStalled"].astype(int)) == [1, 0, 0, 1, 0, 0]
