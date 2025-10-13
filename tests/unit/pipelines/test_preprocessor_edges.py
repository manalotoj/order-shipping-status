import datetime as dt
import pandas as pd
from order_shipping_status.pipelines.preprocessor import Preprocessor


def test_prior_week_boundaries_inclusive():
    # Wed; prior week Sun..Sat = 2025-01-05..2025-01-11
    ref = dt.date(2025, 1, 15)
    df = pd.DataFrame([
        {"X": "x", "Promised Delivery Date": "2025-01-05",
            "Delivery Tracking Status": "in transit"},  # keep (Sun)
        {"X": "x", "Promised Delivery Date": "2025-01-11",
            "Delivery Tracking Status": "exception"},   # keep (Sat)
        {"X": "x", "Promised Delivery Date": "2025-01-04",
            "Delivery Tracking Status": "in transit"},  # drop (before)
        {"X": "x", "Promised Delivery Date": "2025-01-12",
            "Delivery Tracking Status": "in transit"},  # drop (after)
    ])
    out = Preprocessor(reference_date=ref).prepare(df)
    # first column dropped; two boundary rows remain
    assert len(out) == 2
    assert "X" not in out.columns


def test_garbage_dates_and_missing_status_are_kept_by_filters_then_removed_on_status():
    ref = dt.date(2025, 1, 15)
    df = pd.DataFrame([
        {"X": "x", "Promised Delivery Date": "not a date",
            # garbage date -> filtered out by week
            "Delivery Tracking Status": "in transit"},
        # in range and not delivered -> keep
        {"X": "x", "Promised Delivery Date": "2025-01-07",
            "Delivery Tracking Status": None},
        {"X": "x", "Promised Delivery Date": "2025-01-06",
            # delivered (case) -> drop
            "Delivery Tracking Status": "DELIVERED"},
    ])
    out = Preprocessor(reference_date=ref).prepare(df)
    assert len(out) == 1
    assert out["Promised Delivery Date"].iloc[0] == "2025-01-07"
