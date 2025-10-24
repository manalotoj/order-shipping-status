import datetime as dt
from datetime import date

import pandas as pd

from order_shipping_status.pipelines.preprocessor import Preprocessor


def test_prior_week_range_computes_sunday_to_saturday_for_prior_week():
    # reference date: 2025-10-22 (Wednesday)
    ref = date(2025, 10, 22)
    p = Preprocessor(reference_date=ref)
    start, end = p.prior_week_range()
    # prior week should be 2025-10-12 .. 2025-10-18 (Sunday..Saturday)
    assert start == date(2025, 10, 12)
    assert end == date(2025, 10, 18)


def test_prepare_filters_promised_delivery_date_to_prior_week_and_drops_first_column():
    ref = date(2025, 10, 22)
    p = Preprocessor(reference_date=ref, enable_date_filter=True)

    # Build a DataFrame with an extraneous first column (simulates typical input)
    rows = [
        # in prior week (should be kept)
        {"X": 0, "Promised Delivery Date": "2025-10-12", "Tracking Number": "TN-A"},
        {"X": 1, "Promised Delivery Date": "2025-10-15", "Tracking Number": "TN-B"},
        # outside prior week (should be dropped)
        {"X": 2, "Promised Delivery Date": "2025-10-20", "Tracking Number": "TN-C"},
        {"X": 3, "Promised Delivery Date": "2025-10-10", "Tracking Number": "TN-D"},
    ]

    df = pd.DataFrame(rows)

    out = p.prepare(df)

    # drop_first_column removes 'X', so remaining rows should be only the two in prior week
    assert len(out) == 2
    tns = set(out["Tracking Number"].astype(str).tolist())
    assert tns == {"TN-A", "TN-B"}


def test_prepare_skips_date_filter_when_disabled():
    ref = date(2025, 10, 22)
    p = Preprocessor(reference_date=ref, enable_date_filter=False)

    rows = [
        {"X": 0, "Promised Delivery Date": "2025-10-12", "Tracking Number": "TN-A"},
        {"X": 1, "Promised Delivery Date": "2025-10-15", "Tracking Number": "TN-B"},
        {"X": 2, "Promised Delivery Date": "2025-10-20", "Tracking Number": "TN-C"},
    ]
    df = pd.DataFrame(rows)
    out = p.prepare(df)

    # date filter disabled -> all rows preserved (after dropping first column)
    assert len(out) == 3

# tests/unit/pipelines/test_preprocessor.py


def test_prior_week_range_fixed():
    # prior week for 2025-01-15 (Wed) is 2025-01-05..2025-01-11 (Sun..Sat)
    p = Preprocessor(reference_date=dt.date(2025, 1, 15))
    start, end = p.prior_week_range()  # use the public helper
    assert str(start) == "2025-01-05" and str(end) == "2025-01-11"


def test_drop_first_column():
    df = pd.DataFrame([{"X": 1, "A": 2}])
    p = Preprocessor()
    # accessing private helper is fine for unit tests
    out = p._drop_first_column(df)
    assert list(out.columns) == ["A"]


def test_filter_by_prior_week_and_not_delivered_and_prepare():
    rows = [
        {"X": "x", "Promised Delivery Date": "2025-01-05",
            "Delivery Tracking Status": "in transit", "A": 1},  # keep
        {"X": "x", "Promised Delivery Date": "2025-01-11",
            # keep (not delivered)
            "Delivery Tracking Status": "Exception", "A": 2},
        {"X": "x", "Promised Delivery Date": "2025-01-12",
            "Delivery Tracking Status": "in transit", "A": 3},  # out of range
        {"X": "x", "Promised Delivery Date": "2025-01-06",
            # delivered (drop)
            "Delivery Tracking Status": "Delivered", "A": 4},
        {"X": "x", "Promised Delivery Date": "2025-01-07",
            "Delivery Tracking Status": None, "A": 5},          # keep
    ]
    df = pd.DataFrame(rows)
    p = Preprocessor(reference_date=dt.date(2025, 1, 15))
    out = p.prepare(df)
    assert "X" not in out.columns and "A" in out.columns
    assert len(out) == 3


def test_preprocessor_logs_deltas():
    class Logger:
        def __init__(self):
            self.lines = []

        def info(self, fmt, *args):
            try:
                self.lines.append(fmt % args)
            except TypeError:
                # in case fmt is a fully formatted string
                self.lines.append(str(fmt))

    logger = Logger()
    df = pd.DataFrame([
        {"X": "drop", "Promised Delivery Date": "2025-01-06",
            "Delivery Tracking Status": "in transit"},
        {"X": "drop", "Promised Delivery Date": "2025-01-12",
            "Delivery Tracking Status": "in transit"},
        {"X": "drop", "Promised Delivery Date": "2025-01-07",
            "Delivery Tracking Status": "Delivered"},
    ])
    p = Preprocessor(reference_date=dt.date(2025, 1, 15), logger=logger)
    _ = p.prepare(df)

    # smoke: log lines for each stage appeared
    joined = "\n".join(logger.lines)
    assert "drop_first_column" in joined
    assert "filter_by_prior_week" in joined
    assert "filter_not_delivered" in joined
