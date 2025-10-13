# tests/unit/pipelines/test_preprocessor.py
import datetime as dt
import pandas as pd

from order_shipping_status.pipelines.preprocessor import Preprocessor


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
