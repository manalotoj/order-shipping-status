import datetime as dt
import pandas as pd
from order_shipping_status.pipelines.preprocessor import Preprocessor


def test_prior_week_range_fixed():
    # prior week for 2025-01-15 (Wed) is 2025-01-05..2025-01-11 (Sun..Sat)
    p = Preprocessor(reference_date=dt.date(2025, 1, 15))
    start, end = p._prior_week_range()  # accessing protected helper is OK in tests
    assert str(start) == "2025-01-05" and str(end) == "2025-01-11"


def test_drop_first_column():
    df = pd.DataFrame([{"X": 1, "A": 2}])
    out = Preprocessor()._drop_first_column(df)  # protected helper
    assert list(out.columns) == ["A"]


def test_filter_by_prior_week_and_not_delivered_and_prepare():
    rows = [
        {"X": "x", "Promised Delivery Date": "2025-01-05",
         "Delivery Tracking Status": "in transit", "A": 1},  # keep
        {"X": "x", "Promised Delivery Date": "2025-01-11",
         "Delivery Tracking Status": "Exception", "A": 2},   # keep
        {"X": "x", "Promised Delivery Date": "2025-01-12",
         "Delivery Tracking Status": "in transit", "A": 3},  # out of range
        {"X": "x", "Promised Delivery Date": "2025-01-06",
         # delivered (filtered out)
         "Delivery Tracking Status": "Delivered", "A": 4},
        {"X": "x", "Promised Delivery Date": "2025-01-07",
         "Delivery Tracking Status": None, "A": 5},          # keep
    ]
    df = pd.DataFrame(rows)
    out = Preprocessor(reference_date=dt.date(2025, 1, 15)).prepare(df)

    assert "X" not in out.columns and "A" in out.columns
    assert len(out) == 3


def test_preprocessor_logs_deltas(tmp_path, capsys):
    import pandas as pd
    import datetime as dt
    from order_shipping_status.pipelines.preprocessor import Preprocessor

    class Logger:
        def __init__(self): self.lines = []
        def info(self, *args): self.lines.append(args[0] % args[1:])

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
    assert any("drop_first_column" in line for line in logger.lines)
    assert any("filter_by_prior_week" in line for line in logger.lines)
    assert any("filter_not_delivered" in line for line in logger.lines)
