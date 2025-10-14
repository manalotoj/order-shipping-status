from __future__ import annotations
import pandas as pd

from order_shipping_status.rules.status_mapper import map_indicators_to_status


def _df(rows):
    return pd.DataFrame(rows)


def test_mapper_delivered_with_exception_has_precedence_and_reasons():
    df = _df([
        {"IsDelivered": 1, "HasException": 1},  # others absent -> treated as 0
    ])
    out = map_indicators_to_status(df)
    assert out.loc[0, "CalculatedStatus"] == "DeliveredWithIssue"
    assert out.loc[0, "CalculatedReasons"] == "Delivered;Exception"


def test_mapper_rts_overrides_everything():
    df = _df([
        {"IsRTS": 1, "IsDelivered": 1, "HasException": 1, "IsPreTransit": 1},
    ])
    out = map_indicators_to_status(df)
    assert out.loc[0, "CalculatedStatus"] == "ReturnedToSender"
    # Reasons reflect *all* active indicators (order: PreTransit, Delivered, Exception, ReturnedToSender)
    assert out.loc[0, "CalculatedReasons"] == "PreTransit;Delivered;Exception;ReturnedToSender"


def test_mapper_pretransit_when_only_pretransit_is_set():
    df = _df([
        {"IsPreTransit": 1},
    ])
    out = map_indicators_to_status(df)
    assert out.loc[0, "CalculatedStatus"] == "PreTransit"
    assert out.loc[0, "CalculatedReasons"] == "PreTransit"


def test_mapper_empty_when_no_indicators():
    df = _df([{}])
    out = map_indicators_to_status(df)
    assert out.loc[0, "CalculatedStatus"] == ""
    assert out.loc[0, "CalculatedReasons"] == ""
