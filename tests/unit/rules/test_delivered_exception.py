# tests/unit/rules/test_delivered_exception.py
import pandas as pd

from order_shipping_status.rules.classifier import apply_rules


def _run(df: pd.DataFrame) -> pd.DataFrame:
    return apply_rules(df)


def test_delivered_by_code_variants():
    for code in ("DLV", "DL"):
        df = pd.DataFrame([{
            "code": code, "derivedCode": "", "statusByLocale": "", "description": "", "CalculatedStatus": ""
        }])
        out = _run(df)
        assert out.loc[0, "CalculatedStatus"] == "Delivered"


def test_delivered_by_text():
    df = pd.DataFrame([{
        "code": "", "derivedCode": "", "statusByLocale": "Delivered", "description": "", "CalculatedStatus": ""
    }])
    out = _run(df)
    assert out.loc[0, "CalculatedStatus"] == "Delivered"


def test_exception_by_code():
    df = pd.DataFrame([{
        "code": "EXC", "derivedCode": "", "statusByLocale": "", "description": "", "CalculatedStatus": ""
    }])
    out = _run(df)
    assert out.loc[0, "CalculatedStatus"] == "Exception"


def test_exception_by_text():
    df = pd.DataFrame([{
        "code": "", "derivedCode": "", "statusByLocale": "Delivery exception", "description": "", "CalculatedStatus": ""
    }])
    out = _run(df)
    assert out.loc[0, "CalculatedStatus"] == "Exception"


def test_precedence_exception_over_pretransit():
    # Both hints present; Exception should win over PreTransit
    df = pd.DataFrame([{
        "code": "", "derivedCode": "LP",
        "statusByLocale": "Label created",
        "description": "Delivery exception observed",
        "CalculatedStatus": ""
    }])
    out = _run(df)
    assert out.loc[0, "CalculatedStatus"] == "Exception"


def test_precedence_delivered_over_pretransit():
    df = pd.DataFrame([{
        "code": "OC", "derivedCode": "",
        "statusByLocale": "Delivered",  # delivered text
        "description": "",
        "CalculatedStatus": ""
    }])
    out = _run(df)
    assert out.loc[0, "CalculatedStatus"] == "Delivered"
