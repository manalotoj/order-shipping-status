# tests/unit/rules/test_pretransit.py
import pandas as pd
from order_shipping_status.rules.indicators import apply_indicators


def test_pretransit_label_created_sets_indicator():
    df = pd.DataFrame([{
        "statusByLocale": "Label created",
        "description": "Shipment information sent to FedEx",
    }])
    out = apply_indicators(df)
    assert out.loc[0, "IsPreTransit"] == 1
    assert out.loc[0, "IsDelivered"] == 0
    assert out.loc[0, "HasException"] == 0


def test_pretransit_code_variants_set_indicator():
    for code in ("OC", "LP"):
        out = apply_indicators(pd.DataFrame([{"code": code}]))
        assert out.loc[0, "IsPreTransit"] == 1


def test_pretransit_is_independent_of_delivered_or_exception():
    # Even if delivered text is present, the pretransit indicator can still be true
    df = pd.DataFrame([{
        "statusByLocale": "Delivered",     # delivered signal
        "description": "Label created",    # pretransit signal (independent)
        "code": "", "derivedCode": "",
    }])
    out = apply_indicators(df)
    assert out.loc[0, "IsDelivered"] == 1
    assert out.loc[0, "IsPreTransit"] == 1   # independent, non-exclusive
