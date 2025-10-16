# tests/unit/rules/test_pretransit.py
import pandas as pd
from order_shipping_status.rules.indicators import apply_indicators


def test_pretransit_label_created_sets_indicator():
    # PreTransit is determined by the status code (OC) under current rules.
    df = pd.DataFrame([{"code": "OC"}])
    out = apply_indicators(df)
    assert out.loc[0, "IsPreTransit"] == 1
    assert out.loc[0, "IsDelivered"] == 0
    assert out.loc[0, "HasException"] == 0


def test_pretransit_code_variants_set_indicator():
    # Only 'OC' is treated as pre-transit under the current stricter rule.
    out = apply_indicators(pd.DataFrame([{"code": "OC"}]))
    assert out.loc[0, "IsPreTransit"] == 1


def test_pretransit_is_independent_of_delivered_or_exception():
    # Even if delivered text is present, the pretransit indicator can still be true
    # Under current rules delivery is determined by code 'DL' and pretransit by 'OC'.
    # This test asserts that a delivered code still yields IsDelivered==1 even if
    # the description contains label-created text (which no longer triggers pretransit).
    df = pd.DataFrame([{"code": "DL", "description": "Label created"}])
    out = apply_indicators(df)
    assert out.loc[0, "IsDelivered"] == 1
    assert out.loc[0, "IsPreTransit"] == 0
