import pandas as pd
from order_shipping_status.rules.indicators import apply_indicators


def _mk_df(rows):
    return pd.DataFrame(rows)


def test_damaged_with_exception_and_desc():
    df = _mk_df([
        {"code": "DE", "statusByLocale": "DELIVERY EXCEPTION",
            "description": "Package damaged during transit"},
    ])
    out = apply_indicators(df)
    assert out.loc[0, "HasException"] == 1
    assert out.loc[0, "IsDamaged"] == 1


def test_damaged_with_exception_and_status_multiple_labels():
    df = _mk_df([
        {"code": "DE", "statusByLocale": "Multiple tracking labels found",
            "description": ""},
    ])
    out = apply_indicators(df)
    assert out.loc[0, "HasException"] == 1
    assert out.loc[0, "IsDamaged"] == 1


def test_not_damaged_without_exception_text_only():
    df = _mk_df([
        {"code": "XX", "statusByLocale": "DAMAGED", "description": ""},
    ])
    out = apply_indicators(df)
    # Existing HasException logic flags exception also from status/description text
    # so HasException will be 1 here even though code isn't in the exception set.
    assert out.loc[0, "HasException"] == 1
    # IsDamaged requires an exception code AND damaged/multiple-labels text per
    # our implementation, so IsDamaged should be 0 for this row.
    assert out.loc[0, "IsDamaged"] == 0


def test_not_damaged_exception_but_unrelated_text():
    df = _mk_df([
        {"code": "DE", "statusByLocale": "DELIVERY EXCEPTION",
            "description": "Lost in transit"},
    ])
    out = apply_indicators(df)
    assert out.loc[0, "HasException"] == 1
    assert out.loc[0, "IsDamaged"] == 0
