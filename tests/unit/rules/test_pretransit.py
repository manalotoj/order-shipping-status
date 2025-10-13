import pandas as pd
from order_shipping_status.rules.classifier import apply_rules


def test_pretransit_label_created_sets_status():
    df = pd.DataFrame([{
        "statusByLocale": "Label created",
        "description": "Shipment information sent to FedEx",
        "CalculatedStatus": "",
    }])
    out = apply_rules(df)
    assert out.loc[0, "CalculatedStatus"] == "PreTransit"


def test_pretransit_ignored_if_delivered_or_exception():
    df = pd.DataFrame([
        {"statusByLocale": "Delivered",
            "description": "Left at door", "CalculatedStatus": ""},
        {"statusByLocale": "Delivery exception",
            "description": "Address correction requested", "CalculatedStatus": ""},
    ])
    out = apply_rules(df)
    assert out.loc[0, "CalculatedStatus"] == ""
    assert out.loc[1, "CalculatedStatus"] == ""


def test_pretransit_respects_existing_status_not_overwriting():
    df = pd.DataFrame([{
        "statusByLocale": "Label created",
        "description": "Shipment information sent",
        "CalculatedStatus": "InTransit",  # already set by a higher-precedence rule
    }])
    out = apply_rules(df)
    assert out.loc[0, "CalculatedStatus"] == "InTransit"
