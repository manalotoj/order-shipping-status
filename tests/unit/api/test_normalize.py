# tests/unit/api/test_normalize.py
from order_shipping_status.api.normalize import normalize_fedex
from order_shipping_status.models import NormalizedShippingData


def test_normalize_fedex_minimal_flat():
    payload = {"code": "DLV",
               "statusByLocale": "Delivered", "description": "ok"}
    norm = normalize_fedex(payload, tracking_number="123",
                           carrier_code="FDX", source="ReplayTransport")
    assert isinstance(norm, NormalizedShippingData)
    assert norm.to_excel_cols() == {
        "code": "DLV", "derivedCode": "DLV", "statusByLocale": "Delivered", "description": "ok"
    }


def test_normalize_fedex_deep_shape():
    payload = {
        "completeTrackResults": [{"trackResults": [{"latestStatusDetail": {
            "code": "EXC", "derivedCode": "EXC", "statusByLocale": "Exception", "description": "Damage"
        }}]}]
    }
    norm = normalize_fedex(payload, tracking_number="T",
                           carrier_code="FDX", source="replay")
    assert isinstance(norm, NormalizedShippingData)
    assert norm.code == "EXC" and norm.derivedCode == "EXC"


def test_normalize_fallback_scanevents():
    payload = {"scanEvents": [{"date": "2025-01-10T01:00:00Z", "derivedStatusCode": "IT",
                               "derivedStatus": "In transit", "eventDescription": "At facility"}]}
    norm = normalize_fedex(payload, tracking_number="T",
                           carrier_code="FDX", source="replay")
    assert norm.statusByLocale == "In transit" and norm.code == "IT"


def test_normalize_error_shape_graceful():
    payload = {"error": {"code": "TRACKINGNUMBER.NOTFOUND"}}
    norm = normalize_fedex(payload, tracking_number="T",
                           carrier_code="FDX", source="replay")
    # no crash; just empty columns with error captured in raw
    assert norm.code in ("", "ERR")
    assert norm.raw.get("error") is not None
