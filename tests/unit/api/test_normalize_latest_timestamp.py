# tests/unit/api/test_normalize_latest_timestamp.py
from __future__ import annotations

import pandas as pd
# helper is intentionally imported for unit test
from order_shipping_status.api.normalize import normalize_fedex, _latest_event_ts_utc


def test_normalize_latest_event_timestamp_utc_from_scan_events():
    """
    Ensure we pick the MAX across scanEvents[].date and dateAndTimes[].dateTime
    and normalize to a UTC ISO string.
    """
    output = {
        "completeTrackResults": [{
            "trackingNumber": "393832944198",
            "trackResults": [{
                "latestStatusDetail": {
                    "code": "AR",
                    "derivedCode": "DF",
                    "statusByLocale": "Delivery updated",
                    "description": "Arrived at FedEx location",
                },
                "scanEvents": [
                    {"date": "2025-10-02T08:36:00-04:00"},
                    # latest -> 2025-10-03T04:15:00Z
                    {"date": "2025-10-03T00:15:00-04:00"},
                ],
                "dateAndTimes": [
                    {"type": "SHIP", "dateTime": "2025-10-02T00:00:00+00:00"},
                ],
            }]
        }]
    }

    # Assert the timestamp extraction directly from raw payload structure.
    ts = _latest_event_ts_utc(output)  # returns an ISO8601 UTC string or ""
    assert ts, "LatestEventTimestampUtc not derived"
    assert pd.to_datetime(ts, utc=True) == pd.Timestamp("2025-10-03T04:15:00Z")

    # Also sanity-check the core fields are normalized as expected.
    norm = normalize_fedex(
        output,
        tracking_number="393832944198",
        carrier_code="FDX",
        source="unit-test",
    )
    cols = norm.to_excel_cols()
    assert cols["code"] == "AR"
    assert cols["derivedCode"] == "DF"
    assert cols["statusByLocale"] == "Delivery updated"
    assert cols["description"] == "Arrived at FedEx location"
