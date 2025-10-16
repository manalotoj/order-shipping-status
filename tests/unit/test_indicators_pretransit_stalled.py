from __future__ import annotations

import pandas as pd

from order_shipping_status.rules.indicators import apply_indicators


def test_pretransit_and_stalled_mutually_exclusive():
    # Build a DataFrame with one row that is pre-transit (code OC) and days >= threshold
    df = pd.DataFrame([
        {
            "Tracking Number": "T1",
            "code": "OC",
            "statusByLocale": "Label created",
            "description": "Shipment information sent to FedEx",
            "LatestEventTimestampUtc": "2025-10-01T00:00:00Z",
            "ScanEventsCount": 0,
        }
    ])

    # Use threshold 1 so days >= threshold will be true with a far-past timestamp
    out = apply_indicators(df.copy(), stalled_threshold_days=1)

    is_pre = int(out.loc[0, "IsPreTransit"])
    is_stalled = int(out.loc[0, "IsStalled"])

    assert is_pre == 1, "Expected row to be marked pre-transit"
    # Must be mutually exclusive: if pre-transit then not stalled
    assert is_stalled == 0, "Pre-transit row should not be marked stalled"
