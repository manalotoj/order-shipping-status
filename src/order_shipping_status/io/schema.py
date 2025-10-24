# src/order_shipping_status/io/schema.py
from __future__ import annotations


OUTPUT_FEDEX_COLUMNS = ["code", "derivedCode", "statusByLocale", "description"]
OUTPUT_STATUS_COLUMN = "CalculatedStatus"
INDICATOR_COLS = ["IsPreTransit", "IsDelivered",
                  "HasException", "IsRTS", "Damaged", "UnableToDeliver", "IsStalled"]
AUX_COLS = ["CalculatedReasons",
            "LatestEventTimestampUtc", "DaysSinceLatestEvent"]

# desired order suffix (original columns are kept in their original order first)
OUTPUT_SUFFIX_ORDER = OUTPUT_FEDEX_COLUMNS + \
    INDICATOR_COLS + [OUTPUT_STATUS_COLUMN] + AUX_COLS

# Known legacy name in the input (we won't rename input now; this is for reference)
LEGACY_STATUS_COLUMN = "Delivery Tracking Status"

# Only add here if truly required to run the pipeline
REQUIRED_INPUT_COLUMNS = [
    # e.g., "Tracking Number",
]
