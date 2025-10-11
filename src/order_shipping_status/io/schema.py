# src/order_shipping_status/io/schema.py
from __future__ import annotations

# Columns we add to the output (to be populated from API normalization later)
OUTPUT_FEDEX_COLUMNS = [
    "code",
    "derivedCode",
    "statusByLocale",
    "description",
]

# Output-calculated status column name
OUTPUT_STATUS_COLUMN = "CalculatedStatus"

# Known legacy name in the input (we won't rename input now; this is for reference)
LEGACY_STATUS_COLUMN = "Delivery Tracking Status"

# Only add here if truly required to run the pipeline
REQUIRED_INPUT_COLUMNS = [
    # e.g., "Tracking Number",
]
