# tests/unit/pipelines/test_column_order.py
from __future__ import annotations

import pandas as pd

from order_shipping_status.pipelines.column_contract import ColumnContract, INDICATOR_COLS
from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS, OUTPUT_STATUS_COLUMN


def test_column_order_preserves_original_then_contract_suffix():
    """
    ColumnContract.ensure() should keep original columns in their original order,
    then append the known contract suffix in a stable, readable order.
    """
    # Original input columns in this specific order
    df = pd.DataFrame(columns=["B", "A"])
    out = ColumnContract().ensure(df)

    # 1) originals come first, in original order
    assert list(out.columns[:2]) == ["B", "A"]

    # 2) suffix that must appear (all are guaranteed by the contract)
    expected_suffix = list(OUTPUT_FEDEX_COLUMNS) + list(INDICATOR_COLS) + [
        OUTPUT_STATUS_COLUMN,
        "CalculatedReasons",
    ]

    # All expected suffix columns should be present
    for col in expected_suffix:
        assert col in out.columns

    # And they should appear AFTER the original columns, in the exact sequence
    tail = list(out.columns[2: 2 + len(expected_suffix)])
    assert tail == expected_suffix
