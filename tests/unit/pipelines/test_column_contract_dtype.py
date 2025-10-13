import pandas as pd
from order_shipping_status.pipelines.column_contract import ColumnContract
from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS, OUTPUT_STATUS_COLUMN


def test_ensure_is_idempotent_and_string_dtype():
    base = pd.DataFrame([{"A": 1}])
    c = ColumnContract()
    one = c.ensure(base)
    two = c.ensure(one)
    assert list(one.columns) == list(two.columns)  # no duplicates
    for col in OUTPUT_FEDEX_COLUMNS + [OUTPUT_STATUS_COLUMN]:
        assert col in one.columns
        assert one[col].dtype.name == "string"
