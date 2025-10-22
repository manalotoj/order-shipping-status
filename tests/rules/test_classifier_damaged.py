import pandas as pd
from order_shipping_status.rules.classifier import (
    classify_row_damaged,
)


def test_classify_damaged_with_exception_and_desc():
    assert classify_row_damaged("DE", "", "", "Package damaged during transit")


def test_classify_damaged_with_exception_and_status_multiple_labels():
    assert classify_row_damaged("DE", "", "Multiple tracking labels found", "")


def test_not_damaged_without_exception_code():
    # Has damaged text but no exception code; classifier should still allow textual exception
    # + damaged text combination for detection per our conservative rules.
    assert not classify_row_damaged("XX", "", "DAMAGED", "")


def test_not_damaged_exception_but_unrelated_text():
    assert not classify_row_damaged(
        "DE", "", "DELIVERY EXCEPTION", "Lost in transit")
