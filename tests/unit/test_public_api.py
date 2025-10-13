def test_public_api_exports():
    import order_shipping_status as oss
    assert hasattr(oss, "WorkbookProcessor")
    assert hasattr(oss, "process_workbook")  # back-compat wrapper
