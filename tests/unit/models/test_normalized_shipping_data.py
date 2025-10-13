from order_shipping_status.models import NormalizedShippingData


def test_to_excel_cols_derives_code_when_missing():
    m = NormalizedShippingData(
        carrier="FEDEX", tracking_number="T", carrier_code="FDX",
        code="IT", derivedCode="", statusByLocale="In transit", description="ok",
        actual_delivery_dt=None, possession_status=None, service_type=None, service_desc=None,
        origin_city=None, origin_state=None, dest_city=None, dest_state=None,
        received_by_name=None, raw={}
    )
    cols = m.to_excel_cols()
    assert cols["derivedCode"] == "IT"
