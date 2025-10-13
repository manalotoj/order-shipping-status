import json
from pathlib import Path
import pandas as pd
from order_shipping_status.pipelines.enricher import Enricher


class QL:  # quiet logger
    def debug(self, *a, **k): pass


def test_enrich_no_tracking_number_column_is_noop():
    df = pd.DataFrame([{"A": 1}])
    out = Enricher(QL(), client=object(),
                   normalizer=lambda *a, **k: {}).enrich(df)
    assert out.equals(df)


def test_enrich_handles_nans_and_writes_strings(tmp_path: Path):
    class FakeClient:
        def fetch_status(self, tn, carrier=None):
            return {"code": "DLV", "statusByLocale": "Delivered", "description": "ok"}

    def normalizer(p, **_):  # legacy dict normalizer still works
        return p

    df = pd.DataFrame([{"Tracking Number": None, "Carrier Code": None},
                       {"Tracking Number": "123", "Carrier Code": float("nan")}])
    out = Enricher(QL(), client=FakeClient(), normalizer=normalizer).enrich(df)
    # first row stays empty; second row gets values; all string dtype and not NaN
    assert out.loc[0, "code"] == "" and out.loc[1, "code"] == "DLV"
    assert out["code"].dtype.name == "string"
    assert out["description"].dtype.name == "string"


def test_enrich_writes_sidecar_when_dir_passed(tmp_path: Path):
    class FakeClient:
        def fetch_status(self, tn, carrier=None):
            return {"code": "DLV", "statusByLocale": "Delivered", "description": "ok"}

    def normalizer(p, **_):
        return {"code": "DLV", "derivedCode": "DLV", "statusByLocale": "Delivered", "description": "ok"}

    df = pd.DataFrame([{"Tracking Number": "123", "Carrier Code": "FDX"}])
    sidecar = tmp_path / "norm"
    out = Enricher(QL(), client=FakeClient(), normalizer=normalizer).enrich(
        df, sidecar_dir=sidecar)
    files = list(sidecar.glob("*.json"))
    assert files, "expected a sidecar JSON"
    data = json.loads(files[0].read_text())
    assert (data.get("code") or data.get("payload", {}).get("code")) == "DLV"
