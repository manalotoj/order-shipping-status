import os
import json
import pytest
from pathlib import Path
import pandas as pd

from order_shipping_status.api.fedex import FedExClient, FedExConfig, FedExAuth
from order_shipping_status.api.fedex_writer import FedExWriter
from order_shipping_status.api.fedex_helper import FedexHelper


def _env_creds():
    client_id = os.environ.get("SHIPPING_CLIENT_ID")
    client_secret = os.environ.get("SHIPPING_CLIENT_SECRET")
    token_url = os.environ.get("FEDEX_TOKEN_URL")
    base_url = os.environ.get("FEDEX_BASE_URL")
    if not client_id or not client_secret:
        pytest.skip(
            "FedEx credentials not set in environment; skipping integration tests")
    return client_id, client_secret, token_url or "https://apis.fedex.com/oauth/token", base_url or "https://apis.fedex.com/track"


def test_auth_returns_bearer_token():
    client_id, client_secret, token_url, base_url = _env_creds()
    auth = FedExAuth(client_id=client_id,
                     client_secret=client_secret, token_url=token_url)
    cfg = FedExConfig(base_url=base_url)
    client = FedExClient(auth, cfg)
    token = client.authenticate()
    assert token and isinstance(token, str)


def test_post_tracking_single_tn_returns_body():
    client_id, client_secret, token_url, base_url = _env_creds()
    auth = FedExAuth(client_id=client_id,
                     client_secret=client_secret, token_url=token_url)
    cfg = FedExConfig(base_url=base_url)
    client_raw = FedExClient(auth, cfg)
    helper = FedexHelper(client_raw)

    # known test TN from repo fixtures
    tn = "394178781303"
    out = helper.fetch_batch([tn])
    assert isinstance(out, dict)
    assert tn in out
    assert out[tn]  # non-empty body


@pytest.mark.skip()
def test_batch_of_five_tns_and_writer(tmp_path: Path):
    client_id, client_secret, token_url, base_url = _env_creds()
    auth = FedExAuth(client_id=client_id,
                     client_secret=client_secret, token_url=token_url)
    cfg = FedExConfig(base_url=base_url)
    client_raw = FedExClient(auth, cfg)
    helper = FedexHelper(client_raw)

    # read the 10-20-2025 source file and extract 5 TNs
    src = Path("tests/data/RAW_TransitIssues_10-20-2025.xlsx")
    df = pd.read_excel(src, engine="openpyxl")
    # common column name is 'Tracking Number'
    if "Tracking Number" not in df.columns:
        pytest.skip("No 'Tracking Number' column in test fixture")
    tns = [str(x).strip()
           for x in df["Tracking Number"].dropna().astype(str).unique()]
    if len(tns) < 5:
        pytest.skip("Not enough TNs in fixture to run batch test")
    sample = tns[:5]

    out = helper.fetch_batch(sample)
    assert isinstance(out, dict)
    for tn in sample:
        assert tn in out

    # test writer persistence
    writer_path = tmp_path / "fedex_bodies.json"
    writer = FedExWriter(path=writer_path)
    # persist the bodies
    for tn in sample:
        writer.write(tn, out.get(tn))

    saved = writer.read_all()
    assert isinstance(saved, list)
    # there should be at least len(sample) entries (one per write)
    assert len(saved) >= len(sample)
