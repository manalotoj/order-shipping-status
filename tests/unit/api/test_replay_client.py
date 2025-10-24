import json
from pathlib import Path

from order_shipping_status.api.client import ReplayClient


def test_replay_client_indexes_combined_json(tmp_path: Path):
    a = {
        "output": {
            "completeTrackResults": [
                {"trackingNumber": "TN123", "trackResults": []}
            ]
        }
    }
    b = {"trackingNumber": "TN456", "code": "DL"}

    file_path = tmp_path / "combined.json"
    file_path.write_text(json.dumps([a, b]), encoding="utf-8")

    client = ReplayClient(file_path)

    # TN123 should be present via deep shape
    assert client.fetch_status("TN123") == a

    # TN456 should be present via flat shape
    assert client.fetch_status("TN456") == b

    # unknown TN returns empty dict
    assert client.fetch_status("UNKNOWN") == {}
