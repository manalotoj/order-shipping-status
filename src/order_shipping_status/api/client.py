from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol, Any
import json


class ShippingClient(Protocol):
    def fetch_status(self, tracking_number: str, carrier_code: Optional[str] = None) -> dict[str, Any]:
        ...


@dataclass
class ReplayClient:
    """Loads canned responses from JSON files in a directory.

    File naming convention (simple and explicit for now):
      <tracking_number>.json
    Optionally, you can nest by carrier later if needed.
    """
    replay_dir: Path

    def fetch_status(self, tracking_number: str, carrier_code: Optional[str] = None) -> dict[str, Any]:
        path = self.replay_dir / f"{tracking_number}.json"
        if not path.exists():
            # Return empty object if no replay file is found; callers should handle gracefully.
            return {}
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)


def normalize_status(payload: dict[str, Any]) -> dict[str, str]:
    """Map an arbitrary payload into our four FedEx columns.

    This is intentionally defensive and minimal for now. As we learn the real shapes,
    we’ll enrich this to use nested fields and derive `derivedCode`.
    """
    if not isinstance(payload, dict):
        return {"code": "", "derivedCode": "", "statusByLocale": "", "description": ""}

    code = str(payload.get("code", "") or "")
    status = str(payload.get("status", "")
                 or payload.get("statusByLocale", "") or "")
    desc = str(payload.get("description", "") or "")
    # For now, derivedCode just mirrors code when present.
    derived = code or ""

    return {
        "code": code,
        "derivedCode": derived,
        "statusByLocale": status,
        "description": desc,
    }
