# src/order_shipping_status/api/client.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol, Any
import json

# NEW: import the new normalizer + model
from order_shipping_status.api.normalize import normalize_fedex
from order_shipping_status.models import NormalizedShippingData


class ShippingClient(Protocol):
    def fetch_status(self, tracking_number: str, carrier_code: Optional[str] = None) -> dict[str, Any]:
        ...


@dataclass
class ReplayClient:
    replay_dir: Path

    def fetch_status(self, tracking_number: str, carrier_code: Optional[str] = None) -> dict[str, Any]:
        path = self.replay_dir / f"{tracking_number}.json"
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)


# NEW: Back-compat shim. Keeps older tests/callers working.
def normalize_status(
    payload: dict[str, Any],
    *,
    tracking_number: str | None = None,
    carrier_code: str | None = None,
    source: str = "ReplayClient",
    **kwargs,
) -> NormalizedShippingData:
    """
    Back-compat wrapper around normalize_fedex that matches the new enricher signature.
    Returns a NormalizedShippingData instance (not a dict).
    """
    return normalize_fedex(
        payload,
        tracking_number=tracking_number,
        carrier_code=carrier_code,
        source=source,
    )
