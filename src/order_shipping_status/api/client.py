# src/order_shipping_status/api/client.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol, Any, List
import json

# NEW: import the new normalizer + model
from order_shipping_status.api.normalize import normalize_fedex
from order_shipping_status.models import NormalizedShippingData


class ShippingClient(Protocol):
    def fetch_status(self, tracking_number: str, carrier_code: Optional[str] = None) -> dict[str, Any]:
        ...


@dataclass
class ReplayClient:
    """Replay client that uses a single JSON file containing one or more API bodies.

    The provided `replay_dir` must be a path to a file (not a directory). The file
    may contain a single JSON object or a JSON array. The client builds an index
    mapping tracking numbers to payloads on initialization and serves payloads
    from that index for `fetch_status` calls.
    """

    replay_dir: Path
    _index: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.replay_dir.exists():
            raise ValueError(f"Replay file does not exist: {self.replay_dir}")
        if not self.replay_dir.is_file():
            raise ValueError(
                "ReplayClient requires a single JSON file containing one or more API bodies; directories of per-TN files are not supported."
            )

        raw = json.loads(self.replay_dir.read_text(encoding="utf-8"))
        entries: List[Any]
        if isinstance(raw, list):
            entries = raw
        else:
            entries = [raw]

        idx: dict[str, Any] = {}
        for entry in entries:
            for tn in self._extract_tracking_numbers(entry):
                idx[str(tn)] = entry

        self._index = idx

    def _extract_tracking_numbers(self, payload: Any) -> List[str]:
        results: List[str] = []
        try:
            out = payload.get("output", {})
            ctrs = out.get("completeTrackResults", [])
            for item in ctrs:
                tn = item.get("trackingNumber")
                if tn:
                    results.append(str(tn))
                for tr in item.get("trackResults", []) or []:
                    tinfo = tr.get("trackingNumberInfo", {})
                    tn2 = tinfo.get("trackingNumber") or tr.get(
                        "trackingNumber")
                    if tn2:
                        results.append(str(tn2))
        except Exception:
            pass

        if not results:
            def recurse(obj: Any) -> None:
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k in ("trackingNumber", "tracking_number") and isinstance(v, str) and v.strip():
                            results.append(v)
                        else:
                            recurse(v)
                elif isinstance(obj, list):
                    for e in obj:
                        recurse(e)

            recurse(payload)

        seen: set[str] = set()
        out_list: List[str] = []
        for r in results:
            if r not in seen:
                seen.add(r)
                out_list.append(r)
        return out_list

    def fetch_status(self, tracking_number: str, carrier_code: Optional[str] = None) -> dict[str, Any]:
        # Only combined-file mode supported: return indexed payload or empty dict
        return self._index.get(str(tracking_number), {})


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
