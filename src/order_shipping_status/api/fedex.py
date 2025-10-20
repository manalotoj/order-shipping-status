from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import base64
import time

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, List
import base64
import json
import time

from .transport import RequestsTransport


@dataclass
class FedExConfig:
    base_url: str


@dataclass
class FedExAuth:
    client_id: str
    client_secret: str
    token_url: str


class FedExClient:
    """FedEx client with batched POST support for /track/v1/trackingnumbers.

    - Acquires OAuth access token using client credentials (cached in-memory).
    - Supports `fetch_batch(tracking_numbers)` which posts up to 30 TNs per request.
    - Optionally persists raw response bodies into a single JSON file (append semantics).
    """

    def __init__(self, auth: FedExAuth, cfg: FedExConfig, transport: Optional[RequestsTransport] = None, *, save_bodies_path: Optional[Path] = None):
        self.auth = auth
        self.cfg = cfg
        self.transport = transport or RequestsTransport()
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._saved_bodies: List[dict] = []
        self._save_bodies_path = Path(
            save_bodies_path) if save_bodies_path is not None else None

    def _ensure_token(self) -> None:
        now = time.time()
        if self._token and now < self._token_expires_at - 10:
            return

        data = {"grant_type": "client_credentials"}
        auth_header = base64.b64encode(
            f"{self.auth.client_id}:{self.auth.client_secret}".encode()).decode()
        headers = {"Authorization": f"Basic {auth_header}",
                   "Content-Type": "application/x-www-form-urlencoded"}
        resp = self.transport.post(
            self.auth.token_url, headers=headers, data=data)
        try:
            resp.raise_for_status()
            j = resp.json()
            self._token = j.get("access_token")
            expires_in = int(j.get("expires_in", 3600))
            self._token_expires_at = time.time() + expires_in
        except Exception:
            self._token = None
            self._token_expires_at = 0.0

    def _persist_bodies(self) -> None:
        if not self._save_bodies_path:
            return
        try:
            self._save_bodies_path.parent.mkdir(parents=True, exist_ok=True)
            existing: List[dict] = []
            if self._save_bodies_path.exists():
                try:
                    existing = json.loads(
                        self._save_bodies_path.read_text(encoding="utf-8"))
                    if not isinstance(existing, list):
                        existing = []
                except Exception:
                    existing = []
            existing.extend(self._saved_bodies)
            self._save_bodies_path.write_text(json.dumps(
                existing, indent=2, ensure_ascii=False), encoding="utf-8")
            # Clear saved bodies after persisting so subsequent writes don't duplicate
            self._saved_bodies = []
        except Exception:
            # best-effort; ignore failures to persist
            pass

    def _endpoint_for_tracking(self) -> str:
        base = self.cfg.base_url.rstrip("/")
        # If user passed e.g. https://apis.fedex.com/track, append v1/trackingnumbers
        if "trackingnumbers" in base or "/v1/" in base:
            return base
        return base + "/v1/trackingnumbers"

    def fetch_batch(self, tracking_numbers: List[str], carrier_map: Optional[Dict[str, str]] = None) -> Dict[str, dict]:
        """Fetch tracking payloads for up to len(tracking_numbers) TNs.

        Returns a dict mapping tracking_number -> response_payload (the full JSON body for the batch
        response that includes that TN). If multiple batches are required (len>30), performs multiple
        POSTs and merges results.
        """
        if not tracking_numbers:
            return {}

        # Ensure token available
        self._ensure_token()
        if not self._token:
            return {tn: {} for tn in tracking_numbers}

        headers = {"Authorization": f"Bearer {self._token}",
                   "Content-Type": "application/json"}
        endpoint = self._endpoint_for_tracking()

        # Work in chunks of 30
        out: Dict[str, dict] = {}
        CHUNK = 30
        for i in range(0, len(tracking_numbers), CHUNK):
            chunk = tracking_numbers[i:i+CHUNK]
            # Build request body per FedEx track API minimal shape
            tracking_info = []
            for tn in chunk:
                info = {"trackingNumberInfo": {"trackingNumber": tn}}
                # if carrier_map provided, include carrier-specific info if non-empty
                if carrier_map and carrier_map.get(tn):
                    info["carrierCode"] = carrier_map.get(tn)
                tracking_info.append(info)

            body = {"trackingInfo": tracking_info,
                    "includeDetailedScans": True}

            try:
                resp = self.transport.post(
                    endpoint, headers=headers, json=body)
                resp.raise_for_status()
                j = resp.json()
            except Exception:
                j = {}

            # Save raw body for optional persistence
            try:
                self._saved_bodies.append(j)
            except Exception:
                pass

            # Map each tn in the chunk to the batch response (normalizer will narrow)
            for tn in chunk:
                out[tn] = j

            # Persist saved bodies incrementally to avoid losing them on long runs
            self._persist_bodies()

        return out

    # Backwards-compatible single-TN fetch that delegates to fetch_batch
    def fetch_status(self, tracking_number: str, carrier_code: Optional[str] = None) -> Dict[str, Any]:
        res = self.fetch_batch([tracking_number], carrier_map={
                               tracking_number: carrier_code} if carrier_code else None)
        return res.get(tracking_number, {})
