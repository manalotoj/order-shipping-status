from __future__ import annotations

from typing import Any, Dict, Optional
import logging


class FedexHelper:
    """Adapter that exposes fetch_batch/fetch_status to the existing pipeline.

    It performs batching (<=30 TNs per POST), calls the low-level FedExClient
    for auth and POST, and optionally writes API bodies via FedExWriter.
    """

    def __init__(self, client, writer: Optional[Any] = None, logger: Optional[logging.Logger] = None) -> None:
        self._client = client
        self._writer = writer
        self._logger = logger

    def fetch_batch(self, tracking_numbers: list[str], carrier_map: Optional[Dict[str, str]] = None) -> Dict[str, dict]:
        if not tracking_numbers:
            return {}
        token = self._client.authenticate()
        if not token:
            return {tn: {} for tn in tracking_numbers}

        out: Dict[str, dict] = {}
        CHUNK = 30
        for i in range(0, len(tracking_numbers), CHUNK):
            chunk = tracking_numbers[i:i+CHUNK]
            tracking_info = []
            for tn in chunk:
                info = {"trackingNumberInfo": {"trackingNumber": tn}}
                if carrier_map and carrier_map.get(tn):
                    info["carrierCode"] = carrier_map.get(tn)
                tracking_info.append(info)

            body = {"trackingInfo": tracking_info,
                    "includeDetailedScans": True}
            j = self._client.post_tracking(body, access_token=token)

            # persist raw bodies if requested
            if self._writer:
                try:
                    self._writer.write(list(chunk), j)
                except Exception:
                    if self._logger:
                        try:
                            self._logger.warning(
                                "Failed to write API body for chunk %s", chunk)
                        except Exception:
                            pass

            # Map per-TN results similar to earlier behavior
            per_tn_map: Dict[str, dict] = {}
            try:
                ctr = None
                if isinstance(j, dict):
                    if "completeTrackResults" in j and isinstance(j.get("completeTrackResults"), list):
                        ctr = j.get("completeTrackResults")
                    else:
                        cand = j
                        for key in ("output", "body", "response", "data"):
                            if isinstance(cand.get(key, None), dict):
                                cand = cand.get(key)
                        if isinstance(cand.get("completeTrackResults", None), list):
                            ctr = cand.get("completeTrackResults")

                if ctr:
                    for cr in ctr:
                        if not isinstance(cr, dict):
                            continue
                        tn_in = str(cr.get("trackingNumber", "")).strip()
                        if tn_in:
                            per_tn_map[tn_in] = {"completeTrackResults": [cr]}
                            continue
                        tr_list = cr.get("trackResults") or []
                        if isinstance(tr_list, list):
                            for tr in tr_list:
                                if not isinstance(tr, dict):
                                    continue
                                tinfo = tr.get("trackingNumberInfo") or {}
                                tn_nested = str(
                                    tinfo.get("trackingNumber", "")).strip()
                                if tn_nested:
                                    per_tn_map[tn_nested] = {
                                        "completeTrackResults": [cr]}
            except Exception:
                per_tn_map = {}

            for tn in chunk:
                out[tn] = per_tn_map.get(tn, j or {})

        return out

    def fetch_status(self, tracking_number: str, carrier_code: Optional[str] = None) -> Dict[str, Any]:
        res = self.fetch_batch([tracking_number], carrier_map={
                               tracking_number: carrier_code} if carrier_code else None)
        return res.get(tracking_number, {})


# Backwards compatibility: keep the old name available
LiveFedExAdapter = FedexHelper
