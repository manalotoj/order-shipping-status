from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Any, Dict

import pandas as pd
import numpy as np


def _is_blank(val: Any) -> bool:
    """True if value is None/NaN/empty/“nan”/“none” (case-insensitive)."""
    if val is None:
        return True
    if isinstance(val, float) and np.isnan(val):
        return True
    s = str(val).strip()
    return s == "" or s.lower() in {"nan", "none"}


class Enricher:
    def __init__(self, logger, *, client: Optional[Any], normalizer: Optional[Any]):
        self.logger = logger
        self.client = client
        self.normalizer = normalizer

    def _safe_log(self, level: str, msg: str, *args):
        fn = getattr(self.logger, level, None)
        if callable(fn):
            try:
                fn(msg, *args)
            except Exception:
                pass

    def _fetch_payload(self, tn: str, carrier: str):
        """
        Support both modern .fetch(tracking_number=..., carrier_code=...)
        and legacy .fetch_status(tn, carrier=...).
        """
        if hasattr(self.client, "fetch"):
            return self.client.fetch(tracking_number=tn, carrier_code=carrier)
        if hasattr(self.client, "fetch_status"):
            return self.client.fetch_status(tn, carrier)
        raise AttributeError(
            "Replay client has neither .fetch nor .fetch_status")

    def _normalize(self, payload, tn: str, carrier: str) -> Dict[str, Any]:
        """
        Accept either:
          - dict from normalizer (already column mapping)
          - object with .to_excel_cols()
        """
        if self.normalizer is None:
            return {}
        try:
            result = self.normalizer(
                payload,
                tracking_number=tn,
                carrier_code=carrier,
                source="replay",
            )
        except TypeError:
            result = self.normalizer(payload)

        if hasattr(result, "to_excel_cols"):
            return dict(result.to_excel_cols())
        if isinstance(result, dict):
            return result
        return {}

    # ---------- Helpers to scope a FedEx batch payload to ONE tracking number ----------
    @staticmethod
    def _scope_payload_to_tn(payload: dict, tn: str) -> dict:
        """
        If payload is a FedEx 'output' body containing many completeTrackResults,
        return a *new* minimal dict containing only the matching TN. Otherwise return
        the original payload.
        """
        try:
            if not isinstance(payload, dict):
                return payload
            out = payload.get("output", payload)
            if not isinstance(out, dict):
                return payload
            ctr = out.get("completeTrackResults")
            if not isinstance(ctr, list) or not ctr:
                return payload

            matched = None
            for cr in ctr:
                if not isinstance(cr, dict):
                    continue
                # direct match on the container
                if str(cr.get("trackingNumber", "")).strip() == str(tn):
                    matched = cr
                    break
                # nested trackResults[*].trackingNumberInfo.trackingNumber
                tr_list = cr.get("trackResults")
                if isinstance(tr_list, list):
                    for tr in tr_list:
                        if not isinstance(tr, dict):
                            continue
                        tinfo = tr.get("trackingNumberInfo") or {}
                        if str(tinfo.get("trackingNumber", "")).strip() == str(tn):
                            matched = cr
                            break
                if matched is not None:
                    break

            if matched is None:
                return payload  # best-effort

            # Return a scoped shape that mirrors FedEx 'output' schema
            return {"output": {"completeTrackResults": [matched]}}
        except Exception:
            return payload

    @staticmethod
    def _latest_status_detail_from_scoped(payload: dict) -> dict:
        """Return latestStatusDetail dict from a TN-scoped payload (best-effort)."""
        try:
            out = payload.get("output", payload)
            ctr = out.get("completeTrackResults")
            if not (isinstance(ctr, list) and ctr):
                return {}
            tr_list = ctr[0].get("trackResults")
            if not (isinstance(tr_list, list) and tr_list):
                return {}
            lsd = tr_list[0].get("latestStatusDetail")
            return lsd if isinstance(lsd, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _ancillary_text_from_lsd(lsd: dict) -> str:
        try:
            details = lsd.get("ancillaryDetails") or []
            parts = []
            if isinstance(details, list):
                for d in details:
                    if isinstance(d, dict):
                        for k in ("reasonDescription", "actionDescription", "reason", "action"):
                            v = d.get(k)
                            if v:
                                parts.append(str(v))
            return " ".join(parts)
        except Exception:
            return ""

    @staticmethod
    def _compute_latest_ts_scan_counts(scoped_payload: dict) -> tuple[str, int, list[str]]:
        """
        From a TN-scoped payload, compute:
          - LatestEventTimestampUtc (ISO, 'Z' suffix)
          - ScanEventsCount (int)
          - ScanEventTimestamps (list[str])
        """
        from order_shipping_status.api.normalize import _latest_event_ts_utc  # lazy import

        # Count scan events and collect timestamps only within the scoped payload
        def _collect_scan_dates(obj):
            dates = []
            if isinstance(obj, dict):
                se = obj.get("scanEvents")
                if isinstance(se, list):
                    for ev in se:
                        if isinstance(ev, dict):
                            for key in ("date", "dateTime", "eventDate"):
                                v = ev.get(key)
                                if isinstance(v, str) and "T" in v:
                                    dates.append(v)
                outcr = obj.get("completeTrackResults")
                if isinstance(outcr, list):
                    for cr in outcr:
                        if not isinstance(cr, dict):
                            continue
                        tr_list = cr.get("trackResults")
                        if isinstance(tr_list, list):
                            for tr in tr_list:
                                if not isinstance(tr, dict):
                                    continue
                                se2 = tr.get("scanEvents")
                                if isinstance(se2, list):
                                    for ev2 in se2:
                                        if isinstance(ev2, dict):
                                            for key in ("date", "dateTime", "eventDate"):
                                                v2 = ev2.get(key)
                                                if isinstance(v2, str) and "T" in v2:
                                                    dates.append(v2)
            return dates

        def _count_scan_events(obj):
            count = 0
            if isinstance(obj, dict):
                se = obj.get("scanEvents")
                if isinstance(se, list):
                    count += len(se)
                outcr = obj.get("completeTrackResults")
                if isinstance(outcr, list):
                    for cr in outcr:
                        if not isinstance(cr, dict):
                            continue
                        tr_list = cr.get("trackResults")
                        if isinstance(tr_list, list):
                            for tr in tr_list:
                                if not isinstance(tr, dict):
                                    continue
                                se2 = tr.get("scanEvents")
                                if isinstance(se2, list):
                                    count += len(se2)
            return count

        ts = _latest_event_ts_utc(scoped_payload) or ""
        scan_ts = _collect_scan_dates(scoped_payload)
        scan_ct = _count_scan_events(scoped_payload)
        return ts, int(scan_ct), scan_ts

    # -------------------------------- enrich --------------------------------
    def enrich(self, df: pd.DataFrame, *, sidecar_dir: Optional[Path] = None) -> pd.DataFrame:
        """
        Merge normalized API columns row-by-row. Also attach:
          - raw (if normalizer adds it)
          - latestStatusDetail (dict)
          - LatestAncillaryText (str)
          - LatestEventTimestampUtc (str, 'Z')
          - ScanEventsCount (int)
          - ScanEventTimestamps (list[str])
        """
        if "Tracking Number" not in df.columns or "Carrier Code" not in df.columns:
            return df.copy()

        out = df.copy()

        if self.client is None or self.normalizer is None:
            return out

        if sidecar_dir is not None:
            Path(sidecar_dir).mkdir(parents=True, exist_ok=True)

        created_cols: set[str] = set()

        # Optional batch fetch
        batch_payloads: dict[str, dict] = {}
        try:
            if hasattr(self.client, "fetch_batch"):
                tns: list[str] = []
                carrier_map: dict[str, str] = {}
                for _, row in out.iterrows():
                    raw_tn = row.get("Tracking Number")
                    raw_carrier = row.get("Carrier Code")
                    if _is_blank(raw_tn):
                        continue
                    tn = str(raw_tn).strip()
                    tns.append(tn)
                    if not _is_blank(raw_carrier):
                        carrier_map[tn] = str(raw_carrier).strip()
                if tns:
                    try:
                        batch_payloads = self.client.fetch_batch(
                            tns, carrier_map=carrier_map)
                    except Exception:
                        batch_payloads = {}
        except Exception:
            batch_payloads = {}

        for idx, row in out.iterrows():
            raw_tn = row.get("Tracking Number", None)
            raw_carrier = row.get("Carrier Code", None)
            if _is_blank(raw_tn):
                continue

            tn = str(raw_tn).strip()
            carrier = None if _is_blank(
                raw_carrier) else str(raw_carrier).strip()

            # Prefer pre-fetched batch payload
            payload: dict = {}
            if tn in batch_payloads:
                payload = batch_payloads.get(tn, {}) or {}
            else:
                try:
                    payload = self._fetch_payload(tn, carrier)
                except Exception as ex:
                    self._safe_log(
                        "warning", "fetch failed for %s/%s: %s", carrier, tn, ex)
                    continue

            if not payload:
                self._safe_log(
                    "warning", "empty payload for %s/%s", carrier, tn)

            # Normalize to core excel columns
            try:
                cols = self._normalize(payload, tn, carrier)
            except Exception as ex:
                self._safe_log(
                    "warning", "Normalization failed for %s/%s: %s", carrier, tn, ex)
                continue

            for k, v in cols.items():
                created_cols.add(k)
                out.at[idx, k] = v

            # ---------- Attach TN-scoped derived fields (always) ----------
            # Use the raw payload if normalizer propagated it; otherwise use transport payload
            raw_payload = cols.get("raw", payload) if isinstance(
                cols, dict) else payload
            scoped = self._scope_payload_to_tn(raw_payload, tn)

            # latestStatusDetail + ancillary text
            try:
                lsd = self._latest_status_detail_from_scoped(scoped)
                out.at[idx, "latestStatusDetail"] = lsd
                created_cols.add("latestStatusDetail")

                anc = self._ancillary_text_from_lsd(lsd)
                out.at[idx, "LatestAncillaryText"] = anc
                created_cols.add("LatestAncillaryText")
            except Exception:
                # keep going; these are optional
                pass

            # LatestEventTimestampUtc / ScanEventsCount / ScanEventTimestamps (TN-scoped)
            try:
                ts, scan_ct, scan_ts = self._compute_latest_ts_scan_counts(
                    scoped)
                if ts:
                    out.at[idx, "LatestEventTimestampUtc"] = ts
                    created_cols.add("LatestEventTimestampUtc")
                out.at[idx, "ScanEventsCount"] = int(scan_ct)
                created_cols.add("ScanEventsCount")
                out.at[idx, "ScanEventTimestamps"] = scan_ts
                created_cols.add("ScanEventTimestamps")
            except Exception:
                pass

            # Optional sidecar write
            if sidecar_dir is not None:
                try:
                    (Path(sidecar_dir) / f"{carrier}_{tn}.json").write_text(
                        json.dumps(cols, ensure_ascii=False), encoding="utf-8"
                    )
                except Exception as ex:
                    self._safe_log(
                        "warning", "Sidecar write failed for %s/%s: %s", carrier, tn, ex)

        # Normalize newly created cols to string-friendly blanks where appropriate
        for k in created_cols:
            if k not in out.columns:
                out[k] = ""
            # Don't coerce dict/list fields to string dtype
            if k in ("latestStatusDetail", "ScanEventTimestamps"):
                continue
            out[k] = out[k].astype("string").fillna("")

        return out
