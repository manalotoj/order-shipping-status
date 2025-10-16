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
            # ReplayClient.fetch_status historically accepted (tracking_number, carrier_code=None)
            # Use positional call to avoid mismatched keyword names like 'carrier'.
            return self.client.fetch_status(tn, carrier)
        raise AttributeError(
            "Replay client has neither .fetch nor .fetch_status")

    def _normalize(self, payload, tn: str, carrier: str) -> Dict[str, Any]:
        """
        Accept either:
          - dict from normalizer (already column mapping)
          - object with .to_excel_cols()
        Call with kwargs if supported; otherwise fallback to positional.
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
            # Legacy normalizers that only accept the payload
            result = self.normalizer(payload)

        # object case
        if hasattr(result, "to_excel_cols"):
            return dict(result.to_excel_cols())
        # dict case
        if isinstance(result, dict):
            return result
        # unknown; be defensive
        return {}

    def enrich(self, df: pd.DataFrame, *, sidecar_dir: Optional[Path] = None) -> pd.DataFrame:
        """
        Expected columns present on input (from Preprocessor + ColumnContract):
          - "Tracking Number"
          - "Carrier Code"

        If those columns are missing, this function is a NO-OP.
        Otherwise, it merges normalized columns row-by-row.
        """
        # If no key columns, NO-OP (some tests expect exact equality)
        if "Tracking Number" not in df.columns or "Carrier Code" not in df.columns:
            return df.copy()

        out = df.copy()

        # Nothing to do without a client+normalizer
        if self.client is None or self.normalizer is None:
            return out

        # Ensure sidecar directory exists if provided
        if sidecar_dir is not None:
            Path(sidecar_dir).mkdir(parents=True, exist_ok=True)

        # Track which columns we create so we can clean NaNs to "" afterwards
        created_cols: set[str] = set()

        for idx, row in out.iterrows():
            raw_tn = row.get("Tracking Number", None)
            raw_carrier = row.get("Carrier Code", None)
            # Require a tracking number, but allow missing carrier (pass None)
            if _is_blank(raw_tn):
                continue

            tn = str(raw_tn).strip()
            if _is_blank(raw_carrier):
                carrier = None
            else:
                carrier = str(raw_carrier).strip()

            try:
                payload = self._fetch_payload(tn, carrier)
            except Exception as ex:
                self._safe_log(
                    "warning", "Replay fetch failed for %s/%s: %s", carrier, tn, ex)
                continue

            try:
                cols = self._normalize(payload, tn, carrier)
            except Exception as ex:
                self._safe_log(
                    "warning", "Normalization failed for %s/%s: %s", carrier, tn, ex)
                continue

            # Merge normalized columns (strings preferred)
            for k, v in cols.items():
                created_cols.add(k)
                out.at[idx, k] = v

            # Backfill LatestEventTimestampUtc from raw payload if normalizer did not provide it
            if "LatestEventTimestampUtc" not in cols:
                try:
                    # import here to avoid circular import at module load
                    from order_shipping_status.api.normalize import _latest_event_ts_utc

                    # If payload is a batch 'output' object containing multiple
                    # completeTrackResults, find the one corresponding to this TN
                    # and compute the latest event timestamp only from that entry.
                    ts = ""
                    if isinstance(payload, dict) and "completeTrackResults" in payload:
                        ctr = payload.get("completeTrackResults")
                        if isinstance(ctr, list):
                            matched = None
                            for cr in ctr:
                                if not isinstance(cr, dict):
                                    continue
                                # direct trackingNumber on the completeTrackResults entry
                                if str(cr.get("trackingNumber", "")).strip() == tn:
                                    matched = cr
                                    break
                                # or inside nested trackResults/trackingNumberInfo
                                tr_list = cr.get("trackResults")
                                if isinstance(tr_list, list):
                                    for tr in tr_list:
                                        if not isinstance(tr, dict):
                                            continue
                                        tinfo = tr.get(
                                            "trackingNumberInfo") or {}
                                        if str(tinfo.get("trackingNumber", "")).strip() == tn:
                                            matched = cr
                                            break
                                if matched:
                                    break
                            if matched is not None:
                                mini = {"output": {
                                    "completeTrackResults": [matched]}}
                                ts = _latest_event_ts_utc(mini)
                    # Fallback to original payload (best-effort)
                    if not ts:
                        ts = _latest_event_ts_utc(payload)

                    if ts:
                        created_cols.add("LatestEventTimestampUtc")
                        out.at[idx, "LatestEventTimestampUtc"] = ts
                except Exception:
                    # If helper not available or fails, ignore and continue
                    pass

            # Count scanEvents in payload (top-level or nested) and expose as ScanEventsCount
            try:
                def _count_scan_events(obj):
                    count = 0
                    if isinstance(obj, dict):
                        se = obj.get("scanEvents")
                        if isinstance(se, list):
                            count += len(se)
                        # nested completeTrackResults -> trackResults -> scanEvents
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

                scan_count = _count_scan_events(payload)
                created_cols.add("ScanEventsCount")
                out.at[idx, "ScanEventsCount"] = int(scan_count)
            except Exception:
                # best-effort; ignore on failure
                pass
            # Collect scan event timestamps (ISO strings) into ScanEventTimestamps for use by rules
            try:
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

                ts_list = _collect_scan_dates(payload)
                created_cols.add("ScanEventTimestamps")
                out.at[idx, "ScanEventTimestamps"] = ts_list
            except Exception:
                pass

            # Optionally write normalized sidecar
            if sidecar_dir is not None:
                try:
                    (Path(sidecar_dir) / f"{carrier}_{tn}.json").write_text(
                        json.dumps(cols, ensure_ascii=False), encoding="utf-8"
                    )
                except Exception as ex:
                    self._safe_log(
                        "warning", "Sidecar write failed for %s/%s: %s", carrier, tn, ex)

        # Ensure any newly created columns have empty strings (not NaN) where missing
        # Be defensive: if a column somehow wasn't created by the row loop, create it now.
        for k in created_cols:
            if k not in out.columns:
                out[k] = ""
            # Use pandas string dtype and fill missing values with empty string
            out[k] = out[k].astype("string").fillna("")

        return out
