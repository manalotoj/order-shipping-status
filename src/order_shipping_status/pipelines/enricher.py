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

        # If the client supports batch fetching, perform a single batched request
        batch_payloads: dict[str, dict] = {}
        try:
            if hasattr(self.client, "fetch_batch"):
                # Build list of tracking numbers to query (preserve original index mapping)
                tns: list[str] = []
                tn_by_idx: dict[int, str] = {}
                carrier_map: dict[str, str] = {}
                for idx, row in out.iterrows():
                    raw_tn = row.get("Tracking Number", None)
                    raw_carrier = row.get("Carrier Code", None)
                    if _is_blank(raw_tn):
                        continue
                    tn = str(raw_tn).strip()
                    tns.append(tn)
                    tn_by_idx[idx] = tn
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
            # Require a tracking number, but allow missing carrier (pass None)
            if _is_blank(raw_tn):
                continue

            tn = str(raw_tn).strip()
            if _is_blank(raw_carrier):
                carrier = None
            else:
                carrier = str(raw_carrier).strip()

            # Prefer batch payload when available
            payload = {}
            if tn in batch_payloads:
                payload = batch_payloads.get(tn, {}) or {}
            else:
                try:
                    payload = self._fetch_payload(tn, carrier)
                except Exception as ex:
                    self._safe_log(
                        "warning", "fetch failed for %s/%s: %s", carrier, tn, ex)
                    continue

            # If payload is empty, log for visibility
            if not payload:
                self._safe_log(
                    "warning", "empty payload for %s/%s", carrier, tn)

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

        # --- Ensure latestStatusDetail is present (and ancillary text flattened) ---
        def _extract_lsd_from_raw(raw: object) -> dict:
            """Safely extract latestStatusDetail dict from the raw FedEx payload."""
            try:
                if not isinstance(raw, dict):
                    return {}
                outp = raw.get("output", raw)
                if not isinstance(outp, dict):
                    return {}
                ctr = outp.get("completeTrackResults")
                if not isinstance(ctr, list) or not ctr:
                    return {}
                tr_list = ctr[0].get("trackResults")
                if not isinstance(tr_list, list) or not tr_list:
                    return {}
                lsd = tr_list[0].get("latestStatusDetail")
                return lsd if isinstance(lsd, dict) else {}
            except Exception:
                return {}

        def _ancillary_text(lsd: dict) -> str:
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

        try:
            # Only compute when not already present.
            if "latestStatusDetail" not in out.columns and "raw" in out.columns:
                out["latestStatusDetail"] = out["raw"].apply(
                    _extract_lsd_from_raw)
            # A flat text column is useful for diagnostics and optional rules.
            if "latestStatusDetail" in out.columns and "LatestAncillaryText" not in out.columns:
                out["LatestAncillaryText"] = out["latestStatusDetail"].apply(
                    _ancillary_text)
        except Exception as e:
            if self.logger:
                self.logger.debug(
                    "Ancillary extraction skipped due to error: %s", e)

            # Backfill core FedEx fields (code/derivedCode/statusByLocale/description)
            # when the normalizer didn't provide them. Use the same helpers as
            # the normalizer as a best-effort fallback.
            try:
                missing_core = False
                for core_col in ("code", "statusByLocale", "description"):
                    if core_col not in cols or _is_blank(cols.get(core_col, "")):
                        missing_core = True
                        break

                if missing_core:
                    # Import helpers locally to avoid circular import at module load
                    from order_shipping_status.api.normalize import (
                        _from_latest_status_detail,
                        _from_scan_events,
                        _from_flat,
                    )

                    c, d, s, desc = "", "", "", ""
                    try:
                        c, d, s, desc = _from_latest_status_detail(payload)
                    except Exception:
                        c, d, s, desc = "", "", "", ""

                    if not c and not s:
                        try:
                            c, d, s, desc = _from_scan_events(payload)
                        except Exception:
                            c, d, s, desc = "", "", "", ""

                    if not c and not s:
                        try:
                            c, d, s, desc = _from_flat(payload)
                        except Exception:
                            c, d, s, desc = "", "", "", ""

                    if c:
                        created_cols.add("code")
                        out.at[idx, "code"] = c
                    if d:
                        created_cols.add("derivedCode")
                        out.at[idx, "derivedCode"] = d
                    if s:
                        created_cols.add("statusByLocale")
                        out.at[idx, "statusByLocale"] = s
                    if desc:
                        created_cols.add("description")
                        out.at[idx, "description"] = desc
            except Exception:
                # best-effort; don't fail enrichment on backfill errors
                pass

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
