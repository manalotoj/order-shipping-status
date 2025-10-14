from __future__ import annotations

from typing import Any, Callable, Optional
import datetime as _dt

import pandas as pd

from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS

# Type alias: normalizer returns a NormalizedShippingData (with .to_excel_cols())
# note: takes kwargs and returns NormalizedShippingData
Normalizer = Callable[..., Any]


def _parse_iso_to_utc(ts: str) -> Optional[_dt.datetime]:
    """
    Parse many ISO-ish timestamp shapes to an aware UTC datetime.
    Supports:
      - '2025-10-06T22:49:00-04:00'
      - '2025-10-02T00:00:00+00:00'
      - '2025-10-02T00:00:00Z'
      - '2025-10-02T00:00:00' (assumed UTC)
    Returns None on failure.
    """
    if not ts or not isinstance(ts, str):
        return None
    s = ts.strip()
    if not s:
        return None
    # Normalize trailing Z
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = _dt.datetime.fromisoformat(s)
    except Exception:
        return None

    # If naive, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)

    try:
        return dt.astimezone(_dt.timezone.utc)
    except Exception:
        return None


def _iter_track_results(output: dict) -> list[dict]:
    """
    FedEx 'output' shape helper: yield each trackResults[*] dict.
    Safely handles missing keys.
    """
    if not isinstance(output, dict):
        return []
    ctr = output.get("completeTrackResults")
    if not isinstance(ctr, list):
        return []
    out: list[dict] = []
    for cr in ctr:
        if not isinstance(cr, dict):
            continue
        trs = cr.get("trackResults")
        if isinstance(trs, list):
            for tr in trs:
                if isinstance(tr, dict):
                    out.append(tr)
    return out


def _extract_latest_event_ts_utc(payload: dict) -> Optional[str]:
    """
    Walk the payload to find the most recent timestamp from:
      - dateAndTimes[*].dateTime
      - scanEvents[*].date (or .dateTime if present)
    Returns ISO8601 *UTC* string if found, else None.
    The payload can be either the whole item or the 'output' node — we detect both.
    """
    if not isinstance(payload, dict):
        return None

    # Accept either the whole item (with 'output') or 'output' itself
    output = payload.get("output") if "output" in payload else payload
    if not isinstance(output, dict):
        return None

    candidates: list[_dt.datetime] = []

    # latestStatusDetail sometimes has its own timestamp (rare, but capture it if present)
    try:
        for tr in _iter_track_results(output):
            lsd = tr.get("latestStatusDetail")
            if isinstance(lsd, dict):
                # not standard, but just in case
                for key in ("dateTime", "timestamp", "eventDate", "eventDateTime"):
                    val = lsd.get(key)
                    ts = _parse_iso_to_utc(
                        val) if isinstance(val, str) else None
                    if ts:
                        candidates.append(ts)
    except Exception:
        pass

    # dateAndTimes[*].dateTime
    try:
        for tr in _iter_track_results(output):
            dat = tr.get("dateAndTimes")
            if not isinstance(dat, list):
                continue
            for d in dat:
                if not isinstance(d, dict):
                    continue
                s = d.get("dateTime")
                ts = _parse_iso_to_utc(s) if isinstance(s, str) else None
                if ts:
                    candidates.append(ts)
    except Exception:
        pass

    # scanEvents[*].date or .dateTime
    try:
        for tr in _iter_track_results(output):
            se = tr.get("scanEvents")
            if not isinstance(se, list):
                continue
            for ev in se:
                if not isinstance(ev, dict):
                    continue
                s = ev.get("dateTime") or ev.get("date")
                ts = _parse_iso_to_utc(s) if isinstance(s, str) else None
                if ts:
                    candidates.append(ts)
    except Exception:
        pass

    if not candidates:
        return None

    latest = max(candidates)
    # Return canonical ISO8601 Zulu form
    return latest.replace(tzinfo=_dt.timezone.utc).isoformat()


class Enricher:
    def __init__(self, logger, client: Optional[Any] = None, normalizer: Optional[Normalizer] = None) -> None:
        self.logger = logger
        self.client = client
        self.normalizer = normalizer

    def enrich(self, df: pd.DataFrame, *, sidecar_dir: Optional[Any] = None) -> pd.DataFrame:
        # If we cannot enrich, return as-is
        if self.client is None or self.normalizer is None:
            return df
        if "Tracking Number" not in df.columns:
            self.logger.debug(
                "Enrichment skipped: 'Tracking Number' column missing.")
            return df

        out = df.copy()

        # Ensure FedEx target columns exist (string default) — ColumnContract also handles this.
        for col in OUTPUT_FEDEX_COLUMNS:
            if col not in out.columns:
                out[col] = ""

        # Also ensure LatestEventTimestampUtc exists so we can fill it (string)
        if "LatestEventTimestampUtc" not in out.columns:
            out["LatestEventTimestampUtc"] = ""

        tn_series = out["Tracking Number"].fillna("").astype(str)
        cc_series = out["Carrier Code"].fillna("").astype(
            str) if "Carrier Code" in out.columns else None

        for idx, tracking in tn_series.items():
            if not tracking:
                continue
            carrier = cc_series.iloc[idx] if cc_series is not None else None
            try:
                payload = self.client.fetch_status(tracking, carrier)

                # Run normalizer — returns NormalizedShippingData (with .to_excel_cols())
                norm = self.normalizer(
                    payload,
                    tracking_number=tracking,
                    carrier_code=carrier,
                    source=self.client.__class__.__name__,
                )
                cols = norm.to_excel_cols()

                # Back-fill LatestEventTimestampUtc if normalizer didn't populate it
                let_supplied = str(
                    cols.get("LatestEventTimestampUtc", "") or "").strip()
                if not let_supplied:
                    extracted = _extract_latest_event_ts_utc(payload)
                    if extracted:
                        cols["LatestEventTimestampUtc"] = extracted

                # Write into dataframe
                for k in OUTPUT_FEDEX_COLUMNS:
                    out.at[idx, k] = cols.get(k, "")

                # Also write LatestEventTimestampUtc if present in cols
                if "LatestEventTimestampUtc" in cols:
                    out.at[idx, "LatestEventTimestampUtc"] = cols["LatestEventTimestampUtc"]

                # Optional: write sidecar raw body for debugging
                if sidecar_dir:
                    try:
                        p = sidecar_dir / f"{tracking}.json"
                        p.parent.mkdir(parents=True, exist_ok=True)
                        import json as _json

                        with p.open("w", encoding="utf-8") as fh:
                            _json.dump(payload, fh, ensure_ascii=False)
                    except Exception as sidecar_ex:
                        self.logger.debug(
                            "Failed writing sidecar for %s: %s", tracking, sidecar_ex)

            except Exception as ex:
                self.logger.debug("Enrichment failed for %s: %s", tracking, ex)

        return out
