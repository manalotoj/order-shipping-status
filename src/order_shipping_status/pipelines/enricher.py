# src/order_shipping_status/pipelines/enricher.py
from __future__ import annotations
from typing import Any, Callable, Optional
from pathlib import Path
import json
import pandas as pd

from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS
from order_shipping_status.models import NormalizedShippingData

# Normalizer may accept kwargs and return a model, or be legacy and return a dict.
Normalizer = Callable[..., Any]


class Enricher:
    def __init__(
        self,
        logger,
        client: Optional[Any] = None,
        normalizer: Optional[Normalizer] = None,
    ) -> None:
        self.logger = logger
        self.client = client
        self.normalizer = normalizer

    def _apply_normalizer(
        self,
        payload: dict,
        *,
        tracking_number: str | None,
        carrier_code: str | None,
        source: str,
    ) -> tuple[dict[str, str], dict | None]:
        """
        Run the provided normalizer in a backwards-compatible way.

        Returns:
          (excel_cols, full_normalized_dict_or_none)
        where excel_cols has keys for OUTPUT_FEDEX_COLUMNS.
        """
        if self.normalizer is None:
            return {k: "" for k in OUTPUT_FEDEX_COLUMNS}, None

        try:
            # Preferred: new signature → returns NormalizedShippingData
            norm = self.normalizer(
                payload,
                tracking_number=tracking_number,
                carrier_code=carrier_code,
                source=source,
            )
        except TypeError:
            # Legacy: old normalizer that only takes the payload
            norm = self.normalizer(payload)

        # If it's the new model type
        if isinstance(norm, NormalizedShippingData):
            cols = norm.to_excel_cols()
            return (
                {
                    "code": cols.get("code", "") or "",
                    "derivedCode": cols.get("derivedCode", "") or "",
                    "statusByLocale": cols.get("statusByLocale", "") or "",
                    "description": cols.get("description", "") or "",
                },
                norm.to_dict(),
            )

        # Else assume it's a dict-like
        if isinstance(norm, dict):
            return (
                {
                    "code": str(norm.get("code", "") or ""),
                    "derivedCode": str(norm.get("derivedCode", "") or str(norm.get("code", "") or "")),
                    "statusByLocale": str(norm.get("statusByLocale", "") or ""),
                    "description": str(norm.get("description", "") or ""),
                },
                norm,
            )

        # Fallback
        return {k: "" for k in OUTPUT_FEDEX_COLUMNS}, None

    def enrich(
        self,
        df: pd.DataFrame,
        *,
        sidecar_dir: Optional[Path] = None,
    ) -> pd.DataFrame:
        """Populate OUTPUT_FEDEX_COLUMNS using client + normalizer. Safe no-op if misconfigured."""
        if self.client is None or self.normalizer is None:
            return df
        if "Tracking Number" not in df.columns:
            self.logger.debug(
                "Enrichment skipped: 'Tracking Number' column missing.")
            return df

        out = df.copy()

        # Ensure target columns exist
        for col in OUTPUT_FEDEX_COLUMNS:
            if col not in out.columns:
                out[col] = ""

        # Iterate by index label (robust after filtering/slicing)
        for idx in out.index:
            tn = str(out.at[idx, "Tracking Number"]) if pd.notna(
                out.at[idx, "Tracking Number"]) else ""
            if not tn:
                continue
            carrier = None
            if "Carrier Code" in out.columns:
                val = out.at[idx, "Carrier Code"]
                carrier = str(val) if pd.notna(val) else None

            try:
                payload = self.client.fetch_status(tn, carrier)
                excel_cols, full_norm = self._apply_normalizer(
                    payload,
                    tracking_number=tn,
                    carrier_code=carrier,
                    source=self.client.__class__.__name__,
                )
                # Write the four columns
                for k in OUTPUT_FEDEX_COLUMNS:
                    out.at[idx, k] = excel_cols.get(k, "") or ""
                # Optional: write a sidecar for audit/debugging
                if sidecar_dir is not None:
                    sidecar_dir.mkdir(parents=True, exist_ok=True)
                    name = f"{idx:05d}_{tn or 'no_tracking'}.json"
                    (sidecar_dir / name).write_text(
                        json.dumps(full_norm or {
                                   "payload": payload}, default=str),
                        encoding="utf-8",
                    )
            except Exception as ex:
                self.logger.debug(
                    "Replay enrichment failed for tracking %s: %s", tn, ex)

        # Enforce string dtype + no NaN in target columns
        for col in OUTPUT_FEDEX_COLUMNS:
            out[col] = out[col].astype("string").fillna("")

        return out
