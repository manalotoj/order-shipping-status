# src/order_shipping_status/pipelines/process_workbook.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Callable
import datetime as dt

import pandas as pd

from order_shipping_status.models import EnvCfg
from order_shipping_status.io.schema import (
    OUTPUT_FEDEX_COLUMNS,
    OUTPUT_STATUS_COLUMN,
)

Normalizer = Callable[[dict[str, Any]], dict[str, Any]]


def _ensure_output_columns(df_in: pd.DataFrame) -> pd.DataFrame:
    df_out = df_in.copy()
    for col in OUTPUT_FEDEX_COLUMNS:
        if col not in df_out.columns:
            df_out[col] = ""
    if OUTPUT_STATUS_COLUMN not in df_out.columns:
        df_out[OUTPUT_STATUS_COLUMN] = ""
    for col in OUTPUT_FEDEX_COLUMNS + [OUTPUT_STATUS_COLUMN]:
        if col in df_out.columns:
            df_out[col] = df_out[col].astype("string").fillna("")
    return df_out


def _enrich_with_client(
    df_out: pd.DataFrame,
    logger,
    client: Optional[Any] = None,
    normalizer: Optional[Normalizer] = None,
) -> pd.DataFrame:
    if client is None or normalizer is None:
        return df_out
    if "Tracking Number" not in df_out.columns:
        return df_out

    # Ensure target columns exist
    for col in OUTPUT_FEDEX_COLUMNS:
        if col not in df_out.columns:
            df_out[col] = ""

    tn_series = df_out["Tracking Number"].fillna("").astype(str)
    cc_series = (
        df_out["Carrier Code"].fillna("").astype(str)
        if "Carrier Code" in df_out.columns else None
    )

    for idx, tn in tn_series.items():
        if not tn:
            continue
        carrier_code = cc_series.iloc[idx] if cc_series is not None else None
        try:
            payload = client.fetch_status(tn, carrier_code)
            norm = normalizer(payload) or {}
            for k in OUTPUT_FEDEX_COLUMNS:
                if k in norm and k in df_out.columns:
                    val = norm.get(k, "")
                    df_out.at[idx, k] = "" if pd.isna(val) else str(val)
        except Exception as ex:
            logger.debug(
                "Replay enrichment failed for tracking %s: %s", tn, ex)

    return df_out


def process_workbook(
    input_path: Path,
    processed_path: Path,
    logger,  # kept loose
    env_cfg: Optional[EnvCfg] = None,
    *,
    client: Optional[Any] = None,
    normalizer: Optional[Normalizer] = None,
) -> dict[str, Any]:
    input_path = Path(input_path)
    processed_path = Path(processed_path)

    if not input_path.exists():
        logger.error("Input file does not exist: %s", input_path)
        raise FileNotFoundError(input_path)

    # --- Robust read: ALWAYS set df_in ---
    try:
        df_in = pd.read_excel(input_path, sheet_name=0, engine="openpyxl")
        logger.debug(
            "Opened input workbook: %s (rows=%d, cols=%d)",
            input_path.name, len(df_in), len(df_in.columns)
        )
    except Exception as ex:
        logger.warning("Could not read input workbook (%s): %s",
                       input_path.name, ex)
        df_in = pd.DataFrame()  # <— ensure defined

    # Pass-through + append new columns
    df_out = _ensure_output_columns(df_in)

    # Optional enrichment (replay client)
    df_out = _enrich_with_client(
        df_out, logger, client=client, normalizer=normalizer)

    # Marker metadata
    now_utc = dt.datetime.now(dt.timezone.utc).isoformat()
    has_creds = bool(
        getattr(env_cfg, "SHIPPING_CLIENT_ID", "") and
        getattr(env_cfg, "SHIPPING_CLIENT_SECRET", "")
    )
    marker = pd.DataFrame([{
        "_oss_marker": "ok",
        "input_name": input_path.name,
        "input_dir": str(input_path.parent),
        "output_name": processed_path.name,
        "timestamp_utc": now_utc,
        "env_has_creds": has_creds,
        "input_rows": len(df_in),
        "input_cols": len(df_in.columns),
        "output_rows": len(df_out),
        "output_cols": len(df_out.columns),
    }])

    # Write both sheets
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(processed_path, engine="openpyxl", mode="w") as xw:
        df_out.to_excel(xw, sheet_name="Processed", index=False)
        marker.to_excel(xw, sheet_name="Marker", index=False)

    logger.info("Wrote processed workbook → %s", processed_path)

    return {
        "output_path": str(processed_path),
        "env_has_creds": has_creds,
        "timestamp_utc": now_utc,
        "output_cols": list(df_out.columns),
        "output_shape": (len(df_out), len(df_out.columns)),
    }
