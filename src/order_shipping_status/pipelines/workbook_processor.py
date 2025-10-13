# src/order_shipping_status/pipelines/workbook_processor.py
from __future__ import annotations
from pathlib import Path
from typing import Any, Optional
import datetime as dt
import pandas as pd

from order_shipping_status.models import EnvCfg
from order_shipping_status.pipelines.preprocessor import Preprocessor
from order_shipping_status.pipelines.column_contract import ColumnContract
from order_shipping_status.pipelines.enricher import Enricher
from order_shipping_status.rules.classifier import apply_rules


class WorkbookProcessor:
    """Orchestrates pre-processing, column contract, and optional enrichment."""

    def __init__(
        self,
        logger,
        *,
        client: Optional[Any] = None,
        normalizer: Optional[Any] = None,
        reference_date: dt.date | None = None,
    ) -> None:
        self.logger = logger
        self.client = client
        self.normalizer = normalizer
        self.reference_date = reference_date

    def process(
        self,
        input_path: Path,
        processed_path: Path,
        env_cfg: Optional[EnvCfg] = None,
        *,
        sidecar_dir: Optional[Path] = None,  # ← new: forward-compatible arg
    ) -> dict[str, Any]:
        input_path = Path(input_path)
        processed_path = Path(processed_path)

        if not input_path.exists():
            self.logger.error("Input file does not exist: %s", input_path)
            raise FileNotFoundError(input_path)

        # Read first sheet (best effort)
        try:
            df_in = pd.read_excel(input_path, sheet_name=0, engine="openpyxl")
            self.logger.debug(
                "Opened input workbook: %s (rows=%d, cols=%d)",
                input_path.name, len(df_in), len(df_in.columns),
            )
        except Exception as ex:
            self.logger.warning(
                "Could not read input workbook (%s): %s", input_path.name, ex)
            df_in = pd.DataFrame()

        # Pipeline steps
        df_prep = Preprocessor(self.reference_date,
                               logger=self.logger).prepare(df_in)
        df_out = ColumnContract().ensure(df_prep)
        df_out = ColumnContract().ensure(df_prep)
        df_out = Enricher(self.logger, client=self.client,
                          normalizer=self.normalizer).enrich(df_out)
        # ← classify PreTransit (and future rules)
        df_out = apply_rules(df_out)
        # Forward sidecar_dir if your Enricher supports it; harmless if it ignores
        df_out = Enricher(self.logger, client=self.client, normalizer=self.normalizer).enrich(
            df_out, sidecar_dir=sidecar_dir
        )

        # Marker + write
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

        processed_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(processed_path, engine="openpyxl", mode="w") as xw:
            df_out.to_excel(xw, sheet_name="Processed", index=False)
            marker.to_excel(xw, sheet_name="Marker", index=False)

        self.logger.info("Wrote processed workbook → %s", processed_path)
        return {
            "output_path": str(processed_path),
            "env_has_creds": has_creds,
            "timestamp_utc": now_utc,
            "output_cols": list(df_out.columns),
            "output_shape": (len(df_out), len(df_out.columns)),
        }
