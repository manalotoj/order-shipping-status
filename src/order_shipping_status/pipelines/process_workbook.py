# Deprecated shim: prefer order_shipping_status.pipelines.workbook_processor
from __future__ import annotations
from pathlib import Path
from typing import Any, Optional
import datetime as dt

from order_shipping_status.models import EnvCfg
from order_shipping_status.pipelines.workbook_processor import WorkbookProcessor


def process_workbook(
    input_path: Path,
    processed_path: Path,
    logger,
    env_cfg: Optional[EnvCfg] = None,
    *,
    client: Optional[Any] = None,
    normalizer: Optional[Any] = None,
    reference_date: dt.date | None = None,
) -> dict[str, Any]:
    """Backward-compatible wrapper. Prefer WorkbookProcessor.process()."""
    return WorkbookProcessor(
        logger,
        client=client,
        normalizer=normalizer,
        reference_date=reference_date,
    ).process(input_path, processed_path, env_cfg)
