# src/order_shipping_status/cli.py
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config.logging_config import get_logger
from .io.paths import derive_output_paths
from .config.env import get_app_env
from .pipelines.process_workbook import process_workbook
from .api.client import ReplayClient, normalize_status


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="order-shipping-status",
        description="Process a shipping workbook and emit a *_processed.xlsx next to the input.",
    )
    p.add_argument("input", type=Path, help="Path to input .xlsx file.")
    p.add_argument(
        "--no-console",
        action="store_true",
        help="Disable console logging (file logging remains).",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
    )
    p.add_argument(
        "--strict-env",
        action="store_true",
        help="Require SHIPPING_CLIENT_ID/SECRET to be present; otherwise exit 2.",
    )
    p.add_argument(
        "--replay-dir",
        type=Path,
        default=None,
        help="Directory containing <Tracking Number>.json bodies for deterministic replays.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    # Resolve derived paths
    try:
        processed_path, log_path = derive_output_paths(args.input)
    except FileNotFoundError:
        print(f"error: input file not found: {args.input}", file=sys.stderr)
        return 2

    # Configure logging (file + optional console)
    logger = get_logger(
        "order_shipping_status",
        level=args.log_level,
        console=not args.no_console,
        log_file=log_path,
    )
    logger.debug("Logger initialized.")
    logger.info("Input: %s", args.input)
    logger.info("Processed output: %s", processed_path)
    logger.info("Log file: %s", log_path)

    # Load env (don’t fail unless user asked for strict)
    try:
        env_cfg = get_app_env(strict=args.strict_env)
        if args.strict_env:
            logger.info(
                "Strict env passed — required shipping credentials present.")
        else:
            logger.debug("Env loaded (non-strict).")
    except RuntimeError as e:
        logger.error("Environment error: %s", e)
        return 2

    # Optional replay client
    client = None
    normalizer = None
    if args.replay_dir:
        client = ReplayClient(args.replay_dir)
        normalizer = normalize_status
        logger.info("Replay mode enabled: %s", args.replay_dir)

    # Call the pipeline façade
    try:
        process_workbook(
            args.input,
            processed_path,
            logger,
            env_cfg,
            client=client,
            normalizer=normalizer,
        )
    except FileNotFoundError as e:
        logger.error("Input missing: %s", e)
        return 2
    except Exception as e:
        logger.exception("Failed to process workbook: %s", e)
        return 1

    logger.info("Done.")
    return 0


def test_replay_enrichment_populates_fedex_columns(tmp_path):
    import json
    from types import SimpleNamespace
    import pandas as pd
    from order_shipping_status.pipelines.process_workbook import process_workbook
    from order_shipping_status.api.client import ReplayClient, normalize_status
    from order_shipping_status.io.schema import OUTPUT_FEDEX_COLUMNS

    # --- Arrange: replay body for a tracking number ---
    tracking = "123456789012"
    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()
    (replay_dir / f"{tracking}.json").write_text(json.dumps({
        "code": "DLV",
        "statusByLocale": "Delivered",
        "description": "Left at front door",
    }), encoding="utf-8")

    # --- Arrange: input workbook with that tracking number ---
    src = tmp_path / "in.xlsx"
    pd.DataFrame([{
        "A": 1,
        "Tracking Number": tracking,
        "Carrier Code": "FDX",
    }]).to_excel(src, index=False)
    out = tmp_path / "in_processed.xlsx"

    # --- Arrange: quiet logger + env ---
    class Logger:
        def debug(self, *a, **k): pass
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
        def error(self, *a, **k): pass
    env = SimpleNamespace(SHIPPING_CLIENT_ID="", SHIPPING_CLIENT_SECRET="")

    # --- Act: run with ReplayClient + normalizer ---
    process_workbook(
        src, out, Logger(), env,
        client=ReplayClient(replay_dir),
        normalizer=normalize_status,
    )

    # --- Assert: processed sheet has populated FedEx columns for that row ---
    df = pd.read_excel(out, sheet_name="Processed", engine="openpyxl")
    assert df.iloc[0]["code"] == "DLV"
    # mirrors code in current normalizer
    assert df.iloc[0]["derivedCode"] == "DLV"
    assert df.iloc[0]["statusByLocale"] == "Delivered"
    assert df.iloc[0]["description"] == "Left at front door"

    # And: all expected FedEx columns exist
    for col in OUTPUT_FEDEX_COLUMNS:
        assert col in df.columns


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
