# src/order_shipping_status/cli.py
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config.logging_config import get_logger
from .io.paths import derive_output_paths
from .config.env import get_app_env
from .pipelines.process_workbook import process_workbook   # <- add this import


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="order-shipping-status",
        description="Process a shipping workbook and emit a *_processed.xlsx next to the input."
    )
    p.add_argument("input", type=Path, help="Path to input .xlsx file.")
    p.add_argument("--no-console", action="store_true",
                   help="Disable console logging (file logging remains).")
    p.add_argument("--log-level", default="INFO",
                   help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO")
    p.add_argument("--strict-env", action="store_true",
                   help="Require SHIPPING_CLIENT_ID/SECRET to be present; otherwise exit 2.")
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

    # Call the stub pipeline façade
    try:
        process_workbook(args.input, processed_path, logger, env_cfg)
    except FileNotFoundError as e:
        logger.error("Input missing: %s", e)
        return 2
    except Exception as e:
        logger.exception("Failed to process workbook: %s", e)
        return 1

    logger.info("Done.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
