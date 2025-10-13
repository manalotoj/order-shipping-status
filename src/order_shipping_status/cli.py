# src/order_shipping_status/cli.py
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config.logging_config import get_logger
from .io.paths import derive_output_paths
from .config.env import get_app_env
from .pipelines.workbook_processor import WorkbookProcessor


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
    p.add_argument(
        "--use-api",
        action="store_true",
        help="Use live FedEx API (requires credentials in env). Ignored if --replay-dir is set.",
    )
    p.add_argument(
        "--reference-date",
        type=str,
        default=None,
        help="YYYY-MM-DD anchor date for the prior-week filter (Sunday..Saturday).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    # Resolve derived paths (also validates input exists)
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

    # Decide enrichment strategy
    client = None
    normalizer = None

    if args.replay_dir:
        # Lazy imports to keep startup light
        from .api.client import ReplayClient
        from .api.normalize import normalize_fedex

        client = ReplayClient(args.replay_dir)
        normalizer = normalize_fedex
        logger.info("Replay mode enabled: %s", args.replay_dir)

    elif args.use_api:
        from .api.fedex import FedExClient, FedExAuth, FedExConfig
        from .api.transport import RequestsTransport
        from .api.normalize import normalize_fedex

        token_url = getattr(env_cfg, "FEDEX_TOKEN_URL",
                            None) or "https://apis.fedex.com/oauth/token"
        base_url = getattr(env_cfg, "FEDEX_BASE_URL",
                           None) or "https://apis.fedex.com/track"

        auth = FedExAuth(
            client_id=getattr(env_cfg, "SHIPPING_CLIENT_ID", ""),
            client_secret=getattr(env_cfg, "SHIPPING_CLIENT_SECRET", ""),
            token_url=token_url,
        )
        cfg = FedExConfig(base_url=base_url)
        client = FedExClient(auth, cfg, transport=RequestsTransport())
        normalizer = normalize_fedex
        logger.info("Live FedEx API enabled (base=%s)", base_url)

    # Reference date (optional)
    reference_date = None
    if args.reference_date:
        from datetime import date
        try:
            reference_date = date.fromisoformat(args.reference_date)
        except ValueError:
            logger.error(
                "Invalid --reference-date: %s (expected YYYY-MM-DD)", args.reference_date)
            return 2

    # Orchestrate via WorkbookProcessor
    try:
        processor = WorkbookProcessor(
            logger,
            client=client,
            normalizer=normalizer,
            reference_date=reference_date,
        )
        processor.process(args.input, processed_path, env_cfg)
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
