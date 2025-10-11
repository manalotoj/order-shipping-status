from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Union
import os
import sys

# We’ll tag configured loggers to avoid duplicate handlers on repeated calls.
_OSS_LOGGER_MARK = "_oss_logger_configured"

# Default line format: timestamp | level | logger | message
_DEFAULT_FMT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
_DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _coerce_level(level: Optional[Union[int, str]]) -> int:
    """
    Accepts logging levels as int or str (e.g., 'INFO', 'debug').
    Falls back to LOG_LEVEL env, then INFO.
    """
    if level is None:
        env_level = os.getenv("LOG_LEVEL")
        if env_level:
            level = env_level

    if isinstance(level, int):
        return level

    if isinstance(level, str):
        lvl = level.strip().upper()
        # Map common names; logging.getLevelName also works but is reversible
        mapping = {
            "CRITICAL": logging.CRITICAL,
            "ERROR": logging.ERROR,
            "WARNING": logging.WARNING,
            "WARN": logging.WARNING,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG,
            "NOTSET": logging.NOTSET,
        }
        return mapping.get(lvl, logging.INFO)

    return logging.INFO


def default_log_path_for_input(input_path: Union[str, Path]) -> Path:
    """
    Given an input file path, return the log file path in the same directory
    with `.log` extension (e.g., /path/file.xlsx -> /path/file.log).
    """
    p = Path(input_path)
    return p.with_suffix(".log")


def get_logger(
    name: Optional[str] = None,
    *,
    level: Optional[Union[int, str]] = None,
    log_file: Optional[Union[str, Path]] = None,
    console: bool = True,
    propagate: bool = False,
    fmt: str = _DEFAULT_FMT,
    datefmt: str = _DEFAULT_DATEFMT,
    max_bytes: int = 5_000_000,
    backup_count: int = 5,
) -> logging.Logger:
    """
    Create/configure a logger. Safe to call multiple times:
    - Won’t duplicate existing handlers
    - Will add missing targets (e.g., add file later)
    """
    logger = logging.getLogger(name)
    logger.setLevel(_coerce_level(level))
    logger.propagate = propagate

    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    # Detect existing handlers
    def _has_console() -> bool:
        for h in logger.handlers:
            # Exclude FileHandlers; only count real console streams
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
                # Stream may be stderr or stdout
                if getattr(h, "stream", None) in (sys.stderr, sys.stdout):
                    return True
        return False

    def _has_file(path: Path) -> bool:
        for h in logger.handlers:
            if isinstance(h, RotatingFileHandler):
                try:
                    if Path(h.baseFilename) == path:
                        return True
                except Exception:
                    pass
        return False

    # Add console if requested and missing
    if console and not _has_console():
        sh = logging.StreamHandler(stream=sys.stderr)
        sh.setFormatter(formatter)
        sh.setLevel(logger.level)
        logger.addHandler(sh)

    # Add file if requested and missing
    if log_file is not None:
        log_path = Path(log_file)
        if not _has_file(log_path):
            log_path.parent.mkdir(parents=True, exist_ok=True)
            fh = RotatingFileHandler(
                filename=str(log_path),
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
                delay=True,
            )
            fh.setFormatter(formatter)
            fh.setLevel(logger.level)
            logger.addHandler(fh)

    # Mark configured (for potential external checks), but DO NOT early-return above.
    setattr(logger, _OSS_LOGGER_MARK, True)
    return logger
