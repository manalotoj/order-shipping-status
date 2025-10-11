import io
import os
import sys
from pathlib import Path
import logging

import pytest

from order_shipping_status.config.logging_config import (
    get_logger,
    default_log_path_for_input,
)


def test_default_log_path_for_input():
    assert default_log_path_for_input(
        "/x/y/file.xlsx") == Path("/x/y/file.log")
    assert default_log_path_for_input("file.csv") == Path("file.log")


def test_get_logger_idempotent_no_duplicate_handlers(tmp_path, monkeypatch):
    # Ensure clean root handlers for isolation
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    log_path = tmp_path / "run.log"
    logger = get_logger("oss.test", level="DEBUG",
                        log_file=log_path, console=False)
    # Call again with same params — should not add more handlers
    logger2 = get_logger("oss.test", level="DEBUG",
                         log_file=log_path, console=False)

    assert logger is logger2
    assert len(logger.handlers) == 1  # just file handler


def test_get_logger_adds_console_handler(tmp_path):
    root = logging.getLogger("oss.console")
    for h in list(root.handlers):
        root.removeHandler(h)

    logger = get_logger("oss.console", level="INFO",
                        console=True, log_file=None)

    # exactly one StreamHandler
    shs = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
    assert len(shs) == 1


def test_get_logger_writes_to_file(tmp_path):
    log_file = tmp_path / "logs" / "app.log"
    logger = get_logger("oss.file", level="INFO",
                        log_file=log_file, console=False)
    logger.info("hello world")

    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8")
    assert "hello world" in content


def test_get_logger_respects_level_env(monkeypatch, tmp_path):
    monkeypatch.setenv("LOG_LEVEL", "ERROR")
    log_file = tmp_path / "lvl.log"
    logger = get_logger("oss.level.env", log_file=log_file, console=False)

    logger.info("should NOT appear")
    logger.error("should appear")

    text = log_file.read_text(encoding="utf-8")
    assert "should appear" in text
    assert "should NOT appear" not in text


def test_multiple_calls_different_targets_do_not_duplicate(tmp_path):
    """
    If someone calls get_logger first with console, then later adds a file,
    we should end up with exactly two handlers (console + file), not more.
    """
    name = "oss.multi"
    # start clean
    lg = logging.getLogger(name)
    for h in list(lg.handlers):
        lg.removeHandler(h)

    lg1 = get_logger(name, level="INFO", console=True, log_file=None)
    lg2 = get_logger(name, level="INFO", console=True,
                     log_file=tmp_path / "x.log")

    assert lg1 is lg2
    # Expect 2: console + file
    assert len(lg2.handlers) == 2
