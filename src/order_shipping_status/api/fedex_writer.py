from __future__ import annotations

import logging
import threading
import json
from typing import Any, Dict, Optional
from pathlib import Path
from dataclasses import dataclass


@dataclass
class FedExWriter:
    """Persist ONLY FedEx API response bodies as a single JSON array.

    File shape on disk:
        [
          { ...response body 1... },
          { ...response body 2... },
          ...
        ]
    """

    path: Path
    json_list: bool = True
    logger: Optional[logging.Logger] = None

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        self.logger = self.logger or logging.getLogger(
            "order_shipping_status.api.fedex_writer")
        # lock to make append/persist thread-safe
        self._lock = threading.Lock()
        # Ensure parent dir exists lazily on first write

    def write(self, requested: Any, response: Any) -> None:
        """Back-compat entrypoint: now ignores `requested` and persists only `response`."""
        self.add_response(response)

    def add_response(self, response: Any) -> None:
        """Append a single response body (dict-like) into the on-disk JSON array."""
        try:
            with self._lock:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                items = []
                if self.path.exists():
                    try:
                        with self.path.open("r", encoding="utf-8") as fh:
                            data = json.load(fh)
                            if isinstance(data, list):
                                items = data
                    except Exception:
                        items = []
                items.append(response)
                with self.path.open("w", encoding="utf-8") as fh:
                    json.dump(items, fh, ensure_ascii=False, indent=2)
        except Exception as ex:
            try:
                self.logger.warning(
                    "Failed to append FedEx API response to %s: %s", self.path, ex)
            except Exception:
                pass

    def read_all(self) -> list:
        """Read and return all saved response bodies (JSON array)."""
        try:
            if not self.path.exists():
                return []
            with self.path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
                return data if isinstance(data, list) else []
        except Exception as ex:
            try:
                self.logger.warning(
                    "Failed to read FedEx API bodies from %s: %s", self.path, ex)
            except Exception:
                pass
            return []
