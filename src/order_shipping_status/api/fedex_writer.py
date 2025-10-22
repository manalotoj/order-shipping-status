from __future__ import annotations

import logging
import threading
import json
from typing import Any, Dict, Optional
from pathlib import Path
from dataclasses import dataclass


@dataclass
class FedExWriter:
    """Simple writer for FedEx API request/response bodies.

    By default writes newline-delimited JSON (NDJSON) to the given path, one
    JSON object per line with shape {"requested": [...], "response": {...}}.

    Optionally `json_list=True` will cause the writer to maintain a JSON array on
    disk (read existing, append, write back). That mode is less efficient for
    large outputs but sometimes easier for downstream tooling.
    """

    path: Path
    json_list: bool = False
    logger: Optional[logging.Logger] = None

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        self.logger = self.logger or logging.getLogger(
            "order_shipping_status.api.fedex_writer")
        # lock to make append/persist thread-safe
        self._lock = threading.Lock()
        # Ensure parent dir exists lazily on first write

    def write(self, requested: Any, response: Any) -> None:
        """Persist a single requested/response pair.

        - requested: typically a list of tracking numbers or the request body
        - response: the parsed JSON response (or whatever the transport returned)
        """
        obj = {"requested": requested, "response": response}
        try:
            with self._lock:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                if not self.json_list:
                    # NDJSON append
                    with self.path.open("a", encoding="utf-8") as fh:
                        fh.write(json.dumps(obj, ensure_ascii=False))
                        fh.write("\n")
                else:
                    # JSON list mode: read existing (if any), append, write back
                    items = []
                    if self.path.exists():
                        try:
                            with self.path.open("r", encoding="utf-8") as fh:
                                items = json.load(fh)
                                if not isinstance(items, list):
                                    items = []
                        except Exception:
                            items = []
                    items.append(obj)
                    with self.path.open("w", encoding="utf-8") as fh:
                        json.dump(items, fh, ensure_ascii=False, indent=2)
        except Exception as ex:
            try:
                self.logger.warning(
                    "Failed to persist FedEx API body to %s: %s", self.path, ex)
            except Exception:
                pass

    def read_all(self) -> list:
        """Read and return all saved objects.

        Returns a list of objects for both NDJSON and JSON-list modes.
        """
        try:
            if not self.path.exists():
                return []
            if not self.json_list:
                out = []
                with self.path.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            out.append(json.loads(line))
                        except Exception:
                            # skip malformed lines
                            continue
                return out
            else:
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
