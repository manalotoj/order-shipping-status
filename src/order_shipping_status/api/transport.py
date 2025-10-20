from __future__ import annotations

from typing import Any, Dict, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RequestsTransport:
    """Requests session wrapper with retry/backoff.

    Retries on typical transient errors and on specified status codes.
    """

    def __init__(self, timeout: int = 30, max_retries: int = 3, backoff_factor: float = 0.3) -> None:
        self.session = requests.Session()
        self.timeout = timeout

        retry = Retry(
            total=max_retries,
            read=max_retries,
            connect=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
        )
        adapter = HTTPAdapter(max_retries=retry)
        # mount both http and https
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def post(self, url: str, *, headers: Optional[Dict[str, str]] = None, data: Any = None, json: Any = None, params: Optional[Dict[str, Any]] = None):
        return self.session.post(url, headers=headers, data=data, json=json, params=params, timeout=self.timeout)

    def get(self, url: str, *, headers: Optional[Dict[str, str]] = None, params: Optional[Dict[str, Any]] = None):
        return self.session.get(url, headers=headers, params=params, timeout=self.timeout)
