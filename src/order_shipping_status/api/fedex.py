from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import json
import time
import logging

from .transport import RequestsTransport


@dataclass
class FedExConfig:
    base_url: str


@dataclass
class FedExAuth:
    client_id: str
    client_secret: str
    token_url: str


class FedExClient:
    """Minimal FedEx client.

    Responsibilities:
    - authenticate(): acquires an OAuth token using client_id/client_secret in
      the x-www-form-urlencoded body (no Basic auth header).
    - post_tracking(body, access_token=None): POST a single tracking request
      body (which may include up to 30 tracking numbers). The caller is
      responsible for batching and persistence.

    The client uses RequestsTransport for HTTP operations so it fits the
    project's transport abstraction.
    """

    def __init__(
        self,
        auth: FedExAuth,
        cfg: FedExConfig,
        transport: Optional[RequestsTransport] = None,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.auth = auth
        self.cfg = cfg
        self.transport = transport or RequestsTransport()
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self.logger: logging.Logger = logger or logging.getLogger(
            "order_shipping_status.api.fedex"
        )

    def authenticate(self) -> Optional[str]:
        """Ensure an access token is available and return it.

        Acquires token by POSTing application/x-www-form-urlencoded with
        grant_type=client_credentials, client_id and client_secret. Caches
        token in-memory until expiry.
        """
        now = time.time()
        if self._token and now < self._token_expires_at - 10:
            return self._token

        data = {
            "grant_type": "client_credentials",
            "client_id": self.auth.client_id,
            "client_secret": self.auth.client_secret,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        try:
            self.logger.debug(
                "Requesting FedEx OAuth token (form body) from %s",
                self.auth.token_url,
            )
        except Exception:
            pass

        try:
            resp = self.transport.post(
                self.auth.token_url, headers=headers, data=data)
        except Exception as ex:  # network/transport error
            try:
                self.logger.warning("FedEx token request failed: %s", ex)
            except Exception:
                pass
            self._token = None
            self._token_expires_at = 0.0
            return None

        try:
            status = resp.status_code
        except Exception:
            status = None

        try:
            resp.raise_for_status()
            j = resp.json()
            self._token = j.get("access_token")
            expires_in = int(j.get("expires_in", 3600))
            self._token_expires_at = time.time() + expires_in
            try:
                self.logger.debug(
                    "FedEx token acquired (expires_in=%s status=%s)", expires_in, status
                )
            except Exception:
                pass
            return self._token
        except Exception as ex:
            resp_text = None
            try:
                resp_text = resp.text
            except Exception:
                resp_text = None
            try:
                self.logger.warning(
                    "FedEx token request returned error status=%s exception=%s response_body=%s",
                    status,
                    ex,
                    (resp_text[:2000] + "...") if resp_text and len(
                        resp_text) > 2000 else resp_text,
                )
            except Exception:
                pass
            self._token = None
            self._token_expires_at = 0.0
            return None

    def _endpoint_for_tracking(self) -> str:
        base = self.cfg.base_url.rstrip("/")
        if "trackingnumbers" in base or "/v1/" in base:
            return base
        return base + "/v1/trackingnumbers"

    def post_tracking(self, body: Dict[str, Any], access_token: Optional[str] = None) -> Dict[str, Any]:
        """POST a single tracking request body to FedEx Track API and return parsed JSON.

        - `body` should follow the FedEx Track API shape (it may contain up to 30 TNs).
        - If `access_token` is provided, it will be used. Otherwise the client will
          call `authenticate()` and use the cached token.
        - The function returns the parsed JSON response (or an empty dict on error).
        """
        token = access_token or self.authenticate()
        if not token:
            return {}

        headers = {"Authorization": f"Bearer {token}",
                   "Content-Type": "application/json"}
        endpoint = self._endpoint_for_tracking()

        try:
            body_text = json.dumps(body, ensure_ascii=False)
        except Exception:
            body_text = str(body)

        try:
            self.logger.debug(
                "FedEx POST endpoint=%s request_body=%s",
                endpoint,
                (body_text[:4000] +
                 "...") if len(body_text) > 4000 else body_text,
            )
        except Exception:
            pass

        try:
            resp = self.transport.post(endpoint, headers=headers, json=body)
            try:
                status = resp.status_code
            except Exception:
                status = None
            try:
                resp.raise_for_status()
                j = resp.json()
                try:
                    resp_text = json.dumps(j, ensure_ascii=False)
                except Exception:
                    try:
                        resp_text = resp.text
                    except Exception:
                        resp_text = str(j)
                try:
                    self.logger.debug(
                        "FedEx POST endpoint=%s status=%s response_body=%s",
                        endpoint,
                        status,
                        (resp_text[:4000] + "...") if resp_text and len(
                            resp_text) > 4000 else resp_text,
                    )
                except Exception:
                    pass
                return j
            except Exception as ex:
                resp_text = None
                try:
                    resp_text = resp.text
                except Exception:
                    resp_text = None
                try:
                    self.logger.warning(
                        "FedEx POST endpoint=%s returned error status=%s exception=%s response_body=%s",
                        endpoint,
                        status,
                        ex,
                        (resp_text[:4000] + "...") if resp_text and len(
                            resp_text) > 4000 else resp_text,
                    )
                except Exception:
                    pass
                return {}
        except Exception as ex:
            try:
                self.logger.warning(
                    "FedEx transport POST failed for endpoint=%s: %s", endpoint, ex)
            except Exception:
                pass
            return {}
