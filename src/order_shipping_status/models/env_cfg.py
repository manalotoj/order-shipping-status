from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class EnvCfg:
    """Minimal shape we need from get_app_env()."""
    SHIPPING_CLIENT_ID: str = ""
    SHIPPING_CLIENT_SECRET: str = ""
