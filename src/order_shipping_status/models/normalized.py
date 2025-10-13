from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class NormalizedShippingData:
    # identity/context
    carrier: Optional[str]            # e.g., "FEDEX"
    tracking_number: Optional[str]
    carrier_code: Optional[str]       # if present in your sheet

    # core columns you currently write to Excel
    code: str
    derivedCode: str
    statusByLocale: str
    description: str

    # frequently used fields for rules (not all written to Excel yet)
    actual_delivery_dt: Optional[datetime]
    possession_status: Optional[bool]
    service_type: Optional[str]
    service_desc: Optional[str]
    origin_city: Optional[str]
    origin_state: Optional[str]
    dest_city: Optional[str]
    dest_state: Optional[str]
    received_by_name: Optional[str]

    # raw payload for audit and future rules
    raw: dict[str, Any]

    # optional metadata
    source: Optional[str] = None      # "fedex_api" | "replay"
    captured_at: Optional[datetime] = None

    def to_excel_cols(self) -> dict[str, str]:
        """Only the 4 FedEx columns you currently persist."""
        return {
            "code": self.code or "",
            "derivedCode": self.derivedCode or self.code or "",
            "statusByLocale": self.statusByLocale or "",
            "description": self.description or "",
        }

    def to_dict(self) -> dict[str, Any]:
        """Convenience for sidecars/logging/tests."""
        return asdict(self)
