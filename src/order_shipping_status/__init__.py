# src/order_shipping_status/__init__.py
from .pipelines.workbook_processor import WorkbookProcessor
from .pipelines.process_workbook import process_workbook  # <-- shim defines it

__all__ = [
    "WorkbookProcessor",
    "process_workbook",
]
