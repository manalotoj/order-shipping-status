# src/order_shipping_status/pipelines/preprocessor.py
from __future__ import annotations
import datetime as dt
import pandas as pd
from typing import Optional, Tuple
from order_shipping_status.io.schema import LEGACY_STATUS_COLUMN


class Preprocessor:
    """Project’s input normalization and row filtering (prior week; not delivered)."""

    def __init__(self, reference_date: Optional[dt.date] = None, logger=None) -> None:
        self.reference_date = reference_date
        self.logger = logger

    # Public helper (useful for tests/CLI)
    def prior_week_range(self, ref: Optional[dt.date] = None) -> Tuple[dt.date, dt.date]:
        if ref is None:
            ref = self.reference_date or dt.date.today()
        days_since_sun = (ref.weekday() + 1) % 7  # Mon=0..Sun=6
        this_sun = ref - dt.timedelta(days=days_since_sun)
        prior_sun = this_sun - dt.timedelta(days=7)
        prior_sat = prior_sun + dt.timedelta(days=6)
        return prior_sun, prior_sat

    # Back-compat: keep the old private name
    def _prior_week_range(self, ref: Optional[dt.date] = None) -> Tuple[dt.date, dt.date]:
        return self.prior_week_range(ref)

    def _drop_first_column(self, df: pd.DataFrame) -> pd.DataFrame:
        return df if df.shape[1] == 0 else df.iloc[:, 1:].copy()

    def _filter_by_prior_week(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Promised Delivery Date" not in df.columns:
            return df
        start, end = self.prior_week_range()
        dates = pd.to_datetime(
            df["Promised Delivery Date"], errors="coerce", utc=False).dt.date
        return df.loc[(dates >= start) & (dates <= end)].copy()

    def _filter_not_delivered(self, df: pd.DataFrame) -> pd.DataFrame:
        if LEGACY_STATUS_COLUMN not in df.columns:
            return df
        s = df[LEGACY_STATUS_COLUMN].astype("string").fillna("")
        return df.loc[s.str.casefold() != "delivered"].copy()

    def _log_delta(self, label: str, before: int, after: int) -> None:
        if self.logger:
            self.logger.info("%s: %d -> %d (Δ %d)", label,
                             before, after, after - before)

    def prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df1 = self._drop_first_column(df)
        self._log_delta("drop_first_column", before, len(df1))

        before = len(df1)
        df2 = self._filter_by_prior_week(df1)
        self._log_delta("filter_by_prior_week", before, len(df2))

        before = len(df2)
        df3 = self._filter_not_delivered(df2)
        self._log_delta("filter_not_delivered", before, len(df3))

        return df3
