from __future__ import annotations

from pathlib import Path

import pandas as pd
import datetime as dt

from order_shipping_status.pipelines.preprocessor import Preprocessor


def test_preprocess_10_7_capture(tmp_path: Path):
    """Run the Preprocessor against the representative 10-7 capture and
    assert we get the expected number of rows with issues (63).
    """
    # Resolve tests/data relative to repo root so pytest CWD doesn't matter
    repo_root = Path(__file__).resolve().parents[3]
    data_dir = repo_root / "tests" / "data"
    # Accept either RAW_TransitIssues_10-7-2025.xlsx or common typos
    candidates = sorted(data_dir.glob("RAW_Transit*10-7*.xlsx"))
    assert candidates, "Expected a RAW_TransitIssues 10-7 workbook under tests/data"
    src_fixture = candidates[0]

    dest = tmp_path / src_fixture.name
    dest.write_bytes(src_fixture.read_bytes())

    # Read using pandas (keep dtypes loose)
    df = pd.read_excel(dest, engine="openpyxl")

    prep = Preprocessor(reference_date=dt.date(
        2025, 10, 7), enable_date_filter=True)
    out = prep.prepare(df)

    # assert exact expected count
    assert len(
        out) == 63, f"Expected exactly 63 rows after preprocessing, got {len(out)}"
