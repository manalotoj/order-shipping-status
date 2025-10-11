from pathlib import Path
import pytest
from order_shipping_status.io.paths import derive_output_paths, PROCESSED_SUFFIX


def test_derive_output_paths_happy_path(tmp_path: Path):
    src = tmp_path / "RAW_TransitIssues_10-7-2025.xlsx"
    src.write_text("placeholder")

    processed, log = derive_output_paths(src)
    assert processed.parent == src.parent
    assert processed.name == f"{src.stem}{PROCESSED_SUFFIX}"
    assert log.parent == src.parent
    assert log.name == f"{src.stem}.log"


def test_derive_output_paths_missing_input_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        derive_output_paths(tmp_path / "missing.xlsx")
