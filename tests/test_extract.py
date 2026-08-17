import logging

import pytest
from polars.exceptions import PolarsError

from etl_pipeline.pipeline import extract_csv


def test_extract_csv_reads_file(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

    df = extract_csv(csv_file).collect()

    assert df.shape == (2, 2)
    assert df.columns == ["id", "name"]


def test_extract_csv_missing_file_raises(tmp_path, caplog):
    with caplog.at_level(logging.ERROR, logger="etl_pipeline.pipeline"):
        with pytest.raises(FileNotFoundError):
            extract_csv(tmp_path / "missing.csv")

    assert any(r.levelno == logging.ERROR for r in caplog.records)


def test_extract_csv_empty_file_raises(tmp_path, caplog):
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("")

    with caplog.at_level(logging.ERROR, logger="etl_pipeline.pipeline"):
        with pytest.raises(PolarsError):
            extract_csv(csv_file)

    assert any(r.levelno == logging.ERROR for r in caplog.records)
