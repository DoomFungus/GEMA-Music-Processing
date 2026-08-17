import logging

import polars as pl
import pytest
from sqlalchemy.exc import IntegrityError, OperationalError

from etl_pipeline import pipeline as pipeline_module
from etl_pipeline.pipeline import run_pipeline


def _row_lazyframe(isrc: str = "VALID123456", duration_seconds: int = 100) -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "timestamp": ["2026-01-01T00:00:00"],
            "isrc": [isrc],
            "station_id": ["ST1"],
            "duration_seconds": [duration_seconds],
            "listener_count": [10],
        }
    ).lazy()


def _mixed_valid_invalid_lazyframe() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "timestamp": ["2026-01-01T00:00:00", "2026-01-01T00:00:00"],
            "isrc": ["VALID123456", ""],  # second row is invalid: blank isrc
            "station_id": ["ST1", "ST1"],
            "duration_seconds": [100, 100],
            "listener_count": [10, 10],
        }
    ).lazy()


def test_run_pipeline_skips_invalid_rows_and_continues(monkeypatch, caplog):
    monkeypatch.setattr(pipeline_module, "extract_csv", lambda path: _mixed_valid_invalid_lazyframe())
    monkeypatch.setattr(pipeline_module, "enrich", lambda df: df)
    written_df = None

    def fake_write(df):
        nonlocal written_df
        written_df = df
        return df.height

    monkeypatch.setattr(pipeline_module, "write_playback_logs", fake_write)

    with caplog.at_level(logging.WARNING, logger="etl_pipeline.pipeline"):
        written = run_pipeline("unused.csv")

    assert written == 1
    assert written_df.height == 1
    assert written_df["isrc"].to_list() == ["VALID123456"]
    assert any(r.levelno == logging.WARNING for r in caplog.records)
    assert not any(r.levelno == logging.ERROR for r in caplog.records)


def test_run_pipeline_writes_valid_rows(monkeypatch):
    monkeypatch.setattr(pipeline_module, "extract_csv", lambda path: _row_lazyframe())
    monkeypatch.setattr(pipeline_module, "enrich", lambda df: df)
    monkeypatch.setattr(pipeline_module, "write_playback_logs", lambda df: df.height)

    written = run_pipeline("unused.csv")

    assert written == 1


@pytest.mark.parametrize("db_error", [IntegrityError, OperationalError])
def test_run_pipeline_logs_and_reraises_db_errors(monkeypatch, caplog, db_error):
    monkeypatch.setattr(pipeline_module, "extract_csv", lambda path: _row_lazyframe())
    monkeypatch.setattr(pipeline_module, "enrich", lambda df: df)

    def raise_db_error(df):
        raise db_error("stmt", {}, Exception("db failure"))

    monkeypatch.setattr(pipeline_module, "write_playback_logs", raise_db_error)

    with caplog.at_level(logging.ERROR, logger="etl_pipeline.pipeline"):
        with pytest.raises(db_error):
            run_pipeline("unused.csv")

    assert any(r.levelno == logging.ERROR for r in caplog.records)
