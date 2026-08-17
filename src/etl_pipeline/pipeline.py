import logging
from pathlib import Path

import polars as pl
from polars.exceptions import PolarsError
from sqlalchemy.exc import SQLAlchemyError

from etl_pipeline.db import write_playback_logs
from etl_pipeline.enrich import enrich

logger = logging.getLogger(__name__)


def extract_csv(path: str | Path) -> pl.LazyFrame:
    """Scan a CSV file into a LazyFrame. Raises FileNotFoundError if missing, PolarsError if malformed."""
    path = Path(path)
    if not path.exists():
        logger.error("CSV not found: %s", path)
        raise FileNotFoundError(f"CSV not found: {path}")

    df = pl.scan_csv(path)
    try:
        df.collect_schema()
    except PolarsError as exc:
        logger.error("CSV malformed: %s (%s)", path, exc)
        raise
    return df


def validate(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        (
            pl.col("isrc").is_not_null() & (pl.col("isrc").str.strip_chars() != "")
            & pl.col("station_id").is_not_null() & (pl.col("station_id").str.strip_chars() != "")
            & pl.col("duration_seconds").is_not_null() & (pl.col("duration_seconds") > 0)
            & pl.col("listener_count").is_not_null() & (pl.col("listener_count") > 0)
            & pl.col("timestamp").is_not_null()
        ).alias("_is_valid")
    )

def transform(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns((pl.col("duration_seconds") * pl.col("listener_count")).alias("listened_seconds"))


def run_pipeline(csv_path: str) -> int:
    logger.info("extracting %s", csv_path)
    df = extract_csv(csv_path)
    df = validate(df)
    df = transform(df)
    df = enrich(df)

    try:
        df = df.collect()
    except PolarsError as exc:
        # row-level malformation (ragged lines, bad quoting, ...) only surfaces here -
        # collect_schema() in extract_csv only catches structural issues that don't need a full read.
        logger.error("CSV data malformed: %s (%s)", csv_path, exc)
        raise

    invalid = df.filter(~pl.col("_is_valid"))
    if invalid.height > 0:
        logger.warning("skipping %d invalid playback log row(s): %s", invalid.height, invalid.head(5))
    df = df.filter(pl.col("_is_valid")).drop("_is_valid")

    try:
        written = write_playback_logs(df)
    except SQLAlchemyError as exc:
        logger.error("failed to write playback logs: %s", exc)
        raise

    logger.info("done: %d new row(s) written (duplicates skipped)", written)
    return written
