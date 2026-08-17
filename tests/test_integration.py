import random
from pathlib import Path

import polars as pl

from etl_pipeline.enrich import enrich
from etl_pipeline.pipeline import extract_csv, transform, validate

SAMPLE_CSV = Path(__file__).parent.parent / "data" / "sample_playback_logs.csv"


def test_ingest_through_enrich_happy_path():
    df = extract_csv(SAMPLE_CSV)
    df = validate(df)
    df = transform(df)
    df = enrich(df)

    result = df.collect()

    assert result.height == 10
    assert result.filter(~pl.col("_is_valid")).height == 0
    assert result["author"].null_count() == 0
    assert result["listened_seconds"].null_count() == 0

    row = result.row(random.randrange(result.height), named=True)
    assert row["listened_seconds"] == row["duration_seconds"] * row["listener_count"]
