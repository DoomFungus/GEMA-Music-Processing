from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env.test", override=True)

import polars as pl
import pytest


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """Generic placeholder frame. Replace columns with real source schema."""
    return pl.DataFrame({"isrc": ["1", "2", "3"]})
