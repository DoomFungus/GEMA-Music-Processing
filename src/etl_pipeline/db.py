import uuid

import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl_pipeline.config import settings

TABLE_NAME = "playback_logs"

COLUMNS = [
    "isrc",
    "author",
    "title",
    "copyright_holder",
    "station_id",
    "duration_seconds",
    "listener_count",
    "listened_seconds",
    "timestamp",
]


def _get_engine(database_url: str | None = None) -> Engine:
    return create_engine(database_url or settings.database_url, future=True)


def write_playback_logs(df: pl.DataFrame) -> int:
    """Load a DataFrame into Postgres via a staging table, deduping against
    playback_logs_dedup_idx. Returns the number of rows actually inserted.
    """
    engine = _get_engine()
    staging_table = f"playback_logs_staging_{uuid.uuid4().hex}"
    column_list = ", ".join(f'"{c}"' for c in COLUMNS)

    with engine.begin() as conn:
        conn.execute(text(f'CREATE UNLOGGED TABLE "{staging_table}" (LIKE {TABLE_NAME} INCLUDING DEFAULTS)'))
        conn.execute(text(f'ALTER TABLE "{staging_table}" DROP COLUMN playback_id'))

        df.write_database(table_name=staging_table, connection=conn, if_table_exists="append")

        result = conn.execute(
            text(
                f"""
                INSERT INTO {TABLE_NAME} ({column_list})
                SELECT {column_list}
                FROM "{staging_table}"
                ON CONFLICT (md5(isrc || station_id || timestamp::text)) DO NOTHING
                """
            )
        )
        written = result.rowcount

        conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))

    return written
