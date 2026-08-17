import polars as pl
import pytest

from etl_pipeline import db as db_module
from etl_pipeline.db import write_playback_logs


class FakeConnection:
    def __init__(self, merge_side_effect=None):
        self.executed: list[str] = []
        self.merge_side_effect = merge_side_effect
        self.committed = False
        self.rolled_back = False

    def execute(self, stmt):
        sql = str(stmt)
        self.executed.append(sql)
        if "INSERT INTO playback_logs" in sql and self.merge_side_effect is not None:
            raise self.merge_side_effect

        class _Result:
            rowcount = 3

        return _Result()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.committed = True
        else:
            self.rolled_back = True
        return False


class FakeEngine:
    def __init__(self, merge_side_effect=None):
        self.merge_side_effect = merge_side_effect
        self.last_connection: FakeConnection | None = None

    def begin(self):
        self.last_connection = FakeConnection(self.merge_side_effect)
        return self.last_connection


@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame({"isrc": ["1", "2", "3"]})


def _patch_engine(monkeypatch, engine: FakeEngine):
    monkeypatch.setattr(db_module, "_get_engine", lambda: engine)


def test_uses_unique_staging_table_name_per_call(monkeypatch, mocker, df):
    engine1 = FakeEngine()
    _patch_engine(monkeypatch, engine1)
    mocker.patch.object(df, "write_database")
    write_playback_logs(df)
    staging_1 = [sql for sql in engine1.last_connection.executed if "CREATE UNLOGGED TABLE" in sql][0]

    engine2 = FakeEngine()
    _patch_engine(monkeypatch, engine2)
    write_playback_logs(df)
    staging_2 = [sql for sql in engine2.last_connection.executed if "CREATE UNLOGGED TABLE" in sql][0]

    assert staging_1 != staging_2


def test_everything_runs_on_one_connection_and_commits_together(monkeypatch, mocker, df):
    engine = FakeEngine()
    _patch_engine(monkeypatch, engine)
    mocker.patch.object(df, "write_database")

    write_playback_logs(df)

    conn = engine.last_connection
    assert conn.committed
    assert not conn.rolled_back
    assert any("CREATE UNLOGGED TABLE" in sql for sql in conn.executed)
    assert any("DROP TABLE IF EXISTS" in sql for sql in conn.executed)


def test_transaction_rolls_back_when_merge_fails(monkeypatch, mocker, df):
    engine = FakeEngine(merge_side_effect=RuntimeError("merge failed"))
    _patch_engine(monkeypatch, engine)
    mocker.patch.object(df, "write_database")

    with pytest.raises(RuntimeError):
        write_playback_logs(df)

    conn = engine.last_connection
    assert conn.rolled_back
    assert not conn.committed
    assert not any("DROP TABLE IF EXISTS" in sql for sql in conn.executed)


def test_returns_rowcount_from_merge_not_input_height(monkeypatch, mocker):
    # 5 input rows, fake merge reports 3 actually inserted (2 duplicates skipped)
    df = pl.DataFrame({"isrc": ["1", "2", "3", "4", "5"]})
    engine = FakeEngine()
    _patch_engine(monkeypatch, engine)
    mocker.patch.object(df, "write_database")

    written = write_playback_logs(df)

    assert written == 3
    assert written != df.height
