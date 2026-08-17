import json
import logging

import httpx
import polars as pl
import pytest
import respx

from etl_pipeline.config import settings
from etl_pipeline.enrich import enrich
from etl_pipeline.mock_client import MOCK_ISRC_DATA


@respx.mock
def test_enrich_joins_batch_response_onto_df(monkeypatch):
    monkeypatch.setattr(settings, "use_mock_api", False)
    respx.post("https://api.example.com/batch").mock(
        return_value=httpx.Response(
            200,
            json=[{"isrc": "1", "author": "a"}, {"isrc": "2", "author": "b"}],
        )
    )

    df = pl.DataFrame({"isrc": ["1", "2"]}).lazy()

    result = enrich(df).collect()

    assert result.sort("isrc")["author"].to_list() == ["a", "b"]


@respx.mock
def test_enrich_batches_requests_by_settings_api_batch_size(monkeypatch):
    monkeypatch.setattr(settings, "use_mock_api", False)
    monkeypatch.setattr(settings, "api_batch_size", 2)
    seen_batches = []

    def handler(request: httpx.Request) -> httpx.Response:
        keys = json.loads(request.content)["keys"]
        seen_batches.append(keys)
        return httpx.Response(200, json=[{"isrc": k, "author": k} for k in keys])

    respx.post("https://api.example.com/batch").mock(side_effect=handler)

    df = pl.DataFrame({"isrc": ["1", "2", "3"]}).lazy()

    result = enrich(df).collect()

    assert sorted(len(b) for b in seen_batches) == [1, 2]
    assert sorted(k for batch in seen_batches for k in batch) == ["1", "2", "3"]
    assert result.height == 3


@respx.mock
def test_enrich_handles_api_failure_without_raising(monkeypatch, caplog):
    monkeypatch.setattr(settings, "use_mock_api", False)
    respx.post("https://api.example.com/batch").mock(return_value=httpx.Response(500))

    df = pl.DataFrame({"isrc": ["1"]}).lazy()

    with caplog.at_level(logging.ERROR, logger="etl_pipeline.enrich"):
        result = enrich(df).collect()

    assert result.height == 1
    assert any(r.levelno == logging.ERROR for r in caplog.records)


@respx.mock
def test_enrich_raises_on_404(monkeypatch, caplog):
    monkeypatch.setattr(settings, "use_mock_api", False)
    respx.post("https://api.example.com/batch").mock(return_value=httpx.Response(404))

    df = pl.DataFrame({"isrc": ["1"]}).lazy()

    with caplog.at_level(logging.ERROR, logger="etl_pipeline.enrich"):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            enrich(df).collect()

    assert exc_info.value.response.status_code == 404
    assert any(r.levelno == logging.ERROR for r in caplog.records)


@respx.mock
def test_enrich_drops_malformed_record_but_keeps_valid_one(monkeypatch, caplog):
    monkeypatch.setattr(settings, "use_mock_api", False)
    respx.post("https://api.example.com/batch").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"isrc": "1", "author": "Jane Doe"},
                {"isrc": "2", "author": 123},  # wrong type, must be rejected
                {"author": "no isrc at all"},  # missing required field
            ],
        )
    )

    df = pl.DataFrame({"isrc": ["1", "2", "3"]}).lazy()

    with caplog.at_level(logging.ERROR, logger="etl_pipeline.enrich"):
        result = enrich(df).collect()

    result = result.sort("isrc")
    assert result.filter(pl.col("isrc") == "1")["author"][0] == "Jane Doe"
    assert result.filter(pl.col("isrc") == "2")["author"][0] is None
    assert len(caplog.records) == 2
    assert all(r.levelno == logging.ERROR for r in caplog.records)


@respx.mock
def test_enrich_handles_non_list_response(monkeypatch, caplog):
    monkeypatch.setattr(settings, "use_mock_api", False)
    respx.post("https://api.example.com/batch").mock(
        return_value=httpx.Response(200, json={"error": "unexpected shape"})
    )

    df = pl.DataFrame({"isrc": ["1"]}).lazy()

    with caplog.at_level(logging.ERROR, logger="etl_pipeline.enrich"):
        result = enrich(df).collect()

    assert result.height == 1
    assert any(r.levelno == logging.ERROR for r in caplog.records)


def test_enrich_against_mock_isrc_client(monkeypatch):
    monkeypatch.setattr(settings, "api_batch_size", 1)
    isrcs = list(MOCK_ISRC_DATA)

    df = pl.DataFrame({"isrc": isrcs}).lazy()

    result = enrich(df).collect()

    result = result.sort("isrc")
    for i, isrc in enumerate(sorted(isrcs)):
        assert result["author"][i] == MOCK_ISRC_DATA[isrc]["author"]
        assert result["title"][i] == MOCK_ISRC_DATA[isrc]["title"]
        assert result["copyright_holder"][i] == MOCK_ISRC_DATA[isrc]["copyright_holder"]

# TODO: update endpoint shape / response fields once real internal API contract is known.
