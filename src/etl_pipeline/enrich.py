import logging
from itertools import islice

import httpx
import polars as pl
from pydantic import ValidationError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from etl_pipeline.config import settings
from etl_pipeline.mock_client import build_mock_client
from etl_pipeline.models import EnrichmentRecord

logger = logging.getLogger(__name__)
KEY_COLUMN = "isrc"

def build_client() -> httpx.Client:
    if settings.use_mock_api:
        return build_mock_client()
    return httpx.Client(
        base_url=settings.api_base_url,
        headers={"Authorization": f"Bearer {settings.api_key}"},
        timeout=settings.api_timeout_seconds,
    )


def _chunk(items: list, size: int):
    it = iter(items)
    while batch := list(islice(it, size)):
        yield batch


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        # 404 means the endpoint/route is misconfigured, not transient - retrying won't help.
        return exc.response.status_code != 404
    return isinstance(exc, httpx.HTTPError)


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, max=5),
    retry=retry_if_exception(_is_retryable),
)
def _fetch_batch(client: httpx.Client, keys: list[str]) -> list[dict]:
    resp = client.post("/batch", json={"keys": keys})
    resp.raise_for_status()
    return resp.json()


def enrich(df: pl.LazyFrame) -> pl.LazyFrame:
    """Call work catalogue API to request additional data, then join it with the provided dataframe

    ISRC column is collected eagerly (needed to call the API and to dedupe requests), the join back is lazy.
    """
    client = build_client()
    try:
        batch_size = settings.api_batch_size
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")

        distinct_keys = df.select(KEY_COLUMN).unique().collect().to_series().to_list()

        records = []
        for batch in _chunk(distinct_keys, batch_size):
            try:
                results = _fetch_batch(client, [str(key) for key in batch])
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    logger.error("enrich batch endpoint not found (404) for %d keys: %s", len(batch), exc)
                    raise
                logger.error("enrich batch lookup failed for %d keys: %s", len(batch), exc)
                results = []
            except httpx.HTTPError as exc:
                logger.error("enrich batch lookup failed for %d keys: %s", len(batch), exc)
                results = []

            if not isinstance(results, list):
                logger.error(
                    "enrich batch response malformed (expected list, got %s): %r",
                    type(results).__name__,
                    results,
                )
                results = []

            for raw_record in results:
                try:
                    validated = EnrichmentRecord.model_validate(raw_record)
                except ValidationError as exc:
                    logger.error("enrich record failed validation: %r (%s)", raw_record, exc)
                    continue
                records.append(validated.model_dump(exclude_none=True))

        if records:
            enrichment_df = pl.LazyFrame(records)
        else:
            enrichment_df = pl.LazyFrame({KEY_COLUMN: []}, schema={KEY_COLUMN: df.collect_schema()[KEY_COLUMN]})
        return df.join(enrichment_df, on=KEY_COLUMN, how="left")
    finally:
        client.close()
