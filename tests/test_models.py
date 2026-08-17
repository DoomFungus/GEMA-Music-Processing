import pytest
from pydantic import ValidationError

from etl_pipeline.models import EnrichmentRecord


def test_valid_record_passes():
    record = EnrichmentRecord.model_validate(
        {"isrc": "1", "author": "Jane Doe", "title": "Sample Song", "copyright_holder": "Example Records"}
    )
    assert record.isrc == "1"
    assert record.author == "Jane Doe"


def test_missing_isrc_fails():
    with pytest.raises(ValidationError):
        EnrichmentRecord.model_validate({"author": "Jane Doe"})


def test_blank_isrc_fails():
    with pytest.raises(ValidationError):
        EnrichmentRecord.model_validate({"isrc": "   "})


def test_wrong_typed_field_fails_under_strict_mode():
    with pytest.raises(ValidationError):
        EnrichmentRecord.model_validate({"isrc": "1", "author": 123})
