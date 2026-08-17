from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

NonBlankStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
OptionalNonBlankStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)] | None


class PlaybackLog(BaseModel):
    """Initial data object
    """

    model_config = ConfigDict(strict=True)

    timestamp: datetime
    isrc: NonBlankStr
    station_id: NonBlankStr
    duration_seconds: int = Field(..., gt=0)
    listener_count: int = Field(..., gt=0)


class EnrichmentRecord(BaseModel):
    """Return object from Work Catalogue API lookup
    """

    model_config = ConfigDict(strict=True)

    isrc: NonBlankStr
    author: OptionalNonBlankStr = None
    title: OptionalNonBlankStr = None
    copyright_holder: OptionalNonBlankStr = None
