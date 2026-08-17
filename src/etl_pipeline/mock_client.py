import json

import httpx

from etl_pipeline.config import settings

MOCK_ISRC_DATA: dict[str, dict] = {
    "USRC17607839": {
        "author": "Jane Doe",
        "title": "Sample Song",
        "copyright_holder": "Example Records",
    },
    "GBUM71029601": {
        "author": "John Smith",
        "title": "Another Track",
        "copyright_holder": "Sample Music Ltd",
    },
    "DEE250800245": {
        "author": "Max Mustermann",
        "title": "Test Composition",
        "copyright_holder": "GEMA Test Publishing",
    },
    "USMET7600001": {
        "author": "Black Sabbath",
        "title": "Paranoid",
        "copyright_holder": "Vertigo Records",
    },
    "GBMET7600002": {
        "author": "Led Zeppelin",
        "title": "Stairway to Heaven",
        "copyright_holder": "Atlantic Records",
    },
    "GBMET7600003": {
        "author": "Deep Purple",
        "title": "Smoke on the Water",
        "copyright_holder": "Purple Records",
    },
    "USMET8600004": {
        "author": "Metallica",
        "title": "Master of Puppets",
        "copyright_holder": "Elektra Records",
    },
    "GBMET8000005": {
        "author": "Iron Maiden",
        "title": "The Trooper",
        "copyright_holder": "EMI Records",
    },
    "AUMET8000006": {
        "author": "AC/DC",
        "title": "Back in Black",
        "copyright_holder": "Albert Productions",
    },
    "GBMET8000007": {
        "author": "Judas Priest",
        "title": "Breaking the Law",
        "copyright_holder": "CBS Records",
    },
    "USMET8700008": {
        "author": "Guns N' Roses",
        "title": "Sweet Child O' Mine",
        "copyright_holder": "Geffen Records",
    },
    "GBMET7500009": {
        "author": "Queen",
        "title": "Bohemian Rhapsody",
        "copyright_holder": "EMI Records",
    },
    "GBMET8000010": {
        "author": "Motörhead",
        "title": "Ace of Spades",
        "copyright_holder": "Bronze Records",
    },
}


def _handler(request: httpx.Request) -> httpx.Response:
    if request.url.path != "/batch":
        return httpx.Response(404, json={"error": "unknown endpoint"})

    keys = json.loads(request.content).get("keys", [])
    results = [{"isrc": key, **MOCK_ISRC_DATA[key]} for key in keys if key in MOCK_ISRC_DATA]
    return httpx.Response(200, json=results)


def build_mock_client() -> httpx.Client:
    """In-memory ISRC lookup, used until the real internal API is reachable."""
    return httpx.Client(transport=httpx.MockTransport(_handler), base_url=settings.api_base_url)
