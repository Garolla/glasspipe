import json

import pytest

from glasspipe_common.schema import EventValidationError, parse_event, to_flat_record

VALID_EVENT = {
    "meta": {
        "id": "1e1a1a2c-0000-4a00-9a00-abcdef123456",
        "dt": "2026-08-09T12:00:00Z",
        "domain": "en.wikipedia.org",
        "stream": "mediawiki.recentchange",
    },
    "id": 123456,
    "type": "edit",
    "namespace": 0,
    "title": "Data engineering",
    "comment": "typo fix",
    "timestamp": 1754740800,
    "user": "SomeEditor",
    "bot": False,
    "wiki": "enwiki",
    "server_name": "en.wikipedia.org",
    "length": {"old": 1000, "new": 1010},
    "revision": {"old": 111, "new": 112},
}


def test_parse_event_valid():
    event = parse_event(json.dumps(VALID_EVENT))
    assert event.meta.id == VALID_EVENT["meta"]["id"]
    assert event.wiki == "enwiki"
    assert event.bot is False


def test_parse_event_ignores_unknown_fields():
    payload = dict(VALID_EVENT)
    payload["some_future_wikimedia_field"] = {"anything": True}
    event = parse_event(json.dumps(payload))
    assert event.wiki == "enwiki"


@pytest.mark.parametrize(
    "missing_field",
    ["meta", "type", "wiki"],
)
def test_parse_event_rejects_missing_required_field(missing_field):
    payload = dict(VALID_EVENT)
    del payload[missing_field]
    raw = json.dumps(payload)
    with pytest.raises(EventValidationError) as exc_info:
        parse_event(raw)
    assert exc_info.value.raw_line == raw


def test_parse_event_rejects_malformed_json():
    with pytest.raises(EventValidationError):
        parse_event("{not json")


def test_to_flat_record_shape():
    raw = json.dumps(VALID_EVENT)
    event = parse_event(raw)
    record = to_flat_record(event, raw)

    assert record["event_id"] == VALID_EVENT["meta"]["id"]
    assert record["wiki"] == "enwiki"
    assert record["type"] == "edit"
    assert record["length_old"] == 1000
    assert record["length_new"] == 1010
    assert record["raw_json"] == raw
    assert record["event_dt"].tzinfo is not None
