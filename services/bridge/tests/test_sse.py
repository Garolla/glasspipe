from bridge.sse import backoff_seconds, parse_sse_lines


def lines(text: str):
    return text.split("\n")


def test_single_event_basic():
    raw = "id: 1\nevent: message\ndata: {\"a\": 1}\n\n"
    events = list(parse_sse_lines(lines(raw)))
    assert len(events) == 1
    assert events[0].id == "1"
    assert events[0].event == "message"
    assert events[0].data == '{"a": 1}'


def test_multiline_data_is_joined_with_newline():
    raw = "data: line one\ndata: line two\n\n"
    events = list(parse_sse_lines(lines(raw)))
    assert events[0].data == "line one\nline two"


def test_comment_lines_are_ignored():
    raw = ": keep-alive\ndata: hello\n\n"
    events = list(parse_sse_lines(lines(raw)))
    assert len(events) == 1
    assert events[0].data == "hello"


def test_event_id_persists_until_changed():
    raw = "id: 1\ndata: first\n\ndata: second\n\nid: 2\ndata: third\n\n"
    events = list(parse_sse_lines(lines(raw)))
    assert [e.id for e in events] == ["1", "1", "2"]
    assert [e.data for e in events] == ["first", "second", "third"]


def test_event_with_no_data_is_not_dispatched():
    raw = "id: 1\n\ndata: real\n\n"
    events = list(parse_sse_lines(lines(raw)))
    assert len(events) == 1
    assert events[0].data == "real"


def test_field_with_no_colon_treated_as_empty_value():
    raw = "data\n\n"
    events = list(parse_sse_lines(lines(raw)))
    assert events[0].data == ""


def test_backoff_seconds_grows_and_caps():
    assert backoff_seconds(0, base=2, cap=60) == 2
    assert backoff_seconds(1, base=2, cap=60) == 4
    assert backoff_seconds(2, base=2, cap=60) == 8
    assert backoff_seconds(10, base=2, cap=60) == 60
