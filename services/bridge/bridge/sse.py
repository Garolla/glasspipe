"""Minimal Server-Sent Events client.

Hand-rolled on purpose: it's a small, well-specified wire format
(https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream)
and the one behaviour glasspipe actually depends on -- resuming from
Last-Event-ID -- is exactly the part a generic library would abstract
away. Keeping it explicit here keeps the bridge's one non-declarative
piece auditable in one file.

Deliberately source-agnostic: nothing here knows about Wikimedia or the
`recentchange` schema. `data` is forwarded as opaque text. That is what
makes the broker a genuinely generic NRT entry point -- a second SSE
source is just a second instance of this module pointed at a different
URL and topic.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable, Iterator

import httpx


@dataclass
class SSEEvent:
    id: str | None
    event: str | None
    data: str


def parse_sse_lines(lines: Iterable[str]) -> Iterator[SSEEvent]:
    """Pure parser: text lines in, SSEEvent out. No I/O, fully unit-testable."""
    data_lines: list[str] = []
    event_type: str | None = None
    event_id: str | None = None

    for raw_line in lines:
        line = raw_line.rstrip("\n").rstrip("\r")

        if line == "":
            if data_lines:
                yield SSEEvent(id=event_id, event=event_type, data="\n".join(data_lines))
            data_lines = []
            event_type = None
            # per spec, event id persists across events until explicitly reset
            continue

        if line.startswith(":"):
            continue  # comment / keep-alive

        if ":" in line:
            field, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
        else:
            field, value = line, ""

        if field == "data":
            data_lines.append(value)
        elif field == "event":
            event_type = value
        elif field == "id":
            if "\x00" not in value:
                event_id = value
        # "retry" field intentionally ignored: reconnection policy is ours, not the server's


def stream_events(
    client: httpx.Client,
    url: str,
    *,
    last_event_id: str | None,
    user_agent: str,
    connect_timeout: float,
    read_timeout: float,
) -> Iterator[SSEEvent]:
    """Open one SSE connection and yield events until it closes or errors."""
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": user_agent,
    }
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id

    timeout = httpx.Timeout(connect=connect_timeout, read=read_timeout, write=10.0, pool=10.0)
    with client.stream("GET", url, headers=headers, timeout=timeout) as response:
        response.raise_for_status()
        yield from parse_sse_lines(response.iter_lines())


def backoff_seconds(attempt: int, *, base: float, cap: float) -> float:
    return min(base * (2**attempt), cap)


def sleep(seconds: float) -> None:  # thin wrapper so tests can monkeypatch it
    time.sleep(seconds)
