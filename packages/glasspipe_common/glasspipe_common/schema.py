"""Versioned contract for the Wikimedia EventStreams `recentchange` stream.

This is the schema boundary between the outside world (Wikimedia) and
glasspipe: the bridge does not validate (it only checkpoints and forwards),
the landing service validates every record against `RecentChangeEvent`
before it is allowed to become a Parquet row. Anything that fails
validation is quarantined instead of silently dropped or silently
corrupting the raw layer.

Changing a field here is a schema change to the raw layer and belongs in
the same PR as the SQLMesh model that reads it.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class EventValidationError(Exception):
    """Raised when a raw stream line does not satisfy the RecentChangeEvent contract."""

    def __init__(self, raw_line: str, cause: ValidationError):
        self.raw_line = raw_line
        self.cause = cause
        super().__init__(str(cause))


class Meta(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    dt: datetime
    domain: str | None = None
    stream: str | None = None


class Length(BaseModel):
    model_config = ConfigDict(extra="ignore")

    old: int | None = None
    new: int | None = None


class Revision(BaseModel):
    model_config = ConfigDict(extra="ignore")

    old: int | None = None
    new: int | None = None


class RecentChangeEvent(BaseModel):
    """A single `recentchange` event, trimmed to the fields glasspipe cares about.

    Unknown fields are ignored rather than rejected: Wikimedia can add
    fields without breaking ingestion. Fields listed here are the ones the
    pipeline has taken a dependency on, so removing/renaming one is a
    breaking change to the contract.
    """

    model_config = ConfigDict(extra="ignore")

    meta: Meta
    id: int | None = None
    type: str
    namespace: int | None = None
    title: str | None = None
    comment: str | None = None
    timestamp: int | None = None
    user: str | None = None
    bot: bool = False
    wiki: str
    server_name: str | None = None
    length: Length | None = None
    revision: Revision | None = None


def parse_event(raw_line: str) -> RecentChangeEvent:
    """Parse and validate a single SSE `data:` payload.

    Raises EventValidationError (carrying the original line) on failure so
    the caller can route the offending record to the dead-letter area
    without losing it.
    """
    try:
        return RecentChangeEvent.model_validate_json(raw_line)
    except ValidationError as exc:
        raise EventValidationError(raw_line, exc) from exc


def to_flat_record(event: RecentChangeEvent, raw_line: str) -> dict:
    """Flatten a validated event into the row shape written to Parquet.

    `raw_json` keeps the original payload alongside the extracted columns
    so the raw layer stays reconstructable even if we later decide we
    wanted a field we didn't extract.
    """
    dt = event.meta.dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return {
        "event_id": event.meta.id,
        "event_dt": dt,
        "wiki": event.wiki,
        "type": event.type,
        "namespace": event.namespace,
        "title": event.title,
        "user": event.user,
        "bot": event.bot,
        "server_name": event.server_name,
        "length_old": event.length.old if event.length else None,
        "length_new": event.length.new if event.length else None,
        "revision_old": event.revision.old if event.revision else None,
        "revision_new": event.revision.new if event.revision else None,
        "raw_json": raw_line,
    }


__all__ = ["RecentChangeEvent", "Meta", "Length", "Revision", "EventValidationError", "parse_event", "to_flat_record"]
