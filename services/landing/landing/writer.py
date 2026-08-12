"""Append-only Parquet writer for the raw NRT buffer, plus a dead-letter writer.

Both write whole files, never append to an existing one -- ClickHouse reads
the partition directories with a glob, so "new data" just means "a new
file appears". Buffering in memory and flushing periodically (by count or
by time, whichever comes first) is what keeps file counts sane without
needing a streaming Parquet writer.

Buffered by date only, not by (wiki, date): `wiki` is already a real
column in NRT_SCHEMA, so partitioning the directory layout by it too was
pure duplication -- and an expensive one, since Wikimedia's recentchange
stream spans hundreds of concurrently active wikis, so every flush wrote
one file per wiki that had activity in that window. One file per flush
(occasionally two, near midnight UTC) instead of one per wiki per flush.
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from glasspipe_common.paths import ParquetPaths

NRT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_dt", pa.timestamp("us", tz="UTC")),
        ("wiki", pa.string()),
        ("type", pa.string()),
        ("namespace", pa.int64()),
        ("title", pa.string()),
        ("user", pa.string()),
        ("bot", pa.bool_()),
        ("server_name", pa.string()),
        ("length_old", pa.int64()),
        ("length_new", pa.int64()),
        ("revision_old", pa.int64()),
        ("revision_new", pa.int64()),
        ("raw_json", pa.string()),
    ]
)


class BufferedParquetWriter:
    def __init__(self, paths: ParquetPaths):
        self._paths = paths
        self._buffers: dict[str, list[dict]] = defaultdict(list)

    def add(self, record: dict) -> None:
        event_date = record["event_dt"].date().isoformat()
        self._buffers[event_date].append(record)

    @property
    def buffered_count(self) -> int:
        return sum(len(v) for v in self._buffers.values())

    def flush_all(self) -> list[Path]:
        written: list[Path] = []
        for event_date, records in self._buffers.items():
            written.append(self._flush_partition(event_date, records))
        self._buffers.clear()
        return written

    def _flush_partition(self, event_date: str, records: list[dict]) -> Path:
        year, month, day = (int(p) for p in event_date.split("-"))
        partition_dir = self._paths.nrt_partition_dir(datetime(year, month, day).date())
        partition_dir.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pylist(records, schema=NRT_SCHEMA)
        file_path = partition_dir / f"part-{uuid.uuid4().hex}.parquet"
        pq.write_table(table, file_path)
        return file_path


class DeadLetterWriter:
    """Newline-delimited JSON: the raw line is not guaranteed to have a
    uniform shape (that's why it failed validation), so Parquet's typed
    columns would just fight the data. Human-inspectable is the goal here,
    not queryability.
    """

    def __init__(self, paths: ParquetPaths):
        self._paths = paths

    def write(self, raw_line: str, error: str) -> Path:
        today = datetime.now(timezone.utc).date()
        partition_dir = self._paths.dead_letter_partition_dir(today)
        partition_dir.mkdir(parents=True, exist_ok=True)
        file_path = partition_dir / f"part-{uuid.uuid4().hex}.jsonl"
        record = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "error": error,
            "raw_line": raw_line,
        }
        file_path.write_text(json.dumps(record) + "\n")
        return file_path
