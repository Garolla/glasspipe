"""Incremental, idempotent Parquet -> raw.raw_nrt load.

Exists as plain Python rather than a SQLMesh model because of a sqlglot
ClickHouse-dialect limitation with the `file()` table function -- see
transform/README.md for the full explanation. The manifest table
(raw._loaded_files) is what makes this safe to re-run: a file is only
ever inserted once, tracked by its path, so retrying after a crash never
double-counts.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import clickhouse_connect
import pyarrow.parquet as pq

from glasspipe_common.paths import ParquetPaths

from orchestration.config import OrchestrationConfig

logger = logging.getLogger(__name__)

MANIFEST_TABLE = "_loaded_files"
RAW_NRT_TABLE = "raw_nrt"

NRT_COLUMNS = [
    "event_id",
    "event_dt",
    "wiki",
    "type",
    "namespace",
    "title",
    "user",
    "bot",
    "server_name",
    "length_old",
    "length_new",
    "revision_old",
    "revision_new",
    "raw_json",
]


@dataclass
class LoadResult:
    files_loaded: int
    rows_loaded: int


def get_clickhouse_client(config: OrchestrationConfig):
    return clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database="raw",
    )


def list_loaded_files(client, *, since: date | None = None) -> set[str]:
    if since is None:
        result = client.query(f"SELECT file_path FROM {MANIFEST_TABLE}")
    else:
        result = client.query(
            f"SELECT file_path FROM {MANIFEST_TABLE} WHERE loaded_at >= %(since)s",
            parameters={"since": since},
        )
    return {row[0] for row in result.result_rows}


def find_new_files(paths: ParquetPaths, already_loaded: set[str], *, since: date | None = None) -> list[Path]:
    return [f for f in paths.iter_nrt_files(since=since) if str(f) not in already_loaded]


def load_batch(client, batch: list[Path]) -> int:
    """Load a batch of files as one INSERT, not one per file.

    One INSERT per file was the original shape, and it's what caused
    ClickHouse's own memory to climb until it hit MEMORY_LIMIT_EXCEEDED
    partway through a ~14,500-file backlog: each tiny INSERT creates a
    new part, and the accumulated per-part overhead (index, marks) across
    thousands of parts in one session adds up. Manifest rows are still
    recorded one command per file (unchanged) -- they're cheap; it's the
    raw_nrt data insert that needs batching.
    """
    all_rows: list[list] = []
    file_row_counts: list[int] = []
    for path in batch:
        table = pq.read_table(path)
        records = table.select(NRT_COLUMNS).to_pylist()
        file_row_counts.append(len(records))
        all_rows.extend([record[col] for col in NRT_COLUMNS] for record in records)

    if all_rows:
        client.insert(RAW_NRT_TABLE, all_rows, column_names=NRT_COLUMNS)

    for path, count in zip(batch, file_row_counts):
        client.command(
            f"INSERT INTO {MANIFEST_TABLE} (file_path, loaded_at, row_count) VALUES (%(path)s, now(), %(count)s)",
            parameters={"path": str(path), "count": count},
        )

    return sum(file_row_counts)


def load_new_files(client, paths: ParquetPaths, *, batch_size: int = 40, since: date | None = None) -> LoadResult:
    # 200 was the original default and it OOM-killed raw_nrt's step
    # subprocess by itself on the VPS (confirmed via the kernel's cgroup
    # OOM log: single process anon-rss 522MB) even with the step running
    # alone, no concurrent lanes. load_batch's to_pylist() materializes
    # every column -- including raw_json, a full event payload per row --
    # as Python objects for the whole batch before the INSERT, so at ~500
    # rows/file, 200 files held that many events in memory at once. 40
    # keeps the INSERT big enough to avoid the one-part-per-file problem
    # load_batch's docstring describes, at roughly a fifth of the peak.
    # Files land continuously across hundreds of wikis (see landing's
    # buffered writer -- one file per wiki+date per flush), so full-history
    # scans of both the on-disk listing and the _loaded_files manifest grow
    # without bound once the backlog gets big, to the point of OOM-killing
    # this step (dagster.code_server / ChildProcessCrashException, seen
    # with a 180k+ file backlog). Callers should pass a trailing-window
    # `since` (assets.py does) to keep the working set proportional to a
    # day or two of traffic instead of the whole history.
    already_loaded = list_loaded_files(client, since=since)
    new_files = find_new_files(paths, already_loaded, since=since)

    rows_loaded = 0
    for i in range(0, len(new_files), batch_size):
        batch = new_files[i : i + batch_size]
        count = load_batch(client, batch)
        rows_loaded += count
        logger.info("loaded %d rows from %d file(s)", count, len(batch))

    return LoadResult(files_loaded=len(new_files), rows_loaded=rows_loaded)
