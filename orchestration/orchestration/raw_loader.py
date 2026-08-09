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


def list_loaded_files(client) -> set[str]:
    result = client.query(f"SELECT file_path FROM {MANIFEST_TABLE}")
    return {row[0] for row in result.result_rows}


def find_new_files(paths: ParquetPaths, already_loaded: set[str]) -> list[Path]:
    return [f for f in paths.iter_nrt_files() if str(f) not in already_loaded]


def load_file(client, path: Path) -> int:
    table = pq.read_table(path)
    records = table.select(NRT_COLUMNS).to_pylist()
    if records:
        rows = [[record[col] for col in NRT_COLUMNS] for record in records]
        client.insert(RAW_NRT_TABLE, rows, column_names=NRT_COLUMNS)

    client.command(
        f"INSERT INTO {MANIFEST_TABLE} (file_path, loaded_at, row_count) VALUES (%(path)s, now(), %(count)s)",
        parameters={"path": str(path), "count": len(records)},
    )
    return len(records)


def load_new_files(client, paths: ParquetPaths) -> LoadResult:
    already_loaded = list_loaded_files(client)
    new_files = find_new_files(paths, already_loaded)

    rows_loaded = 0
    for path in new_files:
        count = load_file(client, path)
        rows_loaded += count
        logger.info("loaded %d rows from %s", count, path)

    return LoadResult(files_loaded=len(new_files), rows_loaded=rows_loaded)
