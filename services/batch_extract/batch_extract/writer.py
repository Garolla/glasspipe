from __future__ import annotations

from datetime import datetime, timezone

import clickhouse_connect

from batch_extract.config import BatchExtractConfig


def get_clickhouse_client(config: BatchExtractConfig):
    return clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_database,
    )


COLUMN_ORDER = ["date", "project", "access", "article", "views", "rank", "fetched_at"]


def insert_rows(client, rows: list[dict]) -> int:
    """Insert extracted rows into raw_batch. Table DDL lives in
    clickhouse/init/ (versioned, but not SQLMesh-owned -- this service
    writes directly, the way the architecture diagram draws it).
    """
    if not rows:
        return 0

    fetched_at = datetime.now(timezone.utc)
    data = [[row[col] if col != "fetched_at" else fetched_at for col in COLUMN_ORDER] for row in rows]
    client.insert("raw_batch", data, column_names=COLUMN_ORDER)
    return len(data)
