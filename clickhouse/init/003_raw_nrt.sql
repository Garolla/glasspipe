-- Owned here, not by a SQLMesh model -- see transform/README.md for why
-- (a sqlglot ClickHouse-dialect limitation with the `file()` table
-- function). Columns match services/landing/landing/writer.py:NRT_SCHEMA
-- and are loaded incrementally by orchestration/orchestration/raw_loader.py.
CREATE TABLE IF NOT EXISTS raw.raw_nrt
(
    event_id       String,
    event_dt       DateTime64(3),
    wiki           LowCardinality(String),
    type           LowCardinality(String),
    namespace      Nullable(Int64),
    title          Nullable(String),
    user           Nullable(String),
    bot            UInt8,
    server_name    Nullable(String),
    length_old     Nullable(Int64),
    length_new     Nullable(Int64),
    revision_old   Nullable(Int64),
    revision_new   Nullable(Int64),
    raw_json       String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_dt)
ORDER BY (wiki, event_dt, event_id);
