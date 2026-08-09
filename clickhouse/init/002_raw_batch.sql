-- Owned here (not by a SQLMesh model) because batch_extract inserts into it
-- directly on its own schedule -- see services/batch_extract/batch_extract/writer.py
-- for the column order this must match, and ARCHITECTURE.md for why this
-- table exists outside SQLMesh's incremental-by-time-range model.
CREATE TABLE IF NOT EXISTS raw.raw_batch
(
    date        Date,
    project     LowCardinality(String),
    access      LowCardinality(String),
    article     String,
    views       UInt64,
    rank        UInt32,
    fetched_at  DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, project, rank);
