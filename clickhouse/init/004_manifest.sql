-- Tracks which Parquet files have already been loaded into raw.raw_nrt,
-- so orchestration/orchestration/raw_loader.py's incremental load is
-- idempotent (re-running never double-inserts a file) and the Dagster
-- cleanup job knows which files on disk are safe to delete.
CREATE TABLE IF NOT EXISTS raw._loaded_files
(
    file_path  String,
    loaded_at  DateTime,
    row_count  UInt64
)
ENGINE = MergeTree
ORDER BY file_path;
