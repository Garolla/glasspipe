-- Staging: dedup layer over raw.raw_nrt. At-least-once delivery in the
-- landing service (offsets are committed only after a durable Parquet
-- flush) means the same event can legitimately be loaded twice; this view
-- is where that gets resolved, once, instead of every downstream model
-- having to know about it.
MODEL (
  name staging.stg_events_nrt,
  kind VIEW,
  grain (event_id),
);

SELECT
  event_id,
  event_dt,
  wiki,
  type,
  namespace,
  title,
  user,
  bot,
  server_name,
  length_new,
  revision_new
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_dt DESC) AS rn
  FROM raw.raw_nrt
)
WHERE rn = 1
