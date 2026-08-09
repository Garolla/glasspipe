-- Staging pass-through over raw.raw_batch. Kept as its own model (rather
-- than reading raw.raw_batch directly from marts) so the batch lane has
-- the same two-layer shape as the NRT lane, even though there's no
-- dedup work to do here.
MODEL (
  name staging.stg_events_batch,
  kind VIEW,
  grain (date, project, article),
);

SELECT
  date,
  project,
  access,
  article,
  views,
  rank,
  fetched_at
FROM raw.raw_batch
