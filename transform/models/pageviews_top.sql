-- Marts: batch lane's own output. Independent of edits_hourly on purpose --
-- two lanes reaching the marts layer without being forced to reconcile.
MODEL (
  name marts.pageviews_top,
  kind VIEW,
  grain (date, project, article),
);

SELECT
  date,
  project,
  access,
  article,
  views,
  rank
FROM staging.stg_events_batch
WHERE rank <= 100
