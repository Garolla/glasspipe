-- Marts: hourly edit aggregates, NRT lane only. Deliberately not joined
-- or reconciled with the batch lane's pageviews_top -- see
-- ARCHITECTURE.md principle 5.
MODEL (
  name marts.edits_hourly,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column hour_ts
  ),
  cron '@hourly',
  grain (wiki, hour_ts),
  physical_properties (
    order_by = (wiki, hour_ts)
  ),
  audits (not_null_wiki),
);

SELECT
  wiki,
  toStartOfHour(event_dt) AS hour_ts,
  count() AS edit_count,
  countIf(bot) AS bot_edit_count,
  uniqExact(user) AS distinct_editors
FROM staging.stg_events_nrt
WHERE event_dt BETWEEN @start_ts AND @end_ts
GROUP BY wiki, hour_ts
