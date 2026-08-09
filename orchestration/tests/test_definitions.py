from dagster import AssetKey

from orchestration.definitions import defs


def test_definitions_load():
    graph = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert keys == {
        "bridge_heartbeat",
        "parquet_raw_events",
        "batch_raw_events",
        "raw_nrt",
        "stg_events_nrt",
        "stg_events_batch",
        "edits_hourly",
        "pageviews_top",
    }


def test_raw_nrt_depends_on_parquet_raw_events():
    graph = defs.resolve_asset_graph()
    deps = {d.to_user_string() for d in graph.get(AssetKey("raw_nrt")).parent_keys}
    assert "parquet_raw_events" in deps


def test_edits_hourly_depends_on_stg_events_nrt_not_batch():
    graph = defs.resolve_asset_graph()
    deps = {d.to_user_string() for d in graph.get(AssetKey("edits_hourly")).parent_keys}
    assert deps == {"stg_events_nrt"}


def test_pageviews_top_is_independent_of_nrt_lane():
    """Principle 5: the two lanes reach marts without being forced to reconcile."""
    graph = defs.resolve_asset_graph()
    upstream: set[str] = set()
    frontier = [AssetKey("pageviews_top")]
    while frontier:
        key = frontier.pop()
        for parent in graph.get(key).parent_keys:
            if parent.to_user_string() not in upstream:
                upstream.add(parent.to_user_string())
                frontier.append(parent)
    assert "raw_nrt" not in upstream
    assert "stg_events_nrt" not in upstream


def test_cleanup_job_registered():
    assert defs.get_job_def("cleanup_parquet_job") is not None


def test_sensors_registered():
    names = {s.name for s in defs.sensors}
    assert names == {"bridge_heartbeat_sensor", "parquet_landing_sensor", "batch_raw_events_sensor"}
