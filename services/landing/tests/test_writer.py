import json
from datetime import datetime, timezone

import pyarrow.parquet as pq

from glasspipe_common.paths import ParquetPaths
from landing.writer import BufferedParquetWriter, DeadLetterWriter, NRT_SCHEMA


def make_record(event_id="evt-1", wiki="enwiki", dt=None):
    dt = dt or datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
    return {
        "event_id": event_id,
        "event_dt": dt,
        "wiki": wiki,
        "type": "edit",
        "namespace": 0,
        "title": "Some Article",
        "user": "editor",
        "bot": False,
        "server_name": "en.wikipedia.org",
        "length_old": 100,
        "length_new": 110,
        "revision_old": 1,
        "revision_new": 2,
        "raw_json": "{}",
    }


def test_add_buffers_without_writing(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    writer = BufferedParquetWriter(paths)
    writer.add(make_record())
    assert writer.buffered_count == 1
    assert list(paths.iter_nrt_files()) == []


def test_flush_all_writes_partitioned_parquet(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    writer = BufferedParquetWriter(paths)
    writer.add(make_record(event_id="a", wiki="enwiki"))
    writer.add(make_record(event_id="b", wiki="enwiki"))
    writer.add(make_record(event_id="c", wiki="itwiki"))

    written = writer.flush_all()

    assert len(written) == 2  # one file per (wiki, date) partition
    assert writer.buffered_count == 0

    all_ids = set()
    for path in written:
        table = pq.read_table(path)
        assert table.schema.equals(NRT_SCHEMA)
        all_ids.update(table.column("event_id").to_pylist())
    assert all_ids == {"a", "b", "c"}


def test_flush_partitions_by_wiki_and_date(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    writer = BufferedParquetWriter(paths)
    writer.add(make_record(wiki="enwiki", dt=datetime(2026, 8, 9, tzinfo=timezone.utc)))
    writer.add(make_record(wiki="enwiki", dt=datetime(2026, 8, 10, tzinfo=timezone.utc)))

    written = writer.flush_all()

    dirs = {p.parent for p in written}
    assert paths.nrt_root / "wiki=enwiki" / "dt=2026-08-09" in dirs
    assert paths.nrt_root / "wiki=enwiki" / "dt=2026-08-10" in dirs


def test_dead_letter_writes_jsonl_with_error(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    dead_letter = DeadLetterWriter(paths)
    path = dead_letter.write("{bad json", "invalid JSON")

    record = json.loads(path.read_text())
    assert record["raw_line"] == "{bad json"
    assert record["error"] == "invalid JSON"
    assert "received_at" in record
