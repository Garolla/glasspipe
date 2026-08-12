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


def test_flush_all_writes_one_file_per_date_regardless_of_wiki(tmp_path):
    """Regression test: this used to be one file per (wiki, date), which
    meant one flush cycle wrote one file per active wiki -- hundreds of
    files every ~30s given how many wikis Wikimedia's recentchange stream
    covers. wiki is already a real column in NRT_SCHEMA, so it doesn't
    need to be a partition too; records from different wikis on the same
    date must land in the same file."""
    paths = ParquetPaths(base_dir=tmp_path)
    writer = BufferedParquetWriter(paths)
    writer.add(make_record(event_id="a", wiki="enwiki"))
    writer.add(make_record(event_id="b", wiki="enwiki"))
    writer.add(make_record(event_id="c", wiki="itwiki"))

    written = writer.flush_all()

    assert len(written) == 1  # one file per date, mixing wikis
    assert writer.buffered_count == 0

    table = pq.read_table(written[0])
    assert table.schema.equals(NRT_SCHEMA)
    assert set(table.column("event_id").to_pylist()) == {"a", "b", "c"}
    assert set(table.column("wiki").to_pylist()) == {"enwiki", "itwiki"}


def test_flush_partitions_by_date(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    writer = BufferedParquetWriter(paths)
    writer.add(make_record(wiki="enwiki", dt=datetime(2026, 8, 9, tzinfo=timezone.utc)))
    writer.add(make_record(wiki="itwiki", dt=datetime(2026, 8, 10, tzinfo=timezone.utc)))

    written = writer.flush_all()

    dirs = {p.parent for p in written}
    assert paths.nrt_root / "dt=2026-08-09" in dirs
    assert paths.nrt_root / "dt=2026-08-10" in dirs


def test_dead_letter_writes_jsonl_with_error(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    dead_letter = DeadLetterWriter(paths)
    path = dead_letter.write("{bad json", "invalid JSON")

    record = json.loads(path.read_text())
    assert record["raw_line"] == "{bad json"
    assert record["error"] == "invalid JSON"
    assert "received_at" in record
