from datetime import date, datetime, timedelta
from pathlib import Path

from glasspipe_common.paths import ParquetPaths


def test_nrt_partition_dir_layout(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    d = paths.nrt_partition_dir("enwiki", date(2026, 8, 9))
    assert d == tmp_path / "nrt" / "wiki=enwiki" / "dt=2026-08-09"


def test_nrt_partition_dir_sanitizes_wiki_name(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    d = paths.nrt_partition_dir("some/weird wiki", date(2026, 8, 9))
    assert "/" not in d.name and d.parent.name.startswith("wiki=")


def test_iter_nrt_files_finds_nested_parquet(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    part_dir = paths.nrt_partition_dir("enwiki", date(2026, 8, 9))
    part_dir.mkdir(parents=True)
    f = part_dir / "part-0001.parquet"
    f.write_bytes(b"not a real parquet file, just testing discovery")

    found = list(paths.iter_nrt_files())
    assert found == [f]


def test_iter_nrt_files_empty_when_root_missing(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path / "does-not-exist")
    assert list(paths.iter_nrt_files()) == []


def test_iter_nrt_files_since_excludes_older_partitions(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    old_dir = paths.nrt_partition_dir("enwiki", date(2026, 8, 1))
    old_dir.mkdir(parents=True)
    old_file = old_dir / "part-0001.parquet"
    old_file.write_bytes(b"old")

    new_dir = paths.nrt_partition_dir("enwiki", date(2026, 8, 9))
    new_dir.mkdir(parents=True)
    new_file = new_dir / "part-0001.parquet"
    new_file.write_bytes(b"new")

    found = set(paths.iter_nrt_files(since=date(2026, 8, 8)))
    assert found == {new_file}


def test_iter_nrt_files_since_ignores_malformed_partition_names(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    bad_dir = paths.nrt_root / "wiki=enwiki" / "dt=not-a-date"
    bad_dir.mkdir(parents=True)
    (bad_dir / "part-0001.parquet").write_bytes(b"x")

    assert list(paths.iter_nrt_files(since=date(2026, 1, 1))) == []


def test_file_age_seconds(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    f = tmp_path / "x.parquet"
    f.write_bytes(b"x")
    now = datetime.fromtimestamp(f.stat().st_mtime) + timedelta(hours=2)
    age = paths.file_age_seconds(f, now=now)
    assert 7195 <= age <= 7205
