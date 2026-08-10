from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from glasspipe_common.paths import ParquetPaths
from orchestration.raw_loader import NRT_COLUMNS, load_new_files


class FakeQueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    def __init__(self, already_loaded=()):
        self._loaded = list(already_loaded)
        self.inserted_rows = []
        self.insert_call_count = 0
        self.commands = []

    def query(self, sql):
        assert "_loaded_files" in sql
        return FakeQueryResult([[f] for f in self._loaded])

    def insert(self, table, data, column_names=None):
        assert table == "raw_nrt"
        assert column_names == NRT_COLUMNS
        self.inserted_rows.extend(data)
        self.insert_call_count += 1

    def command(self, cmd, parameters=None):
        self.commands.append((cmd, parameters))
        self._loaded.append(parameters["path"])


def write_sample_parquet(path, event_ids):
    rows = []
    for event_id in event_ids:
        rows.append(
            {col: None for col in NRT_COLUMNS}
            | {
                "event_id": event_id,
                "event_dt": datetime(2026, 8, 9, tzinfo=timezone.utc),
                "wiki": "enwiki",
                "type": "edit",
                "bot": False,
                "raw_json": "{}",
            }
        )
    table = pa.Table.from_pylist(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def test_load_new_files_loads_unseen_files_and_records_manifest(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    file_a = paths.nrt_partition_dir("enwiki", datetime(2026, 8, 9).date()) / "part-a.parquet"
    write_sample_parquet(file_a, ["evt-1", "evt-2"])

    client = FakeClient()
    result = load_new_files(client, paths)

    assert result.files_loaded == 1
    assert result.rows_loaded == 2
    assert len(client.inserted_rows) == 2
    assert client.commands[0][1]["path"] == str(file_a)


def test_load_new_files_skips_already_loaded_files(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    file_a = paths.nrt_partition_dir("enwiki", datetime(2026, 8, 9).date()) / "part-a.parquet"
    write_sample_parquet(file_a, ["evt-1"])

    client = FakeClient(already_loaded=[str(file_a)])
    result = load_new_files(client, paths)

    assert result.files_loaded == 0
    assert result.rows_loaded == 0
    assert client.inserted_rows == []


def test_load_new_files_handles_no_files(tmp_path):
    paths = ParquetPaths(base_dir=tmp_path)
    client = FakeClient()
    result = load_new_files(client, paths)
    assert result.files_loaded == 0
    assert result.rows_loaded == 0


def test_load_new_files_batches_inserts_across_files(tmp_path):
    """Regression test: one INSERT per file, at backlog scale, was what
    made ClickHouse's own memory climb until it hit MEMORY_LIMIT_EXCEEDED.
    Multiple files must land in one INSERT per batch, not one each."""
    paths = ParquetPaths(base_dir=tmp_path)
    files = []
    for i in range(5):
        f = paths.nrt_partition_dir("enwiki", datetime(2026, 8, 9).date()) / f"part-{i}.parquet"
        write_sample_parquet(f, [f"evt-{i}-a", f"evt-{i}-b"])
        files.append(f)

    client = FakeClient()
    result = load_new_files(client, paths, batch_size=2)

    assert result.files_loaded == 5
    assert result.rows_loaded == 10
    assert client.insert_call_count == 3  # ceil(5 / 2)
    assert len(client.commands) == 5  # still one manifest entry per file
    assert {c[1]["path"] for c in client.commands} == {str(f) for f in files}
