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
        self.commands = []

    def query(self, sql):
        assert "_loaded_files" in sql
        return FakeQueryResult([[f] for f in self._loaded])

    def insert(self, table, data, column_names=None):
        assert table == "raw_nrt"
        assert column_names == NRT_COLUMNS
        self.inserted_rows.extend(data)

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
