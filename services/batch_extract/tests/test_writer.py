from datetime import date
from unittest.mock import MagicMock

from batch_extract.writer import COLUMN_ORDER, insert_rows


def test_insert_rows_empty_list_is_noop():
    client = MagicMock()
    assert insert_rows(client, []) == 0
    client.insert.assert_not_called()


def test_insert_rows_passes_correct_column_order():
    client = MagicMock()
    rows = [
        {"date": date(2026, 8, 1), "project": "en.wikipedia", "access": "all-access", "article": "Main_Page", "views": 5, "rank": 1},
    ]
    count = insert_rows(client, rows)

    assert count == 1
    client.insert.assert_called_once()
    args, kwargs = client.insert.call_args
    assert args[0] == "raw_batch"
    assert kwargs["column_names"] == COLUMN_ORDER
    inserted_row = args[1][0]
    assert inserted_row[COLUMN_ORDER.index("article")] == "Main_Page"
    assert inserted_row[COLUMN_ORDER.index("fetched_at")] is not None
