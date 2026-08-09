from bridge.checkpoint import Checkpoint


def test_read_missing_returns_none(tmp_path):
    cp = Checkpoint(tmp_path / "checkpoint.txt")
    assert cp.read() is None


def test_write_then_read_roundtrip(tmp_path):
    cp = Checkpoint(tmp_path / "sub" / "checkpoint.txt")
    cp.write("abc-123")
    assert cp.read() == "abc-123"


def test_write_overwrites_previous_value(tmp_path):
    cp = Checkpoint(tmp_path / "checkpoint.txt")
    cp.write("first")
    cp.write("second")
    assert cp.read() == "second"
    # no leftover temp file
    assert not (tmp_path / "checkpoint.txt.tmp").exists()


def test_read_strips_whitespace(tmp_path):
    path = tmp_path / "checkpoint.txt"
    path.write_text("  abc-123  \n")
    cp = Checkpoint(path)
    assert cp.read() == "abc-123"
