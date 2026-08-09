from landing.consumer import OffsetTracker


def test_pending_offsets_empty_initially():
    tracker = OffsetTracker()
    assert tracker.pending_offsets() == []


def test_record_tracks_max_offset_per_partition():
    tracker = OffsetTracker()
    tracker.record("t", 0, 5)
    tracker.record("t", 0, 3)  # out of order / duplicate poll, should not regress
    tracker.record("t", 1, 10)

    offsets = {(tp.topic, tp.partition): tp.offset for tp in tracker.pending_offsets()}
    assert offsets[("t", 0)] == 6  # committed offset is next-to-read, i.e. max seen + 1
    assert offsets[("t", 1)] == 11


def test_clear_resets_tracker():
    tracker = OffsetTracker()
    tracker.record("t", 0, 5)
    tracker.clear()
    assert tracker.pending_offsets() == []
