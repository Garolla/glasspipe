from bridge.main import resolve_resume_id


def test_no_checkpoint_resumes_from_none():
    assert resolve_resume_id(None, None, max_age_seconds=900) is None


def test_fresh_checkpoint_resumes_from_stored_id():
    assert resolve_resume_id("abc-123", 5.0, max_age_seconds=900) == "abc-123"


def test_stale_checkpoint_is_discarded():
    assert resolve_resume_id("abc-123", 901.0, max_age_seconds=900) is None


def test_checkpoint_exactly_at_threshold_is_kept():
    assert resolve_resume_id("abc-123", 900.0, max_age_seconds=900) == "abc-123"


def test_missing_age_with_stored_id_resumes():
    # age_seconds() returning None only happens when the file doesn't exist,
    # which can't coincide with a non-None stored id -- but the function
    # should still fail safe (resume) rather than discard on missing age info.
    assert resolve_resume_id("abc-123", None, max_age_seconds=900) == "abc-123"
