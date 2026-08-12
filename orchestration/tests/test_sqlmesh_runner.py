import multiprocessing
import time
from unittest.mock import MagicMock, patch

from orchestration.sqlmesh_runner import run_sqlmesh_plan


def test_run_sqlmesh_plan_runs_plan_then_run_with_select_model(tmp_path):
    project_dir = str(tmp_path)
    plan_result = MagicMock(returncode=0, stdout="applied", stderr="")
    run_result = MagicMock(returncode=0, stdout="no changes", stderr="")
    with patch("orchestration.sqlmesh_runner.subprocess.run", side_effect=[plan_result, run_result]) as mock_run:
        result = run_sqlmesh_plan(project_dir, select_model="marts.edits_hourly")

    assert result.success is True
    assert mock_run.call_count == 2

    plan_cmd = mock_run.call_args_list[0][0][0]
    run_cmd = mock_run.call_args_list[1][0][0]
    assert plan_cmd == [
        "sqlmesh",
        "--paths",
        project_dir,
        "plan",
        "--auto-apply",
        "--no-prompts",
        "--select-model",
        "marts.edits_hourly",
    ]
    assert run_cmd == [
        "sqlmesh",
        "--paths",
        project_dir,
        "run",
        "--select-model",
        "marts.edits_hourly",
    ]
    for call in mock_run.call_args_list:
        assert call[1]["capture_output"] is True

    assert "applied" in result.stdout
    assert "no changes" in result.stdout


def test_run_sqlmesh_plan_without_select_model(tmp_path):
    fake_result = MagicMock(returncode=0, stdout="", stderr="")
    with patch("orchestration.sqlmesh_runner.subprocess.run", return_value=fake_result) as mock_run:
        run_sqlmesh_plan(str(tmp_path))
    for call in mock_run.call_args_list:
        assert "--select-model" not in call[0][0]


def test_run_sqlmesh_plan_reports_plan_failure_without_running_run(tmp_path):
    fake_result = MagicMock(returncode=1, stdout="", stderr="boom")
    with patch("orchestration.sqlmesh_runner.subprocess.run", return_value=fake_result) as mock_run:
        result = run_sqlmesh_plan(str(tmp_path))

    assert result.success is False
    assert result.stderr == "boom"
    mock_run.assert_called_once()  # `run` never gets a chance if `plan` itself fails


def test_run_sqlmesh_plan_reports_run_failure(tmp_path):
    plan_result = MagicMock(returncode=0, stdout="applied", stderr="")
    run_result = MagicMock(returncode=1, stdout="", stderr="missing interval error")
    with patch("orchestration.sqlmesh_runner.subprocess.run", side_effect=[plan_result, run_result]):
        result = run_sqlmesh_plan(str(tmp_path))

    assert result.success is False
    assert "missing interval error" in result.stderr


def _worker(project_dir: str, out_path: str, sleep_seconds: float) -> None:
    """Runs in its own process (like a Dagster step-worker subprocess would)
    with subprocess.run faked out to sleep and record its [start, end]
    window, so the test can check for overlap across real OS processes --
    an in-memory lock wouldn't catch the actual bug, which only shows up
    across process boundaries."""
    import subprocess

    from orchestration.sqlmesh_runner import run_sqlmesh_plan

    def fake_run(cmd, capture_output, text, timeout):
        start = time.monotonic()
        time.sleep(sleep_seconds)
        end = time.monotonic()
        with open(out_path, "a") as f:
            f.write(f"{start} {end}\n")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    subprocess.run = fake_run
    run_sqlmesh_plan(project_dir)


def test_run_sqlmesh_plan_serializes_concurrent_calls_across_processes(tmp_path):
    """Regression test for the NRT/batch lane race: two lanes calling
    run_sqlmesh_plan concurrently (as separate OS processes, matching how
    Dagster's multiprocess executor actually runs independent asset lanes)
    must not overlap, since SQLMesh's local DuckDB state isn't safe for
    concurrent writers."""
    project_dir = str(tmp_path)
    out_path = str(tmp_path / "timings.txt")
    open(out_path, "w").close()

    ctx = multiprocessing.get_context("spawn")
    procs = [
        ctx.Process(target=_worker, args=(project_dir, out_path, 0.3)),
        ctx.Process(target=_worker, args=(project_dir, out_path, 0.3)),
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=10)
        assert p.exitcode == 0

    lines = [line for line in open(out_path).read().strip().splitlines() if line]
    assert len(lines) == 4  # 2 processes x (plan + run) subprocess calls each

    intervals = sorted(tuple(map(float, line.split())) for line in lines)
    for (_, end), (next_start, _) in zip(intervals, intervals[1:]):
        assert next_start >= end  # no two windows overlap
