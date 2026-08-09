from unittest.mock import MagicMock, patch

from orchestration.sqlmesh_runner import run_sqlmesh_plan


def test_run_sqlmesh_plan_builds_expected_command():
    fake_result = MagicMock(returncode=0, stdout="applied", stderr="")
    with patch("orchestration.sqlmesh_runner.subprocess.run", return_value=fake_result) as mock_run:
        result = run_sqlmesh_plan("/opt/transform", select_model="marts.edits_hourly")

    assert result.success is True
    args, kwargs = mock_run.call_args
    cmd = args[0]
    assert cmd == [
        "sqlmesh",
        "--paths",
        "/opt/transform",
        "plan",
        "--auto-apply",
        "--no-prompts",
        "--select-model",
        "marts.edits_hourly",
    ]
    assert kwargs["capture_output"] is True


def test_run_sqlmesh_plan_without_select_model():
    fake_result = MagicMock(returncode=0, stdout="", stderr="")
    with patch("orchestration.sqlmesh_runner.subprocess.run", return_value=fake_result) as mock_run:
        run_sqlmesh_plan("/opt/transform")
    cmd = mock_run.call_args[0][0]
    assert "--select-model" not in cmd


def test_run_sqlmesh_plan_reports_failure():
    fake_result = MagicMock(returncode=1, stdout="", stderr="boom")
    with patch("orchestration.sqlmesh_runner.subprocess.run", return_value=fake_result):
        result = run_sqlmesh_plan("/opt/transform")
    assert result.success is False
    assert result.stderr == "boom"
