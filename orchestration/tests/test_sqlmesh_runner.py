from unittest.mock import MagicMock, patch

from orchestration.sqlmesh_runner import run_sqlmesh_plan


def test_run_sqlmesh_plan_runs_plan_then_run_with_select_model():
    plan_result = MagicMock(returncode=0, stdout="applied", stderr="")
    run_result = MagicMock(returncode=0, stdout="no changes", stderr="")
    with patch("orchestration.sqlmesh_runner.subprocess.run", side_effect=[plan_result, run_result]) as mock_run:
        result = run_sqlmesh_plan("/opt/transform", select_model="marts.edits_hourly")

    assert result.success is True
    assert mock_run.call_count == 2

    plan_cmd = mock_run.call_args_list[0][0][0]
    run_cmd = mock_run.call_args_list[1][0][0]
    assert plan_cmd == [
        "sqlmesh",
        "--paths",
        "/opt/transform",
        "plan",
        "--auto-apply",
        "--no-prompts",
        "--select-model",
        "marts.edits_hourly",
    ]
    assert run_cmd == [
        "sqlmesh",
        "--paths",
        "/opt/transform",
        "run",
        "--select-model",
        "marts.edits_hourly",
    ]
    for call in mock_run.call_args_list:
        assert call[1]["capture_output"] is True

    assert "applied" in result.stdout
    assert "no changes" in result.stdout


def test_run_sqlmesh_plan_without_select_model():
    fake_result = MagicMock(returncode=0, stdout="", stderr="")
    with patch("orchestration.sqlmesh_runner.subprocess.run", return_value=fake_result) as mock_run:
        run_sqlmesh_plan("/opt/transform")
    for call in mock_run.call_args_list:
        assert "--select-model" not in call[0][0]


def test_run_sqlmesh_plan_reports_plan_failure_without_running_run():
    fake_result = MagicMock(returncode=1, stdout="", stderr="boom")
    with patch("orchestration.sqlmesh_runner.subprocess.run", return_value=fake_result) as mock_run:
        result = run_sqlmesh_plan("/opt/transform")

    assert result.success is False
    assert result.stderr == "boom"
    mock_run.assert_called_once()  # `run` never gets a chance if `plan` itself fails


def test_run_sqlmesh_plan_reports_run_failure():
    plan_result = MagicMock(returncode=0, stdout="applied", stderr="")
    run_result = MagicMock(returncode=1, stdout="", stderr="missing interval error")
    with patch("orchestration.sqlmesh_runner.subprocess.run", side_effect=[plan_result, run_result]):
        result = run_sqlmesh_plan("/opt/transform")

    assert result.success is False
    assert "missing interval error" in result.stderr
