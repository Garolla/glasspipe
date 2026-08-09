"""Thin subprocess wrapper around the SQLMesh CLI.

Dagster shells out to `sqlmesh` rather than using a dedicated
dagster-sqlmesh integration package: the exact API surface of the
available community integrations wasn't something this implementation
could verify against a live install with confidence, whereas the CLI
contract (`sqlmesh plan --auto-apply --no-prompts --select-model X`,
confirmed against the installed sqlmesh 0.236.1 via `--help`) is stable
and easy to reason about. `plan --auto-apply` is used instead of `run` so
each asset materialization both creates/updates the model and backfills
missing intervals -- correct for a project that may not have been planned
yet, and a no-op when there's nothing new.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass


@dataclass
class SQLMeshRunResult:
    returncode: int
    stdout: str
    stderr: str

    @property
    def success(self) -> bool:
        return self.returncode == 0


def run_sqlmesh_plan(project_dir: str, *, select_model: str | None = None, timeout: float = 600.0) -> SQLMeshRunResult:
    cmd = ["sqlmesh", "--paths", project_dir, "plan", "--auto-apply", "--no-prompts"]
    if select_model:
        cmd += ["--select-model", select_model]

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return SQLMeshRunResult(returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)
