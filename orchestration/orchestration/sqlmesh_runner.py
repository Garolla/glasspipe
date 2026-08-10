"""Thin subprocess wrapper around the SQLMesh CLI.

Dagster shells out to `sqlmesh` rather than using a dedicated
dagster-sqlmesh integration package: the exact API surface of the
available community integrations wasn't something this implementation
could verify against a live install with confidence, whereas the CLI
contract is stable and easy to reason about.

Runs two SQLMesh commands, not one -- found by running the hourly
pipeline for real, not from reading the docs. The original version only
ran `plan --auto-apply --no-prompts`, on the assumption that it both
deploys model changes and backfills missing intervals. That's wrong:
`plan` only reacts to the model's *definition* changing (new/edited SQL).
Once a model's SQL is unchanged from what's already in `prod`, `plan`
reports "No changes to plan" and does nothing else -- it does NOT
backfill newly-elapsed incremental intervals for an otherwise-stable
model. Running only `plan` on an hourly schedule looked like it worked
(exit 0, "audits passed" on the first deploy) but silently stopped
producing new rows in edits_hourly after that first backfill, because
every following hourly tick saw an unchanged model and no-opped.

`run` is the command that actually evaluates each model's cron and
backfills whatever intervals are due -- SQLMesh's own `--help` describes
it as "Evaluate missing intervals for the target environment". Both
commands are idempotent no-ops when there's nothing to do, so running
`plan` then `run` every time is cheap and correct for both "first
deploy" and "recurring tick" without needing to tell them apart.
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


def _run(cmd: list[str], timeout: float) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def run_sqlmesh_plan(project_dir: str, *, select_model: str | None = None, timeout: float = 600.0) -> SQLMeshRunResult:
    base = ["sqlmesh", "--paths", project_dir]
    select = ["--select-model", select_model] if select_model else []

    plan = _run(base + ["plan", "--auto-apply", "--no-prompts"] + select, timeout)
    if plan.returncode != 0:
        return SQLMeshRunResult(returncode=plan.returncode, stdout=plan.stdout, stderr=plan.stderr)

    run = _run(base + ["run"] + select, timeout)
    return SQLMeshRunResult(
        returncode=run.returncode,
        stdout=f"{plan.stdout}\n{run.stdout}",
        stderr=f"{plan.stderr}\n{run.stderr}",
    )
