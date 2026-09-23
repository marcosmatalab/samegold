"""The evidence pull request is merged only after the repository's own checks pass on it.

WHAT THIS EXISTS BECAUSE OF. `evidence.yml` opens a pull request and merges it in the same step
(ADR 0014). The pull request and its branch are pushed with GITHUB_TOKEN, and GitHub starts no
workflow for a push or a pull request made with that token, so `fast` and `databricks evidence`
never ran on an evidence pull request: it was merged with no checks at all, and the commit it
left on `main` was one no check had seen either.

A `workflow_dispatch` made with GITHUB_TOKEN is the documented exception - it does create a
run - so the job dispatches both workflows on the evidence branch itself, waits for them, and
merges only when both are green. These tests hold the step to that order.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOW = REPO / ".github" / "workflows" / "evidence.yml"
REQUIRED = ("fast.yml", "databricks-evidence.yml")


def _spec() -> dict[str, object]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _commit_step() -> str:
    steps = _spec()["jobs"]["evidence"]["steps"]  # type: ignore[index]
    (step,) = [s for s in steps if s.get("name") == "commit"]
    return str(step["run"])


def _code(script: str) -> str:
    """The shell, less its comments, so a command quoted in a comment does not count."""
    return "\n".join(line for line in script.splitlines() if not line.lstrip().startswith("#"))


def test_the_checks_are_dispatched_on_the_evidence_branch_and_awaited_before_the_merge() -> None:
    script = _code(_commit_step())
    merge = script.index("gh pr merge")
    for workflow in REQUIRED:
        dispatch = f'gh workflow run {workflow} --ref "$branch"'
        assert dispatch in script, f"the evidence job does not run {workflow} on its branch"
        assert script.index(dispatch) < merge, f"{workflow} is dispatched after the merge"
    assert "gh run watch" in script, "the dispatched checks are not awaited"
    assert script.index("gh run watch") < merge, "the merge does not wait for the checks"
    assert "--exit-status" in script, "a red check would not stop the merge"


def test_the_job_may_dispatch_workflows() -> None:
    permissions = _spec()["permissions"]
    assert isinstance(permissions, dict)
    assert permissions.get("actions") == "write", "dispatching a workflow needs actions: write"


def test_the_run_measures_the_commit_it_was_dispatched_on() -> None:
    """No input or ref may make the job measure a commit other than GITHUB_SHA.

    A `commit` input was added for exactly that, to cite a release's own commit after `main`
    had moved, and its first run was refused by `EvidenceStore` before any record was written:
    `ci_commit_sha` is GITHUB_SHA, the tip of the dispatching ref, and a record whose CI commit
    is not the commit its seeds come from is rejected. That refusal is the chain working, so the
    workflow must not ask for it.
    """
    spec = _spec()
    triggers = spec.get("on", spec.get(True))  # PyYAML reads the bare key `on` as True
    dispatch = triggers["workflow_dispatch"] or {}  # type: ignore[index]
    assert not dispatch.get("inputs"), "evidence.yml must not take an input that picks a commit"
    steps = spec["jobs"]["evidence"]["steps"]  # type: ignore[index]
    checkout = next(s for s in steps if str(s.get("uses", "")).startswith("actions/checkout"))
    assert "ref" not in (checkout.get("with") or {}), "the checkout must be the dispatched commit"
