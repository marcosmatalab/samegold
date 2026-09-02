"""The pre-push gate has to be the whole of CI, and this is what keeps it that way.

`make ci-local` ran the FAST workflow and was named as though it ran CI. So a change under
`tests/spark/` could pass locally and arrive red in the `spark` workflow - which is exactly
what happened in round 12 (a Delta job red for two days while the documents called the lane
unexecuted) and again in round 13, in the commit that added the ADR about it.

`scripts/preflight.sh` runs both workflows. This file is the thing that stops it drifting:
every check command in a covered workflow has to appear in that script, and a NEW workflow
has to be classified as covered or excluded before this test will pass. Without it, the gate
decays the same way the command it replaced did - silently, and in the direction of running
less.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"
PREFLIGHT_PATH = REPO / "scripts" / "preflight.sh"
PREFLIGHT = PREFLIGHT_PATH.read_text(encoding="utf-8")

# The workflows the gate runs. Anything here must be reproducible on a developer machine with
# a JDK and no credentials.
COVERED = {"fast.yml", "spark.yml"}

# The workflows it deliberately does not run, each with the reason. A pre-push gate that
# rewrote the evidence chain, or that spent a Free Edition account's daily quota, would be a
# gate nobody runs - which is worse than one that runs half, because at least half is honest
# about which half.
NOT_A_GATE = {
    "evidence.yml": "an hour of compute, and it WRITES the evidence it would be checking",
    "databricks.yml": "needs an account and a token, and a run can spend the daily quota",
}

# The tools whose invocations are checks rather than setup. `pip install`, `sudo rm -rf` and
# the cache steps are not part of what a developer has to reproduce.
CHECK_TOOLS = ("pytest", "ruff", "mypy", "samegold")


def _check_commands(workflow: Path) -> list[str]:
    """Every check command a workflow runs, one per line, in the order it runs them."""
    document = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    out: list[str] = []
    for job in (document.get("jobs") or {}).values():
        for step in job.get("steps", []):
            script = step.get("run")
            if not script:
                continue
            for line in str(script).splitlines():
                # `samegold readme && samegold check` is two commands on one line.
                for command in line.split("&&"):
                    command = command.strip()
                    if command.startswith(CHECK_TOOLS):
                        out.append(" ".join(command.split()))
    return out


def _workflow_files() -> list[Path]:
    return sorted(WORKFLOWS.glob("*.yml"))


def test_every_workflow_is_either_covered_or_excluded() -> None:
    """A new workflow forces a decision instead of being quietly outside the gate.

    This is the assertion that would have caught the original defect: `spark.yml` existed,
    nothing local ran it, and no test anywhere held the two facts in the same place.
    """
    present = {path.name for path in _workflow_files()}
    classified = COVERED | set(NOT_A_GATE)
    assert present == classified, (
        f"workflows nobody has classified: {sorted(present - classified)}; "
        f"classified but missing: {sorted(classified - present)}"
    )


@pytest.mark.parametrize("name", sorted(COVERED))
def test_the_preflight_runs_every_check_that_workflow_runs(name: str) -> None:
    commands = _check_commands(WORKFLOWS / name)
    # A parser that silently found nothing would make this vacuous, which is the failure mode
    # of every "the scope is fine" check in this repository.
    assert len(commands) >= 2, f"{name}: found no check commands at all"
    normalised = " ".join(PREFLIGHT.split())
    missing = [command for command in commands if command not in normalised]
    assert not missing, (
        f"{name} runs these and scripts/preflight.sh does not: {missing}. A local gate that "
        f"runs less than CI is how a red lane gets pushed with a green run behind it."
    )


@pytest.mark.parametrize("name", sorted(NOT_A_GATE))
def test_the_excluded_workflows_say_so_in_the_script(name: str) -> None:
    """The exclusion lives in the script a reader opens, not only in this test."""
    assert name in PREFLIGHT, f"{name} is excluded from the gate and the script does not say so"


def test_the_gate_refuses_to_exit_zero_for_a_lane_it_did_not_run() -> None:
    """The one property that makes the whole thing worth having.

    A gate that skips the JVM lanes on a machine with no JVM and still exits 0 is a gate that
    reports the scope of the command rather than the state of the repository. Native Windows
    cannot run Spark at all (Hadoop's NativeIO wants winutils.exe), so this is not a
    hypothetical: it is the state of the machine this repository is written on.
    """
    assert "skipped+=(" in PREFLIGHT, "nothing records a lane that could not run"
    verdict = PREFLIGHT.split("the verdict", 1)[1]
    assert "if [ ${#failed[@]} -eq 0 ] && [ ${#skipped[@]} -eq 0 ]; then" in verdict, (
        "the success branch must require BOTH no failures and nothing skipped"
    )
    assert verdict.rstrip().endswith("exit 1"), "the last word of the script is not a failure"


@pytest.mark.parametrize(
    "path",
    sorted(str(p.relative_to(REPO)) for p in [*(REPO / "scripts").glob("*.sh"), REPO / "Makefile"]),
)
def test_the_scripts_are_readable_by_a_posix_shell(path: str) -> None:
    """No CRLF in anything a shell executes.

    `scripts/preflight.sh` was written from Windows, arrived with CRLF, and bash inside WSL2
    answered `$'\r': command not found` on line after line before dying at the first
    `for ... do` - on exactly the path CONTRIBUTING.md tells a Windows contributor to use to
    get a verdict they can push on. `.gitattributes` pins these to LF, and this reads the
    bytes, because a .gitattributes only governs checkouts made after it was added.
    """
    raw = (REPO / path).read_bytes()
    assert b"\r\n" not in raw, (
        f"{path} has CRLF line endings; bash cannot execute it on Linux or in WSL2"
    )
