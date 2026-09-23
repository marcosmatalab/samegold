"""A badge on a front page measures what it is named after, on every push.

WHAT THIS EXISTS BECAUSE OF. The `databricks` badge pointed at `.github/workflows/databricks.yml`,
which runs only when dispatched by hand, only validates or deploys definitions, and does not
start the job. Its colour said nothing about the close in the cloud - it said whether somebody
had last clicked "validate" and whether the credentials worked that day. A green tick with no
measurement behind it is a hand-typed figure drawn as an icon.

So: every workflow badge on either front page must point at a workflow that runs by itself -
on `push`, `pull_request` or a `schedule` - because a workflow that only runs when somebody
dispatches it measures whatever that person last chose to run; and the Databricks badge must
point at the workflow that verifies the committed Databricks evidence offline, on every push -
the pinned record, the figures that quote it, and every closed version recomputed to the cent
on DuckDB.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
PAGES = ("README.md", "README.es.md")
BADGE = re.compile(
    r"\[!\[(?P<label>[^\]]+)\]\([^)]*/actions/workflows/(?P<file>[\w.\-]+)/badge\.svg"
)


def _badges(page: str) -> list[tuple[str, str]]:
    text = (REPO / page).read_text(encoding="utf-8")
    return [(m.group("label"), m.group("file")) for m in BADGE.finditer(text)]


def _triggers(workflow: str) -> dict[str, object]:
    spec = yaml.safe_load((REPO / ".github" / "workflows" / workflow).read_text(encoding="utf-8"))
    # PyYAML reads the bare key `on` as the boolean True.
    triggers = spec.get("on", spec.get(True))
    return triggers if isinstance(triggers, dict) else {name: None for name in triggers}


#: Triggers that run a workflow without anybody choosing to. `workflow_dispatch` is not one.
AUTOMATIC = {"push", "pull_request", "schedule"}


@pytest.mark.parametrize("page", PAGES)
def test_no_badge_is_driven_only_by_hand(page: str) -> None:
    badges = _badges(page)
    assert badges, f"{page} shows no workflow badges"
    for label, workflow in badges:
        assert (REPO / ".github" / "workflows" / workflow).exists(), (label, workflow)
        assert AUTOMATIC & set(_triggers(workflow)), (
            f"{page}: the {label!r} badge points at {workflow}, which runs only when dispatched "
            f"by hand, so its colour measures whatever was last dispatched"
        )


@pytest.mark.parametrize("page", PAGES)
def test_the_databricks_badge_verifies_the_committed_evidence(page: str) -> None:
    databricks = [(label, wf) for label, wf in _badges(page) if "databricks" in label.lower()]
    assert len(databricks) == 1, f"{page} should show exactly one Databricks badge: {databricks}"
    _, workflow = databricks[0]
    assert {"push", "pull_request"} <= set(_triggers(workflow)), (
        f"{page}: the Databricks badge's workflow {workflow} must run on every push and PR"
    )
    source = (REPO / ".github" / "workflows" / workflow).read_text(encoding="utf-8")
    for step in (
        "tests/fast/test_databricks_close_parity.py",
        "tests/fast/test_front_page_figures.py",
    ):
        assert step in source, (
            f"{page}: the Databricks badge points at {workflow}, which does not run {step}; "
            f"a Databricks badge must measure the committed Databricks evidence"
        )
