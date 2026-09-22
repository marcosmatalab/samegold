"""The CLI that CI runs must be the one that owns the deployment state.

WHAT THIS EXISTS BECAUSE OF. On 8 September 2026 the `databricks/setup-cli` action was pinned
by commit SHA, correctly, because `@main` is a moving third-party action in a job that holds a
workspace token. The version it was pinned TO was v0.221.1, released in April - five months
older than the v1.14.1 the laptop was already deploying with, which
`evidence/databricks/fetch.json` had recorded all along.

Nothing noticed, because CI only ever ran `validate`, and `validate` touches no state. The
first `deploy` from CI failed with `cannot create pipeline: The pipeline name 'samegold' is
already used by another pipeline` - the bundle trying to CREATE a resource that exists, because
from v1.x the direct deployment engine is the default and migrates the Terraform state into
`resources.json`, which a v0.221 CLI cannot read.

So the two versions are tied together here. Offline, from a clone, with no credentials: the
workflow says which CLI CI installs, the committed record says which CLI produced the evidence,
and if they stop matching this fails.

WHAT IT DOES NOT PROVE. `fetch.json` records the CLI that ran `fetch`, which is the last step
of the same script invocation that deployed, not the deploy itself - the script has no field
for that. It is the closest thing the repository holds to "the version that owns the state",
and saying so here is cheaper than implying it is more.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
WORKFLOW = REPO / ".github" / "workflows" / "databricks.yml"
RECORD = REPO / "evidence" / "databricks" / "fetch.json"

#: `# databricks/setup-cli vX.Y.Z` on the comment line, and the SHA on the `uses:` line. Both
#: are read: a SHA with no version beside it is unreadable, and a version with no SHA is a
#: moving target.
PIN = re.compile(
    r"#\s*databricks/setup-cli\s+v(?P<version>\d+\.\d+\.\d+).*?"
    r"uses:\s*databricks/setup-cli@(?P<sha>[0-9a-f]{40})",
    re.DOTALL,
)
RECORDED = re.compile(r"v?(?P<version>\d+\.\d+\.\d+)")


def pinned() -> tuple[str, str]:
    found = PIN.search(WORKFLOW.read_text(encoding="utf-8"))
    assert found, (
        "the databricks workflow no longer pins setup-cli as `# databricks/setup-cli vX.Y.Z` "
        "followed by `uses: databricks/setup-cli@<40-hex>`. Both halves are required: the SHA "
        "is what runs, and the version beside it is the only thing a reader can check."
    )
    return found.group("version"), found.group("sha")


def recorded() -> str:
    record = json.loads(RECORD.read_text(encoding="utf-8"))
    raw = record.get("databricks_cli")
    assert raw, f"{RECORD.name} no longer records `databricks_cli`"
    found = RECORDED.search(str(raw))
    assert found, f"cannot read a version out of {raw!r}"
    return found.group("version")


def test_the_pinned_cli_is_the_one_that_produced_the_records() -> None:
    version, _ = pinned()
    assert version == recorded(), (
        f"CI installs databricks CLI v{version} and the committed records were produced by "
        f"v{recorded()}. From v1.x the direct deployment engine is the default and the "
        f"deployment state lives in `resources.json`; a CLI from a different generation reads "
        f"a state it does not understand and plans to CREATE resources that already exist. "
        f"Either move the pin, or re-run the lane and commit the records the new CLI produced."
    )


def test_the_pin_is_a_sha_and_not_a_tag() -> None:
    """A tag can be moved. This job holds a workspace token, so the pin is a commit."""
    _, sha = pinned()
    assert len(sha) == 40


@pytest.mark.parametrize("major", [1])
def test_the_pinned_cli_is_new_enough_to_read_the_state_it_finds(major: int) -> None:
    """The specific property the collision was about, stated as itself.

    Equality with the record is the rule above and is stricter. This one says WHY a v0.x pin
    is wrong even if somebody re-records `fetch.json` from a v0.x laptop: the pipeline in the
    workspace was created by the direct engine, and a CLI that predates it cannot see it.
    """
    version, _ = pinned()
    assert int(version.split(".")[0]) >= major, (
        f"v{version} predates the direct deployment engine. The deployment state in the "
        f"workspace was written by a v1.x CLI, and this one would try to create a pipeline "
        f"that already exists - which is exactly run 35756907303."
    )
