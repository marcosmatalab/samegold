"""The gate that reads the sentences, and the split it publishes about itself.

Both of these are checks the repository makes about its own documents, and both were written on
6 September 2026 after a reviewer read the front page and found three false statements in it.
They live in their own file rather than at the end of `test_documentation.py` because the module
helpers they need would otherwise be attributed to whichever test happened to be last - which is
how `test_every_test_that_reads_the_repository_evidence_is_marked` first reported them.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from samegold.evidence.lane_split import split
from samegold.evidence.prose import (
    DATABRICKS_WORKFLOW_HAS_RUN,
    check_documents,
    databricks_workflow_run_count,
    expired_exemptions,
    expiring_on,
    stale_exemptions,
)

REPO = Path(__file__).resolve().parents[2]

# ------------------------------------------------------- the gate, extended from figures to prose
#
# Every FIGURE in these documents goes through an evidence anchor, and `samegold check` fails when
# a document and the record disagree. The SENTENCES around those figures had no gate at all, and
# on 6 September 2026 five of them were false - one of them false in the same merge that shipped
# it, because that merge added the file the sentence said did not exist.
#
# `samegold.evidence.prose` is the gate. These tests are the two halves it needs: that it fires on
# the real repository, and that it fires on a sentence somebody writes tomorrow.


def _prose_documents() -> list[Path]:
    """Every document the prose gate reads: the same set `samegold check` renders, plus the
    Makefile, which is where one of the five false sentences was."""
    return (
        [
            REPO / name
            for name in (
                "README.md",
                "CLAIMS.md",
                "EXAM_MAP.md",
                "PARITY.md",
                "FINDINGS.md",
                "CONTRACT.md",
                "CONTRIBUTING.md",
                "Makefile",
            )
        ]
        + sorted((REPO / "docs").rglob("*.md"))
        + [REPO / "evidence" / "databricks" / "README.md"]
    )


@pytest.mark.evidence_dependent
def test_no_document_asserts_an_absence_the_repository_contradicts() -> None:
    """The gate itself, over the real documents.

    The three shapes it checks are the three this repository can falsify from its own files: an
    exhaustive enumeration of a directory, a claim that something has never run against the
    committed run records, and a claim that a path does not exist. It cannot read intent, so a
    sentence that is genuinely true and matches one of those shapes is declared in `EXEMPTIONS`
    with the measurement behind it.
    """
    drifted = check_documents(REPO, _prose_documents())
    assert not drifted, (
        "these sentences say something the repository contradicts:\n\n"
        + "\n\n".join(str(d) for d in drifted)
    )


def test_no_exemption_outlives_the_sentence_it_was_written_for() -> None:
    """An exemption is a promise that a matching sentence is true.

    When the sentence is deleted or reworded the promise is about nothing, and a list of
    exceptions nobody prunes is how the next false sentence gets waved through - which is the
    same failure as the closed lists elsewhere in this suite, one level along.
    """
    stale = stale_exemptions(REPO)
    assert not stale, f"these exemptions no longer match anything: {stale}"


def test_the_gate_fires_on_a_sentence_written_tomorrow(tmp_path: Path) -> None:
    """BORN SEEN FAILING, and kept that way.

    The gate was written after five false sentences were found by hand, so it started life
    fitting five known answers. That is the shape of a check that passes because it was written
    against the cases it was written for. This one hands it sentences it has never seen, in a
    document that does not exist, and requires each shape to be caught.
    """
    repo = tmp_path
    (repo / "docs").mkdir()
    (repo / "evidence" / "databricks").mkdir(parents=True)
    (repo / "evidence" / "databricks" / "SG-DBX-01.json").write_text("{}", encoding="utf-8")
    (repo / "databricks" / "resources").mkdir(parents=True)
    for name in ("jobs.yml", "grants.yml", "dashboards.yml"):
        (repo / "databricks" / "resources" / name).write_text("", encoding="utf-8")
    # A REAL CHECKOUT, because the absence check asks git what it tracks instead of guessing
    # from the shape of a name. A tree with no index can be asked nothing, and the
    # `dashboards.yml` sentence below would then pass for that reason rather than because the
    # gate had stopped working - a green that means "unmeasured".
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True, capture_output=True)

    document = repo / "docs" / "invented.md"
    document.write_text(
        "# A document nobody has read\n\n"
        "The lane is ready but nothing has run yet, which is why there are no numbers here.\n\n"
        "`databricks/resources/` holds `jobs.yml` and `grants.yml`, and nothing else.\n\n"
        "There is no `databricks/resources/dashboards.yml` in this repository.\n",
        encoding="utf-8",
    )

    drifted = check_documents(repo, [document])
    reasons = " ".join(d.why for d in drifted)
    assert len(drifted) == 3, [str(d) for d in drifted]
    assert "run record" in reasons, reasons
    assert "dashboards.yml" in reasons, reasons

    # And the same document with the three sentences corrected is silent, so the gate is not
    # simply failing on any document handed to it.
    document.write_text(
        "# A document nobody has read\n\n"
        "The lane has run, and the numbers are in the record.\n\n"
        "`databricks/resources/` holds `jobs.yml`, `grants.yml` and `dashboards.yml`, "
        "and nothing else.\n",
        encoding="utf-8",
    )
    assert not check_documents(repo, [document]), [
        str(d) for d in check_documents(repo, [document])
    ]


# ------------------------------------------------- what counts as a path, asked of the repository
#
# The absence check used to decide what a backticked word was by looking at its SHAPE: a
# candidate counted as a path if it contained a `/` or ended in one of four extensions. That
# filter was written to keep prose words out of the gate - "there is no `deploy` step" is a
# sentence about a word - and it did that. It also made the gate blind to every file at the top
# of the repository, which is where `Makefile`, `pyproject.toml` and `LICENSE` live: three of the
# most-cited paths in the documentation, and the three a sentence claiming an absence is most
# likely to be wrong about.
#
# The shape of a name does not say whether it is a path. The repository does. A candidate counts
# as a path when git lists it as a tracked FILE, which keeps "there is no `evidence`" green - a
# directory is not a tracked path - and puts `Makefile` where it belongs.


def _repo_with(tmp_path: Path, files: tuple[str, ...]) -> Path:
    """A checkout whose index git can be asked about, which is the whole point of the rule."""
    repo = tmp_path / "checkout"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True, capture_output=True)
    for name in files:
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    # ADDED, not committed: `git ls-files` reads the index, and a test that needed an identity
    # configured to commit would fail on a machine that has none for a reason unrelated to prose.
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True, capture_output=True)
    return repo


# The five sentences from the report, and the two that must stay silent. Each is a whole
# sentence rather than a fragment because the gate reads sentences.
TRACKED_FILES = (
    "Makefile",
    "pyproject.toml",
    "LICENSE",
    "scripts/preflight.sh",
    "docs/limits.md",
    "evidence/history.jsonl",
)


@pytest.mark.parametrize(
    ("sentence", "caught", "why"),
    [
        # BLIND BEFORE THIS TEST. No `/`, and an extension the shape filter did not list.
        ("There is no `Makefile` here.", True, "a tracked file at the top of the repository"),
        ("There is no `pyproject.toml` here.", True, "a tracked file, .toml was not in the list"),
        ("There is no `LICENSE` here.", True, "a tracked file with no extension at all"),
        # ALREADY CAUGHT, and kept that way: the fix must not trade one blindness for another.
        ("There is no `scripts/preflight.sh` here.", True, "a tracked file, caught by shape too"),
        ("There is no `docs/limits.md` here.", True, "a tracked file, caught by shape too"),
        # SILENT, and both for the reason the shape filter existed.
        ("There is no `evidence` in the record.", False, "a directory is not a tracked path"),
        ("There is no `deploy` step in this lane.", False, "a word in backticks, not a path"),
    ],
)
def test_an_absence_claim_is_measured_against_what_git_tracks(
    tmp_path: Path, sentence: str, caught: bool, why: str
) -> None:
    """BORN RED for the first three, which is why it is written this way.

    The three top-level files are the cases the shape filter could not see, and the two paths
    with a `/` are the cases it could: a fix that catches the first three by loosening the shape
    would also start catching the two negatives, so both halves are asserted together.
    """
    repo = _repo_with(tmp_path, TRACKED_FILES)
    document = repo / "docs" / "claim.md"
    document.write_text(f"# A document\n\n{sentence}\n", encoding="utf-8")

    drifted = check_documents(repo, [document])
    assert bool(drifted) is caught, (
        f"{sentence!r} should {'be caught' if caught else 'stay green'} because {why}; "
        f"the gate said {[str(d) for d in drifted]}"
    )


def test_an_absence_claim_about_a_path_this_checkout_does_not_track_stays_green(
    tmp_path: Path,
) -> None:
    """The other half of asking git: a file on disk that no commit knows about.

    This is the case the rule is narrower than the filesystem for, and deliberately. A sentence
    is a claim about the REPOSITORY, and a file the repository does not track is not part of it -
    a scratch file in somebody's checkout must not turn another author's true sentence red.
    """
    repo = _repo_with(tmp_path, TRACKED_FILES)
    (repo / "scratch.md").write_text("", encoding="utf-8")  # on disk, never added

    document = repo / "docs" / "claim.md"
    document.write_text("# A document\n\nThere is no `scratch.md` here.\n", encoding="utf-8")
    assert not check_documents(repo, [document]), "an untracked file is not a path claim"


# ------------------------------------------------- the exemption that expires without moving
#
# `stale_exemptions` catches an exemption whose SENTENCE moved. It cannot catch one whose WORLD
# moved, and that is the shape of the three MEASURED_TRUE entries here: they are true because
# `.github/workflows/databricks.yml` has never been dispatched, and the day it is, all three
# become false sentences carrying a note saying somebody checked once. Nothing in any document
# changes, so nothing else in this suite goes red.
#
# It cannot be checked offline - it is a fact about a remote service, which is why those
# sentences are exempt rather than fixed. CI has a network, so CI is where it is asked, and a
# machine that cannot ask skips instead of failing: a check that goes red when the network is
# down is a check people learn to ignore.


def test_the_expiry_fires_when_the_event_happens() -> None:
    """BORN SEEN FAILING, with the answer forced, because the true case cannot be arranged.

    Making this go red for real needs somebody to dispatch a workflow against a Free Edition
    workspace. So the network's single boolean is separated from what it means, and the meaning
    is tested both ways here - which is what stops the check below from being a line of code
    that has never once evaluated its own consequent.
    """
    expiring = expiring_on(DATABRICKS_WORKFLOW_HAS_RUN)
    assert expiring, (
        "no exemption declares that dispatching the databricks workflow would falsify it. "
        "Either the three MEASURED_TRUE entries lost their `expires_when`, or this event name "
        "changed and the exemptions were not moved with it."
    )
    assert expired_exemptions(DATABRICKS_WORKFLOW_HAS_RUN, has_happened=True) == expiring
    assert expired_exemptions(DATABRICKS_WORKFLOW_HAS_RUN, has_happened=False) == []


def test_no_exemption_survives_the_run_that_makes_its_sentence_false() -> None:
    """Asked of the GitHub API, and skipped where it cannot be asked.

    The count is unauthenticated: the endpoint is public for a public repository, and a check
    that needed a token would not run for a contributor. `None` means "could not ask" - offline,
    rate-limited, renamed workflow, no origin remote - and never "has not run".
    """
    runs = databricks_workflow_run_count(REPO)
    if runs is None:
        pytest.skip(
            "the GitHub API could not be asked for the databricks workflow's run count "
            "(offline, rate-limited, or no origin remote); CI has a network and asks there"
        )
    expired = expired_exemptions(DATABRICKS_WORKFLOW_HAS_RUN, has_happened=runs > 0)
    assert not expired, (
        f"`.github/workflows/databricks.yml` has now run {runs} time(s), so these exemptions "
        f"are covering sentences that are no longer true:\n"
        + "\n".join(f"  {e.document}: {e.fragment!r}" for e in expired)
        + "\n\nEach one says a workflow has never run. Re-measure the sentence it covers, fix "
        "the document, and delete the exemption - it was a claim about the present, and the "
        "present moved."
    )


# ------------------------------------------------------------ what this suite is actually about
#
# A reviewer counted the fast lane and asked how much of it tests the PIPELINE and how much tests
# the repository's own paperwork. It is a fair question, the answer is published in CLAIMS.md,
# and the answer used to be TYPED there: "389 of them check the repository", beside figures that
# render through evidence anchors. It drifted the way every typed number here has drifted - a
# commit added tests, the sum moved, the sentence did not.
#
# The criterion and the two sets now live in `samegold.evidence.lane_split`, where the SG-00
# collector imports them and writes the sum into the evidence record; CLAIMS.md renders that sum
# through an anchor, and `samegold check` fails when the document and the record disagree.
#
# What stays HERE is the half that is a judgement rather than a measurement: a new test file
# belongs to neither set until somebody decides which, and this test fails until somebody does.
# That is the property the published sum depends on, and it is the one a test can enforce.


def test_every_test_file_in_the_fast_lane_is_classified() -> None:
    """A new test file forces a decision instead of drifting onto a side.

    The SUM is not asserted here any more, and deliberately: it is an artifact of the SG-00
    record and CLAIMS.md renders it through `<!--sg:SG-00.artifact.fast_lane_repository_tests-->`,
    so a stale sentence is caught by the same drift gate as every other figure rather than by a
    string comparison in a test. What a test can decide, and a renderer cannot, is whether a file
    nobody has classified is allowed to change that sum silently. It is not.
    """
    _repository, _domain, unclassified = split(REPO)
    assert not unclassified, (
        f"these test files are in neither class: {unclassified}. The criterion is in the "
        f"docstring of samegold.evidence.lane_split; add each file to REPOSITORY_TESTS or "
        f"DOMAIN_TESTS there, because the sum SG-00 publishes is that addition."
    )
