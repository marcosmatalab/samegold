"""The gate that reads the sentences, and the split it publishes about itself.

Both of these are checks the repository makes about its own documents, and both were written on
6 September 2026 after a reviewer read the front page and found three false statements in it.
They live in their own file rather than at the end of `test_documentation.py` because the module
helpers they need would otherwise be attributed to whichever test happened to be last - which is
how `test_every_test_that_reads_the_repository_evidence_is_marked` first reported them.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from samegold.evidence.prose import check_documents, stale_exemptions

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


# ------------------------------------------------------------ what this suite is actually about
#
# A reviewer counted the fast lane and asked the obvious question: how much of it tests the
# PIPELINE, and how much of it tests the repository's own paperwork? It is a fair question with
# an uncomfortable answer, and the answer is published rather than waited for.
#
# THE CRITERION, written down so the split can be argued with:
#
#   REPOSITORY - the test's verdict is decided by this repository's own text, configuration,
#   structure, scripts or evidence plumbing. It reads or executes something committed here and
#   compares it against something else committed here. It would survive, unchanged in kind, if
#   the domain were payroll instead of retail revenue.
#
#   DOMAIN - the test's verdict is decided by RUNNING a computation over data: the generator,
#   the close, the dimension, the digests, the invariants, the mutation engine, the parity
#   comparisons against a reference.
#
# Two calls worth naming because a reader may disagree:
#
#   * `test_seeds` is REPOSITORY. Seeds are derived from the commit sha and the evidence store
#     refuses records whose seeds were chosen; that is provenance plumbing, not arithmetic about
#     revenue.
#   * `test_review_regressions` is DOMAIN as a file, and it is the one genuinely mixed one:
#     roughly a third of its tests are regressions in documents and lane declarations rather
#     than in the close. Moving those would shift the split by about ten tests in the
#     repository direction. It is counted whole, on the side its majority sits.
#
# The numbers are RECOMPUTED here rather than typed, for the same reason every other number in
# this repository is: the split published in CLAIMS.md is what this test asserts, and a new test
# file that nobody classifies fails the run rather than landing silently on one side.

REPOSITORY_TESTS = {
    "test_architecture.py",
    "test_contract_documents.py",
    "test_databricks_bundle.py",
    "test_databricks_catalog_step.py",
    "test_documentation.py",
    "test_evidence_gate.py",
    "test_preflight.py",
    "test_prose_gate.py",
    "test_seeds.py",
}
DOMAIN_TESTS = {
    "test_databricks_close_parity.py",
    "test_databricks_dimension_parity.py",
    "test_digest.py",
    "test_generator.py",
    "test_governance.py",
    "test_invariants.py",
    "test_late_arrivals.py",
    "test_money.py",
    "test_mutation.py",
    "test_review_regressions.py",
    "test_rules.py",
    "test_scd2.py",
    "test_serve.py",
    "test_timezone.py",
    "test_verdict.py",
}


def _collected_by_file() -> dict[str, int]:
    """How many tests each file contributes, from pytest's own collection.

    Counted by collecting rather than by counting `def test_`: more than half of this suite is
    parametrised, and `test_architecture.py` has seven functions and a hundred and thirteen
    tests. A split computed from function definitions would be describing a different suite.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/fast",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    counts: dict[str, int] = {}
    for line in result.stdout.splitlines():
        if "::" not in line:
            continue
        name = Path(line.split("::", 1)[0]).name
        counts[name] = counts.get(name, 0) + 1
    assert counts, result.stdout[-2000:] + result.stderr[-2000:]
    return counts


@pytest.mark.evidence_dependent
def test_every_test_file_is_classified_and_the_split_is_what_the_documents_publish() -> None:
    """The split, recomputed. A new file forces a decision instead of drifting onto a side."""
    counts = _collected_by_file()
    unclassified = sorted(set(counts) - REPOSITORY_TESTS - DOMAIN_TESTS)
    assert not unclassified, (
        f"these test files are in neither class: {unclassified}. The criterion is in the "
        f"comment above; add each file to REPOSITORY_TESTS or DOMAIN_TESTS and say which in "
        f"CLAIMS.md, because the published split is this sum."
    )
    repository = sum(n for name, n in counts.items() if name in REPOSITORY_TESTS)
    domain = sum(n for name, n in counts.items() if name in DOMAIN_TESTS)
    published = (REPO / "CLAIMS.md").read_text(encoding="utf-8")
    assert f"{repository} of them check the repository" in published, (
        f"the fast lane is {repository} repository / {domain} domain out of "
        f"{repository + domain}, and CLAIMS.md publishes something else. Re-render the "
        f"sentence rather than the measurement."
    )
    assert f"{domain} check the pipeline" in published, (
        f"the fast lane is {repository} repository / {domain} domain out of "
        f"{repository + domain}, and CLAIMS.md publishes something else."
    )
