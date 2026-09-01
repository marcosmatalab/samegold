"""The documentation is checked like code, because it was wrong like code.

An adversarial review found eleven broken file references, a test that asserted a string that
only survived in a comment, an ADR describing a test that did not exist, and four milestone
pointers to the wrong milestone. Every one of those is the same failure: prose that nothing
executes. These tests execute it.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DOCS = sorted(REPO.glob("*.md")) + sorted((REPO / "docs").rglob("*.md"))
SOURCES = [p for p in (REPO / "src").rglob("*.py") if "__pycache__" not in str(p)]
PATH_LIKE = re.compile(r"\b(?:src|tests|databricks|pipelines|docs)/[\w./\-]+")
# Paths that are created at runtime rather than committed.
RUNTIME_PATHS = {"evidence/refutations.jsonl"}


@pytest.mark.parametrize("document", DOCS + SOURCES, ids=lambda p: str(p.name))
def test_every_repository_path_mentioned_exists(document: Path) -> None:
    text = document.read_text(encoding="utf-8")
    broken = []
    for match in PATH_LIKE.findall(text):
        candidate = match.rstrip(".,;:)`")
        if candidate in RUNTIME_PATHS:
            continue
        if not (REPO / candidate).exists():
            broken.append(candidate)
    assert not broken, f"{document.relative_to(REPO)} cites paths that do not exist: {broken}"


def test_every_milestone_cited_exists() -> None:
    milestones = {
        match
        for match in re.findall(r"\bM\d{1,2}\b", (REPO / "docs" / "milestones.md").read_text())
    }
    for document in DOCS:
        if document.name == "milestones.md":
            continue
        for cited in re.findall(r"\bmilestone (M\d{1,2})\b", document.read_text(encoding="utf-8")):
            assert cited in milestones, f"{document.name} cites {cited}, which is not a milestone"


def test_every_claim_id_in_the_documents_is_a_real_claim() -> None:
    from samegold.claims import ALL_CLAIMS, SLOW_CLAIMS

    known = set(ALL_CLAIMS) | set(SLOW_CLAIMS)
    for document in DOCS:
        for cited in re.findall(r"\bSG-\d{2}\b", document.read_text(encoding="utf-8")):
            assert cited in known, f"{document.name} cites {cited}, which is not a claim"


def test_the_readme_does_not_state_a_test_count_by_hand() -> None:
    """Counts belong in evidence anchors. The README said 127 for a week after it was 152."""
    readme = (REPO / "README.md").read_text(encoding="utf-8")
    for line in readme.splitlines():
        if "tests" in line and "<!--sg:" not in line:
            assert not re.search(r"\b\d{2,4}\s+tests\b", line), (
                f"a hand-written test count in the README: {line.strip()}"
            )


def test_the_postmortem_quotes_the_published_evidence() -> None:
    """The euro figures in the post-mortem are SG-04's, or the test fails.

    The first draft of that document invented all four of them: an incident report with made-up
    numbers, in a repository whose entire argument is that numbers come from runs. An
    adversarial review checked them against `evidence/runs/SG-04.json` and none matched. This
    test is the reason it cannot happen again, and it is deliberately exact rather than
    approximate: a post-mortem that is nearly right about the money is wrong.
    """
    record = json.loads((REPO / "evidence" / "runs" / "SG-04.json").read_text(encoding="utf-8"))
    january = next(
        month
        for month in record["artifacts"]["months_that_moved"]
        if month["accounting_month"] == "2026-01"
    )
    text = (REPO / "docs" / "postmortem-2026-03-06.md").read_text(encoding="utf-8")

    def spanish(cents: int) -> str:
        """67269342 -> '672 693,42', the way the document writes money.

        Spanish convention: a space groups the thousands and a comma separates the decimals.
        Both are plain ASCII on purpose. A non-breaking space would look identical in the
        rendered document and would make this test compare two things a reader cannot tell
        apart, which is how a green assertion stops meaning anything.
        """
        whole, fraction = divmod(abs(int(cents)), 100)
        grouped = f"{whole:,}".replace(",", " ")
        return f"{grouped},{fraction:02d}"

    for cents in (
        january["first_close_net_cents"],
        january["final_net_cents"],
        january["delta_cents"],
    ):
        assert spanish(cents) in text, f"{spanish(cents)} EUR is not in the post-mortem"
    assert f"{abs(january['delta_pct']):.2f}".replace(".", ",") in text
    assert str(january["versions"]) in text
