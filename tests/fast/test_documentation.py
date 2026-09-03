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
# The lookbehind matters. Without it the pattern matches in the MIDDLE of a longer path:
# `evidence/databricks/SG-DBX-01.json` was read as `databricks/SG-DBX-01.json`, which does not
# exist and never did, so the document citing the real file was the one that failed. A path is
# only a path from its first segment.
PATH_LIKE = re.compile(r"(?<![\w/])(?:src|tests|databricks|pipelines|docs)/[\w./\-]+")
# `databricks/cli#4513` is a GitHub issue in another repository, not a path in this one. The
# slug happens to start with a directory name this project also has, which is how a correct
# citation of an upstream bug became a broken-path failure.
GITHUB_SLUG = re.compile(r"#\d")
# Paths that are created at runtime rather than committed.
RUNTIME_PATHS = {"evidence/refutations.jsonl"}

# The same check over the files that are NOT documents but carry the same kind of reference.
# `databricks/databricks.yml` pointed at `tests/fast/test_databricks_bundle.py` for a whole
# round while the file was called something else, and nothing looked: this test read `.md` and
# `src/**/*.py` only. Only `tests/` and `docs/` are checked in these, because a bundle file
# resolves `src/...` against its own directory rather than against the repository root - a
# boundary worth stating rather than a check worth skipping.
CONFIG_FILES = sorted(
    [
        *(REPO / ".github" / "workflows").glob("*.yml"),
        *(REPO / "databricks").rglob("*.yml"),
        *(REPO / "scripts").glob("*.sh"),
    ]
)
CONFIG_PATH_LIKE = re.compile(r"(?<![\w/])(?:tests|docs)/[\w./\-]+")


def _broken(text: str, pattern: re.Pattern[str]) -> list[str]:
    broken = []
    for match in pattern.finditer(text):
        candidate = match.group().rstrip(".,;:)`")
        if candidate in RUNTIME_PATHS:
            continue
        if GITHUB_SLUG.match(text[match.end() : match.end() + 2]):
            continue
        if not (REPO / candidate).exists():
            broken.append(candidate)
    return broken


@pytest.mark.parametrize("config", CONFIG_FILES, ids=lambda p: str(p.name))
def test_every_repository_path_a_config_file_cites_exists(config: Path) -> None:
    broken = _broken(config.read_text(encoding="utf-8"), CONFIG_PATH_LIKE)
    assert not broken, f"{config.relative_to(REPO)} cites paths that do not exist: {broken}"


@pytest.mark.parametrize("document", DOCS + SOURCES, ids=lambda p: str(p.name))
def test_every_repository_path_mentioned_exists(document: Path) -> None:
    broken = _broken(document.read_text(encoding="utf-8"), PATH_LIKE)
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


@pytest.mark.evidence_dependent
def test_the_postmortem_quotes_the_published_evidence() -> None:
    """The euro figures in the post-mortem are SG-04's, and they are ANCHORS, not prose.

    Three drafts of this document, three ways of being wrong about the money: the first
    invented all four figures; the second copied them correctly from the evidence by hand;
    the third was stale two commits later, because every seed derives from the commit SHA and
    so does every figure computed from it. The fix is not a better habit, it is the renderer:
    the numbers live inside `<!--sg:SG-04.artifact.*-->` anchors and `make readme` maintains
    them. This test checks both halves - that the anchors are there, and that what they
    currently show is what the record says.
    """
    record = json.loads((REPO / "evidence" / "runs" / "SG-04.json").read_text(encoding="utf-8"))
    artifacts = record["artifacts"]
    text = (REPO / "docs" / "postmortem-2026-03-06.md").read_text(encoding="utf-8")
    for field in ("worst_first_close_eur", "worst_final_eur", "worst_delta_eur", "worst_move_pct"):
        anchor = f"<!--sg:SG-04.artifact.{field}-->"
        assert anchor in text, f"{field} is quoted as prose rather than rendered"
        rendered = text.split(anchor, 1)[1].split("<!--/sg-->", 1)[0]
        assert rendered == str(artifacts[field]), f"{field}: {rendered!r} != {artifacts[field]!r}"
    # And no hand-typed euro amount survives outside an anchor: a figure the renderer does
    # not own is a figure that will be wrong again.
    stripped = re.sub(r"<!--sg:[^>]+-->.*?<!--/sg-->", "", text, flags=re.DOTALL)
    assert not re.search(r"\d{1,3}(?: \d{3})+,\d{2}", stripped), (
        "an unrendered money figure is left in the post-mortem"
    )


# ------------------------------------------------------------------------------- the diagrams
#
# The REAL check is `scripts/check_mermaid.mjs`, which runs mermaid's own parser and is a step
# in `.github/workflows/fast.yml`. It was run against these three blocks on 4 September 2026
# with **mermaid 11.17.2**, and all three parse as `flowchart-v2`; falsified twice against the
# committed text - `G -->> E`, which is a sequence-diagram arrow, and a `subgraph` with its
# `end` removed - and it reported the line number of each.
#
# What follows is NOT that check and does not pretend to be. Mermaid's grammar is mermaid's,
# and reimplementing it here would be a lint agreeing with itself about a language neither of
# them defines. These are the things that can be said about the TEXT without a grammar, and
# they exist so that `make fast` on a machine with no node is not silent about a diagram that
# has obviously stopped being one.

MERMAID_BLOCK = re.compile(r"```mermaid\r?\n(.*?)```", re.DOTALL)
# The diagram types this repository uses. A closed list: a new one is a decision, and the
# person making it should also decide whether the structural rules below still apply to it.
MERMAID_HEADERS = ("flowchart TB", "flowchart LR", "flowchart TD", "flowchart RL")


def _mermaid_blocks() -> list[tuple[Path, int, str]]:
    out: list[tuple[Path, int, str]] = []
    for document in DOCS:
        text = document.read_text(encoding="utf-8")
        for index, match in enumerate(MERMAID_BLOCK.finditer(text), start=1):
            out.append((document, index, match.group(1)))
    return out


BLOCKS = _mermaid_blocks()


def test_there_are_diagrams_to_check() -> None:
    """Zero blocks is a failure, not a pass.

    Every check below is a loop over `BLOCKS`, and a loop over an empty list is green. A
    renamed file or a changed fence tag would turn this whole section into a check that reports
    the absence of work as success - which is `test_every_quarantine_reason_is_actually_produced`
    and the red Delta job and half of FINDINGS.md.
    """
    assert BLOCKS, "no ```mermaid blocks anywhere in the documents"
    assert (REPO / "scripts" / "check_mermaid.mjs").exists(), (
        "the real parser check is gone; these structural rules are not a substitute for it"
    )


@pytest.mark.parametrize(
    ("document", "index", "source"),
    BLOCKS,
    ids=[f"{d.name}#{i}" for d, i, _ in BLOCKS],
)
def test_every_diagram_is_structurally_a_diagram(document: Path, index: int, source: str) -> None:
    """The failures a grammar is not needed for.

    An unclosed `subgraph` and an unbalanced bracket are both parse errors, and both are what a
    hand edit produces. Catching them here means the answer arrives in a second rather than
    after a push.
    """
    lines = [line for line in source.splitlines() if line.strip()]
    assert lines, f"{document.name} block {index} is empty"
    assert lines[0].strip().startswith(MERMAID_HEADERS), (
        f"{document.name} block {index} starts with {lines[0].strip()!r}, which is not one of "
        f"{MERMAID_HEADERS}. GitHub renders an unrecognised diagram as a raw code block."
    )
    opened = sum(1 for line in lines if line.strip().startswith("subgraph "))
    closed = sum(1 for line in lines if line.strip() == "end")
    assert opened == closed, (
        f"{document.name} block {index}: {opened} subgraph(s) and {closed} end(s)"
    )
    for line in lines:
        assert line.count('"') % 2 == 0, f"{document.name} block {index}: odd quotes in {line!r}"
        for left, right in (("[", "]"), ("(", ")"), ("{", "}")):
            assert line.count(left) == line.count(right), (
                f"{document.name} block {index}: unbalanced {left}{right} in {line!r}"
            )
    # A label with a bare `<br>` is fine in mermaid and a label with an unquoted comma is not,
    # so what is checked is the thing that is unambiguous: every label this repository writes is
    # quoted, which is what lets it contain punctuation at all.
    for line in lines:
        for shape in ("[", "{"):
            position = line.find(shape)
            if position > 0 and line[position - 1].isalnum():
                rest = line[position + 1 :].lstrip("([{")
                assert rest.startswith('"'), (
                    f"{document.name} block {index}: unquoted label in {line!r}. Quote it, or "
                    f"a comma or a bracket inside it becomes syntax."
                )
