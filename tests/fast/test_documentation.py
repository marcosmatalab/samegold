"""The documentation is checked like code, because it was wrong like code.

An adversarial review found eleven broken file references, a test that asserted a string that
only survived in a comment, an ADR describing a test that did not exist, and four milestone
pointers to the wrong milestone. Every one of those is the same failure: prose that nothing
executes. These tests execute it.
"""

from __future__ import annotations

import ast
import json
import re
import xml.etree.ElementTree as ET
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
# with **mermaid 11.17.2**, on Node 20.20.2 and on Node 22.23.2 - the runner's version - and all
# three parse as `flowchart-v2`; falsified twice against the committed text - `G -->> E`, which
# is a sequence-diagram arrow, and a `subgraph` with its `end` removed - reporting the line
# number of each and exiting non-zero.
#
# The two node versions are in that sentence because the first push failed on one of them: Node
# 21 made `globalThis.navigator` a getter with no setter, so the assignment the script used
# threw on the runner and worked in WSL.
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


# ------------------------------------------------------ the diagram is checked like a sentence
#
# The repository had no images at all until 8 September 2026, and a reviewer decides in thirty
# seconds. Two SVGs is the whole budget, and both are hand-written and committed rather than
# exported, so a change to one is a diff and not a new binary.
#
# The risk a picture adds is the one this repository already has a gate for one document along:
# a diagram is prose with boxes, and it goes stale the same way. `docs/databricks-run.md` said
# `NOT RUN` beside twenty measured anchors for four days because nothing read the sentence. A
# figure naming `silver_events` after somebody renamed the table would be the same defect with
# better typography.
#
# So the table names in figure 1 are read out of the CODE THAT DECLARES THEM. Two sources,
# because this lane has two ways of creating a table: the `name=`/`target=` keyword of a
# declarative-pipeline decorator, and a `CREATE TABLE` in the close notebook's SQL.


def _tables_the_lane_declares() -> set[str]:
    """Every table name `databricks/src/` creates, from the two ways it creates one."""
    names: set[str] = set()
    for path in sorted((REPO / "databricks" / "src").glob("*.py")):
        source = path.read_text(encoding="utf-8")
        # `@dp.table(name="...")`, `dp.create_streaming_table(name="...")`,
        # `dp.create_auto_cdc_flow(target="...")` - parsed, not grepped, so a name inside a
        # comment or a docstring cannot satisfy this.
        for node in ast.walk(ast.parse(source)):
            if not isinstance(node, ast.Call):
                continue
            for keyword in node.keywords:
                if keyword.arg not in {"name", "target"}:
                    continue
                value = keyword.value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    names.add(value.value)
        # `CREATE TABLE IF NOT EXISTS {catalog}.main.revenue_closed (` - the close notebook
        # writes its own table in SQL rather than declaring it, and a figure that named it
        # would otherwise be unchecked.
        names.update(re.findall(r"CREATE TABLE IF NOT EXISTS \{catalog\}\.\w+\.(\w+)", source))
    return names


FIGURES = ("pipeline-light.svg", "pipeline-dark.svg")


@pytest.mark.parametrize("figure", FIGURES)
def test_the_figures_name_tables_the_lane_actually_creates(figure: str) -> None:
    """Every monospaced table name in figure 1 is a table `databricks/src/` declares.

    The direction that matters is figure -> code: a diagram may leave a table out, but it may
    not name one that does not exist. A rename that misses the picture fails here.
    """
    svg = (REPO / "docs" / "img" / figure).read_text(encoding="utf-8")
    declared = _tables_the_lane_declares()
    assert declared, "no table names were parsed out of databricks/src/ - the reader is broken"

    # The names drawn in the figure: snake_case words, which is how this lane spells a table
    # and how nothing else in the diagram is spelled.
    drawn = {
        text
        for text in re.findall(r">([a-z][a-z0-9_]*)<", svg)
        if "_" in text and not text.startswith("a-")
    }
    assert drawn, f"{figure} names no tables at all; the figure or this reader has changed"

    unknown = sorted(drawn - declared)
    assert not unknown, (
        f"{figure} names {unknown}, which databricks/src/ does not create. Either the table was "
        f"renamed and the picture was not, or the picture invented a name. The lane declares: "
        f"{sorted(declared)}"
    )


def test_the_two_themes_of_a_figure_say_exactly_the_same_thing() -> None:
    """The light and dark files must not drift apart.

    They are two files because GitHub's `<picture>` is the only reliable way to switch on the
    reader's theme, and two files is two things to edit. Colours may differ; every word must
    not. This is what stops a correction landing in the theme the author happens to use.
    """
    for name in ("pipeline", "restatement"):
        light = (REPO / "docs" / "img" / f"{name}-light.svg").read_text(encoding="utf-8")
        dark = (REPO / "docs" / "img" / f"{name}-dark.svg").read_text(encoding="utf-8")
        assert re.findall(r">([^<>]+)<", light) == re.findall(r">([^<>]+)<", dark), (
            f"{name}-light.svg and {name}-dark.svg carry different text. They are one diagram "
            f"in two palettes; edit both or neither."
        )


def test_the_figures_survive_the_markdown_sanitiser() -> None:
    """GitHub strips what it does not trust, and a stripped diagram is an unstyled mess.

    Checked rather than assumed, because the failure is invisible locally: the file renders
    perfectly in a browser and arrives on GitHub with its colours gone. Presentation
    attributes survive; a stylesheet does not.
    """
    for path in sorted((REPO / "docs" / "img").glob("*.svg")):
        svg = path.read_text(encoding="utf-8")
        for forbidden in ("<style", "class=", "@import", "<script", "<foreignObject", "<image"):
            assert forbidden not in svg, (
                f"{path.name} contains {forbidden!r}, which GitHub's markdown sanitiser removes. "
                f"Colours and strokes have to be presentation attributes on each element."
            )
        # An external font is a font the reader does not get; generic families always resolve.
        assert "https://" not in svg.replace('xmlns="http://www.w3.org/2000/svg"', ""), (
            f"{path.name} references something external. Nothing outside the file is fetched "
            f"when GitHub renders it."
        )
        ET.fromstring(svg)
