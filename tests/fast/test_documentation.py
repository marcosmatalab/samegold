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
# A figure makes three kinds of claim, and only one of them was gated when the figures landed:
#
#   NAMES   the boxes name tables the lane creates          - checked from the first commit
#   EDGES   the arrows are reads the lane actually does     - found by rendering it and looking,
#                                                             which is not a gate
#   NUMBERS the digits drawn are the digits in the record   - typed, in a repository that has
#                                                             rendered anchors for exactly this
#
# All three are checked here, from the repository rather than from a literal: names and edges
# out of `databricks/src/` by AST, numbers out of the canonical evidence record. A number
# compared against a constant in this file would only have moved the typed digit from the SVG
# into the Python.
#
# The edges property earned its place immediately. The first figure drew the chain
# bronze -> classified -> events -> gold, which reads well and is wrong three times: the lane
# reads `silver_events` from `bronze_events`, and gold reads `silver_classified`. The docstring
# on `silver_events` says "Gold reads `silver_classified`, not this one" and the picture said
# otherwise, because nothing compared them.
#
# ONE test, three properties, each failing on its own: a renamed table reddens NAMES, a
# reversed arrow reddens EDGES, a changed digit reddens NUMBERS, and none of the three reddens
# another. A mutation that reddens two would mean this is checking one thing badly rather than
# three things.


def _digits_only(text: str) -> str:
    """`14 198 046` -> `14198046`. ONE place, because two would disagree eventually.

    The figures group digits for a reader and the record stores an integer, so the two can only
    be compared through a normaliser. It is here, it is used by nothing else, and
    `test_the_digit_normaliser` is its own case - a comparison whose normaliser is untested
    passes for the wrong reason the day the grouping character changes.
    """
    return re.sub(r"[\s\u00a0\u202f]", "", text)


@pytest.mark.parametrize(
    ("grouped", "expected"),
    [
        ("14 198 046", "14198046"),
        ("25 582 615", "25582615"),
        ("199 379", "199379"),
        ("14\u00a0198\u00a0046", "14198046"),  # a non-breaking space groups too
        ("1158", "1158"),
        ("", ""),
    ],
)
def test_the_digit_normaliser(grouped: str, expected: str) -> None:
    """The one place grouped digits become comparable, with its own cases."""
    assert _digits_only(grouped) == expected


FIGURES = (
    "pipeline-light.svg",
    "pipeline-dark.svg",
    "restatement-light.svg",
    "restatement-dark.svg",
)


@pytest.mark.parametrize("figure", FIGURES)
@pytest.mark.evidence_dependent
def test_the_figures_agree_with_the_repository(figure: str) -> None:
    """Three properties of one picture, each falsifiable on its own.

    The direction is figure -> repository throughout: a diagram may leave a table, an edge or a
    figure out, and may not invent one. Omission is editorial; invention is a false statement
    with better typography.
    """
    svg = _figure(figure)
    tables, lane_edges = _lane_tables_and_edges()
    assert tables and lane_edges, "nothing was parsed out of databricks/src/; the reader broke"
    failures: list[str] = []

    # ---- NAMES. A box names a table; free text is prose. `order_placed` is an event type
    # drawn beside a timeline, and reading it as a table would make the figure claim something
    # it never says.
    drawn_names = {
        text for text in _boxes(svg) if "_" in text and re.fullmatch(r"[a-z][a-z0-9_]*", text)
    }
    invented = sorted(drawn_names - tables)
    if invented:
        failures.append(
            f"NAMES: {figure} names {invented}, which databricks/src/ does not create. Either "
            f"a table was renamed and the picture was not, or the picture invented a name. "
            f"The lane declares {sorted(tables)}."
        )

    # ---- EDGES
    wrong = sorted(_drawn_edges(svg, tables) - lane_edges)
    if wrong:
        failures.append(
            f"EDGES: {figure} draws {wrong}, which the lane does not read. The reads it does "
            f"are {sorted(lane_edges)}. An arrow is a claim about the dataflow; a wrong one is "
            f"a false statement that renders beautifully."
        )

    # ---- NUMBERS
    known = _record_numbers()
    for grouped in _drawn_numbers(svg):
        digits = _digits_only(grouped)
        if digits not in known:
            failures.append(
                f"NUMBERS: {figure} draws {grouped!r} ({digits}), which is not a value in "
                f"evidence/databricks/SG-DBX-01.json. Figures quote the record like every "
                f"other published figure here, or they go stale where nobody is looking."
            )

    assert not failures, "\n\n".join(failures)


def _lane_tables_and_edges() -> tuple[set[str], set[tuple[str, str]]]:
    """What `databricks/src/` declares: the table names, and the reads between them.

    Parsed, never grepped, so a name in a comment or a docstring cannot satisfy either. Two
    ways this lane creates a table and two ways it reads one:

      * created by the `name=` keyword of a pipeline decorator, by the `target=` of an AUTO CDC
        flow, or by a `CREATE TABLE` in the close notebook's SQL;
      * read by `spark.readStream.table("x")`, by the `source=` of an AUTO CDC flow, or by a
        `FROM x` inside a `spark.sql` string - which is how `revenue_by_month` reads, and
        leaving it out would have made the busiest table in the figure edgeless.
    """
    names: set[str] = set()
    edges: set[tuple[str, str]] = set()

    def read_by(node: ast.AST) -> set[str]:
        found: set[str] = set()
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Call):
                continue
            if isinstance(inner.func, ast.Attribute) and inner.func.attr == "table":
                for arg in inner.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        found.add(arg.value)
            for arg in inner.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    found.update(re.findall(r"\bFROM\s+([a-z][a-z0-9_]*)", arg.value))
        return found

    for path in sorted((REPO / "databricks" / "src").glob("*.py")):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        names.update(re.findall(r"CREATE TABLE IF NOT EXISTS \{catalog\}\.\w+\.(\w+)", source))

        for node in ast.walk(tree):
            # A bare `dp.create_auto_cdc_flow(target=..., source=...)` is an edge on its own.
            if isinstance(node, ast.Call):
                kw = {k.arg: k.value for k in node.keywords}
                target, sourced = kw.get("target"), kw.get("source")
                if isinstance(target, ast.Constant) and isinstance(target.value, str):
                    names.add(target.value)
                    if isinstance(sourced, ast.Constant) and isinstance(sourced.value, str):
                        edges.add((sourced.value, target.value))
                nm = kw.get("name")
                if isinstance(nm, ast.Constant) and isinstance(nm.value, str):
                    names.add(nm.value)

            # A decorated function produces a table and reads the ones in its body.
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                produced = None
                for decorator in node.decorator_list:
                    if not isinstance(decorator, ast.Call):
                        continue
                    for keyword in decorator.keywords:
                        if keyword.arg in {"name", "target"} and isinstance(
                            keyword.value, ast.Constant
                        ):
                            produced = keyword.value.value
                if isinstance(produced, str):
                    edges.update((src, produced) for src in read_by(node))

    return names, edges


def _figure(name: str) -> str:
    return (REPO / "docs" / "img" / name).read_text(encoding="utf-8")


def _boxes(svg: str) -> dict[str, tuple[float, float, float, float]]:
    """Every labelled box, as name -> (x, y, w, h). A label is the text inside a rect."""
    rects = [
        (float(m[0]), float(m[1]), float(m[2]), float(m[3]))
        for m in re.findall(
            r'<rect x="([\d.]+)" y="([\d.]+)" width="([\d.]+)" height="([\d.]+)"', svg
        )
    ]
    out: dict[str, tuple[float, float, float, float]] = {}
    for tx, ty, text in re.findall(r'<text x="([\d.]+)" y="([\d.]+)"[^>]*>([^<]+)</text>', svg):
        x, y = float(tx), float(ty)
        for rx, ry, rw, rh in rects:
            if rx <= x <= rx + rw and ry <= y <= ry + rh:
                out.setdefault(text.strip(), (rx, ry, rw, rh))
                break
    return out


def _touches(point: tuple[float, float], geometry: tuple[float, float, float, float]) -> bool:
    """Is this point on the boundary of that box? Arrows are drawn edge to edge."""
    px, py = point
    x, y, w, h = geometry
    eps = 1.5
    inside_x = x - eps <= px <= x + w + eps
    inside_y = y - eps <= py <= y + h + eps
    on_vertical = abs(px - x) <= eps or abs(px - (x + w)) <= eps
    on_horizontal = abs(py - y) <= eps or abs(py - (y + h)) <= eps
    return (inside_x and inside_y) and (on_vertical or on_horizontal)


def _drawn_edges(svg: str, tables: set[str]) -> set[tuple[str, str]]:
    """Arrows whose two ends are both lane tables. Others are not claims about the dataflow."""
    boxes = {name: geometry for name, geometry in _boxes(svg).items() if name in tables}
    drawn: set[tuple[str, str]] = set()
    for d in re.findall(r'<path d="([^"]+)"[^>]*marker-end', svg):
        points = [(float(a), float(b)) for a, b in re.findall(r"[ML]\s+([\d.-]+)\s+([\d.-]+)", d)]
        if len(points) < 2:
            continue
        starts = [n for n, g in boxes.items() if _touches(points[0], g)]
        ends = [n for n, g in boxes.items() if _touches(points[-1], g)]
        if len(starts) == 1 and len(ends) == 1 and starts[0] != ends[0]:
            drawn.add((starts[0], ends[0]))
    return drawn


# A figure quotes a money figure grouped (`14 198 046`) or bare. Both are ANCHORED, so a label
# like `v0  14 198 046` yields the amount and not `014198046` - which is what the first version
# of this extracted, reporting a failure about itself rather than about the picture.
_GROUPED = re.compile(r"(?<!\w)(\d{1,3}(?:[\s\u00a0\u202f]\d{3})+)(?!\w)")
_BARE = re.compile(r"(?<![\w.])(\d{5,})(?![\w.])")


def _drawn_numbers(svg: str) -> set[str]:
    """Every money-sized figure drawn in the picture, grouped or bare.

    Five digits is the floor for an ungrouped run so that a year is never read as a
    measurement.
    """
    found: set[str] = set()
    for text in re.findall(r">([^<>]+)<", svg):
        found.update(_GROUPED.findall(text))
        found.update(_BARE.findall(text))
    return found


def _record_numbers() -> set[str]:
    """Every integer the canonical Databricks record holds, as a string of digits."""
    record = json.loads(
        (REPO / "evidence" / "databricks" / "SG-DBX-01.json").read_text(encoding="utf-8")
    )
    found: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, bool):
            return
        if isinstance(node, int):
            found.add(str(node))
        elif isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(record)
    return found


def test_the_two_themes_of_a_figure_say_exactly_the_same_thing() -> None:
    """The light and dark files must not drift apart.

    They are two files because GitHub's `<picture>` is the only reliable way to switch on the
    reader's theme, and two files is two things to edit. Colours may differ; every word must
    not. This is what stops a correction landing in the theme the author happens to use.
    """
    for name in ("pipeline", "restatement"):
        light = _figure(f"{name}-light.svg")
        dark = _figure(f"{name}-dark.svg")
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
                f"{path.name} contains {forbidden!r}, which GitHub's markdown sanitiser "
                f"removes. Colours and strokes must be presentation attributes on each element."
            )
        assert "https://" not in svg.replace('xmlns="http://www.w3.org/2000/svg"', ""), (
            f"{path.name} references something external. Nothing outside the file is fetched "
            f"when GitHub renders it."
        )
        ET.fromstring(svg)
