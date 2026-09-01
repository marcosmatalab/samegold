"""Layering, enforced by reading the imports rather than by asking people to be careful.

The rules exist for one reason: the fast lane has to run without a JVM, without Maven and
without credentials, in a couple of seconds. Every one of these rules is the thing that
would quietly break that, and each has a comment saying what it protects.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SRC = Path(__file__).resolve().parents[2] / "src" / "samegold"

# package -> packages it may import from samegold
ALLOWED: dict[str, set[str]] = {
    # The contract and the rules are pure. If they ever import an engine, the rules stop
    # being checkable in milliseconds and the mutation campaign stops being cheap.
    "domain": set(),
    "verify": {"domain"},
    "generator": {"domain"},
    "oracle": {"domain"},
    "mutation": {"domain", "verify", "oracle"},
    # evidence recomputes seeds to validate a record, which is the whole point of the gate.
    "evidence": {"verify", "generator"},
    # faults drives the real pipeline (it has to be the same program) and reads the result
    # back with a different engine, which is why it may reach for pipelines and for duckdb.
    "faults": {"domain", "verify", "evidence", "pipelines", "generator"},
    # The ingest adapters build readers with the bronze schema, which lives with the Spark
    # code because it is a Spark StructType.
    "ingest": {"domain", "pipelines"},
    "pipelines": {"domain", "verify", "ingest"},
    "cost": {"domain", "verify", "evidence"},
    # governance masks rows on their way into gold and purges Delta tables, so it needs the
    # contract and delta-rs and nothing else.
    "governance": {"domain"},
}

# Third-party imports that are only allowed inside certain packages.
HEAVY = {
    "pyspark": {"pipelines", "faults", "cost", "ingest"},
    "delta": {"pipelines", "faults", "cost", "ingest"},
    "duckdb": {"oracle", "mutation", "faults"},
    "deltalake": {"oracle", "faults", "cost", "governance"},
    "sqlglot": {"mutation"},
}


def _modules() -> list[tuple[str, Path]]:
    out = []
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC)
        package = rel.parts[0] if len(rel.parts) > 1 else ""
        out.append((package, path))
    return out


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module)
    return names


@pytest.mark.parametrize(("package", "path"), _modules(), ids=lambda v: str(v))
def test_layering(package: str, path: Path) -> None:
    if package not in ALLOWED:
        return  # top-level modules (cli, claims) are the composition root and may import all
    for name in _imports(path):
        if name.startswith("samegold."):
            target = name.split(".")[1]
            assert target in ALLOWED[package] | {package}, (
                f"{path.relative_to(SRC)} imports samegold.{target}, "
                f"which package '{package}' may not depend on"
            )


@pytest.mark.parametrize(("package", "path"), _modules(), ids=lambda v: str(v))
def test_heavy_dependencies_stay_in_their_lane(package: str, path: Path) -> None:
    if package == "":
        # cli.py and claims.py are the composition root: their job is to reach into every
        # lane. They import the heavy dependencies INSIDE the functions that need them, which
        # is what keeps the fast lane free of a JVM, and tests/fast/test_architecture.py's
        # last test is the one that checks that property directly.
        return
    for name in _imports(path):
        root = name.split(".")[0]
        if root in HEAVY and package not in HEAVY[root] and path.name != "record.py":
            pytest.fail(
                f"{path.relative_to(SRC)} imports {root}, which is only allowed in "
                f"{sorted(HEAVY[root])}. The fast lane must run with none of them installed."
            )


def test_the_fast_lane_does_not_need_pyspark() -> None:
    """A regression guard for the property people actually feel: `make fast` with no JVM."""
    import sys

    assert "pyspark" not in sys.modules, (
        "importing the fast lane pulled in pyspark; something in domain/verify/generator "
        "started depending on Spark and the fast lane just became a 25-second lane"
    )


def test_every_test_that_reads_the_repository_evidence_is_marked() -> None:
    """A hand-maintained deselection list will be wrong; this is the guard on the marker.

    SG-00 runs the fast lane with `-m "not evidence_dependent"` because two tests compare the
    documents with the evidence that claim is in the middle of writing. The previous mechanism
    deselected ONE of them by node id, a second such test was added later, and SG-00 then
    recorded `fast_lane_green: false` on every commit. Replacing the node id with a marker
    fixed that instance and left the same failure mode one file away: nothing said that a
    third such test had to carry the marker.

    "Reads the repository's own evidence" is detectable: the test opens `REPO / "evidence"`
    or calls `check_readme` on a path under `REPO`. That is exactly the set that has to be
    deselected, and it is now computed rather than remembered.
    """
    import re

    marked_pattern = re.compile(
        r"@pytest\.mark\.evidence_dependent\s*\ndef (test_\w+)", re.MULTILINE
    )
    reads_evidence = re.compile(r'REPO\s*/\s*"evidence"|check_readme\(\s*REPO|EvidenceStore\(REPO')
    offenders: list[str] = []
    for path in sorted((REPO / "tests" / "fast").glob("test_*.py")):
        source = path.read_text(encoding="utf-8")
        marked = set(marked_pattern.findall(source))
        # Split into per-test bodies so a module-level import cannot mark every test in it.
        bodies = re.split(r"\ndef (test_\w+)", source)
        for name, body in zip(bodies[1::2], bodies[2::2], strict=True):
            if name == "test_every_test_that_reads_the_repository_evidence_is_marked":
                continue  # this one quotes the pattern, it does not read the evidence
            if reads_evidence.search(body) and name not in marked:
                offenders.append(f"{path.name}::{name}")
    assert not offenders, (
        f"these tests read the repository's own evidence and are not marked "
        f"evidence_dependent, so SG-00 will run them while writing that evidence: {offenders}"
    )
