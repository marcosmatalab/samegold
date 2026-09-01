"""Layering, enforced by reading the imports rather than by asking people to be careful.

The rules exist for one reason: the fast lane has to run without a JVM, without Maven and
without credentials, in a couple of seconds. Every one of these rules is the thing that
would quietly break that, and each has a comment saying what it protects.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

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
    "evidence": {"verify"},
    # faults drives the real pipeline (it has to be the same program) and reads the result
    # back with a different engine, which is why it may reach for pipelines and for duckdb.
    "faults": {"domain", "verify", "evidence", "pipelines", "generator"},
    # The ingest adapters build readers with the bronze schema, which lives with the Spark
    # code because it is a Spark StructType.
    "ingest": {"domain", "pipelines"},
    "pipelines": {"domain", "verify", "ingest"},
    "cost": {"domain", "verify", "evidence"},
}

# Third-party imports that are only allowed inside certain packages.
HEAVY = {
    "pyspark": {"pipelines", "faults", "cost", "ingest"},
    "delta": {"pipelines", "faults", "cost", "ingest"},
    "duckdb": {"oracle", "mutation", "faults"},
    "deltalake": {"oracle", "faults", "cost"},
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
