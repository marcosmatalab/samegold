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
        if root in HEAVY and package not in HEAVY[root]:
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


def test_recording_the_environment_does_not_import_what_it_reports_on() -> None:
    """The version fingerprint is read from distribution metadata, never by importing.

    It used to be `__import__("pyspark")`, which answers the question and leaves `pyspark`,
    `delta` and `py4j` in `sys.modules`. Every fast-lane test that writes an evidence record
    therefore tripped the session hook in `conftest.py` and the fast lane exited 1 on any
    machine with the Spark extras installed - two files, sixteen tests, on both machines this
    project is developed on, while pytest's summary line still read `57 passed` and nobody read
    the exit status. The `fast` workflow, which has no Spark to import, stayed green throughout.

    So this is not a style rule. It is the check that the fast lane's own gate is measuring the
    thing it claims to measure, on the machines where it can be wrong.
    """
    import sys

    from samegold.evidence.record import environment_fingerprint

    before = set(sys.modules)
    fingerprint = environment_fingerprint()
    leaked = sorted(
        name
        for name in set(sys.modules) - before
        if name.split(".")[0] in {"pyspark", "delta", "py4j", "deltalake", "duckdb", "pyarrow"}
    )
    assert not leaked, (
        f"recording the environment imported {leaked}. It is read from installed distribution "
        f"metadata precisely so that it does not have to."
    )
    # And it is not answering "absent" to everything, which would pass the check above while
    # recording nothing. Whatever this machine has, the fingerprint has to name it: python and
    # platform are always there, and duckdb is a hard dependency of every lane.
    assert fingerprint["python"] and fingerprint["platform"]
    assert fingerprint["duckdb"] != "absent", (
        "duckdb is a required dependency and the fingerprint calls it absent, so the metadata "
        "lookup is failing and every version in every evidence record is a blank"
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


# ------------------------------------------------------------------ the numpy shadow
#
# `typings/numpy/` is an empty stand-in that exists so mypy can start at all (numpy 2.x ships
# stubs written in 3.12 syntax, and this project declares python_version 3.11, so mypy refuses
# to PARSE them). Its docstring states the cost of that trade in one sentence:
#
#     "if this project ever uses numpy directly, this file turns its API into `Any` and must
#      be deleted. tests/fast/test_architecture.py is where an import of it would be refused."
#
# That sentence was true about the intent and false about the repository: no such test
# existed, and `grep numpy tests/fast/test_architecture.py` returned nothing. A file whose
# entire job is to declare a cost honestly was claiming a guard that was not installed - the
# same shape as the eleven rounds of "written and not executed here". The guard is below.

SHADOWED = REPO / "typings" / "numpy" / "__init__.pyi"
# Every directory mypy checks or ruff lints, which is every directory whose imports the shadow
# would silently turn into `Any`.
SHADOW_SCOPE = ("src", "tests", "pipelines", "databricks")


def _python_files() -> list[Path]:
    out: list[Path] = []
    for directory in SHADOW_SCOPE:
        for path in sorted((REPO / directory).rglob("*.py")):
            if "__pycache__" not in path.parts:
                out.append(path)
    return out


def test_nothing_imports_numpy_while_the_stub_shadows_it() -> None:
    """The refusal `typings/numpy/__init__.pyi` promises, actually installed.

    The stub defines `__getattr__(name) -> Any`, so mypy accepts EVERY attribute of numpy
    without checking one of them. An import of numpy under the shadow is therefore not a
    type error and never will be: it is a silent hole, and the only thing that can close it
    is a test that reads the imports.
    """
    files = _python_files()
    # A glob that matched nothing would make this vacuous, which is the failure mode this
    # whole file exists to catch elsewhere.
    assert len(files) > 30, f"only found {len(files)} modules under {SHADOW_SCOPE}"

    offenders = sorted(
        str(path.relative_to(REPO))
        for path in files
        if any(name.split(".")[0] == "numpy" for name in _imports(path))
    )
    if not SHADOWED.exists():
        # The shadow is gone, so the rule is gone with it - but then pyproject.toml must not
        # still point mypy at a `typings/` that no longer shadows anything. Without this
        # branch the test above passes for the wrong reason the moment someone deletes the
        # stub, which is exactly the state it is meant to detect.
        pyproject = (REPO / "pyproject.toml").read_text(encoding="utf-8")
        assert 'mypy_path = "typings"' not in pyproject, (
            'typings/numpy/__init__.pyi is gone but pyproject.toml still sets mypy_path = "typings"'
        )
        return
    assert not offenders, (
        f"{offenders} import numpy, and typings/numpy/ shadows it with a stub whose "
        f"__getattr__ returns Any: every numpy call in those files would be type-checked "
        f"against nothing. Delete typings/ first, and the mypy_path line in pyproject.toml "
        f"with it - and then deal with the problem the shadow was hiding: mypy cannot parse "
        f"the numpy 2.x stubs under the python_version = 3.11 this project declares."
    )


def test_the_stub_is_only_a_stub() -> None:
    """The shadow must stay empty: a stub that starts describing numpy starts being wrong.

    The trade the file documents is "nothing here imports numpy, so `Any` costs nothing". A
    stub with real signatures in it would be a hand-maintained copy of another project's API,
    checked by nobody, and the sentence in its docstring would stop being true.
    """
    if not SHADOWED.exists():
        return
    for stub in sorted((REPO / "typings").rglob("*.pyi")):
        tree = ast.parse(stub.read_text(encoding="utf-8"))
        declarations = [
            node
            for node in tree.body
            # The module docstring is an Expr holding a string constant; everything else is a
            # declaration. Counting source lines instead counted the docstring's prose.
            if not (
                isinstance(node, ast.Expr)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
            )
        ]
        rendered = [ast.unparse(node).splitlines()[0] for node in declarations]
        assert len(declarations) <= 2, (
            f"{stub.relative_to(REPO)} has grown real declarations: {rendered}. The shadow is "
            f"a parser workaround, not a stub package anyone maintains."
        )
