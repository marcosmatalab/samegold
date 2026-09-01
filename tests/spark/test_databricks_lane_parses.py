"""Every SQL statement in the Databricks lane goes through a real parser.

No Free Edition workspace is available in CI (and a single run there can exhaust the daily
quota), so this lane is written and not run. That is a defensible position for SEMANTICS. It
is not a defensible position for SYNTAX: an adversarial review found that
``databricks/src/gold_close.py`` had a missing comma before its ``gross`` CTE, which makes the
whole statement a parse error, which means the pipeline would have failed on its first
refresh. The file had been reviewed, documented and committed, and nothing had ever handed it
to a parser.

Spark's own parser is available locally and answers exactly that question, in milliseconds
and with no cluster. It cannot tell us the close is right; it can tell us the close is a
statement. Those are different guarantees and the README says which one this is.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LANE = REPO / "databricks"


# Statements are extracted from the notebook sources rather than kept in fixture files, so
# the thing parsed here is the thing that would be deployed.
def _sql_calls(source: str) -> list[str]:
    """Every `spark.sql(...)` argument in a module, found with the Python parser.

    A regex over triple-quoted strings missed the single-line calls and the implicitly
    concatenated ones: it collected 4 of the 6 statements in this lane and the test then
    asserted "at least 5 statements" and passed. Walking the AST cannot miss a call, and a
    literal it cannot reconstruct (a runtime-built query) raises rather than being skipped.
    """
    out: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr != "sql":
            continue
        # The query can be passed positionally or as `sqlQuery=`. Skipping the keyword form
        # silently was how the first version of this walk collected four of six statements
        # while the test asserted "at least five" and passed.
        argument = node.args[0] if node.args else None
        if argument is None:
            argument = next((kw.value for kw in node.keywords if kw.arg == "sqlQuery"), None)
        if argument is None:
            raise AssertionError(f"spark.sql() with no readable query at line {node.lineno}")
        out.append(_literal(argument))
    return out


def _literal(node: ast.expr) -> str:
    """Reconstruct a string literal, an f-string or a concatenation of them."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        # An interpolation is a bundle identifier or a timestamp; both are replaced by the
        # substitutions below, so the placeholder is written back in its `{name}` form.
        return "".join(
            part.value if isinstance(part, ast.Constant) else "{" + ast.unparse(part.value) + "}"  # type: ignore[union-attr]
            for part in node.values
        )
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _literal(node.left) + _literal(node.right)
    raise AssertionError(
        f"spark.sql() called with something this test cannot read: {ast.dump(node)}"
    )


# The expectation predicates are SQL too, and they are where the NULL-safety bug lived: they
# are not inside a spark.sql() call, so the first version of this test collected none of them
# and reported "6 statements" while the lane had more. A predicate is wrapped in a SELECT so
# the parser has a statement to parse.
_RULES = re.compile(r"^RULES(?:\s*:[^=]+)?\s*=\s*\{(.*?)^\}", re.DOTALL | re.MULTILINE)
# The notebook tasks interpolate a catalog and a close instant. Substituting plausible values
# is enough to make the statement parseable while leaving every clause intact.
_SUBSTITUTIONS = {
    "${catalog}": "samegold",
    "{catalog}": "samegold",
    "{as_of}": "2026-03-05 22:59:59",
    "{pipeline_id}": "0000-000000-abcdefgh",
}


def _resolve(statement: str) -> str:
    for placeholder, value in _SUBSTITUTIONS.items():
        statement = statement.replace(placeholder, value)
    return statement


def _statements() -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for path in sorted(LANE.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        for index, statement in enumerate(_sql_calls(source)):
            out.append((f"{path.relative_to(REPO)}#{index}", _resolve(statement)))
    for path in sorted((LANE / "sql").rglob("*.sql")):
        out.append((str(path.relative_to(REPO)), _resolve(path.read_text(encoding="utf-8"))))
    out.extend(_expectations())
    return out


def _expectations() -> list[tuple[str, str]]:
    """Every pipeline expectation predicate, wrapped in a SELECT so it can be parsed."""
    out: list[tuple[str, str]] = []
    for path in sorted(LANE.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        module: dict[str, object] = {}
        blocks = list(_RULES.finditer(source))
        if not blocks and "expect_all_or_drop" in source:
            raise AssertionError(
                f"{path.name} declares expectations this test cannot read: the rules must be a "
                f"module-level dict literal named RULES"
            )
        # Evaluate only the literals the rules are built from, never the module: importing it
        # would need pyspark.pipelines and a live session. The helper strings are plain
        # concatenations of literals, which ast.literal_eval-style execution handles safely
        # enough for a test that then throws the namespace away.
        helpers = re.findall(
            r"^(_[A-Z_]+)(?:\s*:[^=]+)?\s*=\s*\((.*?)^\)", source, re.DOTALL | re.MULTILINE
        )
        for name, body in helpers:
            module[name] = eval(f"({body})", {"__builtins__": {}}, {})
        for block in blocks:
            rules = eval("{" + block.group(1) + "}", {"__builtins__": {}}, module)
            for name, predicate in rules.items():
                out.append(
                    (
                        f"{path.relative_to(REPO)}::expectation:{name}",
                        f"SELECT * FROM t WHERE {predicate}",
                    )
                )
    return out


# Constructs that exist in the Databricks SQL dialect and not in the OSS parser. Excluding
# them is the honest boundary of this test: a statement here is NOT checked by anything, and
# saying which ones those are is worth more than a test that silently skipped them. The list
# is closed, and test_the_exclusions_are_the_ones_claimed below fails if a statement is
# excluded for any other reason.
DATABRICKS_ONLY = ("SET ROW FILTER", "ALTER COLUMN", "CLUSTER BY AUTO")


def _is_databricks_only(statement: str) -> bool:
    """Comments stripped first: a construct named in a comment is not a construct.

    `-- we deliberately do not ALTER COLUMN here` would otherwise exclude the statement it
    describes from the only check that reads it.
    """
    code = "\n".join(
        line for line in statement.splitlines() if not line.strip().startswith("--")
    ).upper()
    return any(construct in code for construct in DATABRICKS_ONLY)


ALL_STATEMENTS = [
    (f"{name}::{index}", part)
    for name, statement in _statements()
    for index, part in enumerate(s for s in statement.split(";") if s.strip())
]
STATEMENTS = [(name, part) for name, part in ALL_STATEMENTS if not _is_databricks_only(part)]
EXCLUDED = [(name, part) for name, part in ALL_STATEMENTS if _is_databricks_only(part)]


def test_there_is_something_to_parse() -> None:
    """A regex that silently matches nothing would make every test below vacuously green."""
    assert len(STATEMENTS) >= 12, f"only found {len(STATEMENTS)} statements in the lane"


def test_the_exclusions_are_the_ones_claimed() -> None:
    """Nothing is excluded except for a named Databricks-only construct.

    Without this, "the OSS parser does not understand it" becomes a way to make any
    inconvenient statement disappear from the check.
    """
    # An explicit list, not a re-application of the predicate that built it. Asserting
    # `_is_databricks_only(part)` over a list filtered BY `_is_databricks_only` is a loop that
    # can never fail, which is the shape of a test that exists to be counted.
    assert [name for name, _ in EXCLUDED] == [
        "databricks/sql/policies.sql::2",
        "databricks/sql/policies.sql::3",
    ], [name for name, _ in EXCLUDED]


@pytest.mark.spark
@pytest.mark.parametrize("name,statement", STATEMENTS, ids=[n for n, _ in STATEMENTS])
def test_the_statement_parses(spark, name: str, statement: str) -> None:  # type: ignore[no-untyped-def]
    """Parse only. The tables do not exist here, so analysis is not attempted.

    ``spark.sql(...)`` would resolve names and fail on the missing tables, which is a
    different error and would hide the syntax one. The parser is reached directly.
    """
    parser = spark._jsparkSession.sessionState().sqlParser()
    try:
        parser.parsePlan(statement)
    except Exception as error:  # pragma: no cover - the failure message is the point
        pytest.fail(f"{name} does not parse: {error}")
