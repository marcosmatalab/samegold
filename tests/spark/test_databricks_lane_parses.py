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

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LANE = REPO / "databricks"

# Statements are extracted from the notebook sources rather than kept in fixture files, so
# the thing parsed here is the thing that would be deployed.
_TRIPLE = re.compile(r'spark\.sql\(\s*f?"""(.*?)"""\s*\)', re.DOTALL)
# The notebook tasks interpolate a catalog and a close instant. Substituting plausible values
# is enough to make the statement parseable while leaving every clause intact.
_SUBSTITUTIONS = {
    "${catalog}": "samegold",
    "{catalog}": "samegold",
    "{as_of}": "2026-03-05 22:59:59",
}


def _resolve(statement: str) -> str:
    for placeholder, value in _SUBSTITUTIONS.items():
        statement = statement.replace(placeholder, value)
    return statement


def _statements() -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for path in sorted(LANE.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        for index, statement in enumerate(_TRIPLE.findall(source)):
            out.append((f"{path.relative_to(REPO)}#{index}", _resolve(statement)))
    for path in sorted((LANE / "sql").rglob("*.sql")):
        out.append((str(path.relative_to(REPO)), _resolve(path.read_text(encoding="utf-8"))))
    return out


# Constructs that exist in the Databricks SQL dialect and not in the OSS parser. Excluding
# them is the honest boundary of this test: a statement here is NOT checked by anything, and
# saying which ones those are is worth more than a test that silently skipped them. The list
# is closed, and test_the_exclusions_are_the_ones_claimed below fails if a statement is
# excluded for any other reason.
DATABRICKS_ONLY = ("SET ROW FILTER", "ALTER COLUMN", "CLUSTER BY AUTO")


def _is_databricks_only(statement: str) -> bool:
    return any(construct in statement.upper() for construct in DATABRICKS_ONLY)


ALL_STATEMENTS = [
    (f"{name}::{index}", part)
    for name, statement in _statements()
    for index, part in enumerate(s for s in statement.split(";") if s.strip())
]
STATEMENTS = [(name, part) for name, part in ALL_STATEMENTS if not _is_databricks_only(part)]
EXCLUDED = [(name, part) for name, part in ALL_STATEMENTS if _is_databricks_only(part)]


def test_there_is_something_to_parse() -> None:
    """A regex that silently matches nothing would make every test below vacuously green."""
    assert len(STATEMENTS) >= 5, f"only found {len(STATEMENTS)} statements in the lane"


def test_the_exclusions_are_the_ones_claimed() -> None:
    """Nothing is excluded except for a named Databricks-only construct.

    Without this, "the OSS parser does not understand it" becomes a way to make any
    inconvenient statement disappear from the check.
    """
    assert len(EXCLUDED) <= 2, [name for name, _ in EXCLUDED]
    for name, part in EXCLUDED:
        assert _is_databricks_only(part), name


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
