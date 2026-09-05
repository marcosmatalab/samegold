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


# A module-level helper that forwards a string to `spark.sql` is still a door into Spark, and
# the statement behind it is still a statement nothing has parsed. `publish_evidence.py` has
# one (`_rows`) so that a failing section can be named in the record instead of taking the
# whole notebook down; the parse test has to see through it, or the helper becomes the way to
# ship SQL that never met a parser - the exact defect this file was written for.
# A module-level helper that forwards a string to `spark.sql` is still a door into Spark, and
# the statement behind it is still a statement nothing has parsed. `publish_evidence.py` has
# one (`_rows`) so that a failing section can be named in the record instead of taking the
# whole notebook down; the parse test has to see through it, or the helper becomes the way to
# ship SQL that never met a parser - the exact defect this file was written for.
SQL_HELPERS = {"_rows"}


def _forwarding_calls(tree: ast.Module) -> set[int]:
    """The `spark.sql(query)` calls INSIDE the bodies of those helpers, by node id.

    Their argument is a parameter name, not a literal, and the statement it will carry is
    collected at the helper's call sites instead. Skipping them by node identity - rather
    than by "the argument is a Name" - keeps a `spark.sql(built_at_runtime)` anywhere else
    in the lane raising, which is the check that matters.
    """
    out: set[int] = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.FunctionDef) and node.name in SQL_HELPERS):
            continue
        for inner in ast.walk(node):
            if (
                isinstance(inner, ast.Call)
                and isinstance(inner.func, ast.Attribute)
                and inner.func.attr == "sql"
            ):
                out.add(id(inner))
    return out


# Statements are extracted from the notebook sources rather than kept in fixture files, so
# the thing parsed here is the thing that would be deployed.
def _sql_calls(source: str) -> list[str]:
    """Every `spark.sql(...)` argument in a module, found with the Python parser.

    A regex over triple-quoted strings missed the single-line calls and the implicitly
    concatenated ones: it collected 4 of the 6 statements in this lane and the test then
    asserted "at least 5 statements" and passed. Walking the AST cannot miss a call, and a
    literal it cannot reconstruct (a runtime-built query) raises rather than being skipped.
    """
    tree = ast.parse(source)
    forwarded = _forwarding_calls(tree)
    constants = _module_constants(tree)
    out: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or id(node) in forwarded:
            continue
        if isinstance(node.func, ast.Attribute):
            if node.func.attr != "sql":
                continue
        elif isinstance(node.func, ast.Name):
            if node.func.id not in SQL_HELPERS:
                continue
        else:
            continue
        # The query can be passed positionally or as `sqlQuery=`. Skipping the keyword form
        # silently was how the first version of this walk collected four of six statements
        # while the test asserted "at least five" and passed.
        argument = node.args[0] if node.args else None
        if argument is None:
            argument = next((kw.value for kw in node.keywords if kw.arg == "sqlQuery"), None)
        if argument is None:
            raise AssertionError(f"spark.sql() with no readable query at line {node.lineno}")
        out.append(_literal(argument, constants))
    return out


def _module_constants(tree: ast.Module) -> dict[str, ast.expr]:
    """Module-level `NAME = <string literal>` bindings, so a named query can be followed.

    A query sometimes has to exist under a name rather than inline: `publish_evidence.py`
    writes the dimension capture's own statement into the capture's header, and a statement
    that appears twice is a statement that can differ from itself. The alternative was to
    inline it and let the header quote something else, which is worse.

    Only the module's own top level, and only string-valued assignments. Anything else still
    reaches `_literal` as a Name it cannot resolve, and still raises - which is the property
    that matters: a runtime-built query must not slip past by being given a name.
    """
    return {
        target.id: node.value
        for node in tree.body
        if isinstance(node, ast.Assign) and len(node.targets) == 1
        for target in node.targets
        if isinstance(target, ast.Name)
        and isinstance(node.value, ast.Constant | ast.JoinedStr | ast.BinOp)
    }


def _literal(node: ast.expr, constants: dict[str, ast.expr] | None = None) -> str:
    """Reconstruct a string literal, an f-string, a concatenation, or a named constant."""
    constants = constants or {}
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
        return _literal(node.left, constants) + _literal(node.right, constants)
    if isinstance(node, ast.Name) and node.id in constants:
        return _literal(constants[node.id], constants)
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
            out.append((f"{path.relative_to(REPO).as_posix()}#{index}", _resolve(statement)))
    for path in sorted((LANE / "sql").rglob("*.sql")):
        out.append((path.relative_to(REPO).as_posix(), _resolve(path.read_text(encoding="utf-8"))))
    out.extend(_expectations())
    return out


def _module_namespace(source: str, path: Path) -> dict[str, object]:
    """Evaluate the module's top-level string/dict assignments, IN SOURCE ORDER.

    The previous version pulled the helpers out with one regex and the `RULES` dict with
    another, and evaluated all the helpers first. That worked only while every helper was
    built from literals. `_REASON` is now DERIVED from `RULES` - which is the point of it, so
    that the expectations and the classification cannot be two renderings of the same rule
    that disagree - and evaluating it before `RULES` existed raised NameError at collection
    time, taking down the whole module.

    Source order is the only order that works when declarations depend on each other, and the
    parser knows the source order. Anything this cannot evaluate raises, because a helper
    silently skipped is a predicate silently unparsed.

    Module-level FUNCTIONS are evaluated too, and that is round eighteen rather than reach for
    its own sake. `_REASON` is now `_classification(RULES)`: the rendering is a function so a
    test can hand it a rule that is NULL by construction and watch the row be quarantined,
    which is the only way to observe that property once the real rules are all decidable. A
    reader that skipped the `def` would raise NameError on the very next line.

    The namespace is its own globals, so a function defined here sees the constants defined
    above it when it is CALLED, not only when it is compiled. `__builtins__` is empty in it for
    the same reason it was empty before.
    """
    namespace: dict[str, object] = {"__builtins__": {}}
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            # Private, undecorated helpers only. A `@dp.table` in these files decorates the
            # pipeline's own sources, and evaluating that decorator needs a Databricks runtime;
            # a `def` with no decorator evaluates nothing but its own signature.
            if node.decorator_list or not node.name.startswith("_"):
                continue
            exec(
                compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"),
                namespace,
                namespace,
            )
            continue
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if not (target.id.isupper() or target.id.lstrip("_").isupper()):
            continue
        try:
            # No builtins: this evaluates string concatenation, dict literals, f-strings, the
            # comprehension that renders RULES into a CASE and the call to the renderer, and
            # nothing else can reach a name it was not handed.
            namespace[target.id] = eval(
                compile(ast.Expression(node.value), str(path), "eval"),
                namespace,
                namespace,
            )
        except NameError as error:
            # `LANDING = spark.conf.get(...)` is a runtime value, not a SQL fragment: `spark`
            # and `dbutils` are injected by the Databricks runtime and cannot exist here. That
            # is the ONLY thing allowed to be skipped, and it is skipped by name - anything
            # else raises, because a helper quietly passed over is a predicate quietly
            # unparsed, which is the failure this file exists to prevent.
            if error.name in {"spark", "dbutils", "display"}:
                continue
            raise AssertionError(
                f"{path.name}: cannot evaluate {target.id}, so any SQL built from it would "
                f"go unparsed: {type(error).__name__}: {error}"
            ) from error
        except Exception as error:  # pragma: no cover - the message is the point
            raise AssertionError(
                f"{path.name}: cannot evaluate {target.id}, so any SQL built from it would "
                f"go unparsed: {type(error).__name__}: {error}"
            ) from error
    return namespace


def _expectations() -> list[tuple[str, str]]:
    """Every expectation predicate AND every derived SQL expression, wrapped in a SELECT.

    `_REASON` and `_UNDECIDED` are SQL too - they are what actually tags every row - and they
    are generated rather than typed, so nothing had ever handed them to a parser either. They
    are collected here so the analysis test below evaluates them against the typed views: the
    round that found this had a CASE returning `accepted` for a record the expectations
    dropped, and no test could see it because no test ran the CASE at all.
    """
    out: list[tuple[str, str]] = []
    for path in sorted(LANE.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        if "RULES = {" not in source and "expect_all_or_drop" in source:
            raise AssertionError(
                f"{path.name} declares expectations this test cannot read: the rules must be a "
                f"module-level dict literal named RULES"
            )
        namespace = _module_namespace(source, path)
        rules = namespace.get("RULES")
        if isinstance(rules, dict):
            for name, predicate in rules.items():
                out.append(
                    (
                        f"{path.relative_to(REPO).as_posix()}::expectation:{name}",
                        f"SELECT * FROM t WHERE {predicate}",
                    )
                )
        for name in ("_REASON", "_UNDECIDED"):
            expression = namespace.get(name)
            if isinstance(expression, str):
                out.append(
                    (
                        f"{path.relative_to(REPO).as_posix()}::derived:{name}",
                        f"SELECT {expression} AS value FROM t",
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


# --------------------------------------------------------------------------- analysis
#
# Parsing says a statement is a statement. It does not say the columns exist. An adversarial
# review found `gold_close.py` selecting `quarantine_reason` from `silver_events`, a table
# whose definition is `readStream.table("bronze_events")` decorated with expectations: an
# expectation DROPS a row, it does not annotate one. The whole gold close would have failed
# on its first refresh, and the parser was perfectly happy with it.
#
# So the lane's tables are declared here as empty views with the schema they will have, and
# the statements are ANALYSED against them. That still cannot run the pipeline, but it does
# answer "does every column this lane reads exist", which is the question that was open.


def _bronze_columns() -> str:
    """Bronze, as a DDL string, DERIVED from the one declaration the pipeline uses.

    This dict used to spell the schema out by hand, and it spelled `qty`, `new_qty` and
    `unit_price_cents` as BIGINT while the deployed pipeline was producing them as STRING -
    Auto Loader's default for JSON with no hints. So the analysis test asserted the schema it
    had invented, agreed with itself, and could not see that on the real lane every money
    column was text and `gross_cents` came out DOUBLE. A test that declares its own fixture
    schema is testing the fixture.

    `samegold.pipelines.schema.bronze_schema` is now the single source: the OSS reader uses
    it, `databricks/src/bronze_autoloader.py` carries the same declaration as
    `cloudFiles.schemaHints`, and tests/fast/test_databricks_bundle.py fails if those two
    drift. Change the schema and this test changes with it.
    """
    from samegold.pipelines.schema import bronze_schema

    fields = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in bronze_schema().fields)
    # The two columns bronze_autoloader.py adds after the read, which are not part of the
    # declared input schema.
    return f"{fields}, _ingest_file string, _ingested_at timestamp"


BRONZE_COLUMNS = _bronze_columns()
LANE_TABLES = {
    "bronze_events": BRONZE_COLUMNS,
    # `undecided_rules` is the diagnostic column: which rules could not answer on this row.
    "silver_classified": BRONZE_COLUMNS + ", quarantine_reason STRING, undecided_rules STRING",
    "silver_events": BRONZE_COLUMNS,
    "t": BRONZE_COLUMNS,  # the wrapper the expectation predicates are analysed in
    # The quarantine table, which is `silver_classified` filtered down to four columns
    # (silver_expectations.py). It was missing from this dict too, and nothing said so: the
    # only statement that reads it is the row-count query, and that statement was the one the
    # by-ordinal exclusion list below was silently skipping.
    "silver_quarantine": (
        "event_id STRING, event_type STRING, arrival_ts STRING, quarantine_reason STRING"
    ),
    # The AUTO CDC target. `__START_AT` and `__END_AT` are columns the PRIMITIVE adds to a
    # Type 2 target - they are not declared anywhere in this repository, which is exactly why
    # a statement reading them has to be analysed against a view that has them. The other
    # three are what `gold_close.silver_events_customers` feeds the flow (`event_ts` becomes
    # the sequencing column and does not survive into the target).
    "dim_customer_scd2": (
        "customer_id STRING, segment STRING, country STRING, __START_AT STRING, __END_AT STRING"
    ),
    "revenue_by_month": (
        "accounting_month STRING, gross_cents BIGINT, returns_cents BIGINT, net_cents BIGINT, "
        "line_count BIGINT, return_count BIGINT, returns_rejected_count BIGINT"
    ),
    "revenue_closed": (
        "accounting_month STRING, close_version INT, gross_cents BIGINT, returns_cents BIGINT, "
        "net_cents BIGINT, line_count BIGINT, return_count BIGINT, "
        "returns_rejected_count BIGINT, restated_at TIMESTAMP, restatement_reason STRING"
    ),
    # Written by the two verification tasks. `tests/fast/test_databricks_bundle.py` ties this
    # spelling to the CREATE TABLE statements in the lane, because a schema restated here that
    # drifted from the one the lane creates would make the resolution check below pass against
    # a table that does not exist in that shape.
    "close_verification": (
        "job_run_id STRING, task_run_id STRING, checked_at TIMESTAMP, check_name STRING, "
        "accounting_month STRING, close_version INT, ok BOOLEAN, detail STRING"
    ),
}

# Statements this check cannot reach, and the reason has to be IN THE STATEMENT.
#
# This list used to be five statement ids - `publish_evidence.py#0::0` and so on - and the
# index in that id is the statement's ORDINAL within its file. Round 13 inserted three
# statements into publish_evidence.py, every ordinal after the first shifted, and the list
# went on excluding "#2", which by then was a plain `SELECT ... UNION ALL` over seven tables
# and perfectly analysable. So one statement stopped being checked without anyone touching
# the check, which is the failure this whole file exists to catch, one level up.
#
# The ids are POSIX-shaped, and that is not cosmetic: `path.relative_to(REPO)` gives
# `databricks\src\...` on Windows, so the closed list below - written with forward slashes -
# never matched and these two tests failed on every Windows checkout while passing in CI. A
# check that only works on the platform CI happens to use is the "it works on my machine" class
# with the machines swapped, and this repository has now found it three times.
#
# A routine that does not exist outside a workspace is a property of the TEXT, so it is read
# from the text. An id can then only be wrong loudly: the closed list below names which
# statements this currently excludes, and it fails when that set changes for any reason.
DATABRICKS_ONLY_ROUTINES = (
    # A table-valued function over the pipeline event log; there is no OSS equivalent.
    "EVENT_LOG(",
    # Unity Catalog functions. The policy statements are parsed; their bodies call routines
    # that do not exist outside a workspace, and docs/limits.md already says these policies
    # are declared rather than enforced on Free Edition.
    "IS_ACCOUNT_GROUP_MEMBER(",
    "CURRENT_USER_COUNTRY(",
)


def _calls_a_databricks_routine(statement: str) -> bool:
    """Comments stripped, for the same reason `_is_databricks_only` strips them."""
    code = "\n".join(
        line for line in statement.splitlines() if not line.strip().startswith("--")
    ).upper()
    return any(routine in code for routine in DATABRICKS_ONLY_ROUTINES)


@pytest.fixture(scope="module")
def lane_tables(spark):  # type: ignore[no-untyped-def]
    for name, schema in LANE_TABLES.items():
        spark.createDataFrame([], schema).createOrReplaceTempView(name)
    yield spark


def _single_part(statement: str) -> str:
    """`samegold.main.revenue_by_month` -> `revenue_by_month`.

    Unity Catalog's three-part names are not resolvable by the local session catalog, which
    rejects them before it looks at a single column. Flattening them is a rewrite of the
    NAMESPACE only: every column reference, join and predicate the statement makes is left
    exactly as written, which is what this check is about.
    """
    return statement.replace("samegold.main.", "")


ANALYSABLE = [(n, p) for n, p in STATEMENTS if not _calls_a_databricks_routine(p)]
NOT_ANALYSABLE = [(n, p) for n, p in STATEMENTS if _calls_a_databricks_routine(p)]


def test_almost_every_statement_is_analysable() -> None:
    """The exclusion list is closed, and every entry names a Databricks-only routine.

    The previous version of this test asserted `excluded <= NOT_ANALYSABLE` where `excluded`
    had just been intersected WITH `NOT_ANALYSABLE`: a subset check against the set it was
    filtered by, which is true for every input. It reported the size of the list and nothing
    about its contents, and that is how an id kept excluding a statement it no longer named.
    """
    assert [name for name, _ in NOT_ANALYSABLE] == [
        "databricks/src/publish_evidence.py#0::0",
        "databricks/src/publish_evidence.py#1::0",
        # #2 is `update_history`, added after one `bundle run` produced six failed updates and
        # a record that described one of them. Like the two above it, it reads `event_log()`,
        # which is a Databricks-only table function with no local analogue - so it is listed
        # here by id rather than let through by a predicate that would also let through the
        # next statement nobody looked at.
        "databricks/src/publish_evidence.py#2::0",
        "databricks/sql/policies.sql::0",
        "databricks/sql/policies.sql::1",
    ], [name for name, _ in NOT_ANALYSABLE]
    assert len(ANALYSABLE) >= 12, [name for name, _ in ANALYSABLE]


@pytest.mark.spark
@pytest.mark.parametrize("name,statement", ANALYSABLE, ids=[n for n, _ in ANALYSABLE])
def test_every_column_the_statement_reads_exists(lane_tables, name: str, statement: str) -> None:  # type: ignore[no-untyped-def]
    head = statement.strip().upper()
    if head.startswith(("CREATE", "ALTER")):
        pytest.skip("DDL declares the schema rather than reading it")
    if head.startswith("MERGE"):
        # A MERGE needs a real Delta table as its target, which needs jars this container
        # cannot reach. Its SOURCE is the half that reads columns, so that half is analysed.
        source = statement.split("USING", 1)[1].rsplit(") AS s", 1)[0]
        lane_tables.sql(_single_part(source.strip().lstrip("(")))
        return
    try:
        lane_tables.sql(_single_part(statement)).schema  # noqa: B018 - analysis is the assertion
    except Exception as error:  # pragma: no cover - the message is the point
        pytest.fail(f"{name} does not resolve: {error}")
