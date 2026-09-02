"""CONTRACT.md and domain/contract.py must not disagree.

A contract that lives in two places drifts. Here the document is the human-readable half and
the module is the machine-readable one, and this test is the join between them.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from samegold.domain.contract import (
    ACCOUNTING_TIMEZONE,
    CONTRACT_VERSION,
    MAX_LINE_QUANTITY,
    MAX_UNIT_PRICE_CENTS,
    RETURN_WINDOW_DAYS,
    QuarantineReason,
)

REPO = Path(__file__).resolve().parents[2]
CONTRACT = (REPO / "CONTRACT.md").read_text(encoding="utf-8")

# Every lane that decides `amount_out_of_range` by comparing a number against a bound. The
# three of them spell the bound as a LITERAL, because the reference is a .sql file the
# mutation engine parses and the Databricks rules are SQL strings a notebook evaluates:
# neither can import a Python constant, and templating the reference would change the shape
# of the mutants generated from it. So the copies are unavoidable and the test below is what
# stands in for the import.
BOUNDED_LANES = (
    "src/samegold/oracle/gold_revenue.sql",
    "src/samegold/oracle/duckdb_gold.py",
    "databricks/src/silver_expectations.py",
)
# `new_qty` before `qty` so the alternation does not match the tail of the longer name.
COMPARISON = re.compile(r"\b(new_qty|qty|unit_price_cents)\b[^\n]{0,40}?(<=|>=|<|>)\s*(\d+)")


# The bound literals, and the WIDTH they are written with.
#
# `1000000` is an INT32 literal in Spark SQL, and Spark coerces the OTHER operand to the
# literal's type. On the STRING columns Auto Loader inferred before this lane had schema hints
# that meant casting 9223372036854775807 to INT32, which overflows; non-ANSI Spark answers NULL
# for that cast, the rule could not decide, and a classification whose default was `accepted`
# booked three deliberately-bad events as 2.7e19 of revenue. Measured on pyspark 4.2.0 with `v`
# a STRING holding 9223372036854775807: `v > 1000000` is NULL with ANSI off and true with it on;
# `v > 1000000L` is true in both. docs/limits.md carries the table and the reason a single
# workspace has both modes.
#
# The policy this pair of tests enforces, and its one deliberate exemption:
#
#   * Spark SQL (the Databricks rules): every bound literal carries `L`.
#   * PySpark (`transform.py`): every bound literal goes through `_bound()`, which is
#     `lit(value).cast("bigint")` - the same width, said in the dialect that file is written in.
#   * DuckDB (`gold_revenue.sql`, `duckdb_gold.py`): EXEMPT, because the hazard does not exist
#     there and pretending it does would be cargo. Measured on duckdb 1.5.5: comparing a VARCHAR
#     column against an INTEGER literal is a BINDER ERROR, not a NULL - the reference refuses to
#     run rather than quietly answering "unknown". Its numeric columns are JSON, converted
#     through an explicit `json_type` guard and `TRY_CAST(... AS BIGINT)` before any comparison,
#     so a literal never decides a type there.
SPARK_DIALECT_LANE = "databricks/src/silver_expectations.py"
PYSPARK_LANE = "src/samegold/pipelines/transform.py"
BOUNDED_COLUMNS = ("qty", "new_qty", "unit_price_cents")
BOUND_CONSTANTS = ("MAX_LINE_QUANTITY", "MAX_UNIT_PRICE_CENTS")
# The bound literal, plus whatever character follows it. `L` is the one that must.
SUFFIXED = re.compile(r"\b(new_qty|qty|unit_price_cents)\b[^\n]{0,40}?(<=|>=|<|>)\s*(\d+)(.?)")


def _rule_predicates(relative: str) -> dict[str, str]:
    """The lane's `RULES`, read WITHOUT importing it.

    The fast workflow installs `.[dev]` and no Spark, and this file imports `pyspark` at module
    level - which is how round seventeen pushed a red `fast` workflow. So the dict is read from
    the AST, and every string constant inside each value is joined: implicit concatenation, the
    f-string that splices `_PRESENT_FOR_TYPE`, all of it. Nothing is skipped, because a
    predicate silently unread is a predicate silently unchecked.

    Reading the RULES rather than grepping the file is also what keeps this test honest: the
    file's own comments quote `unit_price_cents > 1000000` while explaining why that spelling
    was the defect, and a regex over the text would fail on the explanation.
    """
    tree = ast.parse((REPO / relative).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "RULES" for t in node.targets
        ):
            assert isinstance(node.value, ast.Dict)
            out: dict[str, str] = {}
            for key, value in zip(node.value.keys, node.value.values, strict=True):
                assert isinstance(key, ast.Constant)
                out[str(key.value)] = " ".join(
                    part.value
                    for part in ast.walk(value)
                    if isinstance(part, ast.Constant) and isinstance(part.value, str)
                )
            return out
    raise AssertionError(f"{relative} declares no RULES")


def _bounded_comparisons(relative: str) -> list[tuple[str, ast.expr]]:
    """Every comparison in a PySpark file whose left side is a bounded column.

    Returned as (rendered comparison, the node on the right) so a test can inspect the RIGHT
    side, which is where the literal's width is decided.

    The first version of this helper deleted comments and string literals and ran a regex over
    what was left. That cannot work in PySpark: the column name lives in a string literal, so
    `F.col("qty") > 10000` becomes `F . col ( ) > 10000` and the regex has nothing to anchor on.
    It found no offenders on a file that had them, which is a test that passes by being blind -
    the exact failure mode this whole file exists to catch, committed while writing the test
    for it.
    """
    tree = ast.parse((REPO / relative).read_text(encoding="utf-8"))
    found: list[tuple[str, ast.expr]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        rendered = ast.unparse(node)
        if not any(f"'{column}'" in rendered for column in BOUNDED_COLUMNS):
            continue
        for comparator in node.comparators:
            found.append((rendered, comparator))
    return found


def _spaced(value: int) -> str:
    """1000000 -> "1 000 000", the way CONTRACT.md writes a number."""
    return f"{value:,}".replace(",", " ")


def test_the_version_matches() -> None:
    assert f"Version {CONTRACT_VERSION}" in CONTRACT


def test_the_window_matches() -> None:
    assert f"{RETURN_WINDOW_DAYS} days" in CONTRACT


def test_the_timezone_matches() -> None:
    assert ACCOUNTING_TIMEZONE in CONTRACT


def test_the_money_bounds_match() -> None:
    """The bounds are a contract term, so the document has to carry the same numbers.

    They were not checked here for the round in which they were introduced, and the document
    went on stating ten million and ten billion after the module had been narrowed. A rule a
    reader can read and nothing can execute is the failure this whole file exists for.
    """
    assert _spaced(MAX_LINE_QUANTITY) in CONTRACT
    assert _spaced(MAX_UNIT_PRICE_CENTS) in CONTRACT


def test_every_lane_compares_against_the_contracts_bounds_and_nothing_else() -> None:
    """The three SQL lanes cannot import the constant, so this is the import.

    The check is stronger than "the right number appears somewhere": it collects EVERY
    integer each lane compares a bounded column against and requires the set to be the
    contract's bound and zero. A lane that grew a second, different threshold - which is
    exactly what a hand-edit of one of three copies produces - fails here rather than in a
    claim, or worse, nowhere.
    """
    for name in BOUNDED_LANES:
        text = (REPO / name).read_text(encoding="utf-8")
        found: dict[str, set[int]] = {"qty": set(), "price": set()}
        for column, _operator, literal in COMPARISON.findall(text):
            key = "price" if column == "unit_price_cents" else "qty"
            found[key].add(int(literal))
        assert found["qty"] <= {0, MAX_LINE_QUANTITY}, f"{name} bounds a quantity elsewhere"
        assert found["price"] <= {0, MAX_UNIT_PRICE_CENTS}, f"{name} bounds a price elsewhere"
        assert MAX_LINE_QUANTITY in found["qty"], f"{name} does not apply the quantity bound"
        assert MAX_UNIT_PRICE_CENTS in found["price"], f"{name} does not apply the price bound"


def test_the_spark_dialect_bound_literals_carry_their_width() -> None:
    """The value was already checked. This checks the TYPE, which is what decided a close.

    `test_every_lane_compares_against_the_contracts_bounds_and_nothing_else` above collects the
    NUMBERS each lane compares against and requires them to be the contract's. Every one of them
    was right on the lane that booked 2.7e19 as revenue: `1000000` is the contract's ceiling and
    `1000000` is an INT32, and it was the second fact that decided the row. A test that reads a
    literal's value and not its width cannot see that class of defect at all.
    """
    for name, predicate in _rule_predicates(SPARK_DIALECT_LANE).items():
        for column, operator, literal, following in SUFFIXED.findall(predicate):
            assert following == "L", (
                f"{SPARK_DIALECT_LANE}: rule `{name}` compares {column} {operator} {literal} "
                f"against a bare INT32 literal. Spark coerces the COLUMN to the literal's type, "
                f"so on a string column this is the cast that overflowed and returned NULL. "
                f"Write it {literal}L."
            )


def test_the_pyspark_lane_builds_its_bounds_with_a_declared_width() -> None:
    """The same policy in the other dialect: `_bound()`, never a bare Python int.

    `F.col("qty") > 10000` builds an INT32 literal exactly like the SQL spelling does. This lane
    reads a declared schema, so its columns are BIGINT and the coercion is harmless here - which
    is the argument for writing it anyway. The lane that could reach the hazard should not be
    the only one that remembers it exists, and `transform.py` is the file the declarative
    pipeline imports.
    """
    comparisons = _bounded_comparisons(PYSPARK_LANE)
    typed = [
        rendered
        for rendered, comparator in comparisons
        if isinstance(comparator, ast.Call)
        and isinstance(comparator.func, ast.Name)
        and comparator.func.id == "_bound"
    ]
    offenders = [
        rendered
        for rendered, comparator in comparisons
        if (isinstance(comparator, ast.Constant) and isinstance(comparator.value, int))
        or (isinstance(comparator, ast.Name) and comparator.id in BOUND_CONSTANTS)
    ]
    assert not offenders, (
        f"{PYSPARK_LANE} compares a bounded column against a bare integer: {offenders}. Spark "
        f"builds an INT32 literal for it and coerces the COLUMN to that type. Use _bound(), "
        f"which casts it to bigint."
    )
    # And the check is not vacuous: the helper is what those comparisons are actually built
    # with. The version of this test before it asserted over a token stream with the string
    # literals stripped out, where a column name cannot appear at all, and reported a clean
    # lane without having examined a single comparison.
    assert len(typed) >= 6, (
        f"only {len(typed)} bounded comparisons in {PYSPARK_LANE} go through _bound(); the "
        f"classification declares six of them, so this test is not reading what it thinks"
    )


def test_the_limits_document_records_the_two_ansi_modes() -> None:
    """The measurement that explains the policy has to live somewhere a reader will find it.

    It was in a commit message. A commit message is not a document: nobody reading the rules
    goes looking through `git log` for the reason one of them is spelled `1000000L`.
    """
    limits = (REPO / "docs" / "limits.md").read_text(encoding="utf-8")
    assert "ansi" in limits.lower()
    for phrase in ("1000000L", "9223372036854775807", "spark.sql.ansi.enabled"):
        assert phrase in limits, f"docs/limits.md does not carry {phrase}"


def test_the_bounds_leave_the_headroom_the_contract_states() -> None:
    """Recomputed, because the first version of this rule asserted it and was wrong.

    The bounds exist so that `qty * unit_price_cents` and the SUM over a month cannot
    overflow a BIGINT. The comment defending the original pair claimed a close would need a
    hundred billion maximum-value lines to overflow; the arithmetic gives ninety-two, so
    ninety-three of them re-created the incident the bounds were introduced to prevent. The
    margin is a division, and a division that nothing performs is a sentence.
    """
    bigint_max = 2**63 - 1
    largest_line = MAX_LINE_QUANTITY * MAX_UNIT_PRICE_CENTS
    assert largest_line <= bigint_max, "one legal line already overflows a BIGINT"
    headroom = bigint_max // largest_line
    assert headroom >= 1_000_000, (
        f"only {headroom:,} maximum-value lines fit in a BIGINT sum; the bounds are too "
        f"loose to protect the close they were introduced to protect"
    )
    assert _spaced(headroom) in CONTRACT, (
        f"CONTRACT.md does not state the headroom these bounds actually give ({headroom:,})"
    )


def test_the_document_lists_exactly_the_quarantine_reasons_the_enum_has() -> None:
    """Both directions, because either one alone is satisfied by a stale document.

    CONTRACT.md used to claim this test existed. It did not: nothing read the document's
    reasons, and the document did not list any. An adversarial review noticed the sentence
    and the absence, which is the cheapest kind of finding to make and the most embarrassing
    kind to have.
    """
    documented = set(re.findall(r"`(unparseable_json|[a-z_]+)`", CONTRACT))
    declared = {str(reason) for reason in QuarantineReason}
    missing = declared - documented
    assert not missing, f"declared in the enum and absent from CONTRACT.md: {sorted(missing)}"
    # And the reverse: a reason the document invents is a promise nothing keeps.
    table = CONTRACT.split("## Quarantine reasons", 1)[1].split("##", 1)[0]
    listed = set(re.findall(r"^\| `([a-z_]+)`", table, flags=re.MULTILINE))
    assert listed == declared, f"document {sorted(listed)} != enum {sorted(declared)}"


def test_every_quarantine_reason_is_actually_produced_by_a_run() -> None:
    """Produced, not mentioned. The previous version of this test grepped for the name.

    It asserted `str(reason) in source`, i.e. that the literal string had been typed into
    `transform.py`. `return_exceeds_sold_qty` passed it for the whole life of the repository
    while being UNREACHABLE by construction: the generator drew a return quantity with
    `randrange(1, sold + 1)`, so "more units than were sold" could not happen, and the branch
    existed in all three implementations and was exercised by none of them.

    This version generates a dataset and reads the ledger's own accounting of what it planted.
    A reason nobody can produce is a reason nobody maintains; grepping for its name is not
    producing it.
    """
    import tempfile

    from samegold.generator.events import FAST, generate

    with tempfile.TemporaryDirectory(prefix="samegold-reasons-") as tmp:
        result = generate(Path(tmp) / "g", seed=42, profile=FAST)
    produced = set(result.ledger.quarantine)
    missing = {str(reason) for reason in QuarantineReason} - produced
    assert not missing, f"declared in the contract and produced by no run: {sorted(missing)}"


def test_the_sql_reference_enforces_the_window_in_seconds() -> None:
    """The previous version of this test asserted `INTERVAL 45 DAY in sql`, which was true
    only because the phrase survived in the COMMENT that explains why it is no longer used.
    A test that passes on a comment is a test that stopped watching."""
    sql = (
        Path(__file__).resolve().parents[2] / "src" / "samegold" / "oracle" / "gold_revenue.sql"
    ).read_text(encoding="utf-8")
    code = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    assert f"{RETURN_WINDOW_DAYS} * 86400" in code
    assert "INTERVAL 45 DAY" not in code


def test_the_spark_side_enforces_the_same_window_in_seconds() -> None:
    source = (
        Path(__file__).resolve().parents[2] / "src" / "samegold" / "pipelines" / "transform.py"
    ).read_text(encoding="utf-8")
    assert "RETURN_WINDOW_DAYS * 86400" in source
    code = "\n".join(line for line in source.splitlines() if not line.lstrip().startswith("#"))
    assert "unix_timestamp" not in code, (
        "unix_timestamp truncates to whole seconds and accepted a return one microsecond "
        "outside the window; the two implementations disagreed on exactly that row"
    )


def test_the_delta_coordinate_lives_in_one_place() -> None:
    """ADR 0002 claims there is a single constant and a test that enforces it. This is it.

    The claim was false when it was written: the coordinate was also spelled out in the
    declarative pipeline spec, and nothing checked that the two agreed.
    """
    from samegold.pipelines.session import DELTA_COORDINATE

    root = Path(__file__).resolve().parents[2]
    spec = (root / "pipelines" / "spark-pipeline.yml").read_text(encoding="utf-8")
    assert DELTA_COORDINATE in spec, (
        "the declarative pipeline spec must use the same coordinate as pipelines/session.py"
    )
    sources = [
        path
        for path in (root / "src").rglob("*.py")
        if "__pycache__" not in str(path) and path.name != "session.py"
    ]
    for path in sources:
        assert "io.delta:delta-spark" not in path.read_text(encoding="utf-8"), (
            f"{path.relative_to(root)} spells out the Delta coordinate; it belongs in "
            f"pipelines/session.py alone"
        )


def test_the_declarative_pipeline_spec_is_consistent_with_the_code() -> None:
    """The SDP spec is a file nothing executes in this container, which is exactly the kind of
    file that rots. These are the properties that can be checked without a JVM."""
    import yaml

    root = Path(__file__).resolve().parents[2]
    spec = yaml.safe_load((root / "pipelines" / "spark-pipeline.yml").read_text(encoding="utf-8"))
    assert spec["name"] == "samegold"
    configuration = spec["configuration"]
    # Without these three, open-source SDP writes Parquet and every Delta-specific claim in
    # this repository has nothing to work with.
    assert configuration["spark.sql.sources.default"] == "delta"
    assert "DeltaSparkSessionExtension" in configuration["spark.sql.extensions"]
    assert "DeltaCatalog" in configuration["spark.sql.catalog.spark_catalog"]
    transformations = root / "pipelines" / "transformations"
    assert list(transformations.glob("*.py")), "the spec globs transformations/** and it is empty"
