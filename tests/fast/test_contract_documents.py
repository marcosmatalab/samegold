"""CONTRACT.md and domain/contract.py must not disagree.

A contract that lives in two places drifts. Here the document is the human-readable half and
the module is the machine-readable one, and this test is the join between them.
"""

from __future__ import annotations

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
