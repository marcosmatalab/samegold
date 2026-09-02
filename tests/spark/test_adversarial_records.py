"""The record shapes the generator never emits, and on which the two engines used to differ.

Every test here is a bug that a review found by writing a record by hand, and that no seed
could ever have produced: the generator emits well-formed events with every field present,
because that is what the producer contract promises. A contract violation is exactly the
input on which two implementations quietly stop agreeing, so the violations are written here
on purpose and the two engines are compared on each of them.

The list is not decoration. Before these tests existed:

  * an ``order_placed`` with no ``currency`` key booked 2000 cents of revenue in Spark and
    nothing at all in DuckDB, because ``currency != 'EUR'`` is NULL for a missing value and
    the classification fell through to "accepted";
  * a line with no ``unit_price_cents`` was accepted by Spark with a gross of zero;
  * a ``return_registered`` with no ``qty`` was counted by Spark and not by DuckDB;
  * a malformed ``event_ts`` aborted the entire close, in BOTH engines, with a cast error;
  * two records sharing an ``event_id`` with different payloads were resolved to DIFFERENT
    copies by the two engines, because the tie-break hashed with md5 on one side and sha2 on
    the other. On 48% of such pairs.
"""

from __future__ import annotations

import datetime as dt
import itertools
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from samegold.oracle.duckdb_gold import revenue_by_month_as_of
from samegold.pipelines.transform import (
    as_of_cut,
    classify,
    classify_returns,
    effective_lines,
    read_bronze,
    revenue_by_month,
    silver,
)

pytestmark = pytest.mark.spark

AS_OF = "2026-12-31T23:00:00+00:00"
AS_OF_DT = dt.datetime.fromisoformat(AS_OF)

BASE_ORDER: dict[str, Any] = {
    "event_id": "op-1",
    "event_type": "order_placed",
    "event_ts": "2026-01-10T10:00:00+00:00",
    "arrival_ts": "2026-01-10T10:05:00+00:00",
    "order_id": "O1",
    "customer_id": "C1",
    "sku": "S1",
    "qty": 2,
    "unit_price_cents": 1000,
    "currency": "EUR",
}


def _write(tmp_path: Path, rows: list[dict[str, Any]]) -> Path:
    bronze = tmp_path / "bronze" / "batch=202601010000"
    bronze.mkdir(parents=True, exist_ok=True)
    (bronze / "part-00000.json").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8"
    )
    return tmp_path / "bronze"


def _spark_close(spark: Any, bronze: Path) -> list[tuple[Any, ...]]:
    events = silver(as_of_cut(read_bronze(spark, str(bronze)), AS_OF))
    lines = effective_lines(events)
    close = revenue_by_month(lines, classify_returns(events, lines))
    return [tuple(row) for row in close.collect()]


def _duckdb_close(bronze: Path) -> list[tuple[Any, ...]]:
    return [
        (
            row.accounting_month,
            row.gross_cents,
            row.returns_cents,
            row.net_cents,
            row.line_count,
            row.return_count,
            row.returns_rejected_count,
        )
        for row in revenue_by_month_as_of(bronze, AS_OF_DT)
    ]


def _spark_dimension(spark: Any, bronze: Path) -> list[tuple[Any, ...]]:
    from samegold.pipelines.transform import dim_customer_scd2

    events = silver(as_of_cut(read_bronze(spark, str(bronze)), AS_OF))
    return sorted(tuple(row) for row in dim_customer_scd2(events).collect())


def _duckdb_dimension(bronze: Path) -> list[tuple[Any, ...]]:
    from samegold.oracle.duckdb_gold import scd2_as_of

    return sorted(
        (
            str(row["customer_id"]),
            None if row["valid_from"] is None else str(row["valid_from"]),
            None if row["valid_to"] is None else str(row["valid_to"]),
            row["segment"],
            row["country"],
            bool(row["is_current"]),
        )
        for row in scd2_as_of(bronze, AS_OF_DT)
    )


def _both_agree(spark: Any, tmp_path: Path, rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    """Compare BOTH gold tables, not only the close.

    The dimension used to be compared in exactly one place, on generated data, which is data
    that by contract contains none of the shapes this file is about. A record shape that
    diverges in the dimension and agrees in the close was therefore invisible - and there was
    one: the Spark dimension read quarantined records and the reference did not.
    """
    bronze = _write(tmp_path, rows)
    spark_rows = sorted(_spark_close(spark, bronze))
    duckdb_rows = sorted(_duckdb_close(bronze))
    assert spark_rows == duckdb_rows, f"close: Spark {spark_rows} != DuckDB {duckdb_rows}"
    spark_dim, duckdb_dim = _spark_dimension(spark, bronze), _duckdb_dimension(bronze)
    assert spark_dim == duckdb_dim, f"dimension: Spark {spark_dim} != DuckDB {duckdb_dim}"
    return spark_rows


MISSING_FIELD_CASES = [
    pytest.param("currency", id="no-currency"),
    pytest.param("unit_price_cents", id="no-price"),
    pytest.param("qty", id="no-qty"),
    pytest.param("customer_id", id="no-customer"),
    pytest.param("order_id", id="no-order"),
    pytest.param("sku", id="no-sku"),
]


@pytest.mark.parametrize("field", MISSING_FIELD_CASES)
def test_an_order_line_missing_a_field_is_refused_by_both(spark, tmp_path, field) -> None:  # type: ignore[no-untyped-def]
    """A missing key is not a zero and it is not "accepted with a NULL"."""
    row = {key: value for key, value in BASE_ORDER.items() if key != field}
    assert _both_agree(spark, tmp_path, [row]) == []


def test_a_return_without_a_quantity_is_refused_by_both(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    rows = [
        BASE_ORDER,
        {
            "event_id": "rt-1",
            "event_type": "return_registered",
            "event_ts": "2026-01-20T10:00:00+00:00",
            "arrival_ts": "2026-01-20T10:05:00+00:00",
            "order_id": "O1",
            "sku": "S1",
            "return_id": "R1",
        },
    ]
    close = _both_agree(spark, tmp_path, rows)
    assert close == [("2026-01", 2000, 0, 2000, 1, 0, 0)]


def test_a_malformed_timestamp_does_not_abort_the_close(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """It used to raise a conversion error in both engines: the one shape with no door.

    The close of a month must not be destroyed by one bad line from one producer. The record
    is quarantined and the rest of the month is closed.
    """
    rows = [BASE_ORDER, dict(BASE_ORDER, event_id="op-bad", event_ts="not-a-timestamp")]
    close = _both_agree(spark, tmp_path, rows)
    assert close == [("2026-01", 2000, 0, 2000, 1, 0, 0)]


def test_the_broken_copy_of_a_duplicated_event_does_not_win(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """Two copies of one event_id, one with a malformed event_ts. The readable one must win.

    The first version of the TRY_CAST fix made this diverge by 2000 cents. Spark's ASC is
    NULLS FIRST, so the unparseable copy sorted first in the deduplication window and the
    good one was discarded; the reference excludes an unparseable event_ts before it
    deduplicates, so it kept the good one. Both fixes were correct on their own and the pair
    of them created a divergence, which is the argument for testing the pair.
    """
    rows = [BASE_ORDER, dict(BASE_ORDER, qty=5, event_ts="not-a-timestamp")]
    for order in (rows, list(reversed(rows))):
        close = _both_agree(spark, tmp_path / str(len(order)) / str(order[0]["qty"]), order)
        assert close == [("2026-01", 2000, 0, 2000, 1, 0, 0)], order


def test_an_integer_beyond_bigint_is_refused_by_both(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """json_type calls 2^63 a UBIGINT; a plain CAST to BIGINT then aborts the whole close."""
    rows = [BASE_ORDER, dict(BASE_ORDER, event_id="op-2", order_id="O2", qty=2**63)]
    assert _both_agree(spark, tmp_path, rows) == [("2026-01", 2000, 0, 2000, 1, 0, 0)]


def test_a_line_with_a_float_quantity_is_refused_by_both(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """A JSON number that is not an integer is a schema violation, not a rounding question."""
    rows = [dict(BASE_ORDER, qty=2.5)]
    _both_agree(spark, tmp_path, rows)


def test_both_engines_break_a_payload_tie_the_same_way(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """Two records with one event_id and different payloads: the SAME copy must win.

    This is a contract violation (an event_id identifies one fact), so the answer is not
    "the right one"; there is no right one. The requirement is that both engines choose
    identically, because otherwise "the two implementations agree" is a statement about the
    generator's politeness rather than about the implementations. Twenty pairs, because a
    hash disagreement flips a given pair with probability about one half and one pair would
    be a coin toss.
    """
    rows: list[dict[str, Any]] = []
    for index in range(20):
        left = dict(
            BASE_ORDER,
            event_id=f"op-tie-{index}",
            order_id=f"O{index}",
            sku=f"S{index}",
            qty=1,
        )
        rows.append(left)
        rows.append(dict(left, qty=3))
    _both_agree(spark, tmp_path, rows)


def test_both_engines_pick_the_same_customer_version_from_a_colliding_pair(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """The dimension's tie-break must be the SAME order on both sides, not merely a total one.

    The SCD2 reference hashed four columns and the Spark side hashed twelve. Same function,
    different input, so the induced orders differ and the two engines chose different copies
    of a colliding customer_upserted. The test that existed compared Spark against ITSELF at
    several partition counts, which the four-column hash passes: it is stable, it is just not
    the same stability the reference has.
    """
    from samegold.oracle.duckdb_gold import scd2_as_of
    from samegold.pipelines.transform import dim_customer_scd2

    rows: list[dict[str, Any]] = []
    for index in range(12):
        base = {
            "event_id": f"cu-{index}",
            "event_type": "customer_upserted",
            "event_ts": "2026-01-05T00:00:00+00:00",
            "arrival_ts": "2026-01-05T00:05:00+00:00",
            "customer_id": f"C{index}",
            "segment": "SEG-0",
            "country": "ES",
        }
        rows.append(base)
        rows.append(dict(base, segment="SEG-1"))
    bronze = _write(tmp_path, rows)
    events = silver(as_of_cut(read_bronze(spark, str(bronze)), AS_OF))
    from_spark = sorted(
        (row["customer_id"], row["valid_from"], row["segment"], row["country"])
        for row in dim_customer_scd2(events).collect()
    )
    from_duckdb = sorted(
        (str(row["customer_id"]), str(row["valid_from"]), str(row["segment"]), str(row["country"]))
        for row in scd2_as_of(bronze, AS_OF_DT)
    )
    assert from_spark == from_duckdb


def test_a_customer_tie_is_broken_by_more_than_six_columns(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """For a customer_upserted the old six-column hash was constant, so the tie survived.

    The winner was then whatever the shuffle produced: repartitioning the same file a
    different number of ways gave a different dimension. The test asserts the dimension is
    the same at several partition counts, which is the property the hash exists to provide.
    """
    from samegold.pipelines.transform import dim_customer_scd2

    rows: list[dict[str, Any]] = []
    for index in range(12):
        base = {
            "event_id": f"cu-{index}",
            "event_type": "customer_upserted",
            "event_ts": "2026-01-01T00:00:00+00:00",
            "arrival_ts": "2026-01-01T00:05:00+00:00",
            "customer_id": f"C{index}",
            "segment": "retail",
            "country": "ES",
        }
        rows.append(base)
        rows.append(dict(base, segment="vip", country="FR"))
    bronze = _write(tmp_path, rows)
    answers = set()
    for partitions in (1, 2, 3, 5, 7, 11, 13):
        events = silver(as_of_cut(read_bronze(spark, str(bronze)), AS_OF).repartition(partitions))
        answers.add(tuple(sorted(tuple(row) for row in dim_customer_scd2(events).collect())))
    assert len(answers) == 1, f"{len(answers)} different dimensions from the same input"


def test_a_line_the_parser_could_not_read_gets_a_reason(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """It used to be labelled 'accepted' and then dropped by the event_id filter.

    That is the record leaving the pipeline with no counter, which the whole classification
    exists to make impossible, and it was reachable with one truncated line.
    """
    bronze = tmp_path / "bronze" / "batch=202601010000"
    bronze.mkdir(parents=True)
    (bronze / "part-00000.json").write_text(
        json.dumps(BASE_ORDER) + '\n{"event_id": "bad-1", "event_type": "order_pl\n',
        encoding="utf-8",
    )
    classified = classify(read_bronze(spark, str(tmp_path / "bronze")))
    reasons = sorted(row["quarantine_reason"] for row in classified.collect())
    assert reasons == ["accepted", "unparseable_json"]


def test_the_databricks_rules_and_the_oss_case_agree_record_by_record(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """The two derivations of the closed enum, evaluated on the same records.

    The Databricks lane declares its rules as expectation predicates so the pipeline event log
    reports pass and fail counts per rule; the OSS lane writes them as one CASE. That is two
    implementations of one contract, which is the whole method of this repository applied to
    the quality rules instead of to the close, and until now nothing compared them: the check
    that existed was a substring grep over the source, and it passed while the Databricks side
    accepted a record with a NULL `event_type` that the OSS side quarantined.
    """
    from pyspark.sql import functions as F

    from samegold.pipelines.schema import bronze_schema
    from samegold.pipelines.transform import quarantine_reason

    # Read in SOURCE ORDER through the shared reader: `_REASON` is now derived from `RULES`,
    # and the regex that used to pull the helpers out evaluated them before `RULES` existed,
    # which raised NameError at collection time.
    rules = _databricks_namespace()["RULES"]
    assert isinstance(rules, dict)

    rows = _matrix_rows()
    frame = spark.createDataFrame(rows, bronze_schema())

    # The Databricks side is the expression the lane ACTUALLY evaluates, not a reconstruction
    # of it. It used to be rebuilt here as `when(~predicate, name)`, which is the open-failing
    # form the deployed CASE had - so this test agreed with the defect instead of catching it.
    # `_REASON` is generated from RULES in the lane's own file; that is what runs there and
    # that is what runs here.
    databricks = F.expr(str(_databricks_namespace()["_REASON"]))

    compared = frame.select(
        F.col("event_id"),
        quarantine_reason().alias("oss"),
        databricks.alias("databricks"),
    ).collect()
    disagreements = [
        (row["event_id"], row["oss"], row["databricks"])
        for row in compared
        if row["oss"] != row["databricks"]
    ]
    assert not disagreements, f"the two derivations disagree on {disagreements}"


def test_a_quarantined_customer_upsert_does_not_reach_the_dimension(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """It entered gold with valid_from NULL, violating the target table's own NOT NULL.

    Three upserts for one customer, the middle one with an unreadable event_ts. The Spark
    dimension carried its attributes into a fourth row; the reference had dropped the record
    in `arrived`. Every other consumer of silver filtered on the quarantine reason and this
    one did not.
    """
    base = {
        "event_type": "customer_upserted",
        "arrival_ts": "2026-01-01T00:05:00+00:00",
        "customer_id": "C1",
    }
    rows: list[dict[str, Any]] = [
        dict(
            base,
            event_id="cu-0",
            event_ts="2026-01-01T00:00:00+00:00",
            segment="retail",
            country="ES",
        ),
        dict(base, event_id="cu-1", event_ts="not-a-timestamp", segment="vip", country="PT"),
        dict(
            base, event_id="cu-2", event_ts="2026-03-01T00:00:00+00:00", segment="pro", country="FR"
        ),
    ]
    bronze = _write(tmp_path, rows)
    dimension = _spark_dimension(spark, bronze)
    assert dimension == _duckdb_dimension(bronze)
    assert all(row[1] is not None for row in dimension), (
        f"a NULL valid_from reached gold: {dimension}"
    )


# --------------------------------------------------------------- the types the rules run on
#
# Everything above compares the two derivations on TYPED columns, and on typed columns they
# agree on every record - which is what this file reported for a round while the deployed lane
# was booking 2.7e19 of revenue from three events the contract caps at 1e10.
#
# The rules were right. The types were wrong. Auto Loader reading JSON with no hints inferred
# every bronze column as STRING, and on a STRING column `unit_price_cents > 1000000` compares
# against an INT32 literal: the string is coerced to INT32, 9223372036854775807 overflows it,
# and non-ANSI Spark returns NULL. A NULL predicate does not match a `WHEN`, so the row fell
# through to `ELSE 'accepted'`. Measured on pyspark 4.2.0: ansi off gives NULL, ansi on gives
# true, and `> 1000000L` gives true in both - the defect is the WIDTH of the literal.
#
# So the matrix is evaluated twice: once as bronze now delivers it (declared types), where no
# predicate may be undecidable, and once as bronze used to deliver it (strings), where the one
# thing that must hold is that nothing undecidable can become revenue.


def _databricks_namespace() -> dict[str, object]:
    """The lane's own RULES and derived expressions, read in source order.

    Borrowed from the parse test rather than re-implemented: that module already has to read
    this file in dependency order, and two readers of one file are two chances to read it
    differently.
    """
    import importlib.util

    path = Path(__file__).resolve().parent / "test_databricks_lane_parses.py"
    spec = importlib.util.spec_from_file_location("_lane_parses", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    lane = Path(__file__).resolve().parents[2] / "databricks" / "src" / "silver_expectations.py"
    return dict(module._module_namespace(lane.read_text(encoding="utf-8"), lane))


def event(**overrides: object) -> dict[str, object]:
    """A well-formed `order_placed` with every bronze column present, plus the overrides.

    Module level rather than a closure because `_pair_rows` builds on it too: the pair records
    have to be the SAME base record as the hand-written ones, or a difference between them is
    a difference in the fixture and not in the rules.
    """
    from samegold.pipelines.schema import bronze_schema

    base: dict[str, object] = dict.fromkeys(bronze_schema().fieldNames())
    base.update(
        event_id="e",
        event_type="order_placed",
        event_ts="2026-01-10T10:00:00+00:00",
        arrival_ts="2026-01-10T10:05:00+00:00",
        order_id="O1",
        customer_id="C1",
        sku="S1",
        qty=2,
        new_qty=None,
        unit_price_cents=1000,
        currency="EUR",
    )
    base.update(overrides)
    return base


# How to make each rule say no, as DATA, so that records breaking two rules at once can be
# generated instead of written.
#
# One per rule is not enough and the reason is the interesting part: two rules can want the
# same column. `non_positive_quantity` is broken with `qty = 0` and `amount_out_of_range` with
# `qty = 10001`, and a record cannot carry both - so each rule offers ALTERNATIVES, the
# generator emits every combination whose columns do not collide, and the test below keeps the
# ones that Spark says actually break both. Deciding by evaluation rather than by construction
# is the whole point: a hand-reasoned "this record breaks rules 4 and 7" is a claim about SQL
# semantics, which is the class of claim this file exists because people get wrong.
_BREAKERS: dict[str, tuple[dict[str, object], ...]] = {
    "unparseable_json": ({"event_id": None},),
    # NULL and a real-but-unknown type are not interchangeable here. With
    # `event_type = 'warehouse_pinged'` the value rules are all guarded by
    # `event_type NOT IN (...)` and come out TRUE, so no pair with a value rule is reachable
    # that way; with NULL they come out NULL, which the classification treats as a rejection.
    # Both are offered and the evaluation picks.
    "unknown_event_type": ({"event_type": None}, {"event_type": "warehouse_pinged"}),
    "missing_required_field": ({"order_id": None}, {"event_ts": "not-a-timestamp"}),
    "non_positive_quantity": ({"qty": 0}, {"qty": -3}),
    "negative_price": ({"unit_price_cents": -1},),
    "unknown_currency": ({"currency": "USD"}, {"currency": None}),
    "amount_out_of_range": ({"unit_price_cents": 1000001}, {"qty": 10001}),
}


def _pair_rows(rule_names: Sequence[str]) -> list[dict[str, object]]:
    """One record per pair of rules, per non-colliding way of breaking both.

    Generated from `_BREAKERS` and the lane's own `RULES`, so adding a rule adds its pairs
    without anybody remembering to. Records that turn out not to break both are harmless -
    they are more adversarial records in a file made of them - and the coverage test is what
    decides whether the pair is covered.
    """
    rows: list[dict[str, object]] = []
    for left, right in itertools.combinations(rule_names, 2):
        for index, (a, b) in enumerate(
            itertools.product(_BREAKERS.get(left, ()), _BREAKERS.get(right, ()))
        ):
            if set(a) & set(b):
                continue  # the two want the same column; another combination will do it
            # Tagged in `boundary`, not in `event_id`: one of the breakers is "no event_id at
            # all", so the id is not a place a fixture can keep its own name. `boundary` is a
            # payload column no rule reads. The tag is what makes the coverage below
            # ATTRIBUTABLE - a pair has to be covered by a record generated FOR it, not by some
            # other record that happens to trip both.
            rows.append(event(boundary=f"pair:{left}+{right}#{index}", **{**a, **b}))
    return rows


def _matrix_rows() -> list[dict[str, object]]:
    """The parity matrix, plus the three amounts that broke the real deployment."""
    return [
        event(event_id="ok"),
        event(event_id=None),
        event(event_type=None),
        event(event_type="warehouse_pinged"),
        event(event_ts="not-a-timestamp"),
        event(currency=None),
        event(currency="USD"),
        event(unit_price_cents=None),
        event(unit_price_cents=-1),
        event(qty=None),
        event(qty=0),
        event(customer_id=None),
        event(event_type="order_line_amended", new_qty=3),
        event(event_type="order_line_amended", new_qty=None),
        event(event_type="order_line_amended", new_qty=-5),
        event(event_type="return_registered", qty=1),
        event(event_type="return_registered", qty=None),
        event(event_type="customer_upserted", customer_id=None),
        # The three the deployment turned into revenue. Long.MaxValue is what the generator
        # emits for its `bad-*` events; the others are one past each contract bound.
        event(event_id="maxlong", unit_price_cents=9223372036854775807),
        event(event_id="past-qty-bound", qty=10001),
        event(event_id="past-price-bound", unit_price_cents=1000001),
        # TWO faults in one record, which no record in this matrix had. Every other row here
        # breaks exactly one rule, so the ORDER of the rules decided nothing and the two lanes
        # could declare them in different orders and still agree on all twenty. They did: the
        # OSS CASE tested the bounds before the currency and the Databricks `RULES` declared
        # the currency before the bounds, so this record - priced past the bound AND in dollars
        # - was `amount_out_of_range` on one lane and `unknown_currency` on the other. The
        # rules were identical; their sequence was not, and the sequence is what a quarantine
        # report is grouped by. The OSS branches are built in the order `RULES` declares them
        # now, and this row is what holds them there.
        event(event_id="two-faults", currency="USD", unit_price_cents=1000001),
        *_pair_rows(tuple(_BREAKERS)),
    ]


def _as_strings(rows: list[dict[str, object]]) -> tuple[list[dict[str, object]], str]:
    """The same records with the numeric columns as text, and a DDL to match.

    Not a hypothetical shape: `DESCRIBE silver_classified` on the deployed lane returned 21
    columns and every one of them was STRING.
    """
    from samegold.pipelines.schema import bronze_schema

    numeric = {"qty", "new_qty", "unit_price_cents"}
    ddl = ", ".join(
        f"{f.name} string" if f.name in numeric else f"{f.name} {f.dataType.simpleString()}"
        for f in bronze_schema().fields
    )
    out: list[dict[str, object]] = []
    for row in rows:
        copy = dict(row)
        for column in numeric:
            copy[column] = None if copy[column] is None else str(copy[column])
        out.append(copy)
    return out, ddl


def test_no_rule_is_undecidable_on_any_record_of_the_matrix(spark) -> None:  # type: ignore[no-untyped-def]
    """A predicate that cannot answer is a defect, not a case.

    This is the check that was missing at the root. The parity test asked whether the two
    derivations AGREE; nothing asked whether either could DECIDE. On the types bronze now
    declares, every rule must return TRUE or FALSE for every record - because a rule that
    returns NULL hands the row's fate to whichever rendering happens to evaluate it, which is
    exactly how one predicate came to mean "drop" in the expectations and "accept" in the CASE.
    """
    from pyspark.sql import functions as F

    from samegold.pipelines.schema import bronze_schema

    namespace = _databricks_namespace()
    rules = namespace["RULES"]
    assert isinstance(rules, dict) and rules, "no RULES were read at all"

    # The rule that DECIDES a row is the first one it does not pass, and only that one has to
    # be able to answer. Rules are ordered on purpose - `missing_required_field` runs before
    # the value rules so that those can assume presence - so a record with no `event_type` at
    # all leaves `non_positive_quantity` undecidable and it is harmless, because
    # `unknown_event_type` decided the row three branches earlier. The first version of this
    # test asserted that NO rule was ever NULL and failed on exactly those records: the right
    # property is narrower and sharper.
    frame = spark.createDataFrame(_matrix_rows(), bronze_schema())
    rows = frame.select(
        F.col("event_id"),
        F.expr(str(namespace["_REASON"])).alias("reason"),
        F.expr(str(namespace["_UNDECIDED"])).alias("undecided"),
    ).collect()
    offenders = [(row["event_id"], row["reason"]) for row in rows if row["undecided"]]
    assert not offenders, (
        f"on the types bronze declares, these records were classified by a rule that returned "
        f"NULL - neither true nor false: {offenders}. Fail-closed keeps them out of revenue, "
        f"but the reason on the row was not established by anything."
    )


def test_the_bound_literals_carry_their_width_so_nothing_is_undecidable_on_strings(spark) -> None:  # type: ignore[no-untyped-def]
    """The round-seventeen reproduction, re-run, and it no longer reproduces.

    On STRING columns `unit_price_cents > 1000000` coerces the string to the LITERAL's INT32,
    9223372036854775807 overflows it, and non-ANSI Spark yields NULL - so the predicate could
    not answer, and a CASE whose ELSE was `accepted` booked the row. Round seventeen fixed the
    second half (nothing undecidable can be accepted) and left the first: the rule still could
    not answer, it just failed closed into `negative_price`, which is the wrong door reached
    for the right reason.

    Every bound literal now carries its width - `1000000L`, `0L`, `10000L` - so the STRING is
    coerced to BIGINT instead, and the same expression on the same columns in the same ANSI
    mode DECIDES. This test is the measurement: `undecided_rules` is empty for every record of
    the matrix even here, and `maxlong` leaves through `amount_out_of_range`, which is the door
    the contract has for it, rather than through the first rule that could not answer.

    It is kept in non-ANSI mode on purpose even though it now passes in both. That is the mode
    the deployed pipeline behaved as (docs/limits.md has the table), and a test that only
    exercises the safe mode is the test this file had for a round.
    """
    from pyspark.sql import functions as F

    from samegold.pipelines.transform import quarantine_reason

    namespace = _databricks_namespace()
    reason, undecided = str(namespace["_REASON"]), str(namespace["_UNDECIDED"])
    rows, ddl = _as_strings(_matrix_rows())
    frame = spark.createDataFrame(rows, ddl)

    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        classified = frame.select(
            F.col("event_id"),
            F.expr(reason).alias("reason"),
            F.expr(undecided).alias("undecided"),
            quarantine_reason().alias("oss"),
        ).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)
    by_id = {row["event_id"]: row for row in classified}

    undecidable = [(r["event_id"], r["undecided"]) for r in classified if r["undecided"]]
    assert not undecidable, (
        f"a rule could not answer on STRING columns, which is what the `L` suffixes exist to "
        f"prevent: {undecidable}"
    )
    maxlong = by_id["maxlong"]
    assert maxlong["reason"] == "amount_out_of_range", maxlong["reason"]
    # And the OSS lane, on the same columns in the same mode. Its bound literals go through
    # `_bound()`, which casts them to BIGINT for exactly this reason, so the two lanes agree
    # here as well as on the declared types.
    disagreements = [
        (r["event_id"], r["oss"], r["reason"]) for r in classified if r["oss"] != r["reason"]
    ]
    assert not disagreements, f"the two lanes disagree on STRING columns: {disagreements}"


def test_every_pair_of_rules_has_a_record_that_breaks_both(spark) -> None:  # type: ignore[no-untyped-def]
    """The ORDER of the rules is part of the contract, and only a two-fault record tests it.

    The lanes declare the same seven rules and used to declare them in different orders. Every
    record in this matrix broke exactly one rule, so the order decided nothing and twenty
    records reported agreement; on a record that breaks two - priced past the bound AND in
    dollars - one lane said `amount_out_of_range` and the other `unknown_currency`. Same rules,
    different door, and a quarantine report is grouped by door.

    Adding that one record fixed the case. This is the class: for EVERY pair of rules there has
    to be a record that both reject, so that the relative order of every pair is exercised by
    `test_the_databricks_rules_and_the_oss_case_agree_record_by_record`, which compares the two
    derivations over this same matrix. Seven rules is twenty-one pairs; they are generated from
    `_BREAKERS`, not written out, so a rule added tomorrow brings its six new pairs with it and
    this test fails if it arrives without a way to break it.

    COVERAGE IS ATTRIBUTABLE, and the first version of this test was not. It asked whether ANY
    record in the matrix broke both rules of a pair, and passed with a whole rule's breakers
    deleted - because `event_type = NULL` makes every value rule NULL, so the records generated
    for one pair were incidentally covering five others. The check was satisfied by the fixture
    it was supposed to be checking. A pair is now covered only by a record generated FOR that
    pair, tagged in `boundary`, which is what makes deleting a breaker fail.

    "Breaks" means what the classification means by it: `NOT COALESCE(rule, false)`. A rule that
    cannot answer rejects the row, so it counts - that is the definition the CASE branches and
    the expectations both use, rather than a third one invented here. The distinction still
    matters and is asserted separately below.
    """
    from pyspark.sql import functions as F

    from samegold.pipelines.schema import bronze_schema

    rules = _databricks_namespace()["RULES"]
    assert isinstance(rules, dict)
    assert set(_BREAKERS) == set(rules), (
        f"the rules and the ways to break them have drifted apart: only in RULES "
        f"{sorted(set(rules) - set(_BREAKERS))}, only in _BREAKERS "
        f"{sorted(set(_BREAKERS) - set(rules))}. A rule with no breaker has no pair records, "
        f"so nothing checks where it sits relative to any other rule."
    )

    frame = spark.createDataFrame(_matrix_rows(), bronze_schema())
    # Both readings of each rule on each record: rejected (what the classification acts on),
    # and decidably false (a real second fault rather than a rule that could not answer).
    evaluated = frame.select(
        F.col("boundary"),
        *[
            F.expr(f"NOT COALESCE({predicate}, false)").alias(f"reject:{name}")
            for name, predicate in rules.items()
        ],
        *[
            F.expr(f"({predicate}) IS NOT NULL AND NOT ({predicate})").alias(f"false:{name}")
            for name, predicate in rules.items()
        ],
    ).collect()

    uncovered: list[tuple[str, str]] = []
    weak_only: list[tuple[str, str]] = []
    for left, right in itertools.combinations(rules, 2):
        tag = f"pair:{left}+{right}#"
        mine = [row for row in evaluated if (row["boundary"] or "").startswith(tag)]
        rejecting = [r for r in mine if r[f"reject:{left}"] and r[f"reject:{right}"]]
        if not rejecting:
            uncovered.append((left, right))
        elif not any(r[f"false:{left}"] and r[f"false:{right}"] for r in mine):
            weak_only.append((left, right))

    assert not uncovered, (
        f"no record GENERATED FOR {uncovered} is rejected by both of its rules. The relative "
        f"order of those rules is unobservable: the two lanes could declare them the other way "
        f"round and every test here would still pass, which is exactly what happened to "
        f"`unknown_currency` and `amount_out_of_range`. Add a way to break them to _BREAKERS."
    )

    # And the pairs that are covered only by an UNDECIDABLE rule rather than by two real
    # faults, which is a weaker witness and has one legitimate cause: every value rule is
    # guarded by the event type (`event_type <> 'order_placed' OR ...`), so with an unknown
    # event type the guard is either TRUE - the rule passes - or NULL when the type is missing
    # altogether. There is therefore no record on which `unknown_event_type` and a value rule
    # are both FALSE, and that is a property of the rules rather than a gap in the fixture.
    # Any OTHER pair landing here means a rule cannot be broken outright and nobody noticed.
    unexplained = [pair for pair in weak_only if "unknown_event_type" not in pair]
    assert not unexplained, (
        f"{unexplained} are covered only by a rule that could not ANSWER, not by two faults. "
        f"For a pair not involving `unknown_event_type` that means one of the two rules has no "
        f"breaker that makes it plainly false, so the pair is being tested by an accident of "
        f"NULL propagation rather than by the fault it is named after."
    )


def test_a_rule_that_cannot_answer_quarantines_the_row_instead_of_accepting_it(spark) -> None:  # type: ignore[no-untyped-def]
    """The property the whole round turns on, tested where it can still be observed.

    Both fixes above - typed bronze, and bound literals that carry their width - work by making
    every rule decidable. That is the right fix and it has an awkward consequence: the property
    "a rule that cannot answer must not produce revenue" now holds VACUOUSLY on the real rules,
    and a property nothing can exercise is a property nobody is testing. The next rule someone
    adds, the next column whose type is inferred rather than declared, and it stops holding
    vacuously without any test noticing.

    So a rule that is NULL by construction is injected, and the classification is rendered from
    the lane's OWN function (`_classification`) rather than rebuilt here - the rebuilt version
    is what agreed with the defect for a round. Every record must leave through a door: the
    injected rule cannot pass, so no record can satisfy the conjunction that `accepted`
    requires, and `undecided_rules` must name the rule that could not decide.
    """
    from pyspark.sql import functions as F

    from samegold.pipelines.schema import bronze_schema

    namespace = _databricks_namespace()
    render = namespace["_classification"]
    render_undecided = namespace["_undecided"]
    assert callable(render) and callable(render_undecided)
    rules = dict(namespace["RULES"])  # type: ignore[call-overload]
    assert "unknown_currency" in rules
    rules["unknown_currency"] = "CAST(NULL AS BOOLEAN)"

    frame = spark.createDataFrame(_matrix_rows(), bronze_schema())
    classified = frame.select(
        F.col("event_id"),
        F.expr(str(render(rules))).alias("reason"),
        F.expr(str(render_undecided(rules))).alias("undecided"),
    ).collect()

    accepted = [row["event_id"] for row in classified if row["reason"] == "accepted"]
    assert not accepted, (
        f"a rule that evaluates to NULL let {accepted} through as `accepted`. This is the "
        f"round-seventeen defect exactly: NOT(NULL) is NULL, the WHEN does not match, and the "
        f"row lands in whatever the classification does by default. Acceptance has to be the "
        f"conjunction of the rules, not the remainder after them."
    )
    # And the row that would have been accepted leaves through the rule that could not decide,
    # named, rather than under a business reason nothing established.
    ok = next(row for row in classified if row["event_id"] == "ok")
    assert (ok["reason"], ok["undecided"]) == ("unknown_currency", "unknown_currency")


def test_the_branch_that_cannot_be_reached_raises_rather_than_classifying(spark) -> None:  # type: ignore[no-untyped-def]
    """`accepted` is a conjunction, so the ELSE is dead. Dead has to mean loud.

    The ruling is written where the branch is: a record the classification cannot classify is a
    fault in the PIPELINE, not a reason in the contract's closed enum, so it is not given a name
    in `QuarantineReason` (a reason no run can produce is a dead enum member -
    `test_every_quarantine_reason_is_actually_produced_by_a_run` is the test that says so) and
    it is not allowed to return NULL either, which is the quietest possible answer to "the
    classification did not classify".

    What is asserted here is the MECHANISM, taken from the lane's own source: put that ELSE in a
    CASE whose branches match nothing, and the query fails. Without this, `raise_error` in that
    position is a claim about Spark that nothing in this repository checks.
    """
    unreachable = str(_databricks_namespace()["_UNREACHABLE"])
    with pytest.raises(Exception) as caught:
        spark.sql(f"SELECT CASE WHEN false THEN 'x' ELSE {unreachable} END AS r").collect()
    assert "USER_RAISED_EXCEPTION" in str(caught.value)
    assert "defect in the pipeline" in str(caught.value)


def test_a_value_too_wide_for_its_column_is_counted_not_only_classified(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """The fault that erases itself, and the counter that stops it being silent.

    A price of 2^63 is not a business fault - no rule ever sees it. The READER cannot put it in
    a BIGINT column: Spark nulls that one field and copies the raw line into `_rescued_data`,
    Auto Loader with the schema hints does the same, DuckDB's `json_type` says UBIGINT and
    `TRY_CAST(... AS BIGINT)` gives NULL. After that the record is indistinguishable from one
    whose producer never sent the field, and it leaves through `missing_required_field`, which
    is fail-closed and right.

    Right, and mute. Trading a loud failure for a quiet one is the worst available outcome of
    fixing the types, so this asserts the three things that keep it loud: the column is NULL and
    the rescue column holds the record (nothing was thrown away), the record is classified
    rather than dropped, and the count of values lost this way is carried in the ledger and
    recomputed by the reference. The generator emits these deliberately - corrupt kind
    `beyond_bigint` - so the counter has something to count on every seed.
    """
    from pyspark.sql import functions as F

    from samegold.pipelines.schema import RESCUED_COLUMN
    from samegold.pipelines.transform import classify, read_bronze

    rows = [
        BASE_ORDER,
        dict(BASE_ORDER, event_id="over-1", order_id="O2", unit_price_cents=2**63),
    ]
    bronze = _write(tmp_path, rows)
    classified = {
        row["event_id"]: row for row in classify(read_bronze(spark, str(bronze))).collect()
    }
    over = classified["over-1"]
    assert over["unit_price_cents"] is None, "the value fit a BIGINT after all"
    assert over[RESCUED_COLUMN] is not None, (
        "the value did not fit the column AND was not rescued, which is the shape that leaves "
        "no trace of itself anywhere"
    )
    assert over["quarantine_reason"] == "missing_required_field", over["quarantine_reason"]
    # Not a fourth door: exactly one reason, like every other record.
    assert classified["op-1"]["quarantine_reason"] == "accepted"

    # And the accounting, from two derivations that never met: the generator counted these as
    # it wrote them, the reference recounts them by asking which values need more than 64 bits.
    from samegold.generator.events import FAST, generate
    from samegold.oracle.duckdb_gold import reference_counts
    from samegold.verify.invariants import conservation_against_ledger

    result = generate(tmp_path / "g", seed=20260901, profile=FAST)
    counts = reference_counts(tmp_path / "g" / "bronze")
    assert result.ledger.counts["values_beyond_bigint"] > 0, (
        "no seed produced a value outside BIGINT, so the counter that exists to make that "
        "visible is measuring nothing"
    )
    assert not conservation_against_ledger(result.ledger.counts, counts)
    # The same records, through the Spark reader: rescued, and never accepted.
    frame = classify(read_bronze(spark, str(tmp_path / "g" / "bronze")))
    rescued = frame.where(F.col(RESCUED_COLUMN).isNotNull()).collect()
    assert len(rescued) >= result.ledger.counts["values_beyond_bigint"]
    assert not [row for row in rescued if row["quarantine_reason"] == "accepted"], (
        "a record whose value was lost to the rescue column was published as revenue"
    )


def test_the_databricks_classification_matches_the_ledger_by_reason(spark, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """The comparison that actually closes this lane, and it did not exist.

    Every other check on these rules asks whether two derivations AGREE. Agreement is not
    correctness - this repository says so on its front page - and the two agreed all the way
    through the deployment that booked 2.7e19 as revenue.

    The generator knows, by construction, what verdict each event deserves: it writes the
    corrupt records deliberately and counts them as it writes. So the Databricks
    classification is compared against that count, per reason, the same way
    `verify.invariants.conservation_against_ledger` compares the OSS lane. This is the only
    check here whose right-hand side was not produced by reading the same rules again.
    """
    from pyspark.sql import functions as F

    from samegold.generator.events import FAST, generate
    from samegold.pipelines.schema import bronze_schema

    result = generate(tmp_path / "g", seed=20260901, profile=FAST)
    frame = spark.read.schema(bronze_schema()).json(str(tmp_path / "g" / "bronze"))
    reason = str(_databricks_namespace()["_REASON"])
    counts = {
        row["reason"]: row["n"]
        for row in frame.select(F.expr(reason).alias("reason"))
        .groupBy("reason")
        .agg(F.count("*").alias("n"))
        .collect()
    }

    # Only the ingest-stage reasons: the three return-stage ones are decided in gold, by
    # questions about the SALE, which silver cannot see.
    ingest_reasons = {
        "unparseable_json",
        "unknown_event_type",
        "missing_required_field",
        "non_positive_quantity",
        "negative_price",
        "unknown_currency",
        "amount_out_of_range",
    }
    disagreements = {
        name: (int(result.ledger.quarantine.get(name, 0)), int(counts.get(name, 0)))
        for name in sorted(ingest_reasons)
        if int(result.ledger.quarantine.get(name, 0)) != int(counts.get(name, 0))
    }
    assert not disagreements, (
        f"the Databricks classification and the generator's by-construction ledger disagree, "
        f"as {{reason: (ledger, lane)}}: {disagreements}. The lane that shipped counted the "
        f"three `amount_out_of_range` events as accepted revenue and this is the check that "
        f"would have said so."
    )
