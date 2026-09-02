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
import json
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


def _matrix_rows() -> list[dict[str, object]]:
    """The parity matrix, plus the three amounts that broke the real deployment."""
    from samegold.pipelines.schema import bronze_schema

    ts, arrived = "2026-01-10T10:00:00+00:00", "2026-01-10T10:05:00+00:00"

    def event(**overrides: object) -> dict[str, object]:
        base: dict[str, object] = dict.fromkeys(bronze_schema().fieldNames())
        base.update(
            event_id="e",
            event_type="order_placed",
            event_ts=ts,
            arrival_ts=arrived,
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


def test_the_classification_fails_closed_when_a_predicate_cannot_answer(spark) -> None:  # type: ignore[no-untyped-def]
    """The property that would have stopped the deployment booking 2.7e19 as revenue.

    On STRING columns the bounds predicate IS undecidable, and no amount of care in the rules
    changes that - the coercion happens in the engine. What has to hold is that an undecidable
    predicate cannot produce `accepted`. The old CASE ended in `ELSE 'accepted'`, so everything
    the system could not understand became revenue; the generated one wraps every branch in
    `COALESCE(..., false)`, so acceptance is established positively and anything else leaves
    through the door of the rule that could not pass it.
    """
    from pyspark.sql import functions as F

    namespace = _databricks_namespace()
    reason, undecided = str(namespace["_REASON"]), str(namespace["_UNDECIDED"])
    rows, ddl = _as_strings(_matrix_rows())
    frame = spark.createDataFrame(rows, ddl)

    # ANSI mode is pinned OFF, and that is the whole reproduction rather than a convenience.
    # Spark 4 defaults ANSI ON, where the string is widened and the comparison answers `true`;
    # the Databricks pipeline behaved as ANSI OFF, where the string is coerced to the INT32
    # literal's type, 9223372036854775807 overflows it, and the cast yields NULL. Measured both
    # ways on pyspark 4.2.0. Running this test in the default mode passes without exercising
    # anything - which is the same shape of mistake as running the parity matrix on typed
    # columns and calling it agreement.
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        classified = frame.select(
            F.col("event_id"),
            F.expr(reason).alias("reason"),
            F.expr(undecided).alias("undecided"),
        ).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)
    by_id = {row["event_id"]: row for row in classified}

    maxlong = by_id["maxlong"]
    assert maxlong["reason"] != "accepted", (
        "an event priced at Long.MaxValue came out `accepted` on STRING columns, which is the "
        "deployed defect reproduced exactly: the value coerced to INT32 is NULL, and a CASE "
        "whose ELSE is `accepted` turns NULL into revenue"
    )
    # It leaves through `negative_price`, not through the bounds rule, and that is worth
    # keeping rather than asserting away: on STRING columns `unit_price_cents >= 0` is ALREADY
    # undecidable - the literal `0` is INT32 too - so the first rule that cannot answer is the
    # one that catches the row. Which door it leaves by is an accident of ordering; that it
    # leaves at all is the property. On the types bronze now declares it leaves through
    # `amount_out_of_range`, which is what the ledger comparison checks.
    assert maxlong["reason"] == "negative_price", maxlong["reason"]
    # And the diagnostic names the rule that could not decide, so a run where this happens
    # says so instead of the row being quietly filed under a business reason.
    assert maxlong["undecided"] == "negative_price", maxlong["undecided"]
    # Nothing in the matrix may be accepted while a rule was undecidable about it.
    leaked = [
        (row["event_id"], row["undecided"])
        for row in classified
        if row["reason"] == "accepted" and row["undecided"]
    ]
    assert not leaked, f"accepted despite an undecidable rule: {leaked}"


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
