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
    bronze.mkdir(parents=True)
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


def _both_agree(spark: Any, tmp_path: Path, rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    bronze = _write(tmp_path, rows)
    spark_rows = sorted(_spark_close(spark, bronze))
    duckdb_rows = sorted(_duckdb_close(bronze))
    assert spark_rows == duckdb_rows, f"Spark {spark_rows} != DuckDB {duckdb_rows}"
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
