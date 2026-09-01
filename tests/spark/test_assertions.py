"""Spark's own test helpers, and when each one is the right tool.

The exam guide asks for tests "using assertDataFrameEqual, assertSchemaEqual". They are the
right instrument for a unit test of one transformation: they compare a small DataFrame with an
expected one and print a readable diff of the rows that differ.

They are the WRONG instrument for the comparison this project is built around, and the
difference is worth writing down rather than picking one and moving on:

  * ``assertDataFrameEqual`` needs both sides in one Spark session. The reference
    implementation runs in DuckDB, in the same process but a different engine, and the whole
    point is that it never touches Spark.
  * It compares rows, so it needs both datasets in the driver. The canonical digest compares
    a 16-byte hash, which is what makes it usable as evidence in a JSON file and what lets a
    reader re-check a published number without the data.
  * It has no notion of a projection, so it would happily compare a column whose value is a
    wall clock and report a difference that means nothing.

So: these helpers for unit tests, the digest for cross-engine evidence.
"""

from __future__ import annotations

import pytest

from samegold.pipelines.schema import bronze_schema
from samegold.pipelines.transform import classify, quarantine_reason

pytestmark = pytest.mark.spark


def test_assert_schema_equal_catches_a_type_change(spark) -> None:  # type: ignore[no-untyped-def]
    from pyspark.testing import assertSchemaEqual

    left = spark.createDataFrame([(1, "a")], "qty INT, sku STRING")
    right = spark.createDataFrame([(1, "a")], "qty BIGINT, sku STRING")
    assertSchemaEqual(left.schema, left.schema)
    with pytest.raises(Exception):  # noqa: B017 - the helper raises its own error type
        assertSchemaEqual(left.schema, right.schema)


def test_assert_dataframe_equal_on_the_quarantine_rules(spark) -> None:  # type: ignore[no-untyped-def]
    """A unit test of one transformation, in the shape the exam guide asks for."""
    from pyspark.testing import assertDataFrameEqual

    ts, arrived = "2026-01-10T10:00:00+00:00", "2026-01-10T10:05:00+00:00"

    def event(**overrides: object) -> dict[str, object]:
        """A record with every bronze column present, then the one thing under test changed.

        Built from `bronze_schema()` rather than from an ad-hoc column list on purpose. The
        classification reads columns that only some event types carry (`new_qty` for an
        amendment), so a frame with a convenient subset of columns does not type-check
        against the expression, and a test that quietly used a subset would be testing a
        different function from the one the pipeline runs.
        """
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
            unit_price_cents=1000,
            currency="EUR",
        )
        base.update(overrides)
        return base

    rows = [
        event(event_id="e1"),
        event(event_id="e2", qty=0),  # non-positive quantity
        event(event_id="e3", unit_price_cents=-1),  # negative price
        event(event_id="e4", event_type="warehouse_pinged"),  # unknown type
        event(event_id="e5", currency="USD"),  # unknown currency
        # The four shapes an adversarial review found accepted, because a comparison with a
        # NULL is NULL rather than false: they used to come out of this expression as
        # "accepted" and book revenue the DuckDB reference refused to count.
        event(event_id="e6", currency=None),
        event(event_id="e7", unit_price_cents=None),
        event(event_id="e8", event_ts="not-a-timestamp"),
        event(event_id=None),
    ]
    actual = classify(spark.createDataFrame(rows, bronze_schema())).select(
        "event_id", "quarantine_reason"
    )
    expected = spark.createDataFrame(
        [
            ("e1", "accepted"),
            ("e2", "non_positive_quantity"),
            ("e3", "negative_price"),
            ("e4", "unknown_event_type"),
            ("e5", "unknown_currency"),
            ("e6", "missing_required_field"),
            ("e7", "missing_required_field"),
            ("e8", "missing_required_field"),
            (None, "unparseable_json"),
        ],
        "event_id STRING, quarantine_reason STRING",
    )
    assertDataFrameEqual(actual, expected)


def test_the_quarantine_expression_has_exactly_one_outcome_per_row(spark) -> None:  # type: ignore[no-untyped-def]
    """The conservation invariant depends on this: a record leaves through one door.

    Written as a single CASE rather than a chain of filters precisely so that this is true by
    construction, and asserted anyway because "by construction" is how invariants die.
    """
    from pyspark.sql import functions as F

    row: dict[str, object] = dict.fromkeys(bronze_schema().fieldNames())
    row.update(
        event_id="e1",
        event_type="order_placed",
        event_ts="2026-01-10T10:00:00+00:00",
        arrival_ts="2026-01-10T10:05:00+00:00",
        order_id="O1",
        customer_id="C1",
        sku="S1",
        qty=0,
        unit_price_cents=-5,
        currency="USD",
    )
    frame = spark.createDataFrame([row], bronze_schema())
    reasons = frame.withColumn("r", quarantine_reason()).select(F.col("r")).collect()
    assert len(reasons) == 1
    # Three rules apply to this row; the first one in the CASE wins and the others are not
    # evaluated, which is what makes the outcome a single value rather than a set.
    assert reasons[0]["r"] == "non_positive_quantity"
