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

    rows = [
        ("e1", "order_placed", "O1", "C1", "S1", 2, 1000, "EUR"),
        ("e2", "order_placed", "O2", "C1", "S1", 0, 1000, "EUR"),  # non-positive quantity
        ("e3", "order_placed", "O3", "C1", "S1", 1, -1, "EUR"),  # negative price
        ("e4", "warehouse_pinged", None, None, None, None, None, None),  # unknown type
        ("e5", "order_placed", "O5", "C1", "S1", 1, 1000, "USD"),  # unknown currency
    ]
    schema = (
        "event_id STRING, event_type STRING, order_id STRING, customer_id STRING, "
        "sku STRING, qty BIGINT, unit_price_cents BIGINT, currency STRING"
    )
    actual = classify(spark.createDataFrame(rows, schema)).select("event_id", "quarantine_reason")
    expected = spark.createDataFrame(
        [
            ("e1", "accepted"),
            ("e2", "non_positive_quantity"),
            ("e3", "negative_price"),
            ("e4", "unknown_event_type"),
            ("e5", "unknown_currency"),
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

    rows = [("e1", "order_placed", "O1", "C1", "S1", 0, -5, "USD")]
    frame = spark.createDataFrame(
        rows,
        "event_id STRING, event_type STRING, order_id STRING, customer_id STRING, "
        "sku STRING, qty BIGINT, unit_price_cents BIGINT, currency STRING",
    )
    reasons = frame.withColumn("r", quarantine_reason()).select(F.col("r")).collect()
    assert len(reasons) == 1
    # Three rules apply to this row; the first one in the CASE wins and the others are not
    # evaluated, which is what makes the outcome a single value rather than a set.
    assert reasons[0]["r"] == "non_positive_quantity"
