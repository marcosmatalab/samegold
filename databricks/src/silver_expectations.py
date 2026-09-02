"""Silver on Databricks, with the quality rules as pipeline expectations.

This is the piece open-source SDP does not have. The rules are the same ones the OSS lane
applies as a CASE expression in `samegold.pipelines.transform.quarantine_reason`; here they
are declared, so the pipeline event log reports pass and fail counts per expectation and the
dashboard can show them.

Note which action each rule takes. `expect_or_drop` on the hard rules, never
`expect_or_fail`: a single malformed record must not stop a nightly close. Dropped rows are
not lost - they are the quarantine table below, which is what keeps the conservation
invariant (ingested = accepted + quarantined + rescued + deduplicated) checkable.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()

# The rule names ARE the quarantine reasons from the contract, not names invented here. The
# first version used its own vocabulary (`positive_quantity`, `known_currency`, ...), so the
# same record left through a differently-named door on this lane and `missing_required_field`
# was unreachable in it. A closed enum that only two of three implementations use is not a
# closed enum, and the event log is where an operator reads these names.
#
# Every predicate is NULL-SAFE, in the same direction as the OSS lane. They used to read
# `qty IS NULL OR qty > 0`, which PASSES a record with a missing quantity: an order line with
# no `currency` satisfied every rule here, was not dropped, and was booked as revenue on this
# lane while both OSS engines quarantined it. Presence is checked first, per event type, then
# the value rules apply to columns that are known to be there.
_PRESENT_FOR_TYPE = (
    "CASE event_type"
    " WHEN 'order_placed' THEN order_id IS NOT NULL AND sku IS NOT NULL"
    " AND customer_id IS NOT NULL AND qty IS NOT NULL"
    " AND unit_price_cents IS NOT NULL AND currency IS NOT NULL"
    " WHEN 'order_line_amended' THEN order_id IS NOT NULL AND sku IS NOT NULL"
    " AND new_qty IS NOT NULL"
    " WHEN 'return_registered' THEN order_id IS NOT NULL AND sku IS NOT NULL"
    " AND qty IS NOT NULL"
    " WHEN 'customer_upserted' THEN customer_id IS NOT NULL"
    " ELSE TRUE END"
)

RULES = {
    "unparseable_json": "event_id IS NOT NULL",
    "unknown_event_type": (
        # `IN` is NULL for a NULL left operand, and a NULL predicate does not drop the row:
        # a record with no event_type at all was ACCEPTED on this lane and quarantined by
        # both OSS engines. The same NULL-safety class as the rules below, missed once.
        "event_type IS NOT NULL AND event_type IN ('order_placed','order_line_amended',"
        "'return_registered','customer_upserted')"
    ),
    "missing_required_field": (
        f"try_to_timestamp(event_ts) IS NOT NULL AND try_to_timestamp(arrival_ts) IS NOT NULL"
        f" AND ({_PRESENT_FOR_TYPE})"
    ),
    "non_positive_quantity": (
        "(event_type NOT IN ('order_placed','return_registered') OR qty > 0)"
        # An amendment to a quantity of zero or less is the same fault by another name, and
        # no lane rejected it: `NON_POSITIVE_QUANTITY` was gated on the two event types that
        # carry `qty` and an `order_line_amended` carries `new_qty`. All three lanes agreed,
        # so no parity test could see it, and the generator's `max(1, ...)` guaranteed no
        # seed would produce it. An amendment to -5 drove gross revenue negative. The
        # generator emits the zero now, as boundary case 14.
        " AND (event_type <> 'order_line_amended' OR new_qty > 0)"
    ),
    "negative_price": "event_type <> 'order_placed' OR unit_price_cents >= 0",
    "unknown_currency": "event_type <> 'order_placed' OR currency = 'EUR'",
    # Bounded, because qty * unit_price_cents is a BIGINT multiplication. See
    # domain/contract.py: three lines at the maximum legal price ended the close outright.
    "amount_out_of_range": (
        "(event_type NOT IN ('order_placed','return_registered') OR qty <= 10000)"
        " AND (event_type <> 'order_line_amended' OR new_qty <= 10000)"
        " AND (event_type <> 'order_placed' OR unit_price_cents <= 1000000)"
    ),
}


# The classification, as a column, over EVERY row. This table is what gold reads.
#
# The lane used to have only `silver_events` (expectations, rows dropped) and
# `silver_quarantine` (the dropped rows, with a reason), and `gold_close.py` then selected
# `quarantine_reason` from `silver_events` - a column that table does not have, because an
# expectation drops a row and does not annotate it. The whole gold close would have failed
# analysis on its first refresh. Its statements parse, which is what the parse test checked;
# they did not resolve, which is a different question and is now asked too.
#
# There is a second reason to read the classified table rather than the filtered one, and it
# is the reason the OSS lane is built that way: deduplication runs over the WHOLE population
# and validity is applied after it. Deduplicating only the survivors lets a good copy of a
# duplicated event win here and an invalid copy win there, which is a divergence no parity
# test could see because the two lanes would be computing different things.
_REASON = (
    "CASE"
    " WHEN event_id IS NULL THEN 'unparseable_json'"
    " WHEN event_type IS NULL OR event_type NOT IN ('order_placed','order_line_amended',"
    "'return_registered','customer_upserted') THEN 'unknown_event_type'"
    " WHEN try_to_timestamp(event_ts) IS NULL OR try_to_timestamp(arrival_ts) IS NULL"
    " THEN 'missing_required_field'"
    f" WHEN NOT ({_PRESENT_FOR_TYPE}) THEN 'missing_required_field'"
    " WHEN event_type IN ('order_placed','return_registered') AND qty <= 0"
    " THEN 'non_positive_quantity'"
    " WHEN event_type = 'order_line_amended' AND new_qty <= 0 THEN 'non_positive_quantity'"
    " WHEN event_type = 'order_placed' AND unit_price_cents < 0 THEN 'negative_price'"
    " WHEN event_type = 'order_placed' AND currency <> 'EUR' THEN 'unknown_currency'"
    " WHEN (event_type IN ('order_placed','return_registered') AND qty > 10000)"
    " OR (event_type = 'order_line_amended' AND new_qty > 10000)"
    " OR (event_type = 'order_placed' AND unit_price_cents > 1000000)"
    " THEN 'amount_out_of_range'"
    " ELSE 'accepted' END"
)


@dp.table(name="silver_classified", comment="Every event, tagged with why it was accepted.")
def silver_classified() -> DataFrame:
    return spark.readStream.table("bronze_events").withColumn("quarantine_reason", F.expr(_REASON))


@dp.table(name="silver_events", comment="Validated events. Duplicates are resolved in gold.")
@dp.expect_all_or_drop(RULES)
def silver_events() -> DataFrame:
    """The same rules as `_REASON`, declared as expectations.

    This table exists for the EVENT LOG: expectations are what make pass and fail counts per
    rule appear there and on the dashboard, which is the piece open-source SDP does not have
    and the reason this lane exists at all. Gold reads `silver_classified`, not this one.

    The two derivations of the same rules - these predicates and the CASE expression in
    `samegold.pipelines.transform.quarantine_reason` - are compared record by record by
    `tests/spark/test_adversarial_records.py::
    test_the_databricks_rules_and_the_oss_case_agree_record_by_record`, which evaluates both
    in one Spark session over a matrix of eighteen records including every NULL shape. An
    earlier version of this docstring claimed the comparison happened "as a row count that
    does not match silver_classified", which nothing computed.
    """
    return spark.readStream.table("bronze_events")


@dp.table(name="silver_quarantine", comment="Everything silver_events dropped, with the reason.")
def silver_quarantine() -> DataFrame:
    return (
        spark.readStream.table("silver_classified")
        .where(F.col("quarantine_reason") != "accepted")
        .select("event_id", "event_type", "arrival_ts", "quarantine_reason")
    )
