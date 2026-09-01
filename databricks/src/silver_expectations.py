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
from pyspark.sql import SparkSession
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
        "event_type NOT IN ('order_placed','return_registered') OR qty > 0"
    ),
    "negative_price": "event_type <> 'order_placed' OR unit_price_cents >= 0",
    "unknown_currency": "event_type <> 'order_placed' OR currency = 'EUR'",
}


@dp.table(name="silver_events", comment="Validated events. Duplicates are resolved in gold.")
@dp.expect_all_or_drop(RULES)
def silver_events():
    return spark.readStream.table("bronze_events")


@dp.table(name="silver_quarantine", comment="Everything silver_events dropped, with the reason.")
def silver_quarantine():
    stream = spark.readStream.table("bronze_events")
    reason = F.lit("accepted")
    # Reversed so the FIRST rule in RULES wins, which is the order the OSS CASE evaluates in:
    # a record that breaks three rules leaves through one door, and it must be the same door
    # on both lanes or the two quarantine tables cannot be compared.
    for name, predicate in reversed(list(RULES.items())):
        reason = F.when(~F.expr(predicate), F.lit(name)).otherwise(reason)
    return stream.withColumn("quarantine_reason", reason).where(
        F.col("quarantine_reason") != "accepted"
    )
