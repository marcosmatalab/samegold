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

RULES = {
    "event_id_present": "event_id IS NOT NULL",
    "known_event_type": (
        "event_type IN ('order_placed','order_line_amended','return_registered',"
        "'customer_upserted')"
    ),
    "positive_quantity": "qty IS NULL OR qty > 0",
    "non_negative_price": "unit_price_cents IS NULL OR unit_price_cents >= 0",
    "known_currency": "currency IS NULL OR currency = 'EUR'",
}


@dp.table(name="silver_events", comment="Validated events. Duplicates are resolved in gold.")
@dp.expect_all_or_drop(RULES)
def silver_events():
    return spark.readStream.table("bronze_events")


@dp.table(name="silver_quarantine", comment="Everything silver_events dropped, with the reason.")
def silver_quarantine():
    stream = spark.readStream.table("bronze_events")
    reason = F.lit("accepted")
    for name, predicate in reversed(list(RULES.items())):
        reason = F.when(~F.expr(predicate), F.lit(name)).otherwise(reason)
    return stream.withColumn("quarantine_reason", reason).where(
        F.col("quarantine_reason") != "accepted"
    )
