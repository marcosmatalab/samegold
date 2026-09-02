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

# `active()`, not `getActiveSession()`. The second returns `SparkSession | None`, and every
# use below then reads an attribute off a value that may be None - which mypy says plainly once
# pyspark is installed, and said to nobody for eleven rounds because the fast lane that runs
# mypy does not install it. `active()` returns a session or raises, which is the behaviour this
# file wants: there is no sensible way to run a pipeline source with no session.
spark = SparkSession.active()

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
# DERIVED FROM `RULES`, not written again beside them, and that is this round's finding rather
# than a tidy-up. The two used to be independent renderings of the same rules, and a NULL
# predicate meant the OPPOSITE thing in each:
#
#   * `expect_all_or_drop` treats a predicate that is not TRUE as not satisfied, so the row is
#     dropped. NULL fails closed.
#   * a `CASE ... WHEN p THEN reason ... ELSE 'accepted'` does not match on a NULL `p`, so the
#     row falls past every branch and lands in the ELSE. NULL fails OPEN, into revenue.
#
# On the first real deployment three events with `unit_price_cents = 9223372036854775807` came
# out `accepted` and contributed 2.7e19 to a month's gross - six and a half million times the
# contract's own ceiling for a single line. The expectations were right and the CASE was wrong,
# about the same rule, in the same file. They agreed on every test because every test ran them
# on typed columns where no predicate is ever NULL.
#
# Two changes, and the second is the one that generalises:
#
#  1. one declaration. `RULES` says what must HOLD; the branches below are generated from it,
#     so a rule cannot be fixed in one rendering and left wrong in the other.
#  2. acceptance is POSITIVE. Every branch is wrapped in `COALESCE(..., false)`, so a row is
#     `accepted` only when every rule evaluated explicitly to TRUE. `accepted` used to be what
#     happened when nothing else matched, which makes every predicate that cannot answer into
#     revenue. Now a predicate that cannot answer sends the row through that rule's own door -
#     the same door the expectation sends it through, by construction rather than by review.
#
# `undecided_rules` names any rule that evaluated to NULL on a row. With the types declared at
# ingest it should always be empty; it is carried and counted so that if it is ever non-empty
# the run says so, instead of the row being quietly quarantined under a business reason and the
# defect going unnoticed. tests/spark asserts it is empty for every record in the matrix.
_REASON = (
    "CASE "
    + " ".join(
        f"WHEN NOT COALESCE({predicate}, false) THEN '{name}'" for name, predicate in RULES.items()
    )
    + " ELSE 'accepted' END"
)

# The rule that DECIDED the row, and only if it could not answer.
#
# Rules are ordered, and a later rule is allowed to be undecidable about a record an earlier
# rule already rejected: `non_positive_quantity` is NULL for a record with no `event_type`,
# and it does not matter, because `unknown_event_type` decided that row three branches
# earlier. Reporting every NULL predicate would report those, and a diagnostic that is noisy
# on healthy data is a diagnostic nobody reads.
#
# What matters is the rule the row actually left through. If THAT one returned NULL, the row
# was quarantined under a reason nothing established - fail closed, which is right, but on a
# guess - and the run has to say so.
_UNDECIDED = (
    "CASE "
    + " ".join(
        f"WHEN NOT COALESCE({predicate}, false) "
        f"THEN (CASE WHEN ({predicate}) IS NULL THEN '{name}' ELSE '' END)"
        for name, predicate in RULES.items()
    )
    + " ELSE '' END"
)


@dp.table(name="silver_classified", comment="Every event, tagged with why it was accepted.")
def silver_classified() -> DataFrame:
    return (
        spark.readStream.table("bronze_events")
        .withColumn("quarantine_reason", F.expr(_REASON))
        .withColumn("undecided_rules", F.expr(_UNDECIDED))
    )


@dp.table(name="silver_events", comment="Validated events. Duplicates are resolved in gold.")
# `expect_all_or_drop` is not defined in the open-source `pyspark.pipelines` at all - the
# module docstring above says as much in prose, and mypy says it as an error now that the
# Spark-facing code is type-checked. Expectations are the Databricks-only piece this whole
# file exists to show; PARITY.md records it with the API check that proves it.
@dp.expect_all_or_drop(RULES)  # type: ignore[attr-defined]
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
