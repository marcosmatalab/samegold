"""Silver on Databricks, with the quality rules as pipeline expectations.

This is the piece open-source SDP does not have. The rules are the same ones the OSS lane
applies as a CASE expression in `samegold.pipelines.transform.quarantine_reason`; here they
are declared, so the pipeline event log reports pass and fail counts per expectation and the
dashboard can show them.

Note which action each rule takes. `expect_or_drop` on the hard rules, never
`expect_or_fail`: a single malformed record must not stop a nightly close. Dropped rows are
not lost - they are the quarantine table below, which is what keeps the conservation
invariant (ingested = accepted + quarantined + rescued + deduplicated) checkable.

AND ONE THING IN THIS FILE DOES ABORT THE UPDATE, WHICH LOOKS LIKE THE OPPOSITE OF THE
PARAGRAPH ABOVE AND IS NOT. The asymmetry is deliberate and is worth stating here, where the
principle is, rather than only at the branch that breaks it:

  * A RULE THAT SAYS NO DROPS THE RECORD. `unit_price_cents` is negative, the currency is not
    EUR, the quantity is zero: these are things a producer does, they will happen at 03:00 on
    a Sunday, and a close that stops for one of them is a close that stops. The record goes to
    quarantine with a name on it and the run continues. That is the paragraph above and it is
    unchanged.

  * A CLASSIFICATION THAT CANNOT DECIDE ABORTS. `_REASON` below ends in a branch that calls
    `raise_error`, and reaching it fails the pipeline update. This is not a rule saying no; it
    is every rule having failed to say anything, which after bronze is typed and the bound
    literals carry their width cannot happen. The branch is an assertion, and the whole value
    of an assertion is that it stops.

The line between them is WHO IS AT FAULT. A dropped record is a statement about the DATA, and
the right response to bad data is to set it aside and carry on - the quarantine table is that
response, and it is why this pipeline can be left running. Reaching the final branch is a
statement about the CODE: the rules no longer partition the records, so every verdict the run
is producing is suspect, not just the one row. Continuing there does not save the close, it
publishes a close nobody can vouch for - and that is exactly what happened on the first real
deployment, where a rule that could not answer was not a fault anybody could see and 2.7e19
cents of revenue were published as success. The cost of stopping is one night. The cost of
carrying on was a month's revenue figure that was wrong by a factor of six and a half million,
reported green.

So: THE RULES DROP, THE IMPOSSIBILITY OF DECIDING ABORTS. If the second ever fires on a real
run, the message says it is a defect in the pipeline and not a fault in the record, because
whoever is woken up by it needs to know which of the two paragraphs above they are in.
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

# EVERY BOUND LITERAL CARRIES ITS WIDTH: `0L`, `10000L`, `1000000L`, never the bare number.
#
# `1000000` is an INT32 literal in Spark SQL, and the operand it is compared against is coerced
# to THE LITERAL'S type, not the other way round. On the STRING columns Auto Loader inferred
# before this lane had schema hints, `unit_price_cents > 1000000` therefore cast a string
# holding 9223372036854775807 to INT32, which overflows; non-ANSI Spark yields NULL for that
# cast, and the CASE whose ELSE was `accepted` turned three deliberately-bad events into 2.7e19
# of revenue. Measured on pyspark 4.2.0, `v` a STRING holding 9223372036854775807:
#
#     expression        ansi=false   ansi=true
#     v > 1000000       NULL         true
#     v > 1000000L      true         true
#     v >= 0            NULL         true
#     CAST(v AS INT)    NULL         raises CAST_INVALID_INPUT
#
# The suffix is not a belt on top of the schema hints' braces. It is the cheaper of the two and
# the one that keeps working when the other is not in force: the hints only apply after a full
# refresh re-infers the schema, and the same expression is read by a SQL warehouse whose ANSI
# mode is not the pipeline's (docs/limits.md records the divergence). `tests/fast/
# test_contract_documents.py` refuses a bound literal here without its width.
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
        "(event_type NOT IN ('order_placed','return_registered') OR qty > 0L)"
        # An amendment to a quantity of zero or less is the same fault by another name, and
        # no lane rejected it: `NON_POSITIVE_QUANTITY` was gated on the two event types that
        # carry `qty` and an `order_line_amended` carries `new_qty`. All three lanes agreed,
        # so no parity test could see it, and the generator's `max(1, ...)` guaranteed no
        # seed would produce it. An amendment to -5 drove gross revenue negative. The
        # generator emits the zero now, as boundary case 14.
        " AND (event_type <> 'order_line_amended' OR new_qty > 0L)"
    ),
    "negative_price": "event_type <> 'order_placed' OR unit_price_cents >= 0L",
    "unknown_currency": "event_type <> 'order_placed' OR currency = 'EUR'",
    # Bounded, because qty * unit_price_cents is a BIGINT multiplication. See
    # domain/contract.py: three lines at the maximum legal price ended the close outright.
    "amount_out_of_range": (
        "(event_type NOT IN ('order_placed','return_registered') OR qty <= 10000L)"
        " AND (event_type <> 'order_line_amended' OR new_qty <= 10000L)"
        " AND (event_type <> 'order_placed' OR unit_price_cents <= 1000000L)"
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
# AND `accepted` IS NOT THE ELSE, which is round eighteen and is the same finding one step
# further in. `WHEN NOT COALESCE(p, false) THEN reason ... ELSE 'accepted'` is CORRECT - the
# branches are total, so falling through them means every rule said TRUE - and it is correct by
# an argument a reader has to reconstruct, in the exact place where the last version was wrong
# for want of that argument. Whoever next adds a rule to `RULES` and a hand-written branch
# beside it, or drops a `COALESCE` while rewording a predicate, re-opens the door silently:
# `accepted` is again whatever is left over after the branches, and what is left over is
# exactly what the system did not understand. So it is written as what it means:
#
#     WHEN <every rule holds> THEN 'accepted'
#
# a conjunction over the SAME `RULES`, in the same generated expression. The two forms select
# the same rows today; only this one keeps saying so after an edit.
#
# The ELSE is then unreachable by construction, and gets the only honest occupant for a branch
# that must never be taken: `raise_error`. See the ruling in `_UNREACHABLE` below - the case is
# a PIPELINE fault, not a quarantine reason, and the difference is that a quarantine reason is
# a statement about the data.
#
# `undecided_rules` names any rule that evaluated to NULL on a row. With the types declared at
# ingest it should always be empty; it is carried and counted so that if it is ever non-empty
# the run says so, instead of the row being quietly quarantined under a business reason and the
# defect going unnoticed. tests/spark asserts it is empty for every record in the matrix.

# What happens to a record that reaches no branch at all, and why it is not a quarantine reason.
#
# It was tempting to give this case a name in the closed enum - `undecidable_rule`, say - and
# route it there. That is the wrong shape, and `test_every_quarantine_reason_is_actually_
# produced_by_a_run` is what says so: every declared reason must be PRODUCED by a generated
# run, because a reason nobody can produce is a branch nobody maintains (that test exists
# because `return_exceeds_sold_qty` sat unreachable in all three implementations for the whole
# life of the repository). With the columns typed at ingest, no rule can be undecidable, so no
# seed could produce the new reason and the enum would gain a dead member the moment the fix
# landed. Extending a closed enum to describe something that cannot happen makes the enum
# describe the code's fears rather than the data's shapes.
#
# The other honest exit is the one taken: this is a fault in the PIPELINE, not in the record.
# A quarantine reason is a statement about a record - "this price is negative" - that an
# operator can act on. "The classification did not classify" is a statement about the
# classification, and the right response is to stop and say so, loudly, with the row that did
# it. `raise_error` fails the pipeline update; the event log carries the message.
#
# So the accounting is unchanged: a record is accepted, quarantined under one of the contract's
# reasons, rescued, or deduplicated. There is no fifth door, and this branch does not open one.
_UNREACHABLE = (
    "raise_error('samegold: a record reached no branch of the classification. Acceptance is "
    "the conjunction of every rule in RULES and each rejection branch is COALESCE(rule, "
    "false), so the two are exhaustive and this is unreachable unless the derivation itself "
    "was changed. This is a defect in the pipeline, not a fault in the record.')"
)


def _classification(rules: dict[str, str]) -> str:
    """`RULES` rendered as the CASE the pipeline evaluates. A FUNCTION, so a test can feed it
    rules of its own.

    That is not a convenience. The property the round-seventeen defect turned on - a rule that
    cannot answer must not produce revenue - is unobservable on this lane's real rules, because
    once bronze is typed and the bound literals carry their width, none of them CAN return NULL
    (that is the point of both fixes, and `test_no_rule_is_undecidable_on_any_record_of_the_
    matrix` asserts it). A property that only holds vacuously is a property nothing is testing.

    So the test passes in a rule that is NULL by construction and asserts the classification
    quarantines the row. Rendering it here rather than in the test is the whole argument: the
    version of this comparison that existed before rebuilt the CASE inside the test, in the
    open-failing form, and therefore agreed with the defect instead of finding it.
    """
    return (
        "CASE "
        + " ".join(
            f"WHEN NOT COALESCE({predicate}, false) THEN '{name}'"
            for name, predicate in rules.items()
        )
        + " WHEN "
        + " AND ".join(f"COALESCE({predicate}, false)" for predicate in rules.values())
        + " THEN 'accepted'"
        + f" ELSE {_UNREACHABLE} END"
    )


_REASON = _classification(RULES)


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
def _undecided(rules: dict[str, str]) -> str:
    return (
        "CASE "
        + " ".join(
            f"WHEN NOT COALESCE({predicate}, false) "
            f"THEN (CASE WHEN ({predicate}) IS NULL THEN '{name}' ELSE '' END)"
            for name, predicate in rules.items()
        )
        + " ELSE '' END"
    )


_UNDECIDED = _undecided(RULES)


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
