"""Bronze to gold, as DataFrame functions.

Every function here takes and returns a DataFrame, so each can be tested on its own and the
whole chain can run in batch (for the reference comparison) or inside a declarative pipeline
(for the real thing). There is no I/O in this module on purpose: the crash points in
faults/ need to be able to run these transformations with the writer swapped out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from samegold.domain.contract import ACCOUNTING_TIMEZONE, CURRENCY, RETURN_WINDOW_DAYS

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import Column, DataFrame

ACCEPTED = "accepted"

# The columns the deduplication tie-break hashes. Every payload column, in this order, on
# both sides of the comparison: the reference SQL concatenates the same names in the same
# order and applies sha256 to the result. A column missing from this list is a column two
# records may differ in while hashing identically, which turns the "total order" back into
# whatever the shuffle produced.
PAYLOAD_COLUMNS: tuple[str, ...] = (
    "event_type",
    "order_id",
    "customer_id",
    "sku",
    "qty",
    "new_qty",
    "unit_price_cents",
    "currency",
    "return_id",
    "reason",
    "segment",
    "country",
)

REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "order_placed": ("order_id", "sku", "customer_id", "qty", "unit_price_cents", "currency"),
    "order_line_amended": ("order_id", "sku", "new_qty"),
    "return_registered": ("order_id", "sku", "qty"),
    "customer_upserted": ("customer_id",),
}


def _ts(column: str) -> Column:
    """A timestamp that is NULL when the producer sent something that is not one.

    A plain cast raises under ANSI mode, and a single malformed ``event_ts`` used to abort
    the entire close in BOTH engines: the one record shape for which the pipeline had no
    door. It now becomes NULL here and ``missing_required_field`` in the classification,
    while the reference uses TRY_CAST and filters it out. Same outcome, two derivations.
    """
    F = _f()
    return F.try_to_timestamp(F.col(column).cast("string"))


def _f() -> Any:
    from pyspark.sql import functions as F

    return F


def read_bronze(spark: Any, path: str) -> DataFrame:
    """Batch read of the landing zone, with a declared schema and a rescue column."""
    from samegold.pipelines.schema import RESCUED_COLUMN, bronze_schema

    return (
        spark.read.format("json")
        .schema(bronze_schema())
        .option("columnNameOfCorruptRecord", RESCUED_COLUMN)
        .option("mode", "PERMISSIVE")
        .load(path)
    )


def as_of_cut(df: DataFrame, as_of: str) -> DataFrame:
    """Everything the close could have known, and nothing it could not.

    The cut is on ARRIVAL time, never on event time. Cutting on event time gives the close
    perfect foresight about events still in flight and erases every restatement; that is
    specification mutant SPEC-04, and it is invisible unless the data contains a sale that
    happened before a close and arrived after it.
    """
    F = _f()
    return df.where(_ts("arrival_ts") <= F.lit(as_of).cast("timestamp"))


def deduplicate(df: DataFrame) -> DataFrame:
    """One row per producer event_id, chosen by a TOTAL order.

    The key is the producer's idempotency key alone. Adding the file path (or
    ``_metadata.file_path``) to the key turns a replayed file into double revenue, which is
    specification mutant SPEC-03 and the most expensive one in the set.

    The ordering inside the partition matters as much as the key. Ordering only by
    (event_ts, arrival_ts) leaves ties unbroken, and an unbroken tie makes ``row_number``
    pick whichever row the shuffle happened to place first: the answer would depend on the
    physical layout of the input. The tie is broken by a hash of the record's payload.

    Two things about that hash were wrong, and both were found by an adversarial review
    rather than by a test, which is the uncomfortable part:

      * it was sha2(...,256) here and md5() in the reference. Two different hash functions
        induce two different lexicographic orders, so on a pair of rows sharing an event_id
        with different payloads the two engines picked DIFFERENT copies. Measured over 2000
        synthetic tie pairs, they disagreed on 48% of them. Both were "a total order"; they
        were not the SAME total order, and only the second property makes the parity claim
        mean anything.
      * it covered six columns. For a ``customer_upserted`` all six are NULL, so both copies
        hashed to the same value, the order collapsed back to a tie, and the winning row was
        whatever the shuffle produced: repartitioning the same file 1/2/3/5/7/11/13 ways gave
        two different dimensions. The hash now covers EVERY payload column.

    The generator never emits a colliding pair (an event_id identifies one fact, by
    contract), so no seed would ever have shown either bug. `PAYLOAD_COLUMNS` is the shared
    definition, and tests/spark asserts the two engines pick the same copy.
    """
    from pyspark.sql import Window

    F = _f()
    payload_hash = F.sha2(
        F.concat_ws(
            "|",
            *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in PAYLOAD_COLUMNS],
        ),
        256,
    )
    # NULLS LAST, explicitly. Spark's default for ASC is NULLS FIRST, and a timestamp the
    # producer wrote as something that is not a timestamp is NULL here, so among two copies
    # of one event_id the BROKEN copy won the window and the good one was discarded: a sale
    # the reference booked and Spark did not. The reference excludes an unparseable event_ts
    # before it deduplicates, which is the same decision expressed differently; both now
    # prefer a record the pipeline can read, and only fall back to the broken one when there
    # is nothing else.
    window = Window.partitionBy("event_id").orderBy(
        _ts("event_ts").asc_nulls_last(),
        _ts("arrival_ts").asc_nulls_last(),
        payload_hash.asc_nulls_last(),
    )
    return (
        df.where(F.col("event_id").isNotNull())
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def quarantine_reason() -> Column:
    """The closed enum, as a single expression.

    Written as one CASE rather than as a chain of filters so that a record can only leave
    through one door, which is what makes the conservation invariant checkable at all.

    Every branch is NULL-SAFE, and that is not a stylistic preference. The first version
    tested `currency != 'EUR'`, `unit_price_cents < 0` and `qty <= 0` directly. In SQL
    semantics a comparison with NULL is NULL, not false, so a record with a MISSING currency
    fell through every branch and came out `accepted`: Spark booked 2000 cents of revenue on
    an order line the reference refused to count at all, because the reference filters
    positively (`currency = 'EUR'`, which NULL also fails, but in the excluding direction).
    Three shapes diverged that way. An adversarial review found them by writing the records;
    the generator, which always fills every field, never could.

    So the required fields are enumerated per event type in REQUIRED_FIELDS and checked for
    presence FIRST, and only then are the value rules applied to columns now known to be
    non-NULL.
    """
    F = _f()

    def missing(event_type: str) -> Column:
        columns = REQUIRED_FIELDS[event_type]
        condition = F.col(columns[0]).isNull()
        for name in columns[1:]:
            condition = condition | F.col(name).isNull()
        return (F.col("event_type") == event_type) & condition

    missing_any = missing("order_placed")
    for event_type in ("order_line_amended", "return_registered", "customer_upserted"):
        missing_any = missing_any | missing(event_type)

    return (
        # A line the parser could not read at all arrives with every column NULL, including
        # event_id. It used to fall through to `accepted` and then be dropped by the
        # event_id filter in deduplicate(): a record leaving the pipeline with no counter,
        # which is exactly the failure the rest of this module exists to make impossible.
        F.when(F.col("event_id").isNull(), F.lit("unparseable_json"))
        .when(
            F.col("event_type").isNull()
            | ~F.col("event_type").isin(
                "order_placed", "order_line_amended", "return_registered", "customer_upserted"
            ),
            F.lit("unknown_event_type"),
        )
        # A timestamp the producer sent as something that is not a timestamp is a missing
        # required field, not a crash. _ts() returns NULL for it; so does the reference.
        .when(
            _ts("event_ts").isNull() | _ts("arrival_ts").isNull(),
            F.lit("missing_required_field"),
        )
        .when(missing_any, F.lit("missing_required_field"))
        .when(
            (F.col("event_type").isin("order_placed", "return_registered")) & (F.col("qty") <= 0),
            F.lit("non_positive_quantity"),
        )
        .when(
            (F.col("event_type") == "order_placed") & (F.col("unit_price_cents") < 0),
            F.lit("negative_price"),
        )
        .when(
            (F.col("event_type") == "order_placed") & (F.col("currency") != F.lit(CURRENCY)),
            F.lit("unknown_currency"),
        )
        .otherwise(F.lit(ACCEPTED))
    )


def classify(df: DataFrame) -> DataFrame:
    """Tag every row with why it was accepted or rejected. No rows are removed and none are
    deduplicated.

    Silver is append-only and may contain duplicates: deduplication inside a micro-batch is
    not deduplication across batches, and a stateful dedup with a two-hour watermark cannot
    see a duplicate that arrives days later. Uniqueness is a property of gold.

    This split exists because the declarative pipeline and the batch path had quietly
    diverged: the batch path deduplicated in silver, the streaming one did not, and the two
    were described in the documentation as "the same transformations". They are the same
    now because there is only one function.
    """
    return df.withColumn("quarantine_reason", quarantine_reason())


def silver(df: DataFrame) -> DataFrame:
    """The gold-facing view of silver: classified, then deduplicated by business key.

    Deduplication happens here, at the boundary where uniqueness is required, and not in the
    silver table itself.
    """
    return deduplicate(classify(df))


def effective_lines(silver_df: DataFrame) -> DataFrame:
    """Order lines with the quantity known at the cut.

    A line whose amendment has not arrived keeps its original quantity: that is what finance
    saw, and the late amendment becomes a restatement rather than a retroactive correction
    nobody can explain.
    """
    from pyspark.sql import Window

    F = _f()
    lines = silver_df.where(
        (F.col("event_type") == "order_placed") & (F.col("quarantine_reason") == ACCEPTED)
    ).select(
        "order_id",
        "customer_id",
        "sku",
        F.col("qty").alias("qty0"),
        "unit_price_cents",
        _ts("event_ts").alias("sale_ts"),
    )
    amend_window = Window.partitionBy("order_id", "sku").orderBy(
        _ts("event_ts").desc(), F.col("event_id").desc()
    )
    amendments = (
        silver_df.where(
            (F.col("event_type") == "order_line_amended")
            # ACCEPTED, like every other consumer of silver. This branch was the one place
            # that read quarantined records: an amendment whose event_ts the producer wrote
            # as something that is not a timestamp still won its window and moved 3000 cents
            # of booked revenue, while the reference had dropped the record before it got
            # anywhere near the amendments CTE. A quarantined record must not change a
            # number; that is what the word means.
            & (F.col("quarantine_reason") == ACCEPTED)
            & F.col("new_qty").isNotNull()
        )
        .withColumn("_rn", F.row_number().over(amend_window))
        .where(F.col("_rn") == 1)
        .select("order_id", "sku", F.col("new_qty").alias("amended_qty"))
    )
    return lines.join(amendments, ["order_id", "sku"], "left").select(
        "order_id",
        "customer_id",
        "sku",
        F.coalesce(F.col("amended_qty"), F.col("qty0")).alias("qty"),
        "unit_price_cents",
        "sale_ts",
    )


def classify_returns(silver_df: DataFrame, lines: DataFrame) -> DataFrame:
    """Every return candidate, tagged with why it was accepted or rejected.

    Tagged, not filtered. The first version of this function filtered the invalid returns
    away with three `where` clauses, and a test noticed that three quarantine reasons in the
    contract could never be produced by the pipeline: those returns were vanishing silently.
    A record that disappears without a counter is the failure nobody detects, so returns get
    the same closed-enum treatment as every other record and the conservation invariant
    extends over them.
    """
    F = _f()
    candidates = silver_df.where(
        (F.col("event_type") == "return_registered") & (F.col("quarantine_reason") == ACCEPTED)
    ).select(
        "order_id",
        "sku",
        F.col("qty").alias("return_qty"),
        _ts("event_ts").alias("return_ts"),
    )
    joined = candidates.join(lines, ["order_id", "sku"], "left")
    reason = (
        F.when(F.col("sale_ts").isNull(), F.lit("return_without_order"))
        .when(F.col("return_ts") < F.col("sale_ts"), F.lit("return_outside_window"))
        # Seconds, not INTERVAL 45 DAY: interval arithmetic over a timestamp is calendar
        # arithmetic in the session timezone, so across a daylight-saving boundary the
        # window silently comes out an hour short of, or an hour past, 45 days.
        #
        # And a cast to DOUBLE, not unix_timestamp(). unix_timestamp truncates to whole
        # seconds, so a return exactly one microsecond outside the window came back as
        # exactly 45 days and was ACCEPTED here while the DuckDB reference rejected it. One
        # return per run, five thousand cents, and the only reason it was ever noticed is
        # that two implementations were compared and the generator emits that boundary on
        # purpose. It is the single best argument in this repository for both of those
        # decisions.
        .when(
            F.col("return_ts").cast("double") - F.col("sale_ts").cast("double")
            > RETURN_WINDOW_DAYS * 86400,
            F.lit("return_outside_window"),
        )
        .when(F.col("return_qty") > F.col("qty"), F.lit("return_exceeds_sold_qty"))
        .otherwise(F.lit(ACCEPTED))
    )
    return joined.withColumn("return_reason", reason).select(
        "order_id", "sku", "return_qty", "return_ts", "sale_ts", "unit_price_cents", "return_reason"
    )


def valid_returns(silver_df: DataFrame, lines: DataFrame) -> DataFrame:
    """The accepted subset of classify_returns(); the rest goes to the quarantine sink."""
    F = _f()
    return (
        classify_returns(silver_df, lines)
        .where(F.col("return_reason") == ACCEPTED)
        .drop("return_reason")
    )


def revenue_by_month(lines: DataFrame, classified_returns: DataFrame) -> DataFrame:
    """One close: gross, returns and net per accounting month, plus the counts.

    Takes the CLASSIFIED returns, not the accepted ones, because the rejected ones are
    reported too: a refund the pipeline refused is a number finance asks about, and a record
    that leaves the pipeline without a counter is the failure nobody detects.

    The counts are not decoration either. A line sold for zero cents moves no money, and
    without a count a mutant that drops it is invisible; they earn their place in the
    mutation matrix.
    """
    F = _f()

    def month(column: str) -> Column:
        return F.date_format(F.from_utc_timestamp(F.col(column), ACCOUNTING_TIMEZONE), "yyyy-MM")

    accepted = classified_returns.where(F.col("return_reason") == ACCEPTED)
    rejected = classified_returns.where(
        (F.col("return_reason") != ACCEPTED) & F.col("sale_ts").isNotNull()
    )
    gross = lines.groupBy(month("sale_ts").alias("accounting_month")).agg(
        F.sum(F.col("qty") * F.col("unit_price_cents")).alias("gross_cents"),
        F.count(F.lit(1)).alias("line_count"),
    )
    refunds = accepted.groupBy(month("sale_ts").alias("accounting_month")).agg(
        F.sum(F.col("return_qty") * F.col("unit_price_cents")).alias("returns_cents"),
        F.count(F.lit(1)).alias("return_count"),
    )
    refused = rejected.groupBy(month("sale_ts").alias("accounting_month")).agg(
        F.count(F.lit(1)).alias("returns_rejected_count")
    )
    return (
        gross.join(refunds, ["accounting_month"], "full_outer")
        .join(refused, ["accounting_month"], "left")
        .select(
            "accounting_month",
            F.coalesce(F.col("gross_cents"), F.lit(0)).cast("long").alias("gross_cents"),
            F.coalesce(F.col("returns_cents"), F.lit(0)).cast("long").alias("returns_cents"),
            (
                F.coalesce(F.col("gross_cents"), F.lit(0))
                - F.coalesce(F.col("returns_cents"), F.lit(0))
            )
            .cast("long")
            .alias("net_cents"),
            F.coalesce(F.col("line_count"), F.lit(0)).cast("long").alias("line_count"),
            F.coalesce(F.col("return_count"), F.lit(0)).cast("long").alias("return_count"),
            F.coalesce(F.col("returns_rejected_count"), F.lit(0))
            .cast("long")
            .alias("returns_rejected_count"),
        )
        .orderBy("accounting_month")
    )


def dim_customer_scd2(silver_df: DataFrame) -> DataFrame:
    """Type 2 dimension, computed as a full recomputation.

    The incremental version (a MERGE that closes the open row and inserts the new one) lives
    in gold_scd2_merge.py and is what runs in the pipeline. This one exists so the two can be
    compared: the interesting bugs in an incremental SCD2 are exactly the ones a full
    recomputation does not have.
    """
    from pyspark.sql import Window

    F = _f()
    base = silver_df.where(
        (F.col("event_type") == "customer_upserted") & F.col("customer_id").isNotNull()
    ).select(
        "customer_id",
        _ts("event_ts").alias("valid_from"),
        "segment",
        "country",
        "event_id",
    )
    latest = Window.partitionBy("customer_id", "valid_from").orderBy(F.col("event_id").desc())
    collapsed = (
        base.withColumn("_rn", F.row_number().over(latest)).where(F.col("_rn") == 1).drop("_rn")
    )
    # A Type 2 dimension records CHANGES, not heartbeats. An upsert that repeats the
    # attributes the customer already had is not a new version, and dropping it here is not
    # cosmetic: domain.bitemporal.scd2_from_versions (the rule the incremental MERGE path
    # obeys) has always collapsed them, this function and the DuckDB reference did not, and
    # the three therefore disagreed on any customer with a repeated upsert. The generator
    # produces that shape on roughly a quarter of its attribute changes, so the divergence
    # was live on every seed; nothing compared the incremental path to the other two, which
    # is why no test failed. tests/spark now compares all three.
    ordered = Window.partitionBy("customer_id").orderBy("valid_from")
    changed = (
        collapsed.withColumn("_first", F.lag("valid_from").over(ordered).isNull())
        .withColumn("_previous_segment", F.lag("segment").over(ordered))
        .withColumn("_previous_country", F.lag("country").over(ordered))
        .where(
            F.col("_first")
            | ~(
                F.col("segment").eqNullSafe(F.col("_previous_segment"))
                & F.col("country").eqNullSafe(F.col("_previous_country"))
            )
        )
    )
    lead = Window.partitionBy("customer_id").orderBy("valid_from")
    fmt = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
    return changed.select(
        "customer_id",
        F.date_format(F.col("valid_from"), fmt).alias("valid_from"),
        F.date_format(F.lead("valid_from").over(lead), fmt).alias("valid_to"),
        "segment",
        "country",
        F.lead("valid_from").over(lead).isNull().alias("is_current"),
    ).orderBy("customer_id", "valid_from")


def revenue_versions(snapshots: list[tuple[str, DataFrame]]) -> DataFrame:
    """The versioned close table, computed in Spark.

    A second derivation of the same bookkeeping the reference does in Python
    (``domain.bitemporal.versions_from_snapshots``). Deliberately not a call into that
    function: if both sides shared it, their agreement on the version history would only mean
    the import worked.

    The shape of the computation is a union of the per-close snapshots, a lag over
    (month, as_of) to keep only the closes where a value actually changed, and a row_number to
    number the surviving versions densely from zero.
    """
    from pyspark.sql import Window

    F = _f()
    union = None
    for as_of, snapshot in snapshots:
        stamped = snapshot.withColumn("as_of", F.lit(as_of))
        union = stamped if union is None else union.unionByName(stamped)
    if union is None:
        raise ValueError("revenue_versions needs at least one snapshot")

    value_columns = [
        "gross_cents",
        "returns_cents",
        "net_cents",
        "line_count",
        "return_count",
        "returns_rejected_count",
    ]
    # A month is closed once a close happens AFTER the month ends. Without this, the partial
    # view of the current month appears at every close and manufactures a restatement per
    # close for ever.
    #
    # The close instant is converted to the accounting timezone before its month is taken.
    # Slicing the ISO string (`as_of[:7]`) reads the month in whatever offset the string
    # happens to carry: a close at 2026-02-01 00:30 Europe/Madrid is 2026-01-31T23:30Z, so
    # the UTC prefix said "2026-01", January was not yet closed, and the entire January
    # close silently disappeared. The answer depended on the string representation of the
    # instant rather than on the instant, which is the definition of a bug.
    as_of_month = F.date_format(
        F.from_utc_timestamp(F.col("as_of").cast("timestamp"), ACCOUNTING_TIMEZONE), "yyyy-MM"
    )
    closed = union.where(as_of_month > F.col("accounting_month"))
    fingerprint = F.concat_ws("|", *[F.col(c).cast("string") for c in value_columns])
    # Ordered by the INSTANT the close happened at, not by the text of the timestamp. Two
    # closes written with different UTC offsets sort one way as strings and the other way as
    # instants, and the version numbers would then follow the spelling.
    ordered = Window.partitionBy("accounting_month").orderBy(F.col("as_of").cast("timestamp"))
    changed = (
        closed.withColumn("_fp", fingerprint)
        .withColumn("_previous", F.lag("_fp").over(ordered))
        .where(F.col("_previous").isNull() | (F.col("_fp") != F.col("_previous")))
    )
    numbered = changed.withColumn("close_version", F.row_number().over(ordered) - F.lit(1))
    return numbered.select(
        "accounting_month",
        "close_version",
        *value_columns,
        F.col("as_of").alias("restated_at"),
        F.when(F.col("close_version") == 0, F.lit("first close"))
        .otherwise(F.lit("late arrivals after close"))
        .alias("restatement_reason"),
    ).orderBy("accounting_month", "close_version")
