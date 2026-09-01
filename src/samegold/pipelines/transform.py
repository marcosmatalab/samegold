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
    return df.where(F.col("arrival_ts").cast("timestamp") <= F.lit(as_of).cast("timestamp"))


def deduplicate(df: DataFrame) -> DataFrame:
    """One row per producer event_id.

    The key is the producer's idempotency key alone. Adding the file path (or
    ``_metadata.file_path``) to the key turns a replayed file into double revenue, which is
    specification mutant SPEC-03 and the most expensive one in the set.
    """
    from pyspark.sql import Window

    F = _f()
    window = Window.partitionBy("event_id").orderBy(
        F.col("event_ts").cast("timestamp").asc(), F.col("arrival_ts").cast("timestamp").asc()
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
    """
    F = _f()
    return (
        F.when(
            ~F.col("event_type").isin(
                "order_placed", "order_line_amended", "return_registered", "customer_upserted"
            ),
            F.lit("unknown_event_type"),
        )
        .when(
            (F.col("event_type") == "order_placed")
            & (
                F.col("order_id").isNull()
                | F.col("sku").isNull()
                | F.col("customer_id").isNull()
                | F.col("qty").isNull()
            ),
            F.lit("missing_required_field"),
        )
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


def silver(df: DataFrame) -> DataFrame:
    """Deduplicated, validated, and tagged with why a record was rejected."""
    return deduplicate(df).withColumn("quarantine_reason", quarantine_reason())


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
        F.col("event_ts").cast("timestamp").alias("sale_ts"),
    )
    amend_window = Window.partitionBy("order_id", "sku").orderBy(
        F.col("event_ts").cast("timestamp").desc(), F.col("event_id").desc()
    )
    amendments = (
        silver_df.where(
            (F.col("event_type") == "order_line_amended") & F.col("new_qty").isNotNull()
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
        F.col("event_ts").cast("timestamp").alias("return_ts"),
    )
    joined = candidates.join(lines, ["order_id", "sku"], "left")
    reason = (
        F.when(F.col("sale_ts").isNull(), F.lit("return_without_order"))
        .when(F.col("return_ts") < F.col("sale_ts"), F.lit("return_outside_window"))
        .when(
            F.col("return_ts") > F.expr(f"sale_ts + INTERVAL {RETURN_WINDOW_DAYS} DAY"),
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


def revenue_by_month(lines: DataFrame, returns: DataFrame) -> DataFrame:
    """The close: gross, returns and net per accounting month, plus the counts.

    The counts are not decoration either: a line sold for zero cents moves no money, and
    without a count a mutant that drops it is invisible. They earn their place in the
    mutation matrix.
    """
    F = _f()
    month = lambda column: F.date_format(  # noqa: E731
        F.from_utc_timestamp(F.col(column), ACCOUNTING_TIMEZONE), "yyyy-MM"
    )
    gross = lines.groupBy(month("sale_ts").alias("accounting_month")).agg(
        F.sum(F.col("qty") * F.col("unit_price_cents")).alias("gross_cents"),
        F.count(F.lit(1)).alias("line_count"),
    )
    refunds = returns.groupBy(month("sale_ts").alias("accounting_month")).agg(
        F.sum(F.col("return_qty") * F.col("unit_price_cents")).alias("returns_cents"),
        F.count(F.lit(1)).alias("return_count"),
    )
    return (
        gross.join(refunds, ["accounting_month"], "full_outer")
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
        F.col("event_ts").cast("timestamp").alias("valid_from"),
        "segment",
        "country",
        "event_id",
    )
    latest = Window.partitionBy("customer_id", "valid_from").orderBy(F.col("event_id").desc())
    collapsed = (
        base.withColumn("_rn", F.row_number().over(latest)).where(F.col("_rn") == 1).drop("_rn")
    )
    lead = Window.partitionBy("customer_id").orderBy("valid_from")
    fmt = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
    return collapsed.select(
        "customer_id",
        F.date_format(F.col("valid_from"), fmt).alias("valid_from"),
        F.date_format(F.lead("valid_from").over(lead), fmt).alias("valid_to"),
        "segment",
        "country",
        F.lead("valid_from").over(lead).isNull().alias("is_current"),
    ).orderBy("customer_id", "valid_from")
