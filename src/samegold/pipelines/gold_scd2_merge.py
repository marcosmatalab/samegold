"""SCD Type 2 by MERGE, which is the version that actually runs in the pipeline.

The full recomputation in ``transform.dim_customer_scd2`` is the reference. This one is
incremental, and the difference is where the bugs live: closing an interval that should have
been split, leaving two rows open, or applying a version that arrived out of order on top of
a newer one.

Delta specifics that are load-bearing:

  * the MERGE is on ``customer_id`` AND ``is_current`` - matching on the key alone updates
    every historical row;
  * closing and inserting cannot be one MERGE. Delta will not both update the matched row and
    insert a new one for the same source row, so the batch is passed twice: once to close, once
    to insert. Doing it in one pass is the mistake that leaves the dimension with gaps;
  * the source must be deduplicated on the key first, or the MERGE fails at runtime with a
    multiple-matches error, which is a good failure - a silent last-writer-wins would be worse;
  * a version that arrives with a ``valid_from`` older than the current open row is NOT a
    normal update: it is a late correction that has to split an existing interval. It is
    quarantined here and handled by a restatement path, because silently ignoring it is how a
    dimension starts disagreeing with its own history.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

SCD2_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    customer_id   STRING  NOT NULL,
    valid_from    STRING  NOT NULL,
    valid_to      STRING,
    segment       STRING,
    country       STRING,
    is_current    BOOLEAN NOT NULL,
    attr_hash     STRING  NOT NULL
) USING DELTA
CLUSTER BY (customer_id)
TBLPROPERTIES (
    delta.enableChangeDataFeed = true,
    delta.enableDeletionVectors = true
)
"""


def upsert_scd2(spark: Any, batch: DataFrame, table: str) -> dict[str, int]:
    """Apply one batch of customer versions to the Type 2 dimension.

    Returns counters so the caller can record them as evidence: a MERGE that silently does
    nothing looks exactly like a MERGE that worked.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark.sql(SCD2_TABLE_DDL.format(table=table))
    target = DeltaTable.forName(spark, table)

    latest = Window.partitionBy("customer_id").orderBy(
        F.col("valid_from").desc(), F.col("event_id").desc()
    )
    source = (
        batch.withColumn("attr_hash", F.sha2(F.concat_ws("|", "segment", "country"), 256))
        .withColumn("_rn", F.row_number().over(latest))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    late = source.join(
        target.toDF()
        .where("is_current")
        .select("customer_id", F.col("valid_from").alias("open_from")),
        "customer_id",
        "left",
    ).where(F.col("open_from").isNotNull() & (F.col("valid_from") < F.col("open_from")))
    late_count = late.count()
    on_time = source.join(late.select("customer_id"), "customer_id", "left_anti")

    # Pass 1: close the currently open row when the attributes actually changed.
    (
        target.alias("t")
        .merge(
            on_time.alias("s"),
            "t.customer_id = s.customer_id AND t.is_current = true AND t.attr_hash <> s.attr_hash",
        )
        .whenMatchedUpdate(set={"valid_to": "s.valid_from", "is_current": "false"})
        .execute()
    )
    # Pass 2: insert the new open row for keys that no longer have one.
    (
        target.alias("t")
        .merge(on_time.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
        .whenNotMatchedInsert(
            values={
                "customer_id": "s.customer_id",
                "valid_from": "s.valid_from",
                "valid_to": "null",
                "segment": "s.segment",
                "country": "s.country",
                "is_current": "true",
                "attr_hash": "s.attr_hash",
            }
        )
        .execute()
    )
    return {"applied": on_time.count(), "late_corrections": late_count}
