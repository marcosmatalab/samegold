"""The declarative half of the pipeline: bronze and silver as streaming tables.

Same transformations as the batch path: ``classify`` is imported, not copied, so the
declarative pipeline and the batch comparison cannot drift apart. Deduplication is
deliberately NOT here (see the note on silver_events below).

What open-source SDP does NOT have, and how this file lives without it:

  * expectations. There is no @dp.expect in Apache Spark 4.2, so the same rules are applied as
    a column (`quarantine_reason`) and a second streaming table carries the rejected rows.
    Nothing is dropped, which is what makes the conservation invariant checkable.
  * AUTO CDC / apply_changes. The Type 2 dimension is a hand-written MERGE
    (`pipelines/gold_scd2_merge.py`) rather than a primitive.
"""

from __future__ import annotations

import os

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from samegold.pipelines.schema import RESCUED_COLUMN, bronze_schema
from samegold.pipelines.transform import classify

LANDING = os.environ.get("SAMEGOLD_LANDING", "./_landing/bronze")
# `active()`, not `getActiveSession()`. The second returns `SparkSession | None`, and every
# use below then reads an attribute off a value that may be None - which mypy says plainly once
# pyspark is installed, and said to nobody for eleven rounds because the fast lane that runs
# mypy does not install it. `active()` returns a session or raises, which is the behaviour this
# file wants: there is no sensible way to run a pipeline source with no session.
spark = SparkSession.active()


@dp.table(name="bronze_events")
def bronze_events() -> DataFrame:
    return (
        spark.readStream.format("json")
        .schema(bronze_schema())
        .option("columnNameOfCorruptRecord", RESCUED_COLUMN)
        .option("maxFilesPerTrigger", 40)
        .load(LANDING)
        .withColumn("_ingest_file", F.col("_metadata.file_path"))
    )


# Silver is append-only and MAY contain duplicates: deduplication within a micro-batch is not
# deduplication across batches, and a stateful dedup with a two-hour watermark cannot see a
# duplicate that arrives days later. Uniqueness is a property of gold, enforced there by a
# stateless dedup on event_id. This is not a workaround, it is where the property belongs -
# and the size of the effect is measured in milestone M11 rather than assumed away.
@dp.table(name="silver_events")
def silver_events() -> DataFrame:
    return classify(spark.readStream.table("bronze_events")).where(
        F.col("quarantine_reason") == "accepted"
    )


@dp.table(name="silver_quarantine")
def silver_quarantine() -> DataFrame:
    return classify(spark.readStream.table("bronze_events")).where(
        F.col("quarantine_reason") != "accepted"
    )
