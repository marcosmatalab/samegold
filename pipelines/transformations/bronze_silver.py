"""The declarative half of the pipeline: bronze and silver as streaming tables.

Same transformations as the batch path - they are imported, not copied, so the declarative
pipeline and the batch comparison cannot drift apart.

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
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from samegold.pipelines.schema import RESCUED_COLUMN, bronze_schema
from samegold.pipelines.transform import quarantine_reason

LANDING = os.environ.get("SAMEGOLD_LANDING", "./_landing/bronze")
spark = SparkSession.getActiveSession()


@dp.table(name="bronze_events")
def bronze_events():  # type: ignore[no-untyped-def]
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
def silver_events():  # type: ignore[no-untyped-def]
    return (
        spark.readStream.table("bronze_events")
        .withColumn("quarantine_reason", quarantine_reason())
        .where(F.col("quarantine_reason") == "accepted")
    )


@dp.table(name="silver_quarantine")
def silver_quarantine():  # type: ignore[no-untyped-def]
    return (
        spark.readStream.table("bronze_events")
        .withColumn("quarantine_reason", quarantine_reason())
        .where(F.col("quarantine_reason") != "accepted")
    )
