"""Bronze on Databricks: Auto Loader into a streaming table.

This is the file that has no open-source twin, which is why the ingestion layer is an adapter
(`samegold.ingest.adapter`) rather than one implementation pretending to be portable.

Free Edition constraints that shape it:
  * no external locations, so the landing zone is a Unity Catalog volume;
  * file-notification mode needs cloud credentials that Free Edition cannot hold, so this is
    directory listing;
  * serverless only, and time-based streaming triggers are rejected there, so the pipeline is
    triggered rather than continuous.
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
LANDING = spark.conf.get("samegold.landing", "/Volumes/samegold/raw/landing")

# The types, DECLARED, at the place the data enters. Auto Loader reading JSON without
# `cloudFiles.inferColumnTypes` or `cloudFiles.schemaHints` infers every column as STRING, and
# it did: the first real run produced a bronze table whose 21 columns were all strings, a
# `gross_cents` of type DOUBLE (because `qty * unit_price_cents` on two strings promotes to
# double), and a close that died with DELTA_CAST_OVERFLOW_IN_TABLE_WRITE writing that double
# into a BIGINT column. In a project whose thesis is that money is an integer number of cents,
# the money on this lane was floating point.
#
# Worse than the arithmetic: on STRING columns the rule `unit_price_cents > 1000000` compares a
# string against an INT32 literal, so the string is coerced to INT32, and 9223372036854775807
# overflows it. Non-ANSI Spark returns NULL for that cast, the predicate is NULL, and a CASE
# whose ELSE was `accepted` booked three deliberately-bad events as 2.7e19 of revenue. Measured
# on pyspark 4.2.0: with `spark.sql.ansi.enabled=false` the comparison is NULL, with it true it
# is `true`, and `> 1000000L` is `true` in both - the defect is the WIDTH of the literal, and it
# is unreachable once the column is a BIGINT.
#
# These hints are the same declaration the OSS lane uses in `samegold.pipelines.schema`, and
# tests/fast/test_databricks_bundle.py fails if the two drift apart. Only the three numeric
# columns actually matter; the strings are written out so that the hint IS the schema rather
# than a patch on part of it.
#
# TWO CONSEQUENCES, both managed rather than discovered later:
#
#  * a value that does not fit its hinted type goes to `_rescued_data` instead of the column,
#    which is a counted bucket in the conservation invariant and not a hole. The three events
#    that caused this are emitted by the generator as JSON NUMBERS equal to Long.MaxValue, so
#    they parse as BIGINT, fail `unit_price_cents <= 1000000`, and land in
#    `amount_out_of_range` - the same door the other two lanes send them through.
#  * `cloudFiles.schemaLocation` CACHES the inferred schema, so adding hints to an existing
#    pipeline changes nothing until the schema is re-inferred. That needs a FULL REFRESH:
#    `databricks bundle run samegold_pipeline -t free --full-refresh-all`, which
#    `scripts/databricks_run.sh run-full-refresh` does. Without it this fix looks like it
#    did not work.
SCHEMA_HINTS = (
    "event_id STRING, event_type STRING, event_ts STRING, arrival_ts STRING, "
    "order_id STRING, customer_id STRING, sku STRING, "
    "qty BIGINT, new_qty BIGINT, unit_price_cents BIGINT, "
    "currency STRING, return_id STRING, reason STRING, "
    "segment STRING, country STRING, boundary STRING"
)


@dp.table(
    name="bronze_events",
    comment="Raw events exactly as they landed, plus the file they came from.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
def bronze_events() -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/_schema")
        .option("cloudFiles.schemaHints", SCHEMA_HINTS)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 200)
        .load(LANDING)
        .withColumn("_ingest_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
