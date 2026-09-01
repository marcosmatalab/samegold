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
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()
LANDING = spark.conf.get("samegold.landing", "/Volumes/samegold/raw/landing")


@dp.table(
    name="bronze_events",
    comment="Raw events exactly as they landed, plus the file they came from.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
def bronze_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/_schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 200)
        .load(LANDING)
        .withColumn("_ingest_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
