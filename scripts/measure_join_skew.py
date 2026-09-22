"""Measure what a skewed join actually does, and print the table `docs/join-skew.md` shows.

WHY THIS IS A SCRIPT AND NOT A CLAIM. Everything this repository publishes as a claim is
recomputed from the seeds its own record names, by `samegold verify-latest`, on every push.
This measurement cannot be one, and the reason is in the numbers it prints rather than in a
budget: with adaptive execution left on - which `docs/adr/0005-adaptive-execution-stays-on.md`
requires - a local Spark coalesces two million rows into a single shuffle partition, and a
single partition has no skew. The only configuration in which the thing being measured EXISTS
is the one that ADR calls tuning the experiment to fit the claim.

So it is a script, it is run by hand, and the page it feeds says so. Run it:

    make skew            # or: python scripts/measure_join_skew.py

It needs pyspark and a JVM, so it belongs on the same machine as the Spark lanes - WSL2 or
Linux, never Windows. Nothing in CI runs it.
"""

from __future__ import annotations

import os
import sys
import time

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#: The share of rows the hot key takes. Thirty per cent is the interview question's number.
HOT_SHARE = 0.30
#: How many other keys share the rest. Small enough that the hot key is unmistakable.
KEYS = 500
#: Requested shuffle partitions. AQE is free to coalesce these, and that is the finding.
SHUFFLE_PARTITIONS = 16
#: (rows, coalesce) - the four rows of the published table, in order.
CASES: tuple[tuple[int, bool], ...] = (
    (200_000, True),
    (200_000, False),
    (2_000_000, True),
    (8_000_000, True),
)


def build(spark: SparkSession, rows: int):
    """A fact table where one key holds `HOT_SHARE` of the rows, and a dimension to join it to.

    Deterministic without a seed: the hot rows are chosen by an arithmetic function of `id`,
    so two runs on the same row count produce the same table. `pad` is there because AQE
    coalesces against a size in BYTES, and rows of three small columns make a partition that
    is nearly empty however many of them there are.
    """
    base = spark.range(rows).withColumn("r", (F.col("id") * 2654435761 % 1000).cast("int"))
    fact = (
        base.withColumn(
            "customer_id",
            F.when(F.col("r") < int(HOT_SHARE * 1000), F.lit("C-HOT")).otherwise(
                F.concat(F.lit("C-"), (F.col("id") % KEYS).cast("string"))
            ),
        )
        .withColumn("amount_cents", (F.col("id") % 9999).cast("long"))
        .withColumn("pad", F.concat(F.lit("x" * 60), F.col("id").cast("string")))
    )
    dim = spark.range(KEYS).select(
        F.concat(F.lit("C-"), F.col("id").cast("string")).alias("customer_id"),
        F.concat(F.lit("name-"), F.col("id").cast("string")).alias("name"),
    )
    return fact, dim.union(spark.createDataFrame([("C-HOT", "name-hot")], dim.schema))


def partition_profile(df) -> dict[str, float]:
    """Rows per shuffle partition after the join: the fact the skew IS, with no clock in it.

    `docs/adr/0008-cost-is-measured-in-files-and-bytes.md` forbids this repository from
    publishing a latency claim. A ratio of rows is not one: it is a property of how the data
    landed, and it is the thing salting or a broadcast would change.
    """
    counts = df.withColumn("p", F.spark_partition_id()).groupBy("p").count().collect()
    sizes = sorted((row["count"] for row in counts), reverse=True)
    mean = sum(sizes) / len(sizes)
    return {
        "partitions": len(sizes),
        "max_rows": sizes[0],
        "mean_rows": round(mean, 1),
        "skew_factor": round(sizes[0] / mean, 2),
    }


def measure(rows: int, coalesce: bool) -> dict[str, object]:
    spark = (
        SparkSession.builder.master("local[4]")
        .appName(f"join-skew-{rows}-{coalesce}")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", str(coalesce).lower())
        # Without this the dimension is broadcast and there is no shuffle to be skewed. The
        # question is about a shuffle join, so the shuffle join is what is measured.
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.driver.memory", "3g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        fact, dim = build(spark, rows)
        joined = fact.join(dim, "customer_id", "inner")
        started = time.perf_counter()
        profile = partition_profile(joined)
        seconds = round(time.perf_counter() - started, 2)
        plan = joined._jdf.queryExecution().executedPlan().toString()
        return {
            "rows": rows,
            "coalesce": coalesce,
            "seconds": seconds,
            "plan_mentions_skew": "skew" in plan.lower(),
            **profile,
        }
    finally:
        spark.stop()


def as_markdown(results: list[dict[str, object]]) -> str:
    lines = [
        "| rows joined | AQE coalescing | partitions | max rows | mean rows | **skew factor** |",
        "|---:|---|---:|---:|---:|---:|",
    ]
    for row in results:
        lines.append(
            f"| {row['rows']:,} | {'on (the default)' if row['coalesce'] else 'OFF'} "
            f"| {row['partitions']} | {row['max_rows']:,} | {row['mean_rows']:,} "
            f"| **{row['skew_factor']:.2f}** |"
        )
    return "\n".join(lines).replace(",", " ")


def main() -> int:
    results = [measure(rows, coalesce) for rows, coalesce in CASES]
    print(as_markdown(results))
    print()
    for row in results:
        print(
            f"{row['rows']:>9} rows, coalescing "
            f"{'on ' if row['coalesce'] else 'off'}: {row['seconds']:>5}s to profile, "
            f"plan mentions skew: {row['plan_mentions_skew']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
