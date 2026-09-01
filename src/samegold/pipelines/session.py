"""The Spark session, with every setting that is not optional and why.

The version combination is pinned in one place and only one place. Getting it wrong costs
an afternoon and the error message does not say what is wrong, so it is worth writing down:

  * pyspark 4.2.0 with delta-spark 4.4.0. The wheel declares ``pyspark<=4.2.0,>=4.0.1``, so
    this pair is the newest legal one as of 2026-09-01.
  * The Maven coordinate is ``io.delta:delta-spark_4.2_2.13:4.4.0``. Delta moved to a
    Spark-version-qualified artifact name: the old ``io.delta:delta-spark_2.13:4.4.0`` does
    not exist and fails at session start with "module not found", which reads like a network
    problem and is not one.
  * ``configure_spark_with_delta_pip`` only sets ``spark.jars.packages``. It does NOT set
    the SQL extension or the catalog, and without those there is no MERGE, no OPTIMIZE, no
    time travel by table name - queries fail with a plain "is not a Delta table".
  * Java: Spark 4.2 supports 17, 21 and 25, and ``spark-class`` already injects every
    ``--add-opens`` it needs. Adding them by hand through JAVA_TOOL_OPTIONS *replaces* that
    list rather than extending it, and the result is an InaccessibleObjectException that
    looks like a JDK incompatibility. Do not set JAVA_TOOL_OPTIONS.

``StorageMode.PARQUET`` exists for one situation, and it is documented in PARITY.md rather
than hidden: an environment with no route to Maven Central cannot resolve the Delta jars, so
the pipeline can still be exercised end to end for its *transformation* logic while every
Delta-specific claim (MERGE, time travel, CDF, OPTIMIZE, the crash points inside a commit)
is reported as NOT RUN. It is a degraded mode, never a fallback: no evidence record produced
under it is allowed to back a Delta claim.
"""

from __future__ import annotations

import os
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

DELTA_VERSION = "4.4.0"
SPARK_VERSION = "4.2.0"
DELTA_COORDINATE = f"io.delta:delta-spark_4.2_2.13:{DELTA_VERSION}"


class StorageMode(StrEnum):
    DELTA = "delta"
    PARQUET = "parquet"

    @classmethod
    def from_env(cls) -> StorageMode:
        return cls(os.environ.get("SAMEGOLD_STORAGE", "delta"))


def build_session(
    app_name: str = "samegold",
    mode: StorageMode | None = None,
    master: str = "local[2]",
    extra: dict[str, str] | None = None,
) -> SparkSession:
    from pyspark.sql import SparkSession

    mode = mode or StorageMode.from_env()
    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        # Small, fixed shuffle. The default of 200 partitions over a few million rows
        # produces thousands of tiny files, which is the single most expensive mistake in a
        # local lakehouse and the one the cost lab measures.
        .config("spark.sql.shuffle.partitions", os.environ.get("SAMEGOLD_SHUFFLE", "8"))
        .config("spark.driver.memory", os.environ.get("SAMEGOLD_DRIVER_MEMORY", "3g"))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # Adaptive execution changes the number of output files between runs. It stays ON
        # because turning it off to make a digest stable would be tuning the experiment to
        # fit the claim; instead the digest is taken over a projection with a total order,
        # which is insensitive to it. See docs/adr/0005-adaptive-execution-stays-on.md.
        .config("spark.sql.adaptive.enabled", "true")
    )
    if mode is StorageMode.DELTA:
        builder = (
            builder.config("spark.jars.packages", DELTA_COORDINATE)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.sources.default", "delta")
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
            # Ivy 2.5.3 caches under ~/.ivy2.5.2, not ~/.ivy2. Naming it explicitly is what
            # makes the CI cache key actually hit.
            .config(
                "spark.jars.ivy",
                os.environ.get("SAMEGOLD_IVY_HOME", os.path.expanduser("~/.ivy2.5.2")),
            )
        )
    else:
        builder = builder.config("spark.sql.sources.default", "parquet")
    for key, value in (extra or {}).items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def session_fingerprint(spark: Any) -> dict[str, str]:
    """What a claim needs to record about the engine it ran on."""
    conf = spark.sparkContext.getConf()
    return {
        "spark": spark.version,
        "storage_mode": str(StorageMode.from_env()),
        "shuffle_partitions": conf.get("spark.sql.shuffle.partitions", "?"),
        "adaptive": conf.get("spark.sql.adaptive.enabled", "?"),
        "delta_coordinate": conf.get("spark.jars.packages", "none"),
    }
