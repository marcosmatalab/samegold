# ADR 0002 - one pinned version combination, in one place

**Status** accepted, 2026-09-01

## Context

Spark, Delta and the Delta Maven artefact move independently and their failure modes read
like network problems. The specific traps as of September 2026:

- `pyspark 4.2.0` ships as an sdist, not a wheel: it builds locally (~4 minutes, ~450 MB) and
  fails on a Debian system Python with `AttributeError: install_layout`. A clean venv is not
  a style preference here.
- `delta-spark 4.4.0` declares `pyspark<=4.2.0,>=4.0.1`, so the pair is legal and is the
  newest legal one.
- The Maven coordinate is **`io.delta:delta-spark_4.2_2.13:4.4.0`**. Delta moved to a
  Spark-qualified artefact name; the older `io.delta:delta-spark_2.13:4.4.0` does not exist
  and fails with `module not found`, which looks like no internet.
- `configure_spark_with_delta_pip` sets only `spark.jars.packages`. Without
  `spark.sql.extensions` and the `DeltaCatalog`, `MERGE`, `OPTIMIZE` and time travel are not
  available and the error is `is not a Delta table`.
- Ivy 2.5.3 caches in `~/.ivy2.5.2`, not `~/.ivy2`. A CI cache keyed on `~/.ivy2` never hits.
- Java 17, 21 and 25 are supported and `spark-class` already injects every `--add-opens`
  Spark needs. Setting `JAVA_TOOL_OPTIONS` by hand REPLACES that list and produces an
  `InaccessibleObjectException` that looks like a JDK incompatibility.

## Decision

One constant, `DELTA_COORDINATE`, in `pipelines/session.py`, used by the session builder, the
declarative pipeline spec and the CI workflow. A test asserts there is no second copy of a
version string anywhere in the repository.

## What we gave up

Version agility. Moving to a new Spark means changing one constant and re-running the delta
lane, which is the point.
