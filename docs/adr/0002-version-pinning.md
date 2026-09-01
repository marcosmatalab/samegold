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
- The Maven coordinate is **`io.delta:delta-spark_4.2_2.13:4.4.0`**. Delta 4.4 publishes one
  artefact per Spark minor version. The unqualified `io.delta:delta-spark_2.13:4.4.0` also
  exists as a backward-compatibility artefact that defaults to Spark 4.2, so both resolve
  today; the qualified one is used because it says which Spark it is for, and because the
  unqualified default moves with each release. (An earlier version of this ADR claimed the
  unqualified coordinate did not exist. It does, and an adversarial review checked the release
  notes.)
- `configure_spark_with_delta_pip` sets only `spark.jars.packages`. Without
  `spark.sql.extensions` and the `DeltaCatalog`, `MERGE`, `OPTIMIZE` and time travel are not
  available and the error is `is not a Delta table`.
- Ivy 2.5.3 caches in `~/.ivy2.5.2`, not `~/.ivy2`. A CI cache keyed on `~/.ivy2` never hits.
- Java 17, 21 and 25 are supported and `spark-class` already injects every `--add-opens`
  Spark needs. Setting `JAVA_TOOL_OPTIONS` by hand REPLACES that list and produces an
  `InaccessibleObjectException` that looks like a JDK incompatibility.

## Decision

One constant, `DELTA_COORDINATE`, in `src/samegold/pipelines/session.py`.
`tests/fast/test_contract_documents.py::test_the_delta_coordinate_lives_in_one_place` asserts
that the declarative pipeline spec carries the same coordinate and that no other module spells
one out. The CI workflow repeats the version numbers in its cache key, deliberately: a cache
key has to be a literal, and a stale cache is the failure that key exists to prevent.

(The test is named here because the previous version of this ADR claimed it existed before it
did. That is the kind of sentence an adversarial review checks first.)

## What we gave up

Version agility. Moving to a new Spark means changing one constant and re-running the delta
lane, which is the point.
