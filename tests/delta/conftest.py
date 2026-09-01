from __future__ import annotations

import pytest

from samegold.pipelines.session import DELTA_COORDINATE, StorageMode, build_session


@pytest.fixture(scope="session")
def delta_spark():  # type: ignore[no-untyped-def]
    """A session with the Delta jars, or a loud skip.

    Two ways this can fail to be a Delta session, and both used to look like a broken test
    instead of a missing dependency:

      * Maven Central is unreachable, so the jars cannot be resolved at all;
      * a Spark session already exists in this process WITHOUT the Delta extension - which is
        exactly what happens when `pytest tests/spark tests/delta` runs in one process, since
        `getOrCreate` returns the existing session and quietly ignores the new configuration.
        The Spark lane runs in parquet mode, so the delta lane inherited a session that could
        not read a Delta table and reported five failures that had nothing to do with Delta.

    Both are skips with a message that says which one happened, because a skip that explains
    itself is information and a red test that means "you have no internet" is noise.
    """
    try:
        session = build_session("samegold-delta", mode=StorageMode.DELTA)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no Spark session available: {type(exc).__name__}: {exc}"[:300])

    extensions = session.conf.get("spark.sql.extensions", "")
    if "DeltaSparkSessionExtension" not in extensions:
        pytest.skip(
            "a Spark session without the Delta extension already exists in this process "
            "(the Spark lane creates one in parquet mode). Run this lane on its own: "
            "`pytest tests/delta`, or `make delta`."
        )
    try:
        session.sql("SELECT 1").collect()
        probe = session.createDataFrame([(1,)], "x INT")
        probe.write.format("delta").mode("overwrite").save("/tmp/samegold-delta-probe")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"the Delta jars could not be resolved ({type(exc).__name__}). This lane needs "
            f"Maven Central and the coordinate {DELTA_COORDINATE}. Detail: {exc}"[:400]
        )
    yield session
    session.stop()
