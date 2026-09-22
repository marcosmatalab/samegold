from __future__ import annotations

import shutil
import urllib.error
import urllib.request
from typing import NoReturn

import pytest

from samegold.pipelines.session import DELTA_COORDINATE, StorageMode, build_session

#: The host the Delta jars come from. Probed rather than inferred from an exception message,
#: because the message does not carry the cause: `build_session` fails with
#: `PySparkRuntimeError: [JAVA_GATEWAY_EXITED]`, and the line that says
#: `unresolved dependency: io.delta#delta-spark_4.2_2.13;4.4.0: not found` is about a hundred
#: lines further down the JVM's stderr, which pytest has already captured elsewhere.
MAVEN_CENTRAL = "https://repo1.maven.org/maven2/io/delta/"


def maven_central_reachable(timeout: float = 8.0) -> bool:
    """Whether the jars could be fetched at all. Cheap, and it decides skip versus fail."""
    try:
        with urllib.request.urlopen(MAVEN_CENTRAL, timeout=timeout) as response:
            return bool(200 <= response.status < 400)
    except (TimeoutError, urllib.error.URLError, OSError, ValueError):
        return False


def _no_delta_session(detail: str) -> NoReturn:
    """Skip or fail, and the difference is the whole point of this file.

    Measured on 20 September 2026, behind a proxy that blocked `repo1.maven.org`:

        SPARK_LOCAL_IP=127.0.0.1 pytest tests/delta -q -rs
        1 passed, 5 skipped in 3.49s          <- exit 0

    with the reason `no Spark session available: PySparkRuntimeError: [JAVA_GATEWAY_EXITED]`.
    That reason was FALSE. Spark worked perfectly on that machine: `pytest tests/spark` with
    `SAMEGOLD_STORAGE=parquet` gave 143 passed in 212 s. What had happened was
    `unresolved dependency: io.delta#delta-spark_4.2_2.13;4.4.0`, a hundred lines down in the
    captured stderr, and the lane reported it as an absent runtime.

    So the whole Delta lane exited 0 having verified nothing, under a reason that named the
    wrong cause. This repository's entire argument is that a gate which does not bite is worse
    than no gate, and here was one of its own gates going green on zero verification.

    The rule:

      * no JDK, no pyspark - **skip**. A runtime this machine does not have is a lane that did
        not run, which is what a skip means, and `scripts/preflight.sh` counts it as "not run"
        and still refuses to exit 0.
      * a JDK, pyspark, and no route to Maven Central - **fail**. The claims that need Delta
        have not been verified, and saying so is the only honest answer. It is not a defect in
        anybody's repository; it is a network that cannot reach the jars, and the message says
        that in the first line so a reader behind a corporate proxy does not go hunting.
    """
    if shutil.which("java") is None:
        pytest.skip(
            "no JDK on PATH: this lane needs a JDK 21, see docs/adr/0002-version-pinning.md"
        )
    if not maven_central_reachable():
        pytest.fail(
            f"Maven Central is unreachable, so the Delta claims have NOT been verified.\n"
            f"\n"
            f"This is a network result, not a defect: {MAVEN_CENTRAL} could not be opened, so "
            f"Spark could not resolve {DELTA_COORDINATE} and the session never started.\n"
            f"\n"
            f"This lane FAILS rather than skipping, because five skipped tests and exit 0 read "
            f"as 'the Delta claims hold' to everything that consumes this lane, and nothing was "
            f"checked. Run it on a network with a route to repo1.maven.org, or pre-populate "
            f"~/.ivy2 with the coordinate above.\n"
            f"\n"
            f"What the session actually said: {detail}"[:1200]
        )
    pytest.skip(f"no Spark session available: {detail}"[:300])


@pytest.fixture(scope="session")
def delta_spark():  # type: ignore[no-untyped-def]
    """A session with the Delta jars, or a verdict that says which of three things went wrong.

    Three ways this can fail to be a Delta session, and they are not the same event:

      * the machine has no JDK or no pyspark - the lane did not run, and it skips;
      * the machine has both and cannot reach Maven Central - the lane FAILED to verify the
        claims it exists to verify, and it says so rather than going green (see
        `_no_delta_session`, and ADR 0013);
      * a Spark session already exists in this process WITHOUT the Delta extension - which is
        exactly what happens when `pytest tests/spark tests/delta` runs in one process, since
        `getOrCreate` returns the existing session and quietly ignores the new configuration.
        The Spark lane runs in parquet mode, so the delta lane inherited a session that could
        not read a Delta table and reported five failures that had nothing to do with Delta.
        That one is a skip with an instruction, because the fix is to run the lane on its own.
    """
    try:
        session = build_session("samegold-delta", mode=StorageMode.DELTA)
    except Exception as exc:
        _no_delta_session(f"{type(exc).__name__}: {exc}")

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
    except Exception as exc:
        # The same fork, at the other place it can happen: the session starts and the jars are
        # still missing, so the first Delta write is what discovers it.
        _no_delta_session(
            f"the Delta jars could not be resolved ({type(exc).__name__}). This lane needs "
            f"Maven Central and the coordinate {DELTA_COORDINATE}. Detail: {exc}"
        )
    yield session
    session.stop()
